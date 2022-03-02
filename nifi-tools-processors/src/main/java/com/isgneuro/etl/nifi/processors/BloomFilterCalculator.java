package com.isgneuro.etl.nifi.processors;

import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StopWatch;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"Attribute Expression Language", "state", "data science", "session", "window"})
@CapabilityDescription("Aggregate value of BloomFilter processed in the current session time window.")//TODO
@Stateful(scopes = {Scope.CLUSTER}, description = "Stores locks for bloom files that are currently being written")

public class BloomFilterCalculator extends AbstractProcessor {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-stats-reader")
            .displayName("Record Reader")
            .description("A record reader to use for reading the records.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor BUCKET_ID_VALUE = new PropertyDescriptor.Builder()
            .displayName("BucketID")
            .name("BucketID")
            .description("Value used to group flow files. Bloom filter calculates independently for each value of bucketID")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor TIME_GAP = new PropertyDescriptor.Builder()
            .displayName("Time gap")
            .name("Time gap")
            .description("If records for bucketId doest arrive for this time, bloom is calculated")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor BLOOM_FILE_NAME = new PropertyDescriptor.Builder()
            .displayName("Bloom file name")
            .name("Bloom file name")
            .defaultValue("bloom")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("Name of bloom file")
            .required(false)
            .build();

    static final PropertyDescriptor EXPECTED_NUM_TOKENS = new PropertyDescriptor.Builder()
            .displayName("Expected number of tokens")
            .name("Expected number of tokens")
            .defaultValue("100000")
            .description("Number of expected unique tokens in bucket. " +
                    "If you want to change this parameter, make sure that there is not bloom file in bucket." +
                    " Otherwise you should delete existing file, but previous results will be lost.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .required(false)
            .build();

    static final PropertyDescriptor FALSE_POSITIVE_PROBABILITY = new PropertyDescriptor.Builder()
            .displayName("False positive probabilty")
            .name("False positive probabilty")
            .defaultValue("0.05")
            .description("False positive probabilty of bloom prediction." +
                    " If you want to change this parameter, make sure that there is not bloom file in bucket." +
                    " Otherwise you should delete existing file, but previous results will be lost.")
            .addValidator((subject, value, context) -> {
                String reason = null;
                try {
                    double doubleVal = Double.parseDouble(value);
                    if (doubleVal <= 0.0) {
                        reason = "not a positive value";
                    }else if(doubleVal >= 1.0){
                        reason = "value can't be more than 1";
                    }
                } catch (NumberFormatException var7) {
                    reason = "not a valid double";
                }

                return (new ValidationResult.Builder()).subject(subject).input(value).explanation(reason).valid(reason == null).build();
            })
            .required(false)
            .build();
    static final PropertyDescriptor JSON_TOKENIZER = new PropertyDescriptor.Builder()
            .name("json-tokenizer")
            .displayName("Use json tokenizer")
            .description("If set to 'true', json parsing used to get tokens")
            .required(true)
            .defaultValue("false")
            .allowableValues(new String[]{"true","false"})
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .description("All FlowFiles that was putted to BloomFilter are routed to this relationship")
            .name("success")
            .build();
    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When a flowFile fails it is routed here.")
            .build();

    private BloomFiltersInfo bloomFilters;
    private String bloomFilename;
    private Long expectedNumTokens;
    private Double fpp;
    private Boolean useJsonTokenizer;


    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        context.getStateManager().clear(Scope.CLUSTER);
        this.bloomFilters = new BloomFiltersInfo(context.getProperty(TIME_GAP).asTimePeriod(TimeUnit.MILLISECONDS));
        this.bloomFilename = context.getProperty(BLOOM_FILE_NAME).getValue();
        this.expectedNumTokens = context.getProperty(EXPECTED_NUM_TOKENS).asLong();
        this.fpp = context.getProperty(FALSE_POSITIVE_PROBABILITY).asDouble();
        this.useJsonTokenizer = context.getProperty(JSON_TOKENIZER).asBoolean();
    }

    @OnStopped
    @OnShutdown
    public void flushBloomToFile(final ProcessContext context) throws IOException {
        bloomFilters.getAll().entrySet().stream().forEach( rec -> {
            try {
                writeBloom(rec.getKey(), rec.getValue());
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
            }
        });
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }


        try {
            String id = context.getProperty(BUCKET_ID_VALUE).evaluateAttributeExpressions(flowFile).getValue();

            BloomFilter bloomFilter = calcBloom(flowFile,context,session);
            if(bloomFilters.hasElapsed()) {
                makeWithElection(
                        () -> bloomFilters.getElapsed().entrySet().stream().forEach(rec -> {
                            try {
                                writeBloom(rec.getKey(), rec.getValue());
                            } catch (Exception e) {
                                getLogger().error(e.getMessage(), e);
                                //session.transfer(flowFile, FAILURE);
                            }
                        }), context.getStateManager());
            }

            updateBloom(id, bloomFilter);
            session.transfer(flowFile, SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private void makeWithElection(Runnable procedure, StateManager stateManager) throws IOException{
        Map<String, String> stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        stateMap = stateMap.entrySet().stream()
                .filter(e -> new Date().getTime() - Long.parseLong(e.getValue()) > 15 * 1000)//TODO
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);
        String uuid = UUID.randomUUID().toString();
        stateMap.put(uuid, String.valueOf(new Date().getTime()));//TODO
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);

        stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        Optional<Map.Entry<String,String>> winner = stateMap.entrySet().stream()
                .sorted((a, b) -> {
                    if (a.getValue().equals(b.getValue()))
                        return (int) (Long.parseLong(a.getValue()) - Long.parseLong(b.getValue()));
                    else
                        return a.getKey().compareTo(b.getKey());
                })
                .findFirst();
        if (winner.isPresent() && uuid.equals(winner.get().getKey())) {
            procedure.run();
        }
        stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        stateMap.remove(uuid);
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);
    }

    private void updateBloom(String id, BloomFilter curBloom){
        BloomWithStopWatch bloomInfo = bloomFilters.get(id);
        if(bloomInfo != null){
            try {
                bloomInfo.getBloomFilter().mergeInPlace(curBloom);
            } catch (IncompatibleMergeException e) {
                e.printStackTrace();
            }
            bloomInfo.getStopWatch().stop();
        }else{
            bloomInfo = new BloomWithStopWatch(curBloom, new StopWatch(false));
        }
        bloomInfo.getStopWatch().start();
        bloomFilters.put(id, bloomInfo);
    }

    private void writeBloom(String pathToDir, BloomFilter bloomFilter) throws Exception{
        if(Files.isDirectory(Paths.get(pathToDir))) {
            // Files.createDirectories(Paths.get(pathToDir));
            if (Files.isRegularFile(Paths.get(pathToDir, bloomFilename))) {
                try (FileInputStream fis = new FileInputStream(new File(pathToDir, bloomFilename))) {
                    bloomFilter.mergeInPlace(BloomFilter.readFrom(fis));
                }catch(EOFException e){
                    if(! Files.isDirectory(Paths.get(pathToDir))) return;
                    //TODO check if merging is failed
                }
            }

            try (OutputStream os = new FileOutputStream(new File(pathToDir, bloomFilename))) {
                int numTries = 0;
                while (numTries < 3) {
                    try {
                        bloomFilter.writeTo(os);
                        break;
                    } catch (Exception e) {
                        if(! Files.isDirectory(Paths.get(pathToDir))) return;
                        numTries++;
                        if (numTries == 3) {
                            getLogger().error("Error when writing bloom to disk: {}. Try one more time...",
                                    new Object[]{e.getMessage()});
                            Thread.sleep(1000);
                        } else {
                            getLogger().error("Error when writing bloom to disk: {}. Bloom data will be erased for bucket {}",
                                    new Object[]{e.getMessage(), pathToDir});
                            throw e;
                        }
                    }
                }
            }catch (Exception e){
                //TODO
            }
        }
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(RECORD_READER, BUCKET_ID_VALUE, TIME_GAP, BLOOM_FILE_NAME, EXPECTED_NUM_TOKENS, FALSE_POSITIVE_PROBABILITY,JSON_TOKENIZER);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return ImmutableSet.of(SUCCESS, FAILURE);
    }


    private static class BloomFiltersInfo {
        private ConcurrentHashMap<String, BloomWithStopWatch> bloomFilters;
        private final long timeFrameMilliseconds;

        BloomFiltersInfo(long timeframeMilliseconds) {
            bloomFilters = new ConcurrentHashMap<>();
            this.timeFrameMilliseconds = timeframeMilliseconds;
        }

        Map<String, BloomFilter> getAll(){
            return bloomFilters.entrySet().stream().collect(Collectors.toMap(r -> r.getKey(), r-> r.getValue().getBloomFilter()));
        }

        Map<String, BloomFilter> getElapsed(){
            List<String> elapsed =  bloomFilters.entrySet().stream()
                    .filter(w -> w.getValue().getStopWatch().getElapsed(TimeUnit.MILLISECONDS) >= timeFrameMilliseconds)
                    .map(w -> w.getKey())
                    .collect(Collectors.toList());
            Map<String, BloomFilter> res = elapsed.stream()
                    .map(e ->{
                        BloomWithStopWatch bloomInfo = bloomFilters.remove(e);
                        return new AbstractMap.SimpleImmutableEntry<>(e, bloomInfo != null ?  bloomInfo.getBloomFilter() : null);
                    })
                    .filter(e -> e.getValue() != null)
                    .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
            return res;
        }

        Boolean hasElapsed(){
            return bloomFilters.entrySet().stream()
                    .filter(w -> w.getValue().getStopWatch().getElapsed(TimeUnit.MILLISECONDS) >= timeFrameMilliseconds)
                    .count() > 0;
        }

        BloomWithStopWatch get(String id){
            return bloomFilters.get(id);
        }

        BloomWithStopWatch put(String id, BloomWithStopWatch bloomWithStopWatch){
            return bloomFilters.put(id, bloomWithStopWatch);
        }
    }
    static class BloomWithStopWatch{
        BloomFilter bloomFilter;
        StopWatch stopWatch;

        public BloomWithStopWatch(BloomFilter bloomFilter, StopWatch stopWatch) {
            this.bloomFilter = bloomFilter;
            this.stopWatch = stopWatch;
        }

        public BloomFilter getBloomFilter() {
            return bloomFilter;
        }

        public void setBloomFilter(BloomFilter bloomFilter) {
            this.bloomFilter = bloomFilter;
        }

        public StopWatch getStopWatch() {
            return stopWatch;
        }

        public void setStopWatch(StopWatch stopWatch) {
            this.stopWatch = stopWatch;
        }
    }

    protected BloomFilter calcBloom(FlowFile flowFile, ProcessContext context, ProcessSession session) {
        try (InputStream is = session.read(flowFile)) {
            RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            RecordReader reader = factory.createRecordReader(flowFile, is, getLogger());

            BloomFilter retVal = BloomFilter.create(expectedNumTokens, fpp);
            Record record;
            while ((record = reader.nextRecord()) != null) {
                String curRaw = record.getAsString("_raw");
                Set<String> tokens = StringSegmenter.parse(curRaw);
                if(useJsonTokenizer)
                    tokens.addAll(StringSegmenter.parseJson(curRaw));
                tokens.stream().forEach(retVal::put);
            }
            return retVal;
        } catch (Exception e) {
            getLogger().error("Could not read flowfile", e);
            throw new ProcessException(e);
        }
    }

}
