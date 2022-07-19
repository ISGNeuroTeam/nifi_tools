package com.isgneuro.nifi.tools.bloom;

import com.google.common.collect.ImmutableMap;
import com.isgneuro.nifi.tools.StringSegmenter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
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
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.StopWatch;
import org.apache.spark.util.sketch.BloomFilter;
import org.apache.spark.util.sketch.IncompatibleMergeException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@TriggerWhenEmpty
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"bloom", "filter", "tokens", "raw"})
@CapabilityDescription("Calculates the bloom filter for the bucket given the _raw field ")
@Stateful(scopes = {Scope.CLUSTER}, description = "Stores locks for bloom files that are currently being written")
public class BloomFilterCalculator extends AbstractProcessor {
    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("A record reader to use for reading the records.")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();
    static final PropertyDescriptor BUCKET_ID_VALUE = new PropertyDescriptor.Builder()
            .name("BucketID")
            .description("Value used to group flow files. Bloom filter calculates independently for each value of bucketID")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor EXPECTED_NUM_TOKENS = new PropertyDescriptor.Builder()
            .name("Expected number of tokens")
            .defaultValue("100000")
            .description("Number of expected unique tokens in bucket. " +
                    "If you want to change this parameter, make sure that there is not bloom file in bucket." +
                    " Otherwise you should delete existing file, but previous results will be lost.")
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor FALSE_POSITIVE_PROBABILITY = new PropertyDescriptor.Builder()
            .name("False positive probability")
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

    static final PropertyDescriptor TIME_GAP = new PropertyDescriptor.Builder()
            .name("Time gap")
            .defaultValue("10 sec")
            .description("If records for bucketId doest arrive for this time, bloom is calculated")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor BLOOM_FILE_NAME = new PropertyDescriptor.Builder()
            .name("Bloom file name")
            .defaultValue("bloom")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .description("Name of bloom file")
            .required(false)
            .build();
    static final PropertyDescriptor FILTER_NUMERIC_TOKENS = new PropertyDescriptor.Builder()
            .name("Filter numeric tokens")
            .description("If set to 'true', bloom tokens that are numbers will be removed")
            .required(false)
            .defaultValue("false")
            .allowableValues(new String[]{"true","false"})
            .build();

    static final PropertyDescriptor MIN_TOKEN_LENGTH = new PropertyDescriptor.Builder()
            .name("Minimal token length")
            .description("shorter tokens will not be added to the bloom filter")
            .defaultValue("3")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(false)
            .build();
    static final PropertyDescriptor SAVE_TOKENS = new PropertyDescriptor.Builder()
            .name("Save tokens")
            .description("If set to 'true', bloom tokens will be written to separate txt file (bloom file name with txt extension)")
            .required(false)
            .defaultValue("false")
            .allowableValues(new String[]{"true","false"})
            .build();
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .description("All FlowFiles that was putted to BloomFilter are routed to this relationship")
            .name("success")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("When a flowFile fails it is routed here.")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private BloomFiltersInfo bloomFilters;
    private String bloomFilename;
    private Long expectedNumTokens;
    private Long timeGap;
    private Double fpp;
    private Boolean saveTokens;
    private Boolean filterNumericTokens;
    private String tokensFileName;
    private Integer minTokenLength;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(BUCKET_ID_VALUE);
        properties.add(EXPECTED_NUM_TOKENS);
        properties.add(FALSE_POSITIVE_PROBABILITY);
        properties.add(TIME_GAP);
        properties.add(BLOOM_FILE_NAME);
        properties.add(FILTER_NUMERIC_TOKENS);
        properties.add(MIN_TOKEN_LENGTH);
        properties.add(SAVE_TOKENS);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        context.getStateManager().clear(Scope.CLUSTER);
        this.timeGap = context.getProperty(TIME_GAP).asTimePeriod(TimeUnit.MILLISECONDS);
        this.bloomFilters = new BloomFiltersInfo(this.timeGap);
        this.bloomFilename = context.getProperty(BLOOM_FILE_NAME).getValue();
        this.expectedNumTokens = context.getProperty(EXPECTED_NUM_TOKENS).asLong();
        this.fpp = context.getProperty(FALSE_POSITIVE_PROBABILITY).asDouble();
        this.minTokenLength = context.getProperty(MIN_TOKEN_LENGTH).asInteger();
        this.saveTokens = context.getProperty(SAVE_TOKENS).asBoolean();
        this.filterNumericTokens = context.getProperty(FILTER_NUMERIC_TOKENS).asBoolean();
        this.tokensFileName = String.format("%s.txt", this.bloomFilename);
    }

    @OnStopped
    @OnShutdown
    public void flushBloomToFile(final ProcessContext context) {
        bloomFilters.getAll().forEach((key, value) -> {
            try {
                writeBloom(key, value);
            } catch (Exception e) {
                getLogger().error(e.getMessage(), e);
            }
        });
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        try {
            if(bloomFilters.hasElapsed()) {
                makeWithElection(
                        () -> bloomFilters.getElapsed().forEach((key, value) -> {
                            try {
                                writeBloom(key, value);
                            } catch (Exception e) {
                                getLogger().error(e.getMessage(), e);
                            }
                        }), context.getStateManager());
            }
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            String id = context.getProperty(BUCKET_ID_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            getLogger().info("Processing flowfile with bucket-id {}", id);
            BloomFilter bloomFilter = calcBloom(flowFile, context,session);
            updateBloom(id, bloomFilter);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            getLogger().error(e.getMessage(), e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private void makeWithElection(Runnable procedure, StateManager stateManager) throws IOException{
        Map<String, String> stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        stateMap = stateMap.entrySet().stream()
                .filter(e -> System.currentTimeMillis() - Long.parseLong(e.getValue()) > 2 * this.timeGap)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);
        String uuid = UUID.randomUUID().toString();
        stateMap.put(uuid, String.valueOf(System.currentTimeMillis()));
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);

        stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        Optional<Map.Entry<String,String>> winner = stateMap.entrySet().stream().min((a, b) -> {
            if (a.getValue().equals(b.getValue()))
                return (int) (Long.parseLong(a.getValue()) - Long.parseLong(b.getValue()));
            else
                return a.getKey().compareTo(b.getKey());
        });
        if (winner.isPresent() && uuid.equals(winner.get().getKey())) {
            procedure.run();
        }
        stateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());
        stateMap.remove(uuid);
        stateManager.setState(ImmutableMap.copyOf(stateMap), Scope.CLUSTER);
    }

    private void updateBloom(String id, BloomFilter curBloom){
        BloomWithStopWatch bloomInfo = bloomFilters.get(id);
        if (bloomInfo != null) {
            try {
                bloomInfo.getBloomFilter().mergeInPlace(curBloom);
            } catch (IncompatibleMergeException e) {
                e.printStackTrace();
            }
            bloomInfo.getStopWatch().stop();
        } else {
            bloomInfo = new BloomWithStopWatch(curBloom, new StopWatch(false));
        }
        bloomInfo.getStopWatch().start();
        bloomFilters.put(id, bloomInfo);
    }

    protected BloomFilter calcBloom(FlowFile flowFile, ProcessContext context, ProcessSession session) {
        try (InputStream is = session.read(flowFile)) {
            String pathToDir = context.getProperty(BUCKET_ID_VALUE).evaluateAttributeExpressions(flowFile).getValue();
            RecordReaderFactory factory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            RecordReader reader = factory.createRecordReader(flowFile, is, getLogger());

            BloomFilter retVal = BloomFilter.create(expectedNumTokens, fpp);
            Record record;
            StringSegmenter parser = new StringSegmenter(filterNumericTokens, minTokenLength);
            Set<String> tokens = new HashSet<>();
            while ((record = reader.nextRecord()) != null) {
                String curRaw = record.getAsString("_raw");
                tokens.addAll(parser.parseString(curRaw));
            }
            if (!tokens.isEmpty()){
                tokens.forEach(retVal::put);
                if (saveTokens) {
                    if (Files.isDirectory(Paths.get(pathToDir))) {
                        if (Files.isRegularFile(Paths.get(pathToDir, tokensFileName))) {
                            try (BufferedReader bufferedReader = Files.newBufferedReader(new File(pathToDir, tokensFileName).toPath(), StandardCharsets.UTF_8)) {
                                bufferedReader.lines().forEach(tokens::add);
                            }
                        }
                        // BufferWriter default options CREATE, TRUNCATE_EXISTING, and WRITE
                        try (BufferedWriter bufferedWriter = Files.newBufferedWriter(new File(pathToDir, tokensFileName).toPath(), StandardCharsets.UTF_8)) {
                            tokens.forEach(line -> {
                                try {
                                    bufferedWriter.write(line);
                                    bufferedWriter.newLine();
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                            bufferedWriter.flush();
                        }
                    }
                }
            }
            return retVal;
        } catch (Exception e) {
            getLogger().error("Could not read flowfile", e);
            throw new ProcessException(e);
        }
    }

    private void writeBloom(String pathToDir, BloomFilter bloomFilter) throws Exception {
        if (Files.isDirectory(Paths.get(pathToDir))) {
            if (Files.isRegularFile(Paths.get(pathToDir, bloomFilename))) {
                try (FileInputStream fis = new FileInputStream(new File(pathToDir, bloomFilename))) {
                    bloomFilter.mergeInPlace(BloomFilter.readFrom(fis));
                } catch (EOFException e) {
                    getLogger().error("Error while merging bloom filter: {}", e.getMessage());
                }
            }

            try (OutputStream os = Files.newOutputStream(new File(pathToDir, bloomFilename).toPath())) {
                int MAX_RETRIES = 5;
                for (int i = 0; i <= MAX_RETRIES; i++) {
                    try {
                        bloomFilter.writeTo(os);
                        break;
                    }
                    catch (Exception e) {
                        getLogger().error("Error when writing bloom to disk: {}. Try one more time...", e.getMessage());
                        Thread.sleep(1000);
                        if (i == MAX_RETRIES) {
                            getLogger().error(
                                    "Error when writing bloom to disk: {}. Bloom data will be erased for bucket {}", e.getMessage(), pathToDir);
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                getLogger().error("Error when writing bloom to disk: {}", e.getMessage());
            }
        } else {
            getLogger().error("Invalid bucket_id. Path {} is not directory", pathToDir);
        }
    }
}