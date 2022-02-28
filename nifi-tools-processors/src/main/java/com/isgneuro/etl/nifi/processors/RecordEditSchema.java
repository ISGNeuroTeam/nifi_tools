package com.isgneuro.etl.nifi.processors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@CapabilityDescription("Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). "
        + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
        + "be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from "
        + "the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.")

public class RecordEditSchema extends AbstractProcessor {

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
        .name("record-writer")
        .displayName("Record Writer")
        .description("Specifies the Controller Service to use for writing out the records")
        .identifiesControllerService(RecordSetWriterFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are successfully transformed will be routed to this relationship")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("If a FlowFile cannot be transformed from the configured input format to the configured output format, "
            + "the unchanged FlowFile will be routed to this relationship")
        .build();

    private volatile Map<String, DataType> Fields;
    private volatile RecordPathCache recordPathCache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(RECORD_WRITER);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies the value to use to replace fields in the record that match the RecordPath: " + propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final boolean containsDynamic = validationContext.getProperties().keySet().stream().anyMatch(PropertyDescriptor::isDynamic);

        if (containsDynamic) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
                .subject("User-defined Properties")
                .valid(false)
                .explanation("At least one RecordPath must be specified")
                .build());
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

		final Map<String, DataType> recordPaths = new HashMap<>(context.getProperties().size() - 2);
		for (final Map.Entry<PropertyDescriptor, String> property : context.getProperties().entrySet()) {
            if (property.getKey().isDynamic()) {
                String val = property.getValue();
				DataType field_type;
				if (val.equals("drop") || val.equals("")) {
					field_type = null;
				} else {
					field_type = RecordFieldType.valueOf(val.toUpperCase()).getDataType();
				}
				recordPaths.put(recordPathCache.getCompiled(property.getKey().getName()).getPath().replaceFirst("/", ""), field_type);
                //recordPaths.put(property.getKey().getName(), dval);
            }
        }
		this.Fields = recordPaths;
		getLogger().debug("Pathes: {}", new Object[] { recordPaths });
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();
        
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet()? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean():true;
        
        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
				public void process(final InputStream in, final OutputStream out) throws IOException {

					try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, original.getSize(), getLogger())) {
						final RecordSchema writeSchema = writerFactory.getSchema(originalAttributes, processSchema(reader.getSchema()));
						// Get the first record and process it before we create the Record Writer. We do this so that if the Processor
						// updates the Record's schema, we can provide an updated schema to the Record Writer. If there are no records,
						// then we can simply create the Writer with the Reader's schema and begin & end the Record Set.
						Record firstRecord = reader.nextRecord();
						if (firstRecord == null) {
							try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
								writer.beginRecordSet();
								final WriteResult writeResult = writer.finishRecordSet();
								attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
								attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
								attributes.putAll(writeResult.getAttributes());
								recordCount.set(writeResult.getRecordCount());
							}
							return;
						}

						try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out, originalAttributes)) {
							writer.beginRecordSet();
							writer.write(firstRecord);
							Record record;
							while ((record = reader.nextRecord()) != null) {
								writer.write(record);
							}
							final WriteResult writeResult = writer.finishRecordSet();
							attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
							attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
							attributes.putAll(writeResult.getAttributes());
							recordCount.set(writeResult.getRecordCount());
						}
					} catch (final SchemaNotFoundException e) {
						throw new ProcessException(e.getLocalizedMessage(), e);
					} catch (final MalformedRecordException e) {
						throw new ProcessException("Could not parse incoming data", e);
					}
				}
            });
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAllAttributes(flowFile, attributes);
        if(!includeZeroRecordFlowFiles && recordCount.get() == 0){
            session.remove(flowFile);
        } else {
            session.transfer(flowFile, REL_SUCCESS);
        }

        final int count = recordCount.get();
        session.adjustCounter("Records Processed", count, false);
        getLogger().info("Successfully converted {} records for {}", new Object[] {count, flowFile});
    }

    public RecordSchema processSchema(RecordSchema inschema){
		final List<RecordField> outfields = new ArrayList<>();
		Map<String, DataType> runfields = new HashMap<>(Fields);

		getLogger().debug("Fields: {}", new Object[] { Fields });
		for (RecordField inf : inschema.getFields()) {
			String fn = inf.getFieldName();
			getLogger().debug("Field: {}", new Object[] { fn });

			if (runfields.containsKey(fn)) {
				DataType ft = runfields.get(fn);
				runfields.remove(fn);
				if (ft == null)
					continue;
				getLogger().debug("Add field:{}={}", new Object[] { fn, ft });
				outfields.add(new RecordField(fn, ft, true));
			} else {
				outfields.add(inf);
			}
		}

		for (Map.Entry<String, DataType> f : runfields.entrySet()) {
			if (f != null) {
				getLogger().debug("Add field:{}", new Object[] { f });
				outfields.add(new RecordField(f.getKey(), f.getValue(), true));
			}
		}

		RecordSchema resschema = new SimpleRecordSchema(outfields);
		getLogger().debug("Old Schema:{}\nNew Schema:{}", new Object[] { inschema, resschema });

		return resschema;
    }
}
