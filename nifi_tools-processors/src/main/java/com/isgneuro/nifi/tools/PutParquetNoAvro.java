package com.isgneuro.nifi.tools;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.parquet.utils.ParquetConfig;
import org.apache.nifi.parquet.utils.ParquetUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processors.hadoop.AbstractPutHDFSRecord;
import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"put", "parquet", "hadoop", "HDFS", "filesystem", "record"})
@CapabilityDescription("Reads records from an incoming FlowFile using the provided Record Reader, and writes those records " +
        "to a Parquet file. The schema for the Parquet file must be provided in the processor properties. This processor will " +
        "first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the " +
        "dot file to it's final name. If the dot file cannot be renamed, the rename operation will be attempted up to 10 times, and " +
        "if still not successful, the dot file will be deleted and the flow file will be routed to failure. " +
        " If any error occurs while reading records from the input, or writing records to the output, " +
        "the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.")
@ReadsAttribute(attribute = "filename", description = "The name of the file to write comes from the value of this attribute.")
@WritesAttributes({
        @WritesAttribute(attribute = "filename", description = "The name of the file is stored in this attribute."),
        @WritesAttribute(attribute = "absolute.hdfs.path", description = "The absolute path to the file is stored in this attribute."),
        @WritesAttribute(attribute = "record.count", description = "The number of records written to the Parquet file")
})
@Restricted(restrictions = {
        @Restriction(
                requiredPermission = RequiredPermission.WRITE_FILESYSTEM,
                explanation = "Provides operator the ability to write any file that NiFi has access to in HDFS or the local filesystem.")
})
public class PutParquetNoAvro extends AbstractPutHDFSRecord {

    public static final PropertyDescriptor REMOVE_CRC_FILES = new PropertyDescriptor.Builder()
            .name("remove-crc-files")
            .displayName("Remove CRC Files")
            .description("Specifies whether the corresponding CRC file should be deleted upon successfully writing a Parquet file")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor ADD_SPARK_META = new PropertyDescriptor.Builder()
            .name("make-spark-mets")
            .displayName("Make spark meta")
            .description("Processor adds spark schema to meta in Parquet file if set to true")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor CREATE_SCHEMA_FILES = new PropertyDescriptor.Builder()
            .name("create-schema-files")
            .displayName("Create schema files")
            .description("Processor writes .schema file to bucket if set to true")
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REPLACE_SPEC_SYMBOLS = new PropertyDescriptor.Builder()
            .name("replace-special-symbols")
            .displayName("Replace special symbols")
            .description("Specifies symbols and sequences to be replaced")
            .addValidator((subject, value, context) ->
                    (new ValidationResult.Builder()).subject(subject).input(value).explanation("Invalid format." +
                                    " Format shoud be {\"symbol1\": \"replace1\", \"symbol2\": \"replace2\"}")
                            .valid(value.startsWith("{\"") && value.endsWith("\"}")).build())
            .defaultValue("{\" \": \"\"}")
            .build();



    @Override
    public List<AllowableValue> getCompressionTypes(final ProcessorInitializationContext context) {
        return ParquetUtils.COMPRESSION_TYPES;
    }

    @Override
    public String getDefaultCompressionType(final ProcessorInitializationContext context) {
        return CompressionCodecName.UNCOMPRESSED.name();
    }

    @Override
    public List<PropertyDescriptor> getAdditionalProperties() {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ParquetUtils.ROW_GROUP_SIZE);
        props.add(ParquetUtils.PAGE_SIZE);
        props.add(ParquetUtils.DICTIONARY_PAGE_SIZE);
        props.add(ParquetUtils.MAX_PADDING_SIZE);
        props.add(ParquetUtils.ENABLE_DICTIONARY_ENCODING);
        props.add(ParquetUtils.ENABLE_VALIDATION);
        props.add(ParquetUtils.WRITER_VERSION);
        props.add(REMOVE_CRC_FILES);
        props.add(ADD_SPARK_META);
        props.add(CREATE_SCHEMA_FILES);
        props.add(REPLACE_SPEC_SYMBOLS);
        return Collections.unmodifiableList(props);
    }

    @Override
    public HDFSRecordWriter createHDFSRecordWriter(final ProcessContext context, final FlowFile flowFile, final Configuration conf, final Path path, final RecordSchema schema)
            throws IOException {
        final boolean createSchemaFiles = context.getProperty(CREATE_SCHEMA_FILES).asBoolean();
        final boolean addSparkMeta = context.getProperty(ADD_SPARK_META).asBoolean();

        final Map<String,String> replaces = getReplacesMap(context,flowFile,schema);
        final MessageType parquetSchema = makeParquetSchema(schema, replaces);
        if(createSchemaFiles) {
            final FileSystem fs = getFileSystem();
            final Path schemaTmpFile = new Path(path.toString() + ".schema");
            try {
                FSDataOutputStream outputStream = fs.create(schemaTmpFile);
                outputStream.writeBytes(makeDDLSchema(parquetSchema));
                outputStream.close();
            } catch (Exception e) {
                this.getLogger().error("Unable to create temp schema file {} due to {}", "." + path.getName() + ".schema", e);
            }
        }
        ParquetWriter.Builder builder;

        if(addSparkMeta) {
            HashMap<String, String> meta = new HashMap<>();
            String sparkSchema = generateSparkMetadata(parquetSchema);
            meta.put("org.apache.spark.sql.parquet.row.metadata", sparkSchema);
            builder = ExampleParquetWriter.builder(path).withType(parquetSchema).withExtraMetaData(meta);
        } else {
            builder = ExampleParquetWriter.builder(path).withType(parquetSchema);
        }
        final ParquetConfig parquetConfig = ParquetUtils.createParquetConfig(context, flowFile.getAttributes());
        ParquetUtils.applyCommonConfig(builder, conf, parquetConfig);
        final ParquetWriter<Group> writer = builder.build();
        return new ParquetHDFSRecordWriter(writer, parquetSchema, replaces);
    }

    private String generateSparkMetadata(MessageType parquetSchema){
        return parquetSchema.getFields().stream().map(f -> {
                    String type;
                    LogicalTypeAnnotation logicalType = f.getLogicalTypeAnnotation();
                    logicalType = logicalType == null ? LogicalTypeAnnotation.enumType() : logicalType; //Just to make OriginalType not UTF8 or TIMESTAMP_MILLIS if it is null
                    switch (logicalType.toOriginalType()){
                        case UTF8: type="string";
                            break;
                        case TIMESTAMP_MILLIS: type="timestamp";
                            break;
                        default:
                            switch(f.asPrimitiveType().getPrimitiveTypeName()){
                                case INT32: type="integer";
                                    break;
                                case INT64: type="long";
                                    break;
                                case DOUBLE: type="double";
                                    break;
                                case FLOAT: type="float";
                                    break;
                                case BOOLEAN: type="boolean";
                                    break;
                                default: type="string";
                            }
                    }
                    return "{\"name\":\"" + f.getName() + "\"," +
                            "\"type\":\"" + type + "\"," +
                            "\"nullable\":" +f.getRepetition().name().equals("OPTIONAL")+ "," +
                            "\"metadata\":{}" +
                            "}";})
                .collect(Collectors.joining(",","{\"type\":\"struct\",\"fields\":[","]}"));
    }

    @Override
    protected FlowFile postProcess(final ProcessContext context, final ProcessSession session, final FlowFile flowFile, final Path destFile) {
        final String filename = destFile.getName();
        final String hdfsPath = destFile.getParent().toString();
        final boolean processSchemaFiles = context.getProperty(CREATE_SCHEMA_FILES).asBoolean();

        if(processSchemaFiles) {
            final Path schemaTmpFile = new Path(hdfsPath, "." + filename + ".schema");
            final Path schemaFile = new Path(hdfsPath, filename + ".schema");
            try {
                rename(getFileSystem(), schemaTmpFile, schemaFile);
            } catch (Exception e) {
                this.getLogger().error("Unable to rename schema file {} due to {}", filename + ".schema", e);
            }
        }

        final boolean removeCRCFiles = context.getProperty(REMOVE_CRC_FILES).asBoolean();
        if (removeCRCFiles) {

            final Path crcFile = new Path(hdfsPath, "." + filename + ".crc");
            deleteQuietly(getFileSystem(), crcFile);
            if(processSchemaFiles) {
                final Path crcSchemaFile = new Path(hdfsPath, "." + filename + ".schema.crc");
                deleteQuietly(getFileSystem(), crcSchemaFile);
            }
        }

        return flowFile;
    }
    private String makeDDLSchema(MessageType parquetSchema) {
        return parquetSchema.getFields().stream().map(f -> {
                    String type;
                    LogicalTypeAnnotation logicalType = LogicalTypeAnnotation.fromOriginalType(f.getOriginalType(), null);
                    logicalType = logicalType == null ? LogicalTypeAnnotation.enumType() : logicalType; //Just to make OriginalType not UTF8 or TIMESTAMP_MILLIS if it is null
                    switch (logicalType.toOriginalType()){
                        case UTF8: type="STRING";
                            break;
                        case TIMESTAMP_MILLIS: type="TIMESTAMP";
                            break;
                        default:
                            switch(f.asPrimitiveType().getPrimitiveTypeName()){
                                case INT32: type="INTEGER";
                                    break;
                                case INT64: type="BIGINT";
                                    break;
                                case DOUBLE: type="DOUBLE";
                                    break;
                                case FLOAT: type="FLOAT";
                                    break;
                                case BOOLEAN: type="BIT";
                                    break;
                                default: type="STRING";
                            }
                    }
                    return "`" + f.getName() + "` " + type;})
                .collect(Collectors.joining("\n"));
    }


    private MessageType makeParquetSchema(RecordSchema nifiSchema, Map<String,String> replaces){
        Types.MessageTypeBuilder msgBuilder = Types.buildMessage();
        for(RecordField name: nifiSchema.getFields()){
            String newName = replaces.getOrDefault(name.getFieldName(), name.getFieldName());
            switch (name.getDataType().getFieldType()) {
                case LONG:
                    if(name.getFieldName().equals("_time")) {
                        msgBuilder.required(INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)).named(newName);
                    }else{
                        msgBuilder.optional(INT64).named(newName);
                    }
                    break;
                case STRING:
                    //Если данное имя гарантированно однажды попадет в схему, не следует добавлять его еще раз
                    if(!replaces.containsValue(name.getFieldName())) {
                        if (name.getFieldName().equals("_raw")) {
                            msgBuilder.required(BINARY).as(LogicalTypeAnnotation.stringType()).named(newName);
                        } else {
                            msgBuilder.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named(newName);
                        }
                    }
                    break;
                case INT:
                    msgBuilder.optional(INT32).named(newName);
                    break;
                case DOUBLE:
                    msgBuilder.optional(DOUBLE).named(newName);
                    break;
                case FLOAT:
                    msgBuilder.optional(FLOAT).named(newName);
                    break;
                case BOOLEAN:
                    msgBuilder.optional(BOOLEAN).named(newName);
                    break;
                default: msgBuilder.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named(newName);
            }
        }
        return msgBuilder.named("nifirecord");
    }

    Map<String, String> getReplacesMap(final ProcessContext context, final FlowFile flowFile, final RecordSchema schema){
        String replaces = context.getProperty(REPLACE_SPEC_SYMBOLS).evaluateAttributeExpressions(flowFile).getValue();
        Map<String, String> replaceSymbols = Arrays.stream(replaces.substring(2, replaces.length()-2).split("\", \"")).map(e -> {
                    String[] pair = e.split("\": \"");
                    return new Tuple<>(pair[0], pair.length > 1 ? pair[1] : "");
                })
                .collect(Collectors.toMap(Tuple::getKey,Tuple::getValue));

        return schema.getFieldNames().stream()
                .map(name -> {
                    String newName = name;
                    for (Map.Entry<String,String> e : replaceSymbols.entrySet())
                        newName = newName.replace(e.getKey(), e.getValue());
                    if(! newName.equals(name)) {
                        return new Tuple<>(name, newName);
                    } return null;
                }).filter(Objects::nonNull)
                .collect(Collectors.toMap(Tuple::getKey,Tuple::getValue));
    }
}
