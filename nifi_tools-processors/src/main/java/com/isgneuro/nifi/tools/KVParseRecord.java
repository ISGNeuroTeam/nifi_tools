package com.isgneuro.nifi.tools;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.processor.util.StandardValidators;
import javax.xml.bind.DatatypeConverter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "csv", "avro", "log", "logs", "freeform", "text"})
@CapabilityDescription("Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). "
    + "This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should "
    + "be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from "
    + "the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.")

public class KVParseRecord extends AbstractRecordProcessorWithSchemaUpdates {


	private Pattern Pair_pat = null;
	private Pattern KV_pat = null;
	private final Pattern quot = Pattern.compile("\"(?<value>.+)\"$");
	private final Pattern GetLastWorld = Pattern.compile("^.*?\\W*(\\w\\S+)$");

	private volatile RecordPathCache recordPathCache;
	private volatile List<String> recordPaths;

    static final AllowableValue PARENT_FIELD_PREFIX_SET = new AllowableValue("true","true",
            "Set prefix of child fields as parent field name");

    static final AllowableValue PARENT_FIELD_PREFIX_NONE = new AllowableValue("false","false",
            "Do not set prefix to child fields (default)");

    static final PropertyDescriptor PARENT_FIELD_PREFIX = new PropertyDescriptor.Builder()
            .name("parent_field_prefix")
            .displayName("New Fields Prefix")
            .description("Specifies if prefix for child fields should be set")
            .allowableValues(PARENT_FIELD_PREFIX_SET, PARENT_FIELD_PREFIX_NONE)
            .defaultValue(PARENT_FIELD_PREFIX_NONE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor KVCON = new PropertyDescriptor.Builder()
            .name("kv_con")
            .displayName("Key Value connector")
            .description("Specifies regular expression to match connector between key and value")
            .defaultValue("(?:::)|=")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor KVSEP = new PropertyDescriptor.Builder()
            .name("kv_sep")
            .displayName("Key-Value pairs separator")
            .description("Specifies regular expression to separate key-value pairs")
            .defaultValue("\\s+")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor MAXVALLEN = new PropertyDescriptor.Builder()
            .name("maxvallen")
            .displayName("Maximum value length")
            .description("Specifies maximum length of value string")
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor MAXINPUTLEN = new PropertyDescriptor.Builder()
            .name("maxinputlen")
            .displayName("Maximum input string length")
            .description("Specifies maximum length of input string. -1 for unlimited.")
            .defaultValue("-1")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final AllowableValue ENCODE_KEYS_SET = new AllowableValue("true","true",
            "URLEncode field names");

    static final AllowableValue ENCODE_KEYS_NONE = new AllowableValue("false","false",
            "Do not encode field names");

    static final PropertyDescriptor ENCODE_KEYS = new PropertyDescriptor.Builder()
            .name("encode_keys")
            .displayName("Encode field names")
            .description("Specifies if to do encode field names")
            .allowableValues(ENCODE_KEYS_SET, ENCODE_KEYS_NONE)
            .defaultValue(ENCODE_KEYS_NONE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PARENT_FIELD_PREFIX);
        properties.add(KVCON);
        properties.add(KVSEP);
        properties.add(MAXVALLEN);
        properties.add(MAXINPUTLEN);
        properties.add(ENCODE_KEYS);
        return properties;
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
		final String kvcon = "(?:" + context.getProperty(KVCON).getValue() + ")";
		final String separator = context.getProperty(KVSEP).getValue();
		final String maxfieldlength = context.getProperty(MAXVALLEN).getValue();

		final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
		for (final PropertyDescriptor property : context.getProperties().keySet()) {
			if (property.isDynamic()) {
				recordPaths.add(property.getName());
			}
		}

		this.recordPaths = recordPaths;

		Pair_pat = Pattern.compile("(?ms)(?<=" + kvcon + "\\s{0,100}([^\"\\s]{0," + maxfieldlength + "}?|(\".{0,"
				+ maxfieldlength + "}\")))" + separator);
		KV_pat = Pattern.compile("(?ms)\\s*" + kvcon + "\\s*");
	}

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context) {
        final boolean set_child_fields_prefix=context.getProperty(PARENT_FIELD_PREFIX).getValue().equals(PARENT_FIELD_PREFIX_SET.getValue());
        //final String kvcon="(?:"+context.getProperty(KVCON).getValue()+")";
        //final String separator=context.getProperty(KVSEP).getValue();
        //final String maxfieldlength=context.getProperty(MAXVALLEN).getValue();
		final Integer maxinputlength = Integer.parseInt(context.getProperty(MAXINPUTLEN).getValue());
		final boolean do_encode_keys = context.getProperty(ENCODE_KEYS).getValue().equals(ENCODE_KEYS_SET.getValue());

        final List<RecordField> newfields = new ArrayList<>();

        //        final Pattern kvpat = Pattern.compile("(?ms)(?:^|" + separator + ")" +
//                "(?:(?:\"(?<key>[^\"]*?(?:(?:\\\\\")*?[^\"]*?)*?[^\\\\])\")|(?<key1>[^\"\\s]*?))\\s*"
//                + kvcon +
//                "\\s*(?:(?:\"(?<value>.*)\")|(?<value1>[^\"\\s]*))\\s*$");
//                "\\s*(?:(?:\"(?<value>[^\"]*?(?:(?:\\\\\")*?[^\"]*?)*?[^\\\\])\")|(?<value1>[^\"\\s]*?))" +
//               "(?=" + separator + "|$)");

        for (String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);
            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());

            for (FieldValue fieldVal:selectedFields) {
                this.getLogger().debug("Field value:{}, name:{}", new Object[]{fieldVal.getValue(), fieldVal.getField().getFieldName()});
                final String field_prefix=set_child_fields_prefix?fieldVal.getField().getFieldName()+".":"";

                String srcval=fieldVal.getValue().toString();
                if(maxinputlength>-1 && srcval.length() > maxinputlength)srcval=srcval.substring(0,maxinputlength);
                this.getLogger().debug("SrcVal:" + srcval);
                //final String[] pairs=srcval.split("(?ms)(?<="+kvcon+"\\s{0,100}([^\"\\s]{0,"+maxfieldlength+
                //       "}?|(\".{0,"+maxfieldlength+"}\")))"+separator);
                final String[] pairs=Pair_pat.split(srcval);
                for (String pp: pairs) {
                    this.getLogger().debug("Pair:" + pp);
                    //final String[] kv=pp.split("(?ms)\\s*"+kvcon+"\\s*",2);
                    final String[] kv=KV_pat.split(pp,2);
                    if (kv.length>1){
                        String key=kv[0];
                        //final Pattern quot=Pattern.compile("\"(?<value>.+)\"$");
                        Matcher qm=quot.matcher(key);
                        if (qm.find()){
                            key=qm.group("value");
                        }
                        else {
                            //key=key.replaceFirst("^.*?\\W*(\\w\\S+)$","$1");
                            key=GetLastWorld.matcher(key).replaceFirst("$1");
                        }

                        String value=kv[1];
                        qm=quot.matcher(value);
                        if (qm.find()){
                            value=qm.group("value");
                        }
                       // try {
                            //if (do_encode_keys) key = java.net.URLEncoder.encode(key, "UTF-8");
                            if (do_encode_keys)key= DatatypeConverter.printHexBinary((field_prefix + key).getBytes());
                       // }
                        /*catch (java.io.UnsupportedEncodingException e){
                            this.getLogger().error("Unsupported Encoding:{} in:{} Parsing error:",new Object[]{key,srcval},e);
                        }*/

                        this.getLogger().debug("Key:{} Value:{}",new Object[]{key,value});
                    //while (kvmat.find()) {
                       // final String key = kvmat.group("key") != null ? kvmat.group("key") : kvmat.group("key1");
                        //final String value = kvmat.group("value") != null ? kvmat.group("value") : kvmat.group("value1");
                        //this.getLogger().info("Key:{} Value:{} Key1:{} Value1:{}", new Object[]{kvmat.group("key"), kvmat.group("value"), kvmat.group("key1"), kvmat.group("value1")});

                        final RecordField keyf = new RecordField(Utils.FIELD_PREFIX + key, RecordFieldType.STRING.getDataType(), true);
                        record.setValue(keyf, value);
                        newfields.add(keyf);
                    }
                }
            }
        }

        //RecordSchema oldsch=record.getSchema();
        //this.getLogger().info("schema:{}",new Object[]{oldsch});
        //record.incorporateInactiveFields(); //performance issue
        record.incorporateSchema(new SimpleRecordSchema(newfields));

        return record;
    }

}
