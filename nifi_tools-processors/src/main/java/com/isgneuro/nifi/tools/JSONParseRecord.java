package com.isgneuro.nifi.tools;

import com.github.wnameless.json.flattener.JsonFlattener;

//import org.apache.commons.lang3.StringEscapeUtils;
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
import java.util.Map;
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
//@SeeAlso({ConvertRecord.class})
public class JSONParseRecord extends AbstractRecordProcessorWithSchemaUpdates {

	private final Pattern kvpat = Pattern.compile("(?ms)^[^\\{]*(?<json>\\{.+\\})[^\\}]*$");
	private Pattern mjson_pat = null;
	private Pattern jsonsep_pat = null;
	private final Pattern new_line = Pattern.compile("\n");
	private final Pattern tab = Pattern.compile("\t");
	private final Pattern SplunkPar = Pattern.compile("\\[\\\\\"([^\\[\\]]+?)\\\\\"\\]");
	private final Pattern SplunkParL = Pattern.compile("\\[\\\\\"");
	private final Pattern SplunkParR = Pattern.compile("\\\\\"\\]");
	private int maxdepth = -1;

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

    static final AllowableValue MULTIJSON_SET = new AllowableValue("true","true",
            "Collect multiple JSONs into array");

    static final AllowableValue MULTIJSON_NONE = new AllowableValue("false","false",
            "Do not Collect multiple JSONs into array");

    static final PropertyDescriptor MULTIJSON = new PropertyDescriptor.Builder()
            .name("multijson")
            .displayName("Collect multiple JSONs into array")
            .description("Collect multiple JSONs into array")
            .allowableValues(MULTIJSON_SET, MULTIJSON_NONE)
            .defaultValue(MULTIJSON_NONE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor JSONSEP = new PropertyDescriptor.Builder()
            .name("json_sep")
            .displayName("MultiJSON separator")
            .description("Specifies regular expression to separate multiple JSONs")
            .defaultValue("(?<=\\})\\s*\\n+\\s*(?=\\{)")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final AllowableValue SPLUNK_SET = new AllowableValue("true","true",
            "Fields in Splunk style");
    static final AllowableValue SPLUNK_UNSET = new AllowableValue("false","false",
            "Field names as is");

    static final PropertyDescriptor SPLUNK_STYLE = new PropertyDescriptor.Builder()
            .name("splunk-style")
            .displayName("Splunk style field names")
            .description("Splunk style field names")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(SPLUNK_SET,SPLUNK_UNSET)
            .defaultValue(SPLUNK_UNSET.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor MAXDEPTH = new PropertyDescriptor.Builder()
            .name("maxdepth")
            .displayName("Maximum depth of JSON")
            .description("Specifies maximum depth of JSON. -1 for unlimited.")
            .defaultValue("-1")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();

    static final AllowableValue ILCHAR_SET = new AllowableValue("true","true",
            "Fields in Splunk style");
    static final AllowableValue ILCHAR_UNSET = new AllowableValue("false","false",
            "Field names as is");

    static final PropertyDescriptor REMOVE_ILCHAR = new PropertyDescriptor.Builder()
            .name("remove_ilchar")
            .displayName("Illegal characters replace")
            .description("Illegal characters replace")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(ILCHAR_SET,ILCHAR_UNSET)
            .defaultValue(ILCHAR_SET.getValue())
            .required(true)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PARENT_FIELD_PREFIX);
        properties.add(ENCODE_KEYS);
        properties.add(MULTIJSON);
        properties.add(JSONSEP);
        properties.add(SPLUNK_STYLE);
        properties.add(MAXDEPTH);
        properties.add(REMOVE_ILCHAR);
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

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        for (final PropertyDescriptor property : context.getProperties().keySet()) {
            if (property.isDynamic()) {
                recordPaths.add(property.getName());
            }
        }

        this.recordPaths = recordPaths;
        final String json_sep=context.getProperty(JSONSEP).getValue();
        this.mjson_pat=Pattern.compile("(?ms)\\{.+"+json_sep+".+\\}");
        this.jsonsep_pat=Pattern.compile(json_sep);
        this.maxdepth=Integer.parseInt(context.getProperty(MAXDEPTH).getValue());
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context) {
        final boolean set_child_fields_prefix=context.getProperty(PARENT_FIELD_PREFIX).getValue().equals(PARENT_FIELD_PREFIX_SET.getValue());
        final boolean do_encode_keys=context.getProperty(ENCODE_KEYS).getValue().equals(ENCODE_KEYS_SET.getValue());
        final boolean do_multijson=context.getProperty(MULTIJSON).getValue().equals(MULTIJSON_SET.getValue());
        //final String json_sep=context.getProperty(JSONSEP).getValue();
        final boolean splunk_style=context.getProperty(SPLUNK_STYLE).getValue().equals(SPLUNK_SET.getValue());
        final boolean remove_ilchar=context.getProperty(REMOVE_ILCHAR).getValue().equals(ILCHAR_SET.getValue());

        final List<RecordField> newfields=new ArrayList<>();

        for (String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);
            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());
            //final Pattern kvpat = Pattern.compile("(?ms)^[^\\{]*(?<json>\\{.+\\})[^\\}]*$");
            //final Pattern mjson_pat=Pattern.compile("(?ms)\\{.+"+json_sep+".+\\}");

            for (FieldValue fieldVal:selectedFields) {
                //this.getLogger().debug("Field value:{}, name:{}", new Object[]{fieldVal.getValue(), fieldVal.getField().getFieldName()});
                final String field_prefix=set_child_fields_prefix?fieldVal.getField().getFieldName()+".":"";

                final String srcval=fieldVal.getValue().toString();
                //final Matcher kvmat = kvpat.matcher(srcval);
                final int icurv=srcval.indexOf('{');
                final int isq=srcval.indexOf('[');
                int istart=-1;
                if(icurv != -1 && isq != -1){
                    istart = Math.min(icurv, isq);
                }
                else if (icurv!=-1) istart=icurv;
                else if (isq!=-1) istart=isq;
                //this.getLogger().debug("Start:{}",new Object[]{istart});
                //while (kvmat.find()) {
                if(istart>=0){
                    //String json=kvmat.group("json");
                    String json=istart==0?srcval:srcval.substring(istart);
                    //if (json.length()<999000) {// temporary possible incorrect json
                        //this.getLogger().debug("JSON:{}", new Object[]{json});
                        if (do_multijson && mjson_pat.matcher(json).matches()) {
                            json = "[" + jsonsep_pat.matcher(json).replaceAll(",") + "]";
                        }
                        if(remove_ilchar) {
                            //json=json.replaceAll("\n","\\\\n");
                            json = new_line.matcher(json).replaceAll("\\\\n");
                            json = tab.matcher(json).replaceAll("    ");
                        /*String nlj="";
                        for (int i=0;i<json.length();i++){
                            char c=json.charAt(i);
                            if(c=='\n'){
                                nlj+="\\\\n";
                            }
                            else nlj+=c;
                        }
                        json=nlj;*/
                        }
                        int reccnt=0;
                        try {
                            Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(json);
                            //this.getLogger().debug("Parsed JSON:{}", new Object[]{flattenJson});
                            this.getLogger().debug("Total keys:{}",new Object[]{flattenJson.size()});
                            for (Map.Entry<String, Object> jf : flattenJson.entrySet()) {
                                reccnt++;
                                String key = jf.getKey();
                                int depth=0;
                                int nesteddot=0;
                                boolean overdepth=false;
                                if (maxdepth>0){
                                    for (int i=0;i<key.length();i++){
                                        if (key.startsWith("\\\"][\\\"",i)){
                                            i+=5;
                                            depth++;
                                        }
                                        else if(key.startsWith("[\\\"",i)){
                                            nesteddot++;
                                            if(i>0)depth++;
                                        }
                                        else if(key.startsWith("\\\"]",i)){
                                            if (nesteddot>0)nesteddot--;
                                        }
                                        else if(key.charAt(i)=='.'){
                                            if (nesteddot==0)depth++;
                                        }

                                        if(depth>=maxdepth){
                                            overdepth=true;
                                            break;
                                        }
                                    }
                                    if (overdepth)continue;
                                }
                                if(splunk_style){
                                    final boolean startbr=key.startsWith("[\\\"");
                                    key=SplunkPar.matcher(key).replaceAll(".$1");
                                    //key=SplunkParL.matcher(key).replaceAll(".");
                                    //key=SplunkParR.matcher(key).replaceAll("");
                                    if (startbr){
                                        key=key.substring(1);
                                    }

                                    /*
                                    String tmpkey="";
                                    boolean changed=false;
                                    for (int i=0;i<key.length();i++){
                                        if(key.startsWith("[\\\"",i)){
                                            changed=true;
                                            if(i>0)tmpkey+='.';
                                            i+=2;
                                        }
                                        else if(key.startsWith("\\\"]",i)){
                                            changed=true;
                                            i+=2;
                                        }
                                        else {
                                            tmpkey += key.charAt(i);
                                        }
                                    }
                                    if (changed)key=tmpkey;

                                     */
                                }
//                            if (do_encode_keys)key=java.net.URLEncoder.encode(key,"UTF-8");
                                if (do_encode_keys)
                                    key = DatatypeConverter.printHexBinary((field_prefix + key).getBytes());
                                final String value = jf.getValue() == null ? null : jf.getValue().toString();
                                this.getLogger().debug("Key:{}; Value:{}", key, value);
                                //if (reccnt<10000) {
                                    final RecordField keyf = new RecordField(Utils.FIELD_PREFIX + key, RecordFieldType.STRING.getDataType(), true);
                                    record.setValue(keyf, value);
                                    newfields.add(keyf);
                                //}
                                //break;
                            }
                        } catch (com.eclipsesource.json.ParseException e) {
                            final String mess = e.getLocalizedMessage();
                            int fpos = e.getLocation().offset;
                            //this.getLogger().info("Length:{} Pos:{}",new Object[]{json.length(),fpos});
                            if (fpos >= json.length()) fpos = json.length() - 1;
                            final int spos = fpos < 10 ? 0 : fpos - 10;
                            final int epos = fpos > (json.length() - 10) ? json.length() : fpos + 10;
                            final int eposs = fpos < (json.length() - 1) ? fpos + 1 : fpos;
                            final char fchar = json.charAt(fpos);
                            final String fstr = json.substring(spos, fpos) + "<" + fchar + ">" + json.substring(eposs, epos);
                            final RecordField keyf = new RecordField("_JSON_parsing_error", RecordFieldType.STRING.getDataType(), true);
                            record.setValue(keyf, mess + " " + fstr);
                            newfields.add(keyf);
                            if (fpos>=(json.length() - 1)){
                                this.getLogger().debug("Object:"+json);
                                this.getLogger().info("Error at:{}", fstr);
                                this.getLogger().error("Parsing error:",e);
                            }
                            else {
                                this.getLogger().debug("Object:"+json);
                                this.getLogger().error("Error at:{} Parsing error:", fstr, e);
                            }
                        }
                    /*catch (java.io.UnsupportedEncodingException e){
                        this.getLogger().error("Unsupported Encoding:{} Parsing error:",new Object[]{json},e);
                    }*/
                    //}
                    //else {
                    //    this.getLogger().info("Huge json received {} chars",new Object[json.length()]);
                    //}
                }
            }
        }

        //this.getLogger().info("Fields added");
        //record.incorporateInactiveFields(); //performance issue
        record.incorporateSchema(new SimpleRecordSchema(newfields));
        //this.getLogger().info("Fields incorporaded");
        //RecordSchema oldsch=record.getSchema();
        //this.getLogger().debug("schema:{}",new Object[]{oldsch});

        return record;
    }

}
