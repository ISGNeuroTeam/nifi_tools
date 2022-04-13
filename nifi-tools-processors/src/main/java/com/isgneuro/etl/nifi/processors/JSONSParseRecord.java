package com.isgneuro.etl.nifi.processors;

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
import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.json.Json;
import javax.json.stream.JsonParser;


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
public class JSONSParseRecord extends AbstractRecordProcessorWithSchemaUpdates {
	private static final String FIELD_NAME = "field.name";
	private static final String FIELD_VALUE = "field.value";
	private static final String FIELD_TYPE = "field.type";

	private final Pattern kvpat = Pattern.compile("(?ms)^[^\\{\\[]*(?<json>[\\{\\[].+)$");
	private Pattern mjson_pat = null;
	private Pattern jsonsep_pat = null;
	private final Pattern new_line = Pattern.compile("\n");
	private final Pattern tab = Pattern.compile("\t");
	// private final Pattern SplunkPar=Pattern.compile("\\[\\\\\"([^\\[\\]]+?)\\\\\"\\]");
	private int maxdepth = -1;
	private boolean remove_ilchar = true;
	private boolean splunk_style = false;
	private boolean set_child_fields_prefix = false;
	private boolean do_encode_keys = false;
	private boolean do_multijson = true;
	private boolean overdepthastext = true;

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

    static final AllowableValue OVERDEPTHASTEXT_SET = new AllowableValue("true","true",
            "Print overdepth fields as text");
    static final AllowableValue OVERDEPTHASTEXT_UNSET = new AllowableValue("false","false",
            "Do not print overdepth fields");

    static final PropertyDescriptor OVERDEPTHASTEXT = new PropertyDescriptor.Builder()
            .name("overdepthastext")
            .displayName("Print overdepth fields as text")
            .description("Print overdepth fields as text")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(OVERDEPTHASTEXT_SET,OVERDEPTHASTEXT_UNSET)
            .defaultValue(OVERDEPTHASTEXT_SET.getValue())
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
        properties.add(OVERDEPTHASTEXT);
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
        this.remove_ilchar=context.getProperty(REMOVE_ILCHAR).getValue().equals(ILCHAR_SET.getValue());
        this.splunk_style=context.getProperty(SPLUNK_STYLE).getValue().equals(SPLUNK_SET.getValue());
        this.set_child_fields_prefix=context.getProperty(PARENT_FIELD_PREFIX).getValue().equals(PARENT_FIELD_PREFIX_SET.getValue());
        this.do_encode_keys=context.getProperty(ENCODE_KEYS).getValue().equals(ENCODE_KEYS_SET.getValue());
        this.do_multijson=context.getProperty(MULTIJSON).getValue().equals(MULTIJSON_SET.getValue());
        this.overdepthastext=context.getProperty(OVERDEPTHASTEXT).getValue().equals(OVERDEPTHASTEXT_SET.getValue());
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context) {

        final List<RecordField> newfields=new ArrayList<>();

        for (String recordPathText : recordPaths) {
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);
            final List<FieldValue> selectedFields = result.getSelectedFields().collect(Collectors.toList());

            for (FieldValue fieldVal:selectedFields) {
                this.getLogger().debug("Field value:{}, name:{}", new Object[]{fieldVal.getValue(), fieldVal.getField().getFieldName()});
                final String field_prefix=set_child_fields_prefix?fieldVal.getField().getFieldName()+".":"";
                /*
                final String srcval=fieldVal.getValue().toString();
                final int icurv=srcval.indexOf('{');
                final int isq=srcval.indexOf('[');

                 */
                final StringBuilder json=new StringBuilder(fieldVal.getValue().toString());
                final int icurv=json.indexOf("{");
                final int isq=json.indexOf("[");
                int istart=-1;
                if(icurv != -1 && isq != -1){
                    istart=icurv < isq ? icurv : isq;
                }
                else if (icurv!=-1) istart=icurv;
                else if (isq!=-1) istart=isq;
                //this.getLogger().debug("Start:{}",new Object[]{istart});
                if(istart>=0){
                    //String json=istart==0?srcval:srcval.substring(istart);
                    if (istart>0)json.delete(0,istart);
                        //this.getLogger().debug("JSON:{}", new Object[]{json});
                        if (do_multijson /*&& mjson_pat.matcher(json).matches()*/) {
                            //json = "[" + jsonsep_pat.matcher(json).replaceAll(",") + "]";
                            if(replaceall(json,jsonsep_pat,",")>0) {
                                json.insert(0, '[');
                                json.append(']');
                            }
                        }
                        if(remove_ilchar) {
                            //json = new_line.matcher(json).replaceAll("\\\\n");
                            //json = tab.matcher(json).replaceAll("    ");
                            replaceall(json,new_line,"\\\\n");
                            replaceall(json,tab,"    ");
                        }
                        final StringReader sr=new StringReader(json.toString());
                        try {
                            final JsonParser parser = Json.createParser(sr);
                            List<memory> path = new ArrayList<>();
                            boolean isobject = false;
                            boolean isarray = false;
                            Integer arrayidx = null;
                            String Key = "";
                            int depth=0;

                            while (parser.hasNext()) {
                                JsonParser.Event event = parser.next();

                                switch (event) {
                                    case START_ARRAY:
                                        if (maxdepth>0 && depth>=maxdepth){
                                            if (overdepthastext) wrel(record, isarray, Key, path, parser.getValue().toString(), newfields, arrayidx);
                                            parser.skipArray();
                                        }
                                        else {
                                            if (isarray) {
                                                path.add(new memory(Key, arrayidx));
                                                //arrayidx++;
                                            } else {
                                                path.add(new memory(Key, null));
                                            }
                                            Key = "";
                                            isarray = true;
                                            arrayidx = 0;
                                            depth++;
                                        }
                                        break;
                                    case END_ARRAY:
                                        if (path.size()>0){
                                            memory last=path.remove(path.size() -1);
                                            if (last.arrayidx!=null){
                                                isarray=true;
                                                arrayidx=last.arrayidx;
                                                arrayidx++;
                                            }
                                            else {
                                                isarray=false;
                                                arrayidx=null;
                                            }
                                            Key=last.field;
                                        }
                                        /*
                                        else {
                                            isarray = false;
                                            arrayidx = null;
                                        }
                                        */
                                        //if (path.size() > 0) path.remove(path.size() - 1);
                                        if(depth>0)depth--;
                                        break;
                                    case START_OBJECT:
                                        if (maxdepth>0 && depth>=maxdepth){
                                            if (overdepthastext) wrel(record, isarray, Key, path, parser.getValue().toString(), newfields, arrayidx);
                                            parser.skipObject();
                                        }
                                        else {
                                            path.add(new memory(Key, arrayidx));
                                            isobject = true;
                                            isarray = false;
                                            arrayidx = null;
                                            depth++;
                                        }
                                        break;
                                    case END_OBJECT:
                                        if (path.size() > 0){
                                            memory last=path.remove(path.size() - 1);
                                            Key=last.field;
                                            arrayidx=last.arrayidx;
                                            if (arrayidx!=null){
                                                arrayidx++;
                                                isarray=true;
                                            }
                                        }
                                        isobject = false;
                                        if (depth>0)depth--;
                                        break;
                                    case KEY_NAME:
                                        Key=parser.getString();
                                        break;
                                    case VALUE_FALSE:
                                    case VALUE_TRUE:
                                    case VALUE_NUMBER:
                                        if(maxdepth<=0 || depth<=maxdepth){
                                            wrel(record, isarray, Key, path, parser.getValue(), newfields, arrayidx);
                                        }
                                        if (isarray) arrayidx++;
                                        break;
                                    case VALUE_NULL:
                                        if(maxdepth<=0 || depth<=maxdepth) {
                                            wrel(record, isarray, Key, path, null, newfields, arrayidx);
                                        }
                                        if (isarray) arrayidx++;
                                        break;
                                    case VALUE_STRING:
                                        if(maxdepth<=0 || depth<=maxdepth) {
                                            wrel(record, isarray, Key, path, parser.getString(), newfields, arrayidx);
                                        }
                                        if (isarray) arrayidx++;
                                        break;
                                }
                            }
                        }
                        catch (Exception e){
                            this.getLogger().error("Parsing Error:",e);
                            wrel(record,false, "_JSON_parsing_error",null, e.getLocalizedMessage(), newfields, 0);
                        }

                        sr.close();


                }
            }
        }

        //this.getLogger().info("fields:{}",new Object[]{newfields});
        record.incorporateSchema(new SimpleRecordSchema(newfields));
        //record.incorporateInactiveFields();
        //RecordSchema oldsch=record.getSchema();
        //this.getLogger().debug("schema:{}",new Object[]{oldsch});

        return record;
    }

    /*
    public String getpath(List<memory> path){
            if (path != null && path.size()>0) {
                List<String> ps=new ArrayList<>();
                int iter=0;
                for (memory m: path){
                    if (m.arrayidx!=null){
                        ps.add( ((iter!=0 && m.field.length()>0)?'.':"") + m.field + "[" + m.arrayidx + "]");
                    }
                    else ps.add(((iter!=0 && m.field.length()>0)?'.':"") + m.field);
                    if(m.field.length()>0 || m.arrayidx!=null)iter=1;
                }
                return String.join("",ps);
            }
            else return "";
    }

     */

    public StringBuilder getpathsb(List<memory> path){
        if (path != null && path.size()>0) {
            StringBuilder ps=new StringBuilder(1024);
            int iter=0;
            for (memory m: path){
                if (iter==0)iter=ps.length();
                escapefieldname(ps,m,iter);
                /*
                boolean nosplunk=false;
                if (!splunk_style && m.field.indexOf('.')!=-1){
                    nosplunk=true;
                }
                if(iter!=0 && m.field.length()>0 && !nosplunk)ps.append('.');
                if (nosplunk)ps.append("[\\\"");
                ps.append(m.field);
                if(nosplunk)ps.append("\\\"]");
                if (m.arrayidx!=null){
                    //ps.append( ((iter!=0 && m.field.length()>0 && !splunk)?'.':"") + m.field + "[" + m.arrayidx + "]");
                    ps.append("["+ m.arrayidx +"]");
                }
                //else ps.append(((iter!=0 && m.field.length()>0)?'.':"" ) + m.field);

                 */
                //if(m.field.length()>0 || m.arrayidx!=null)iter=1;
            }
            return ps;
        }
        else return new StringBuilder("");
    }

    public void escapefieldname(StringBuilder ps,final memory m, int iter){
        boolean nosplunk=false;
        if (!splunk_style && m.field.indexOf('.')!=-1){
            nosplunk=true;
        }
        if(iter>0 && m.field.length()>0 && !nosplunk)ps.append('.');
        if (nosplunk)ps.append("[\\\"");
        ps.append(m.field);
        if(nosplunk)ps.append("\\\"]");
        if (m.arrayidx!=null){
            //ps.append( ((iter!=0 && m.field.length()>0 && !splunk)?'.':"") + m.field + "[" + m.arrayidx + "]");
            ps.append("["+ m.arrayidx +"]");
        }

        //else ps.append(((iter!=0 && m.field.length()>0)?'.':"" ) + m.field);
        /*
        ps.append(
                ((iter>0 && m.field.length()>0 && !nosplunk)?'.':"") +
                        (nosplunk?"[\\\"":"") +
                        m.field +
                        (nosplunk?"\\\"]":"") +
                        (m.arrayidx!=null?"["+ m.arrayidx +"]":"")
        );

         */

    }

    private void wrel(Record record,boolean isarray,String Key,List<memory> path,Object value,List<RecordField> newfields,Integer arrayidx){
//        String key=this.getpath(path)+((Key.length()>0 && (path != null && path.size()>0))?".":"")+Key;
        //StringBuilder key=this.getpathsb(path).append((Key.length()>0 && (path != null && path.size()>0))?".":"").append(Key);
        StringBuilder key=this.getpathsb(path);
        this.escapefieldname(key,new memory(Key,arrayidx),key.length());
        //if (key.startsWith("."))key=key.substring(1);
        /*
        if(key.charAt(0)=='.')key.deleteCharAt(0);
        if (isarray){
            //key += "["+ arrayidx +"]";
            key.append("["+ arrayidx +"]");
        }

         */
        /*
        else {
            if (path.size() > 0) path.remove(path.size() - 1);
        }

         */

//        final RecordField keyf = new RecordField(ot_constants.FieldPrefix + key, RecordFieldType.STRING.getDataType(), true);
        final RecordField keyf = new RecordField(key.insert(0,ot_constants.FieldPrefix).toString(), RecordFieldType.STRING.getDataType(), true);
        record.setValue(keyf, value);
        newfields.add(keyf);

    }

    private int replaceall(StringBuilder sb,Pattern p,String replace){
        Matcher matcher = p.matcher(sb);

        //index of StringBuilder from where search should begin
        int startIndex = 0;
        int replaces=0;

        while( matcher.find(startIndex) ){

            sb.replace(matcher.start(), matcher.end(), replace);

            //set next start index as start of the last match + length of replacement
            startIndex = matcher.start() + replace.length();
            replaces++;
        }
        return replaces;
    }

    class memory {
        public final Integer arrayidx;
        public final String field;

        public memory(String field,Integer idx){
            this.arrayidx=idx;
            this.field=field;
        }
    }

}
