package com.isgneuro.etl.nifi.processors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.AbstractRecordProcessor;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.regex.Pattern;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "generic", "schema", "json", "text"})
@CapabilityDescription("Adds _raw field")

public class AddRaw extends AbstractRecordProcessor {
    private Pattern LISTSEP = Pattern.compile("\\s*,\\s*");

    public static final PropertyDescriptor FIELD_LIST = new PropertyDescriptor.Builder()
            .name("Field List")
            .description("Field list to include into _raw")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor IGNORE_LIST = new PropertyDescriptor.Builder()
            .name("Ignore List")
            .description("Field list to exclude from _raw")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(FIELD_LIST);
        properties.add(IGNORE_LIST);
        return properties;
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, long count) {
        HashSet<String> field_list = new HashSet<>();
        final String rawfields = context.getProperty(FIELD_LIST).evaluateAttributeExpressions(flowFile).getValue();
        if(rawfields != null)
        	field_list = new HashSet<String>(Arrays.asList(LISTSEP.split(rawfields)));
        
        HashSet<String> ignore_list = new HashSet<>();
        final String ignorefields = context.getProperty(IGNORE_LIST).evaluateAttributeExpressions(flowFile).getValue();
        if(ignorefields != null)
        	ignore_list = new HashSet<String>(Arrays.asList(LISTSEP.split(ignorefields)));
        
        String _raw = "";
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            if (!field_list.isEmpty()){
                final Map<String, Object> raw_content = new HashMap<>();
                for (Map.Entry<String, Object> f : record.toMap().entrySet()){
                    if (field_list.contains(f.getKey())){
                    	raw_content.put(f.getKey(),f.getValue());
                    }
                }
                _raw = objectMapper.writeValueAsString(raw_content);
            }
            else if (!ignore_list.isEmpty()){
                final Map<String, Object> raw_content = new HashMap<>();
                for (Map.Entry<String, Object> f : record.toMap().entrySet()){
                    if (!ignore_list.contains(f.getKey())){
                        raw_content.put(f.getKey(), f.getValue());
                    }
                }
                _raw = objectMapper.writeValueAsString(raw_content);
            }
            else {
                _raw = objectMapper.writeValueAsString(record.toMap());
            }
        } catch (JsonProcessingException e) {
            this.getLogger().error("Error while processing record {} {}", new Object[]{record}, e);
        }
        final RecordField keyf = new RecordField("_raw", RecordFieldType.STRING.getDataType(), true);
        record.setValue(keyf, _raw);
        final List<RecordField> newfields = new ArrayList<>();
        newfields.add(keyf);
        record.incorporateSchema(new SimpleRecordSchema(newfields));
        return record;
    }

}
