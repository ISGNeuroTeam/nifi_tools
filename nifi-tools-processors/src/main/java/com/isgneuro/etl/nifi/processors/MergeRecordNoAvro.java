package com.isgneuro.etl.nifi.processors;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.standard.MergeRecord;
import org.apache.nifi.serialization.record.RecordSchema;

public class MergeRecordNoAvro extends MergeRecord {
    @Override
    protected String getGroupId(final ProcessContext context, final FlowFile flowFile, final RecordSchema schema, final ProcessSession session) {
        final String mergeStrategy = context.getProperty(MERGE_STRATEGY).getValue();
        if (MERGE_STRATEGY_DEFRAGMENT.getValue().equals(mergeStrategy)) {
            return flowFile.getAttribute(FRAGMENT_ID_ATTRIBUTE);
        }

        final String schemaText = schema.toString();
        this.getLogger().debug("Schema:{}",new Object[]{schemaText});

        final String groupId;
        final String correlationshipAttributeName = context.getProperty(CORRELATION_ATTRIBUTE_NAME).getValue();
        if (correlationshipAttributeName != null) {
            final String correlationAttr = flowFile.getAttribute(correlationshipAttributeName);
            groupId = correlationAttr == null ? schemaText : schemaText + correlationAttr;
        } else {
            groupId = schemaText;
        }
        return groupId;
    }
}
