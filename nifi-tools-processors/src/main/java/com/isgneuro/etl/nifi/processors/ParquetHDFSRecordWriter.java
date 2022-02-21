package com.isgneuro.etl.nifi.processors;

import org.apache.nifi.processors.hadoop.record.HDFSRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

public class ParquetHDFSRecordWriter implements HDFSRecordWriter {
    private final MessageType parquetSchema;
    private final ParquetWriter<Group> parquetWriter;
    private final Map<String,String> replaces;
    private final SimpleGroupFactory groupFactory;

    public ParquetHDFSRecordWriter(ParquetWriter<Group> parquetWriter, MessageType parquetSchema, Map<String,String> replaces) {
        this.parquetSchema = parquetSchema;
        this.parquetWriter = parquetWriter;
        this.replaces = replaces;
        groupFactory = new SimpleGroupFactory(parquetSchema);
    }

    public void write(Record record) throws IOException {
        Group g = createGroup(record);
        this.parquetWriter.write(g);
    }

    public void close() throws IOException {
        this.parquetWriter.close();
    }

    Group createGroup(Record record) throws IOException{
        Group g = groupFactory.newGroup();
        for (Map.Entry<String, Object> e : record.toMap().entrySet()) {
            if(e.getValue() == null) continue;

            String newName = replaces.getOrDefault(e.getKey(), e.getKey());
            PrimitiveTypeName type = parquetSchema.getType(newName).asPrimitiveType().getPrimitiveTypeName();
            if (type.equals(BINARY)) {
                g.append(newName, String.valueOf(e.getValue()));
            } else if (type.equals(INT64))
                g.append(newName, (Long) e.getValue());
            else if (type.equals(INT32))
                g.append(newName, (Integer) e.getValue());
            else if (type.equals(BOOLEAN))
                g.append(newName, (Boolean) e.getValue());
            else if (type.equals(FLOAT))
                g.append(newName, (Float) e.getValue());
            else if (type.equals(DOUBLE))
                g.append(newName, (Double) e.getValue());
            else if(type.equals(BINARY)) g.append(newName, String.valueOf(e.getValue()));
            else throw new IOException("Field name '" + newName +"' doesn't consistent with schema: " + parquetSchema);
        }
        return g;
    }



}