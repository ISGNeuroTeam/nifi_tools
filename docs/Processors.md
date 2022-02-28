# ISGneuro Ni-Fi custom processors description

This page provides a detailed description of the processors package com.isgneuro.etl.

Some processors are designed from the ground up, some extend the functionality of standard. If the processor does not extend the functionality of one of the standard processors (it extends only abstract processors), then all properties will be described in section Properties. Otherwise, only the added properties will be described.

### AddRaw
<p align="center"><a href="AddRaw.png"><img src="AddRaw.png" width="600" /></a></p>
 
Description: Adds _raw field. 

Extends: AbstractRecordProcessor

Properties (default values are in parentheses):
1. Record Reader
2. Record Writer
3. Field list (empty). This property contains comma separated list of fields that will be included in the _raw field. 
4. Ignore List (empty). This property contains comma separated list of fields that will be excluded from the _raw field

The processor logic allows the use of only one of the properties 'Field list' and 'Ignore list'

### BloomFilterCalculator
<p align="center"><a href="BloomFilterCalculator.png"><img src="BloomFilterCalculator.png" width="600" /></a></p>

Description: Aggregate value of BloomFilter processed in the current session time window.

Extends: AbstractProcessor

Properties (default values are in parentheses):
1. Record Reader
2. BucketID
3. Time gap
4. Bloom file name (bloom)
5. Expected number of tokens (100000)
6. False positive probabilty (0.05)
7. Use json tokeniser (false)

### JSONParseRecord
<p align="center"><a href="JSONParseRecord.png"><img src="JSONParseRecord.png" width="600" /></a></p>

Description: Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.

Extends: AbstractRecordProcessorWithSchemaUpdates

Properties (default values are in parentheses):
1. Record Reader
2. Record Writer
3. New Fields Prefix (false)
4. Encode field names (false)
5. Collect multiple JSONs into array (false)
6. MultiJSON separator ((?<=\})\s*\n+\s*(?=\{))
7. Splunk style field names (false)
8. Maximum depth of JSON (-1)
9. Illegal characters replace (true)

### JSONSParseRecord
<p align="center"><a href="JSONSParseRecord.png"><img src="JSONSParseRecord.png" width="600" /></a></p>

Description: Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.

Extends: AbstractRecordProcessorWithSchemaUpdates

Properties (default values are in parentheses):
1. Record Reader
2. Record Writer
3. New Fields Prefix (false)
4. Encode field names (false)
5. Collect multiple JSONs into array (false)
6. MultiJSON separator ((?<=\})\s*\n+\s*(?=\{))
7. Splunk style field names (false)
8. Maximum depth of JSON (-1)
9. Illegal characters replace (true)
10. Print overdepth fields as text (true)

### KVParseRecord
<p align="center"><a href="KVParseRecord.png"><img src="KVParseRecord.png" width="600" /></a></p>

Description: Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from the Record. Whether the Property value is determined to be a RecordPath or a literal value depends on the configuration of the <Replacement Value Strategy> Property.

Extends: AbstractRecordProcessorWithSchemaUpdates

Properties (default values are in parentheses):
1. Record Reader
2. Record Writer
3. New Fields Prefix (false)
4. Key Value connector ((?:::)|=)
5. Key-Value pairs separator (\s+)
6. Maximum value length (1000)
7. Maximum input string length (-1)
8. Encode field names (false)


### ListenTCPRecordWithDump
<p align="center"><a href="ListenTCPRecordWithDump.png"><img src="ListenTCPRecordWithDump.png" width="600" /></a></p>

Description: Listens for incoming TCP connections and reads data from each connection using a configured record reader, and writes the records to a flow file using a configured record writer. The type of record reader selected will determine how clients are expected to send data. For example, when using a Grok reader to read logs, a client can keep an open connection and continuously stream data, but when using an JSON reader, the client cannot send an array of JSON documents and then send another array on the same connection, as the reader would be in a bad state at that point. Records will be read from the connection in blocking mode, and will timeout according to the Read Timeout specified in the processor. If the read times out, or if any other error is encountered when reading, the connection will be closed, and any records read up to that point will be handled according to the configured Read Error Strategy (Discard or Transfer). In cases where clients are keeping a connection open, the concurrent tasks for the processor should be adjusted to match the Max Number of TCP Connections allowed, so that there is a task processing each connection.

Extends: AbstractProcessor. Judging by the source code, it was written based on source of the <a href="https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-standard-bundle/nifi-standard-processors/src/main/java/org/apache/nifi/processors/standard/ListenTCPRecord.java">ListenTCPRecord processor</a>. <a href="https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.15.3/org.apache.nifi.processors.standard.ListenTCPRecord/index.html">Description of ListenTCPRecord processor</a>


### MergeRecordNoAvro
<p align="center"><a href="MergeRecordNoAvro.png"><img src="MergeRecordNoAvro.png" width="600" /></a></p>

Description: None

Extends: MergeRecord

### PutParquetNoAvro
<p align="center"><a href="PutParquetNoAvro.png"><img src="PutParquetNoAvro.png" width="600" /></a></p>

Description: Reads records from an incoming FlowFile using the provided Record Reader, and writes those records to a Parquet file. The schema for the Parquet file must be provided in the processor properties. This processor will first write a temporary dot file and upon successfully writing every record to the dot file, it will rename the dot file to it's final name. If the dot file cannot be renamed, the rename operation will be attempted up to 10 times, and if still not successful, the dot file will be deleted and the flow file will be routed to failure. If any error occurs while reading records from the input, or writing records to the output, the entire dot file will be removed and the flow file will be routed to failure or retry, depending on the error.

Extends: AbstractPutHDFSRecord. Judging by the source code, it was written based on the source of the <a href="https://github.com/apache/nifi/blob/main/nifi-nar-bundles/nifi-parquet-bundle/nifi-parquet-processors/src/main/java/org/apache/nifi/processors/parquet/PutParquet.java">PutParquet processor</a>. <a href="https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-parquet-nar/1.15.3/org.apache.nifi.processors.parquet.PutParquet/index.html">Description of PutParquet processor</a>

### RecordEditSchema
<p align="center"><a href="RecordEditSchema.png"><img src="RecordEditSchema.png" width="600" /></a></p>

Description: Updates the contents of a FlowFile that contains Record-oriented data (i.e., data that can be read via a RecordReader and written by a RecordWriter). This Processor requires that at least one user-defined Property be added. The name of the Property should indicate a RecordPath that determines the field that should be updated. The value of the Property is either a replacement value (optionally making use of the Expression Language) or is itself a RecordPath that extracts a value from the Record. Whether the Property value is determined to be a RecordPath or a literal value depends. User-defined properties values must contain new data type for record field. Use 'drop' or empty string for remove field from record.

Extends: AbstractProcessor

Properties (default values are in parentheses):
1. Record Reader
2. Record Writer

The rest of the properties are user defined. For example, you can add next properties: 
1. /_message with value drop (field will be removed)
2. /time with value LONG (field type will be changed to LONG)