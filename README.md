# ISGneuro Ni-Fi tools

This repository contains custom processors designed for Apache Ni-Fi. These processors are used in various data processing flows.

Processors group name

```
com.isgneuro.etl
```

Contains processors:
1. AddRaw
2. BloomFilterCalculator
3. JSONParseRecord
4. JSONSParseRecord
5. KVParseRecord
6. ListenTCPRecordWithDump
7. MergeRecordNoAvro
8. PutParquetNoAvro
9. RecordEditSchema


### Prerequisites

* Java JRE 1.8
* Maven (https://maven.apache.org/) - Dependency Management
* Maven catalog with Apache Ni-Fi archetype (https://repo.maven.apache.org/maven2/archetype-catalog.xml)
* Apache Ni-Fi 1.15.3

### Built With

* Eclipse IDE with m2e plugin

## Building

```
mvn clean install -X
```

### Deployment

1. Put .nar file from nifi-tools-nar/target/ to lib directory in NIFI_HOME
2. Restart Ni-Fi

