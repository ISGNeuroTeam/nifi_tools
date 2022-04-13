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


## Prerequisites

* Java SE 8 JDK
* Maven 3.8
* Apache Ni-Fi 1.15.3

## Building

For build

```
mvn clean install -X
```

For checking dependencies

```
mvn dependency:tree
```

## Deployment

1. Put .nar file from nifi-tools-nar/target/ to lib directory in NIFI_HOME
2. Restart Ni-Fi

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the tags on this repository. 

## Authors

Mikhail Rubashenkov (mikhail@isgneuro.com)   
Dmitriy Gusarov (dgusarov@isgneuro.com)  
Nizam Keramov (nkeramov@isgneuro.com, nkeramov@isgneuro.ru)  

## License

[OT.PLATFORM. License agreement.](LICENSE.md)


