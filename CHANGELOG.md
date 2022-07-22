All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.5] - 2022-07-22
### Added
- Debug options for BloomFilterCalculator
### Added
- New settings for BloomFilterCalculator

## [1.0.4] - 2022-07-18
### Fixed
- Tokenizer class for BloomFilterCalculator

## [1.0.3] - 2022-07-12
### Added
- New settings for BloomFilterCalculator
### Fixed
- Refactored AddRaw, PutParquetNoAvro

## [1.0.2] - 2022-03-24
### Fixed
- Changed hadoop.version in pom.xml

## [1.0.1] - 2022-03-16
### Fixed
- Changed trigger mode for BloomFilterCalculator.

## [1.0.0] - 2022-02-25
### Initial version
- Added processors AddRaw, BloomFilterCalculator, JSONParseRecord, JSONSParseRecord, KVParseRecord, ListenTCPRecordWithDump, MergeRecordNoAvro, PutParquetNoAvro, RecordEditSchema.