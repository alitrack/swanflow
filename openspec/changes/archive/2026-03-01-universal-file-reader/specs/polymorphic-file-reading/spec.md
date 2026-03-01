## ADDED Requirements

### Requirement: Automatic Format Detection
The `UniversalReadNode` SHALL automatically identify the format of an input file by checking its extension (e.g., `.parquet`, `.csv`, `.json`, `.avro`, `.arrow`).

#### Scenario: Read Parquet file automatically
- **WHEN** a user provides a path to `data.parquet`
- **THEN** the system SHALL invoke the DataFusion Parquet reader without explicit format configuration.

### Requirement: Manual Format Override
The system SHALL allow users to explicitly specify the file format in the node configuration to override automatic detection.

#### Scenario: Force CSV on extensionless file
- **WHEN** a file named `raw_data_stream` is provided with `format: "csv"`
- **THEN** the system SHALL treat it as a CSV file.
