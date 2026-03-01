## ADDED Requirements

### Requirement: Hive-style Partition Discovery
The `UniversalReadNode` SHALL automatically detect Hive-style partitioning patterns (e.g., `key=value/`) in directory paths and expose them as columns in the resulting Arrow schema.

#### Scenario: Discover year and month partitions
- **WHEN** reading from a path like `data/year=2024/month=03/file.parquet`
- **THEN** the output schema SHALL contain `year` (int) and `month` (int) columns with the values 2024 and 03 respectively.

### Requirement: Partition Value Type Inference
The system SHALL attempt to infer the correct data type (Int, Float, or Utf8) for discovered partition values.

#### Scenario: Infer integer partition
- **WHEN** a partition is `day=15`
- **THEN** the system SHALL treat the `day` column as an integer type.
