## ADDED Requirements

### Requirement: Unified Directory Scanning
The `UniversalReadNode` SHALL support paths that point to directories, treating all compatible files within that directory as a single logical table.

#### Scenario: Read folder of Parquet files
- **WHEN** a path points to a directory containing 10 Parquet files
- **THEN** the system SHALL return a single data stream representing the union of all files in that directory.

### Requirement: Recursive File Discovery
The system SHALL provide an option to recursively scan subdirectories for data files.

#### Scenario: Scan nested directory
- **WHEN** `recursive: true` is set and a directory has subfolders with data files
- **THEN** all files matching the detected format SHALL be included in the output.
