## ADDED Requirements

### Requirement: Support for Cloud Storage
The `UniversalReadNode` SHALL support reading data from cloud providers via `s3://`, `gcs://`, and `http://` protocols.

#### Scenario: Read JSON from S3
- **WHEN** a path starts with `s3://bucket/data.json`
- **THEN** the system SHALL use DuckDB's `httpfs` extension to pull and parse the file.

### Requirement: Cloud Authentication
The system SHALL support configuring cloud credentials (access keys, tokens) for the DuckDB `httpfs` driver.

#### Scenario: Auth with AWS Keys
- **WHEN** AWS credentials are provided in the node config
- **THEN** DuckDB SHALL set the corresponding `s3_access_key_id` and `s3_secret_access_key` variables before reading.
