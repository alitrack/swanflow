## ADDED Requirements

### Requirement: Automated S3 Data Handoff
The system SHALL automatically upload input Arrow RecordBatches to an S3-compatible storage bucket before executing sandboxed code.

#### Scenario: Input data preparation
- **WHEN** a `BoxedPythonNode` receives input data
- **THEN** it SHALL convert the data to Parquet format
- **AND** it SHALL upload it to a temporary S3 path
- **AND** it SHALL generate a pre-signed GET URL for that path.

### Requirement: Secure Result Recovery
The system SHALL generate pre-signed PUT URLs to allow the sandbox to upload results back to S3 without possessing long-term credentials.

#### Scenario: Capturing sandbox output
- **WHEN** the `BoxedPythonNode` orchestrates a task
- **THEN** it SHALL provide the sandbox with a pre-signed PUT URL for the `results.parquet` file
- **AND** it SHALL download the file from S3 once the sandbox reports success.
