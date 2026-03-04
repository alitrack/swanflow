## ADDED Requirements

### Requirement: Standardized Sandbox Runtime
The BoxLite VM SHALL use a base image pre-configured with `duckdb`, `polars`, and `boto3`.

#### Scenario: Running data analysis in sandbox
- **WHEN** the sandbox starts
- **THEN** it SHALL be able to import these libraries without additional installation steps.

### Requirement: S3-to-Table Bootstrap
The system SHALL execute a bootstrap script inside the sandbox that automatically attaches S3 pre-signed URLs as DuckDB tables.

#### Scenario: Transparent data access
- **WHEN** the code refers to an input table `sales_data`
- **THEN** the bootstrap logic SHALL have already executed `CREATE VIEW sales_data AS SELECT * FROM read_parquet('PRESIGNED_URL')` using DuckDB's HTTPFS.
