## ADDED Requirements

### Requirement: SQLx Any Integration
The `DbConnectorNode` SHALL use `sqlx::Any` to connect to any database protocol supported by SQLx (MySQL, MSSQL, etc.) when a specialized DuckDB scanner is unavailable or disabled.

#### Scenario: Connect to MySQL
- **WHEN** a user provides a `mysql://` URL
- **THEN** the system SHALL create a `sqlx::AnyPool` and execute the query via SQLx.

### Requirement: Runtime Row-to-Arrow Mapping
The system SHALL dynamically map SQLx `AnyRow` objects to Arrow `RecordBatch`es by inspecting column types at runtime.

#### Scenario: Map varied types
- **WHEN** a database returns a row with VARCHAR, INT, and BOOLEAN columns
- **THEN** the system SHALL correctly build a RecordBatch with corresponding Arrow types.
