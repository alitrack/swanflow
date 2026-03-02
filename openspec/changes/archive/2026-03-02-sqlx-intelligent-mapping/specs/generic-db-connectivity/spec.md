## MODIFIED Requirements

### Requirement: SQLx Any Integration
The `DbConnectorNode` SHALL use `sqlx::Any` to connect to any database protocol supported by SQLx (MySQL, MSSQL, etc.) when a specialized DuckDB scanner is unavailable or disabled. The system MUST preserve original data types during this process.

#### Scenario: Connect to MySQL with type preservation
- **WHEN** a user provides a `mysql://` URL
- **THEN** the system SHALL create a `sqlx::AnyPool` and execute the query, ensuring numeric columns are mapped to Arrow numeric types.

### Requirement: Runtime Row-to-Arrow Mapping
The system SHALL dynamically map SQLx `AnyRow` objects to Arrow `RecordBatch`es by inspecting column types at runtime and using appropriate Arrow builders.

#### Scenario: Map varied types with high fidelity
- **WHEN** a database returns a row with VARCHAR, INT, and BOOLEAN columns
- **THEN** the system SHALL build a RecordBatch using `StringBuilder`, `Int64Builder`, and `BooleanBuilder` respectively.
