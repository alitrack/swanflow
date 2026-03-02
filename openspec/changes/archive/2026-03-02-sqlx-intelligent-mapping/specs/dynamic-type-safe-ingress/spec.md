## ADDED Requirements

### Requirement: Dynamic SQLx Type Detection
The system SHALL inspect the `TypeInfo` of each column in a SQLx result set at runtime to determine the most appropriate Arrow `DataType`.

#### Scenario: Inferred Integer column
- **WHEN** a database returns a column with type `BIGINT`
- **THEN** the system SHALL map this to Arrow `DataType::Int64`.

#### Scenario: Inferred Boolean column
- **WHEN** a database returns a column with type `BOOLEAN` or `TINYINT(1)`
- **THEN** the system SHALL map this to Arrow `DataType::Boolean`.

### Requirement: Type-Safe Array Building
The system SHALL use specialized Arrow ArrayBuilders for each inferred column type to ensure high-fidelity data ingress.

#### Scenario: Numeric data preservation
- **WHEN** a database returns a column with `FLOAT` or `DECIMAL` types
- **THEN** the system SHALL use `Float64Builder` to store the data, ensuring it is ready for mathematical aggregation in DataFusion.
