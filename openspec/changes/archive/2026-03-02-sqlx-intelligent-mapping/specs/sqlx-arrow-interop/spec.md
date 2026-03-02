## ADDED Requirements

### Requirement: Protocol-Aware Type Normalization
The system SHALL normalize database-specific types from different SQLx drivers (Postgres, MySQL, SQLite) into a unified set of Arrow types.

#### Scenario: Unified Timestamp handling
- **WHEN** reading from MySQL `DATETIME` or Postgres `TIMESTAMP`
- **THEN** the system SHALL map both to Arrow `DataType::Timestamp(Nanosecond, None)`.

### Requirement: Robust Type Fallback
The system SHALL provide a robust fallback to UTF-8 strings for any database types that cannot be safely mapped or built using specialized builders.

#### Scenario: Fallback for JSON types
- **WHEN** a database returns a native `JSON` or `BLOB` column
- **THEN** the system SHALL attempt to stringify the content and store it in a `StringBuilder`.
