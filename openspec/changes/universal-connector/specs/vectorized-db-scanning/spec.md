## ADDED Requirements

### Requirement: DuckDB Postgres Scanner Integration
The `DbConnectorNode` SHALL use the DuckDB `postgres_scanner` extension when the connection URL starts with `postgres://` or `postgresql://`.

#### Scenario: High-speed Postgres read
- **WHEN** a user provides a `postgres://user:pass@host:5432/db` URL
- **THEN** the system SHALL attempt to use DuckDB to scan the database directly into Arrow RecordBatches.

### Requirement: DuckDB SQLite Scanner Integration
The `DbConnectorNode` SHALL use the DuckDB `sqlite_scanner` extension when the connection URL starts with `sqlite://`.

#### Scenario: SQLite performance path
- **WHEN** a user provides a `sqlite:///path/to/db.sqlite` URL
- **THEN** the system SHALL use DuckDB to pull data from the SQLite file for vectorized processing.
