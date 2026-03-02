## MODIFIED Requirements

### Requirement: Enhanced JSON Discovery
The `UniversalReadNode` SHALL use DuckDB's `read_json_auto` for all JSON and NDJSON files, enabling advanced type inference for deeply nested or inconsistent structures.

#### Scenario: Read nested JSON
- **WHEN** a JSON file contains nested objects or arrays
- **THEN** DuckDB SHALL automatically flatten or map them to Arrow Struct/List types.

### Requirement: DuckDB-backed Directory Globbing
The system SHALL use DuckDB's `glob` function to identify files in a directory when complex patterns (e.g., `**/*.parquet`) are used.

#### Scenario: Recursive scan via DuckDB
- **WHEN** a path contains `**/*.csv`
- **THEN** DuckDB SHALL resolve all matching files and treat them as a single logical table.
