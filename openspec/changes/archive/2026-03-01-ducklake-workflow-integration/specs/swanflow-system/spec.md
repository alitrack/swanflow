## MODIFIED Requirements

### Requirement: Execution Plan Node Interface
The engine SHALL initialize a DuckLake catalog provider upon startup and make it available to all `LogicalPlan` translations.

#### Scenario: Catalog injection
- **WHEN** the `SwanFlowEngine` is created
- **THEN** it SHALL instantiate a `DuckdbMetadataProvider` pointing to the workspace cache directory.

### Requirement: Stream-based Professional Writing
The `Materializer` physical plan SHALL use the DuckLake write path to stream `RecordBatch` data to Parquet while updating metadata in DuckDB.

#### Scenario: Stream to Lakehouse
- **WHEN** materializing a large dataset
- **THEN** the system SHALL write Parquet files incrementally and only commit the snapshot upon full completion.
