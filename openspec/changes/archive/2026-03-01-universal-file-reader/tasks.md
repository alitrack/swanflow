## 1. Skeleton & Registration

- [x] 1.1 Define `UniversalReadNode` struct in `src/nodes/io.rs`.
- [x] 1.2 Register `core.io.universal_read` in `src/nodes/registry.rs`.
- [x] 1.3 Implement the basic configuration schema validation.

## 2. Format Detection & Basic Reading

- [x] 2.1 Implement a helper function to detect format from file extension and magic numbers.
- [x] 2.2 Implement `infer_schema` for single files (routing to specific readers).
- [x] 2.3 Implement `propose_plan` for single files (routing to `LogicalPlan` nodes).

## 3. Directory Scanning & Partitioning

- [x] 3.1 Integrate DataFusion's `ListingOptions` and `ListingTable` in `propose_plan`.
- [x] 3.2 Implement Hive-style partition discovery and column exposure.
- [x] 3.3 Add support for recursive scanning and hidden file filtering.

## 4. Verification

- [x] 4.1 Create a new integration test `tests/universal_read_tests.rs`.
- [x] 4.2 Verify automatic detection for Parquet, CSV, and JSON.
- [x] 4.3 Verify directory scanning with mixed filenames but same format.
- [x] 4.4 Verify Hive partition column extraction in a test scenario.
