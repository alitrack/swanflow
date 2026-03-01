## Context

The current `rustflow` IO module consists of specialized nodes for each file format. This design introduces a `UniversalReadNode` that serves as a single entry point for various file types, similar to the `qv` tool in the DataFusion ecosystem. It also enables directory scanning, allowing a folder of files to be treated as a unified table.

## Goals / Non-Goals

**Goals:**
- **Automatic Format Detection**: Determine if a file is Parquet, CSV, JSON, etc., without user configuration.
- **Directory-as-a-Table**: Support reading all compatible files in a folder using `ListingTable`.
- **Partition Support**: Automatically discover and expose Hive-style partition columns.
- **Backward Compatibility**: Existing specialized nodes will remain, but `UniversalReadNode` will be the preferred default.

**Non-Goals:**
- **In-place Editing**: This is a source-only node.
- **Complex Schema Mapping**: We will rely on DataFusion's automatic schema merging.

## Decisions

### 1. Extension-based Initial Detection
**Rationale**: For speed and reliability, we will first check the file extension. If ambiguous, we will sample the first few bytes (magic numbers) to distinguish between formats like Parquet and Avro.

### 2. Leverage DataFusion's `ListingOptions`
**Rationale**: Instead of manual file iteration, we will use DataFusion's `ListingTable` API. This provides out-of-the-box support for:
- Glob patterns.
- Partition discovery.
- Parallel file scanning.

### 3. Unified Configuration Schema
**Rationale**: The node will accept a generic `options` object to pass format-specific parameters (e.g., `delimiter` for CSV) to the underlying DataFusion reader.

## Risks / Trade-offs

- **[Risk] Schema Incompatibility** → **Mitigation**: In directory mode, we will validate that all files share a compatible schema or return a clear error indicating the conflicting files.
- **[Risk] Detection False Positives** → **Mitigation**: Allow users to explicitly override the format in the configuration if automatic detection fails.
