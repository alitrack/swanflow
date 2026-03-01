## Why

Workflow construction in `rustflow` currently requires users to select specific nodes for each file format (CSV, Parquet, etc.). This adds friction, especially when dealing with data lakes where formats might be mixed or directories contain multiple files. Inspired by the `qv` tool, the `UniversalReadNode` provides a single, intelligent entry point that automatically detects formats and handles directory-based scanning, greatly simplifying the user experience and aligning with modern data engineering patterns.

## What Changes

- **Intelligent Input Node**: A new `UniversalReadNode` that automatically detects file formats (Parquet, CSV, JSON, Avro, Arrow) based on extensions or content.
- **Directory Scanning**: Support for reading entire directories as a single logical table using DataFusion's `ListingTable`.
- **Partition Discovery**: Automatic detection of Hive-style partitioning in directory structures.
- **Unified Configuration**: A simplified schema that accepts a `path` and optional format overrides.

## Capabilities

### New Capabilities
- `polymorphic-file-reading`: Automatically adapt to multiple file formats within a single node.
- `directory-data-lake-scanning`: Treat folders of files as unified DataFusion tables.
- `hive-partition-discovery`: Expose directory-based partitions as columns in the resulting schema.

### Modified Capabilities
- `swanflow-system`: The core node registry will include this polymorphic reader, potentially replacing format-specific nodes in high-level tutorial examples.

## Impact

- **Affected Code**: `src/nodes/io.rs`, `src/nodes/registry.rs`.
- **APIs**: New configuration schema for `core.io.universal_read`.
- **User Experience**: Users can now use a single node for almost all file-based inputs.
