# Universal File Reader (qv-style)

## 1. Overview
Inspired by the `qv` tool, this design introduces a single, intelligent input node that automatically detects file formats and handles both individual files and directory-based data lakes (using DataFusion's `ListingTable`). This simplifies workflow construction by removing the need for format-specific nodes (Parquet, CSV, JSON, etc.).

## 2. Component Architecture

### 2.1 UniversalReadNode (NodeBehavior)
A polymorphic source node that adapts its execution strategy based on the input path.
- **Auto-Detection**: Scans the file extension or the first few bytes to identify the format.
- **ListingTable Integration**: Leverages `ListingOptions` to treat a folder of files as a single logical table.
- **Supported Formats**: Parquet, CSV, JSON, NDJSON, Avro, Arrow (IPC).

## 3. Execution Logic

### 3.1 Format Detection Strategy
1. If `format` is explicitly provided in config, use it.
2. Otherwise, check the file extension.
3. If the path is a directory, find the first non-hidden, non-system file to determine the format for the entire group.

### 3.2 Directory Scanning (Hybrid Mode)
- **Implementation**: Uses `ctx.read_table` with `ListingTable`.
- **Partitioning**: Automatically detects hive-style partitioning (e.g., `/year=2024/month=03/`) and exposes them as columns.
- **File Filtering**: Ignores hidden files (starting with `.`) and system metadata files (starting with `_`).

## 4. Configuration Schema
- `path` (String, Required): Path to file or directory.
- `format` (String, Default: "auto"): Force a specific format.
- `has_header` (Boolean, Default: true): For CSV.
- `delimiter` (String, Default: ","): For CSV.
- `multi_line` (Boolean, Default: false): For JSON.

## 5. Error Handling
- **Mixed Formats**: If a directory contains inconsistent formats, the node will issue a warning and skip incompatible files.
- **Schema Drift**: If files in a directory have incompatible schemas, a `NodeEvent::Error` is broadcast with details on the conflicting files.

## 6. Implementation Steps
1. Define `UniversalReadNode` in `src/nodes/io.rs`.
2. Implement format detection helper logic.
3. Integrate with `datafusion::datasource::listing`.
4. Register the node in `src/nodes/registry.rs`.
5. Add verification tests for various formats and directory scanning.
