## Why
Excel is the most common data format in data science. To outperform KNIME and Pandas, SwanFlow needs a high-performance, fault-tolerant, and interactive Excel handling system. This pack introduces industrial-grade reading/writing with real-time data preview.

## What Changes
- Implement `ExcelReadNode` powered by **Calamine** with a Dual-Output design (Success/Rejects).
- Implement `ExcelWriteNode` powered by **rust_xlsxwriter** supporting dynamic multi-sheet writing.
- Introduce **Instant Preview** capability: nodes can now return a sample of data (RecordBatches) during the schema inference phase.
- Implement a **Type Promotion** logic for Excel ingestion to handle mixed-type columns gracefully.

## Capabilities

### New Capabilities
- `excel-ingestion`: High-speed reading with DuckDB-style reject port for dirty data.
- `excel-export`: Constant-memory multi-sheet writing.
- `streaming-preview`: Ability to "peek" into data streams before or during full execution.

### Modified Capabilities
- `node-protocol`: Updated `infer_schema` to optionally return `sample_data`.
- `schema-inference`: Enhanced to include type-promotion logic for flexible ingestion.

## Impact
- **Dependencies**: Added `calamine` and `rust_xlsxwriter`.
- **API**: New endpoints for fetching preview data.
- **Reliability**: Pipelines no longer crash on single-cell type mismatches in Excel.
