## Context
Excel is a notoriously "dirty" data format where types are often inconsistent. Traditional tools either crash or force a full-string conversion. We need a system that handles these inconsistencies gracefully while leveraging Rust's high-speed library ecosystem.

## Goals / Non-Goals

**Goals:**
- Implement `ExcelReadNode` with **Dual-Output**: Port 0 for success, Port 1 for rejected rows (DuckDB-style).
- Implement `ExcelWriteNode` with constant memory usage for large files.
- Enable **Sampling Preview** in the compiler: Fetching the first N rows without full execution.
- Support automatic **Type Promotion**: Safely merging `Int` -> `Float` -> `String`.

**Non-Goals:**
- Excel macro execution (VBA).
- Complex cell styling (Focus is on data content).

## Decisions

### 1. Library Selection
**Decision**: `calamine` for reading, `rust_xlsxwriter` for writing.
**Rationale**: `calamine` is the fastest reader in the Rust ecosystem. `rust_xlsxwriter` allows writing millions of rows without loading them all into RAM, aligning with our performance goals.

### 2. The "Rejects" Mechanism
**Decision**: Use a secondary output port for rows that fail type validation.
**Rationale**: This mimics DuckDB's industrial ETL approach. It prevents a single "bad" cell from killing a massive pipeline while providing actionable error data.

### 3. Sampling Logic for Preview
**Decision**: In `infer_schema`, we will add an optional `sample_limit` parameter. If set, the node will read only the first N rows and return them as a `RecordBatch`.
**Rationale**: This provides immediate visual feedback in the UI without requiring a full DAG execution.

## Risks / Trade-offs
- **[Risk] Type Ambiguity** → Mitigation: Use a "Type Promotion" table (e.g. if column has both `Int` and `Float`, use `Float`).
- **[Risk] Large File Preview** → Mitigation: Calamine's lazy iterator allows reading only the headers and the first N rows very efficiently.
