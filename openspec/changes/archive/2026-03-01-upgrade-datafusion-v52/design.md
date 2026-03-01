## Context

DataFusion has evolved significantly from version 45.0 to 52.1.0. The upgrade involves not just a version bump but major API changes in how physical plans are handled, how literals are represented, and how dependencies are structured. Our current implementation is tightly coupled with version 45.0 API signatures.

## Goals / Non-Goals

**Goals:**
- **Successful Compilation**: Resolve all dependency conflicts and API mismatches.
- **Zero Regression**: Ensure all existing Excel, SQL, and Python nodes function identically.
- **Dependency Alignment**: Match versions across the entire arrow-rs ecosystem.

**Non-Goals:**
- **New Feature Implementation**: This change is strictly a migration.
- **Performance Benchmarking**: While we expect improvements, measuring them is not the primary focus of this task.

## Decisions

### 1. Dependency Pinning
**Rationale**: To avoid version mismatches (e.g., `arrow-schema` 54 vs 57), we will pin all dependencies to the versions brought in by DataFusion 52.1.0.
- `arrow`: 57.0.0
- `pyo3`: 0.26.0 (required by arrow 57)
- `pyo3-arrow`: 0.10.0 (or latest compatible)

### 2. Standardize on `datafusion::arrow`
**Rationale**: Using `datafusion` re-exports ensures we are always using the exact Arrow type expected by the engine, eliminating the "found type X, expected type X" errors caused by different version hashes of the same crate.

### 3. Systematic Refactoring of `ExecutionPlan`
**Rationale**: `children()` now returns owned `Arc`s. We will update all custom nodes (`ExcelSinkExec`, `StateProbeExec`, etc.) to return clones of their inputs.

## Risks / Trade-offs

- **[Risk] Hidden Logic Changes in DataFusion** → **Mitigation**: Rely on our extensive test suite (`tests/excel_tests.rs`, `tests/agent_tests.rs`) to catch behavioral changes.
- **[Risk] `pyo3-arrow` API Changes** → **Mitigation**: Verify the Python script data flow explicitly.
