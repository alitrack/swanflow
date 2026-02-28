## Context
SwanFlow's `PythonNode` currently acts as a pass-through during logical planning but lacks a corresponding physical operator that executes Python code on the actual data stream. To maintain SwanFlow's high-performance promise, we need a physical bridge that allows data to flow from Rust to Python and back without the massive overhead of serialization (e.g., JSON or CSV strings).

## Goals / Non-Goals

**Goals:**
- Implement `PythonExec` as a DataFusion physical operator.
- Leverage `pyo3-arrow` for zero-copy exchange of `RecordBatch` objects.
- Support standard Python data science libraries (Pandas, Scikit-learn) as part of the node's `script` configuration.
- Ensure thread safety when calling into the Python GIL (Global Interpreter Lock) from the asynchronous DataFusion stream.

**Non-Goals:**
- Real-time management of Python virtual environments (Assumes a pre-configured environment).
- Support for distributed Python execution (Single-machine focus for now).

## Decisions

### 1. Choice of Bridge: `pyo3-arrow` (FFI)
**Decision**: Use `pyo3-arrow` to convert between Rust `arrow::RecordBatch` and Python `pyarrow.RecordBatch`.
**Rationale**: This uses the Arrow C Data Interface (FFI), which is the industry standard for zero-copy exchange. It avoids copying memory and is significantly faster than any serialization-based approach.

### 2. Physical Operator Integration: `ExtensionPlanner`
**Decision**: Register a custom `ExtensionPlanner` in `SwanFlowPlanner` to translate logical `PythonLogical` nodes into physical `PythonExec` nodes.
**Rationale**: This follows the established pattern in SwanFlow for `ExcelScan` and `StateProbe` nodes, allowing seamless integration into the DataFusion optimizer and execution engine.

### 3. Execution Model: GIL-aware Blocking
**Decision**: Use `Python::with_gil` inside the `execute` method of `PythonExec`, and potentially `spawn_blocking` if the Python script is expected to run for a long time.
**Rationale**: Python's GIL is a bottleneck. To prevent blocking the main DataFusion async workers, heavy Python computation should be isolated where possible, or carefully managed within the GIL scope.

## Risks / Trade-offs
- **[Risk] Memory Management** → Mitigation: Ensure Python objects are explicitly dropped or managed correctly via PyO3 to prevent leaks during long-running streams.
- **[Risk] Version Mismatch** → Mitigation: Explicitly lock `pyo3-arrow` and `arrow` versions in `Cargo.toml` to prevent FFI breakage.
- **[Risk] GIL Contention** → Mitigation: For now, we accept serialized GIL access. High-concurrency Python execution is a future optimization goal.
