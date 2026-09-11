## Why
To enable high-performance, complex data science workflows within SwanFlow by optimizing the Python interoperability layer. Standard Python execution often suffers from serialization overhead; this change implements a true "Zero-Copy 2.0" bridge using Arrow's C Data Interface, allowing DataFusion physical streams to pass batches to Python libraries (like Scikit-learn or Pandas) with minimal latency.

## What Changes
- Implement `PythonExec` physical operator: Move Python execution from a logical placeholder to a high-performance physical stream operator.
- Implement **Zero-Copy Batch Exchange**: Use `pyo3-arrow` and Arrow's FFI to pass `RecordBatch` to Python without serialization.
- Support **Dynamic Script Injection**: Allow the Analyst Agent to inject sophisticated Python logic into the physical execution pipeline.
- Implement **Environment Isolation stopgap**: Ensure Python dependencies are managed or checked during the compilation pass.

## Capabilities

### New Capabilities
- `physical-python-execution`: Native integration of Python code within the DataFusion physical execution plan.
- `zero-copy-ffi-bridge`: High-speed data exchange between Rust/Arrow and Python/PyArrow using the C Data Interface.

### Modified Capabilities
- `python-scripting`: Updated from a logical-only placeholder to a fully functional physical operator.

## Impact
- **src/nodes/script/python.rs**: Major update to implement physical operators.
- **src/core/engine.rs**: Planner updated to recognize and plan `PythonNode`.
- **Dependencies**: Solidify `pyo3` and `pyo3-arrow` integration.
- **Agent**: Expanded capabilities for advanced data science (ML, specialized cleaning).
