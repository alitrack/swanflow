pub mod adapter;
pub mod agent;
pub mod core;
pub mod hotl;
pub mod mcp;
pub mod nodes;

// Python wheel bindings — only compiled with feature "python", keeping the
// default build free of any libpython linkage.
#[cfg(feature = "python")]
pub mod py;

#[cfg(feature = "python")]
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// PyO3 module entry point — the Python package is named "swanflow"
// (wheel builds must pass --features python)
// ---------------------------------------------------------------------------

/// SwanFlow: High-performance Rust-native Data Science Engine.
///
/// Usage:
///   import swanflow
///   wf = swanflow.Workflow.from_file("my_workflow.yaml")
///   engine = swanflow.SwanFlowEngine()
///   events = engine.run(wf)
#[cfg(feature = "python")]
#[pymodule]
fn swanflow(module: &Bound<'_, PyModule>) -> PyResult<()> {
    py::register(module)
}
