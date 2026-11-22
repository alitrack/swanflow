pub mod adapter;
pub mod agent;
pub mod core;
pub mod deploy;
pub mod docs_portal;
pub mod hotl;
pub mod mcp;
pub mod nodes;

/// Load a workflow from a YAML/JSON file (shared by the CLI and the
/// daemon's automation sensor registration).
pub fn load_workflow_file(path: &std::path::Path) -> anyhow::Result<core::graph::Workflow> {
    let extension = path
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("File must have .yaml or .json extension"))?;
    let content = std::fs::read_to_string(path)?;
    if extension == "yaml" || extension == "yml" {
        Ok(serde_yaml::from_str(&content)?)
    } else {
        Ok(serde_json::from_str(&content)?)
    }
}

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
