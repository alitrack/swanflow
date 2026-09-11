pub mod adapter;
pub mod agent;
pub mod core;
pub mod hotl;
pub mod mcp;
pub mod nodes;

use std::sync::Arc;

use pyo3::exceptions::{PyFileNotFoundError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::Workflow;
use crate::nodes::registry::NodeRegistry;

// ---------------------------------------------------------------------------
// PySwanFlowEngine — Python-facing wrapper
// ---------------------------------------------------------------------------

/// SwanFlow engine: load, inspect, and execute data workflows.
#[pyclass(name = "SwanFlowEngine")]
pub struct PySwanFlowEngine {
    engine: SwanFlowEngine,
    rt: tokio::runtime::Runtime,
}

#[pymethods]
impl PySwanFlowEngine {
    #[new]
    pub fn new() -> PyResult<Self> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {e}")))?;
        let registry = Arc::new(NodeRegistry::default());
        let engine = SwanFlowEngine::new(registry);
        Ok(Self { engine, rt })
    }

    /// Execute a Workflow and return a list of event JSON strings.
    #[pyo3(signature = (workflow))]
    pub fn run(&self, workflow: &PyWorkflow) -> PyResult<Vec<String>> {
        let wf = workflow.inner.clone();
        let e = &self.engine;
        self.rt.block_on(async {
            let mut rx = e
                .execute_workflow(&wf)
                .await
                .map_err(|dfe| PyRuntimeError::new_err(format!("Execution failed: {dfe}")))?;
            let mut results = Vec::new();
            while let Some(event) = rx.recv().await {
                let json_str = serde_json::to_string(&event)
                    .map_err(|se| PyRuntimeError::new_err(format!("Serialization error: {se}")))?;
                results.push(json_str);
            }
            Ok(results)
        })
    }

    /// Infer metadata (schemas, diagnostics) from a Workflow without executing it.
    #[pyo3(signature = (workflow))]
    pub fn inspect(&self, workflow: &PyWorkflow) -> PyResult<String> {
        let wf = workflow.inner.clone();
        let e = &self.engine;
        self.rt.block_on(async {
            let metadata = e
                .infer_metadata(&wf)
                .await
                .map_err(|dfe| PyRuntimeError::new_err(format!("Inspection failed: {dfe}")))?;
            let schemas_json: Vec<String> = metadata
                .schemas
                .iter()
                .map(|(uid, fields)| {
                    format!(
                        r#""{}":{{"schemas":[{}],"diagnostics":[]}}"#,
                        uid,
                        fields
                            .iter()
                            .map(|s| {
                                format!(
                                    r#""{}""#,
                                    s.fields()
                                        .iter()
                                        .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                                        .collect::<Vec<_>>()
                                        .join(",")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>();
            // Return a simplified JSON representation
            Ok(format!(
                r#"{{"schemas":[{}],"nodes":{}}}"#,
                schemas_json.join(","),
                metadata.diagnostics.len()
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// PyWorkflow — deserialize a workflow from YAML / JSON string or file
// ---------------------------------------------------------------------------

/// A SwanFlow workflow graph, loaded from a YAML or JSON definition.
#[pyclass(name = "Workflow")]
pub struct PyWorkflow {
    inner: Workflow,
}

#[pymethods]
impl PyWorkflow {
    /// Load a Workflow from a YAML or JSON file path.
    #[staticmethod]
    pub fn from_file(path: &str) -> PyResult<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| PyFileNotFoundError::new_err(format!("Cannot read file '{path}': {e}")))?;
        Self::from_str(&content)
    }

    /// Load a Workflow from a YAML or JSON string.
    /// The string is auto-detected (JSON if it starts with '{', otherwise YAML).
    #[staticmethod]
    pub fn from_str(content: &str) -> PyResult<Self> {
        let trimmed = content.trim();
        let workflow: Workflow = if trimmed.starts_with('{') {
            serde_json::from_str(trimmed)
                .map_err(|e| PyValueError::new_err(format!("Invalid JSON workflow: {e}")))?
        } else {
            serde_yaml::from_str(trimmed)
                .map_err(|e| PyValueError::new_err(format!("Invalid YAML workflow: {e}")))?
        };
        Ok(PyWorkflow { inner: workflow })
    }

    /// Return a summary dict: name, id, node_count, edge_count.
    pub fn summary(&self) -> PyResult<Py<PyDict>> {
        Python::with_gil(|py| {
            let d = PyDict::new(py);
            d.set_item("name", &self.inner.name)?;
            d.set_item("id", self.inner.id.to_string())?;
            d.set_item("node_count", self.inner.graph.node_count())?;
            d.set_item("edge_count", self.inner.graph.edge_count())?;
            Ok(d.unbind())
        })
    }
}

// ---------------------------------------------------------------------------
// Convenience free functions
// ---------------------------------------------------------------------------

/// Load a Workflow from a YAML/JSON file and execute it.
/// Returns a list of event JSON strings.
#[pyfunction]
pub fn run_workflow(file_path: &str) -> PyResult<Vec<String>> {
    let workflow = PyWorkflow::from_file(file_path)?;
    let engine = PySwanFlowEngine::new()?;
    engine.run(&workflow)
}

/// Load a Workflow from a YAML/JSON file and return its inferred metadata as JSON.
#[pyfunction]
pub fn inspect_workflow(file_path: &str) -> PyResult<String> {
    let workflow = PyWorkflow::from_file(file_path)?;
    let engine = PySwanFlowEngine::new()?;
    engine.inspect(&workflow)
}

/// List all registered node type specifications as JSON strings.
#[pyfunction]
pub fn list_node_types() -> PyResult<Vec<String>> {
    let registry = NodeRegistry::default();
    let specs = registry.list_specs();
    specs
        .into_iter()
        .map(|spec| {
            serde_json::to_string(&spec)
                .map_err(|e| PyRuntimeError::new_err(format!("Serialization error: {e}")))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// PyO3 module entry point — the Python package is named "swanflow"
// ---------------------------------------------------------------------------

/// SwanFlow: High-performance Rust-native Data Science Engine.
///
/// Usage:
///   import swanflow
///   wf = swanflow.Workflow.from_file("my_workflow.yaml")
///   engine = swanflow.SwanFlowEngine()
///   events = engine.run(wf)
#[pymodule]
fn swanflow(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PySwanFlowEngine>()?;
    module.add_class::<PyWorkflow>()?;
    module.add_function(wrap_pyfunction!(run_workflow, module)?)?;
    module.add_function(wrap_pyfunction!(inspect_workflow, module)?)?;
    module.add_function(wrap_pyfunction!(list_node_types, module)?)?;
    Ok(())
}
