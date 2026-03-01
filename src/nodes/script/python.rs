use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::ffi::CString;

use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::physical_plan::execution_plan::{EmissionType, Boundedness};
use futures::Stream;
use futures::StreamExt;
use serde_json::Value as JsonValue;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3::ffi::c_str;
use pyo3_arrow::PyRecordBatch;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Logical node for Python script execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PythonLogical {
    pub script: String,
    pub input: LogicalPlan,
}

impl PartialOrd for PythonLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for PythonLogical {
    fn name(&self) -> &str {
        "PythonScript"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PythonScript(script_len={})", self.script.len())
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            script: self.script.clone(),
            input: inputs[0].clone(),
        })
    }
}

/// Physical execution plan for Python script execution.
#[derive(Debug)]
pub struct PythonExec {
    pub script: String,
    pub input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl PythonExec {
    pub fn new(script: String, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            script,
            input,
            cache,
        }
    }
}

impl DisplayAs for PythonExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PythonExec")
    }
}

impl ExecutionPlan for PythonExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PythonExec"
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(self.script.clone(), children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        
        // Compile the script with security restrictions
        let compiled_fn = Python::with_gil(|py| -> Result<Py<PyAny>> {
            let script = &self.script;
            
            // Define restricted import function
            let restricted_mod = PyModule::from_code(py, c_str!(r#"
import sys

def make_restricted_import(original_import):
    def restricted_import(name, globals=None, locals=None, fromlist=(), level=0):
        dangerous = [
            "os", "sys", "subprocess", "socket", "requests", "urllib", "http",
            "pickle", "marshal", "code", "builtins", "importlib", "pathlib"
        ]
        # Check if any dangerous module is being imported
        for d in dangerous:
            if name == d or name.startswith(d + "."):
                raise ImportError(f"Access to module '{name}' is restricted for security reasons")
        
        return original_import(name, globals, locals, fromlist, level)
    return restricted_import
"#), c_str!("restricted_import.py"), c_str!("restricted_import"))?;
            let make_restricted_import_fn = restricted_mod.getattr("make_restricted_import")?;
            
            // Set up the globals with our restricted __import__
            let builtins = PyModule::import(py, "builtins")?;
            let original_import = builtins.getattr("__import__")?;
            
            let restricted_import_fn = make_restricted_import_fn.call1((original_import.clone(),))?;
            
            let locals = PyDict::new(py);
            locals.set_item("__import__", restricted_import_fn)?;
            
            let full_script = format!(
                "import pyarrow as pa\n\
                 def transform(batch):\n\
                 {}\n\
                 ",
                script.lines().map(|l| format!("    {}", l)).collect::<Vec<_>>().join("\n")
            );
            
            let module = PyModule::from_code(py, &CString::new(full_script)?, c_str!("swanflow_transform.py"), c_str!("swanflow_transform"))?;
            
            // Update module's __builtins__ to use our restricted import
            let mod_dict = module.dict();
            mod_dict.set_item("__builtins__", locals)?;
            
            let fun = module.getattr("transform")
                .map_err(|_| anyhow!("Python script must define 'transform(batch)' function"))?.into();
            
            tracing::debug!("Python script compiled successfully ({} bytes)", script.len());
            Ok(fun)
        }).map_err(|e| datafusion::error::DataFusionError::Execution(format!(
            "Python script security/compilation error: {}", e
        )))?;

        Ok(Box::pin(PythonStream {
            input: input_stream,
            compiled_fn,
        }))
    }
}

struct PythonStream {
    input: SendableRecordBatchStream,
    compiled_fn: Py<PyAny>,
}

impl RecordBatchStream for PythonStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

impl Stream for PythonStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let compiled_fn = &self.compiled_fn;
                let result = Python::with_gil(|py| -> Result<RecordBatch> {
                    let py_batch = PyRecordBatch::from(batch);
                    let result_py_obj = compiled_fn.call1(py, (py_batch,))?;
                    
                    if result_py_obj.is_none(py) {
                        anyhow::bail!("Python transform function must return a PyRecordBatch, but returned None");
                    }
                    
                    let result_py_batch: PyRecordBatch = result_py_obj.extract(py)
                        .map_err(|e| anyhow!("Failed to extract PyRecordBatch from Python return value: {}. Ensure your script returns the result (e.g., 'return batch').", e))?;
                    Ok(result_py_batch.into())
                });

                match result {
                    Ok(out_batch) => Poll::Ready(Some(Ok(out_batch))),
                    Err(e) => Poll::Ready(Some(Err(datafusion::error::DataFusionError::Execution(e.to_string())))),
                }
            }
            other => other,
        }
    }
}

/// A node that executes a Python script for data transformation.
pub struct PythonNode;

#[async_trait]
impl NodeBehavior for PythonNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.script.python".to_string(),
            display_name: "Python Script".to_string(),
            description: "Transform data using a Python script with zero-copy".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Source Table".into(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Transformed Table".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "script": { "type": "string", "description": "Python code logic. Variable 'batch' is available." }
                },
                "required": ["script"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["script"]
            .as_str()
            .ok_or_else(|| anyhow!("Python script is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let script = config["script"].as_str().unwrap().to_string();
        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node: Arc::new(PythonLogical {
                script,
                input: inputs[0].clone(),
            }),
        })])
    }
}

/// A node that registers a Python-based UDF into DataFusion.
pub struct PythonUdfNode;

#[async_trait]
impl NodeBehavior for PythonUdfNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.python_udf".to_string(),
            display_name: "Register Python UDF".to_string(),
            description: "Register a custom Python function as a DataFusion UDF".to_string(),
            input_ports: vec![],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "code": { "type": "string" },
                    "input_types": { "type": "array", "items": { "type": "string" } },
                    "return_type": { "type": "string" }
                },
                "required": ["name", "code", "input_types", "return_type"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["name"]
            .as_str()
            .ok_or_else(|| anyhow!("UDF name is required"))?;
        config["code"]
            .as_str()
            .ok_or_else(|| anyhow!("UDF code is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(vec![])
    }
}
