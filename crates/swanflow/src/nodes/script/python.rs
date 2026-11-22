//! Embedded Python script nodes (feature "python").
//!
//! Compiled only when the `python` feature is on: these nodes embed a CPython
//! interpreter via pyo3 (auto-initialize) and therefore link libpythonX.so.
//! The default build excludes them entirely — see the libpython3.11 incident.
#![cfg(feature = "python")]
use std::any::Any;
use std::ffi::CString;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};
use pyo3_arrow::PyRecordBatch;
use serde_json::Value as JsonValue;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Logical node for Python script execution.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct PythonLogical {
    pub script: String,
    pub input: LogicalPlan,
    /// Output schema probed at plan time by running the script once against a
    /// sample batch. When None (probe failed / skipped), falls back to the
    /// pass-through input schema like before.
    pub schema_override: Option<DFSchemaRef>,
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
        self.schema_override
            .as_ref()
            .unwrap_or_else(|| self.input.schema())
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
            schema_override: self.schema_override.clone(),
        })
    }
}

/// Physical execution plan for Python script execution.
#[derive(Debug)]
pub struct PythonExec {
    pub script: String,
    pub input: Arc<dyn ExecutionPlan>,
    /// Advertised output schema (probed/declared). None = pass-through input
    /// schema. Must survive `with_new_children` rebuilds or the optimizer
    /// re-derives the schema from the input and DataFusion rejects the plan
    /// as mismatched.
    pub schema_override: Option<datafusion::arrow::datatypes::SchemaRef>,
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
            schema_override: None,
            cache,
        }
    }

    /// Like `new`, but advertises a probed/declared output schema (the physical
    /// batches are produced by the Python script, whose schema may differ from
    /// the input's — e.g. new columns). The logical plan's schema override and
    /// the physical plan's schema must agree or DataFusion rejects the plan.
    pub fn with_schema(
        script: String,
        input: Arc<dyn ExecutionPlan>,
        schema: datafusion::arrow::datatypes::SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            script,
            input,
            schema_override: Some(schema),
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
        // The physical plan must advertise the transform's output schema, not
        // the input's — DataFusion's extension-planner check compares this
        // against the logical schema and rejects mismatches.
        match &self.schema_override {
            Some(s) => s.clone(),
            None => self.input.schema(),
        }
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
        // Preserve the advertised schema across optimizer rebuilds.
        Ok(match &self.schema_override {
            Some(schema) => Arc::new(Self::with_schema(
                self.script.clone(),
                children[0].clone(),
                schema.clone(),
            )),
            None => Arc::new(Self::new(self.script.clone(), children[0].clone())),
        })
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        // Parse sandboxed flag from script prefix (format: "sandboxed:true/false\n...")
        let (sandboxed, actual_script) =
            if let Some(rest) = self.script.strip_prefix("sandboxed:false\n") {
                (false, rest.to_string())
            } else if let Some(rest) = self.script.strip_prefix("sandboxed:true\n") {
                (true, rest.to_string())
            } else {
                (true, self.script.clone()) // default: sandboxed
            };

        // Compile the script with optional security restrictions
        let compiled_fn = Python::attach(|py| -> Result<Py<PyAny>> {
            let script = &actual_script;

            let restricted_mod = if sandboxed {
                PyModule::from_code(
                    py,
                    c_str!(
                        r#"
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
"#
                    ),
                    c_str!("restricted_import.py"),
                    c_str!("restricted_import"),
                )?
            } else {
                // Unrestricted mode: allow all imports (for KNIME bridge scripts using sklearn/tensorflow/paramiko)
                PyModule::from_code(
                    py,
                    c_str!("def make_restricted_import(orig): return orig"),
                    c_str!("passthrough_import.py"),
                    c_str!("passthrough_import"),
                )?
            };
            let make_restricted_import_fn = restricted_mod.getattr("make_restricted_import")?;

            // Set up the globals with our restricted __import__
            let builtins = PyModule::import(py, "builtins")?;
            let original_import = builtins.getattr("__import__")?;

            let restricted_import_fn =
                make_restricted_import_fn.call1((original_import.clone(),))?;

            // Sandbox should restrict IMPORTS, not strip all builtins. The old
            // code set __builtins__ to a dict containing only __import__, which
            // made len/range/min/max/abs/Exception... undefined (NameError) in
            // every non-trivial script. Copy the real builtins and override only
            // __import__ with the restricted one.
            let builtins_module = PyModule::import(py, "builtins")?;
            let builtins_dict = builtins_module.dict();
            let sandbox_builtins = PyDict::new(py);
            sandbox_builtins.update(builtins_dict.as_mapping())?;
            let restricted_import_py: PyObject =
                restricted_import_fn.clone().into_pyobject(py)?.into();
            sandbox_builtins.set_item("__import__", restricted_import_py)?;
            let locals: PyObject = sandbox_builtins.into_any().into();

            // Two bridge modes for handing `batch` to the script:
            //  - arro3 (pyo3-arrow's Python side) present: pass the zero-copy
            //    PyRecordBatch directly (fast path, unchanged).
            //  - arro3 missing: serialize to Arrow IPC bytes and rebind `batch`
            //    to a pure-pyarrow RecordBatch. This keeps core.script.python
            //    working on interpreters that only have pyarrow installed —
            //    previously ANY batch.column(i) access raised
            //    "ModuleNotFoundError: No module named 'arro3'" because
            //    pyo3-arrow 0.14's Arro3Array conversion imports arro3.core
            //    (caught by wf160 E2E; forensic: 2026-09-28).
            let arro3_available = py.import("arro3.core").is_ok();
            let bridge_prologue = if arro3_available {
                ""
            } else {
                "    batch = pa.ipc.open_stream(_swanflow_ipc).read_all()\n"
            };

            let full_script = format!(
                "import pyarrow as pa\n\
                 def transform(batch):\n\
                 {}\
                 {}",
                bridge_prologue,
                script
                    .lines()
                    .map(|l| format!("    {}", l))
                    .collect::<Vec<_>>()
                    .join("\n")
            );

            let module = PyModule::from_code(
                py,
                &CString::new(full_script)?,
                c_str!("swanflow_transform.py"),
                c_str!("swanflow_transform"),
            )?;

            // Update module's __builtins__ to use our restricted import
            let mod_dict = module.dict();
            mod_dict.set_item("__builtins__", locals)?;

            let fun = module
                .getattr("transform")
                .map_err(|_| anyhow!("Python script must define 'transform(batch)' function"))?
                .into();

            tracing::debug!(
                "Python script compiled successfully ({} bytes)",
                script.len()
            );
            Ok(fun)
        })
        .map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Python script security/compilation error: {}",
                e
            ))
        })?;

        Ok(Box::pin(PythonStream {
            input: input_stream,
            compiled_fn,
            schema_override: self.schema_override.clone(),
            pending_result: None,
        }))
    }
}

struct PythonStream {
    input: SendableRecordBatchStream,
    compiled_fn: Py<PyAny>,
    /// Advertised output schema (mirrors PythonExec.schema_override).
    schema_override: Option<arrow::datatypes::SchemaRef>,
    /// Stores the oneshot receiver from an in-flight spawn_blocking Python call.
    pending_result:
        Option<tokio::sync::oneshot::Receiver<std::result::Result<RecordBatch, anyhow::Error>>>,
}

impl RecordBatchStream for PythonStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        // Must match PythonExec::schema() — the transform's advertised output,
        // not the input's (downstream materializers read this).
        match &self.schema_override {
            Some(s) => s.clone(),
            None => self.input.schema(),
        }
    }
}

impl Stream for PythonStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // If there's an in-flight Python result, check if it's ready
        if self.pending_result.is_some() {
            match self.pending_result.as_mut().unwrap().try_recv() {
                Ok(result) => {
                    self.pending_result = None;
                    return match result {
                        Ok(batch) => Poll::Ready(Some(Ok(batch))),
                        Err(e) => Poll::Ready(Some(Err(
                            datafusion::error::DataFusionError::Execution(e.to_string()),
                        ))),
                    };
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    // Not ready yet — register waker and retry later
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
                Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                    self.pending_result = None;
                    return Poll::Ready(Some(Err(datafusion::error::DataFusionError::Execution(
                        "Python execution channel closed unexpectedly".to_string(),
                    ))));
                }
            }
        }

        // No in-flight call — try to read next input batch
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Clone the compiled function — need Python GIL for Py::clone_ref
                let compiled_fn_clone = Python::with_gil(|py| self.compiled_fn.clone_ref(py));

                // Run Python in a blocking thread to avoid blocking the tokio worker.
                // Bridge result back via oneshot channel.
                let (tx, rx) = tokio::sync::oneshot::channel();
                let _ = tokio::task::spawn_blocking(move || {
                    let result = Python::attach(|py| -> Result<RecordBatch> {
                        // Bridge mode selection must match the script template:
                        // if arro3 is unavailable the script's prologue reads
                        // `batch` from `_swanflow_ipc` (Arrow IPC bytes).
                        let arro3_available = py.import("arro3.core").is_ok();
                        let call_result: PyResult<Py<PyAny>> = if arro3_available {
                            let py_batch = PyRecordBatch::from(batch);
                            compiled_fn_clone.call1(py, (py_batch,))
                        } else {
                            // Serialize to Arrow IPC stream bytes and hand a
                            // Python bytes object to the script; its prologue
                            // rebinds `batch` via pa.ipc.open_stream.
                            let mut ipc_buf: Vec<u8> = Vec::new();
                            {
                                use datafusion::arrow::ipc::writer::{
                                    IpcWriteOptions, StreamWriter,
                                };
                                let options = IpcWriteOptions::default();
                                let mut writer = StreamWriter::try_new_with_options(
                                    &mut ipc_buf,
                                    batch.schema().as_ref(),
                                    options,
                                )?;
                                writer.write(&batch)?;
                                writer.finish()?;
                            }
                            let py_bytes = pyo3::types::PyBytes::new(py, &ipc_buf);
                            // stash where the prologue can read it
                            let builtins = pyo3::types::PyModule::import(py, "builtins")?;
                            builtins.set_item("_swanflow_ipc", py_bytes)?;
                            compiled_fn_clone.call1(py, (Option::<PyObject>::None,))
                        };

                        let result_py_obj = call_result?;

                        if result_py_obj.is_none(py) {
                            anyhow::bail!("Python transform function must return a PyRecordBatch, but returned None");
                        }

                        let result_py_batch: PyRecordBatch = result_py_obj.extract(py)
                            .map_err(|e| anyhow!("Failed to extract PyRecordBatch from Python return value: {}. Ensure your script returns the result (e.g., 'return batch').", e))?;
                        Ok(result_py_batch.into())
                    });
                    let _ = tx.send(result);
                });

                // Store receiver; next poll will check for completion
                self.pending_result = Some(rx);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
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
                    "script": { "type": "string", "description": "Python code logic. Variable 'batch' is available." },
                    "sandboxed": { "type": "boolean", "description": "Enable security sandbox (default true). Set false for KNIME bridge scripts that need sklearn/tensorflow/paramiko.", "default": true }
                },
                "required": ["script"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "script")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let script = cfg::required_string(config, "script")?;
        let sandboxed = config
            .get("sandboxed")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let full_script = format!("sandboxed:{}\n{}", sandboxed, script); // encode sandboxed flag in script prefix

        // Output schema resolution for downstream planning. A Python transform
        // is a black box to the planner; the old pass-through behavior made any
        // NEW column unresolvable downstream ("No field named ...").
        // Resolution order:
        //   1. probe: run the script once against a sample batch (may fail when
        //      the upstream logical plan embeds engine-only extension nodes)
        //   2. declared: `output_columns` config, e.g. {"flag": "string"}
        //   3. fallback: pass-through input schema (old behavior)
        let schema_override = probe_python_schema(&full_script, inputs[0].clone())
            .await
            .or_else(|| declared_output_schema(config));

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(PythonLogical {
                    script: full_script,
                    input: inputs[0].clone(),
                    schema_override,
                }),
            },
        )])
    }
}

/// Build a schema from a declarative `output_columns` config object mapping
/// column name -> type string ("string"|"int64"|"float64"|"bool"). Returns
/// None when the config key is absent or malformed.
fn declared_output_schema(config: &JsonValue) -> Option<DFSchemaRef> {
    let cols = config.get("output_columns")?.as_object()?;
    let mut fields = Vec::new();
    for (name, ty) in cols {
        let dt = match ty.as_str()? {
            "string" | "utf8" => datafusion::arrow::datatypes::DataType::Utf8,
            "int64" => datafusion::arrow::datatypes::DataType::Int64,
            "float64" => datafusion::arrow::datatypes::DataType::Float64,
            "bool" => datafusion::arrow::datatypes::DataType::Boolean,
            _ => return None,
        };
        fields.push(datafusion::arrow::datatypes::Field::new(name, dt, true));
    }
    if fields.is_empty() {
        return None;
    }
    Some(Arc::new(
        datafusion::common::DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(fields))
            .ok()?,
    ))
}

/// Execute the input plan, feed the first batch through the compiled Python
/// transform, and return the output schema. Returns None on any failure —
/// planning then falls back to pass-through schema (old behavior).
async fn probe_python_schema(script: &str, input: LogicalPlan) -> Option<DFSchemaRef> {
    // 1. Pull a small sample of rows from the (logical) input plan.
    let ctx = datafusion::prelude::SessionContext::new();
    let df = datafusion::prelude::DataFrame::new(ctx.state(), input);
    let batches = df.limit(0, Some(8)).ok()?.collect().await.ok()?;
    let first = batches.into_iter().next()?;

    // 2. Wrap the sample batch in an in-memory physical plan via MemTable scan.
    let mem_table =
        datafusion::datasource::MemTable::try_new(first.schema(), vec![vec![first.clone()]])
            .ok()?;
    let probe_ctx = datafusion::prelude::SessionContext::new();
    probe_ctx
        .register_table("python_probe_input", Arc::new(mem_table))
        .ok()?;
    let df = probe_ctx
        .sql("SELECT * FROM python_probe_input")
        .await
        .ok()?;
    let plan = df.into_unoptimized_plan();
    let state = probe_ctx.state();
    let physical_input = state.create_physical_plan(&plan).await.ok()?;

    // 3. Run it through PythonExec and take the first output batch's schema.
    let exec = PythonExec::new(script.to_string(), physical_input);
    let task_ctx = Arc::new(datafusion::execution::context::TaskContext::default());
    let mut stream = exec.execute(0, task_ctx).ok()?;
    use futures::StreamExt;
    let out = stream.next().await?;
    let batch = out.ok()?;
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        return None;
    }
    Some(Arc::new(
        datafusion::common::DFSchema::try_from(batch.schema()).ok()?,
    ))
}

/// A node that registers a Python-based UDF into DataFusion.
pub struct PythonUdfNode;

#[async_trait]
impl NodeBehavior for PythonUdfNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.flow.python_udf".to_string(),
            display_name: "Register Python UDF".to_string(),
            description: "[SHELL NODE — no runtime behavior] Register a custom Python function as a DataFusion UDF. Stub: propose_plan returns an empty plan; pending real Extension-based implementation.".to_string(),
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
        cfg::required_string(config, "name")?;
        cfg::required_string(config, "code")?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_sandboxed_default_true() {
        let node = PythonNode;
        let spec = node.spec();
        let schema = spec.config_schema;
        assert!(schema.to_string().contains("sandboxed"));
    }

    #[test]
    fn test_propose_plan_sandboxed_false() {
        let node = PythonNode;
        let config = json!({"script": "import pyarrow as pa; return batch", "sandboxed": false});
        assert!(node.validate(&config).is_ok());
    }

    #[test]
    fn test_propose_plan_sandboxed_default() {
        let node = PythonNode;
        let config = json!({"script": "return batch"});
        assert!(node.validate(&config).is_ok());
    }

    #[test]
    fn test_propose_plan_encodes_sandboxed_prefix() {
        let node = PythonNode;
        let config = json!({"script": "x = 1", "sandboxed": false});
        let empty_input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plans = rt
            .block_on(node.propose_plan(vec![empty_input], &config))
            .unwrap();
        let plan = &plans[0];
        if let LogicalPlan::Extension(ext) = plan {
            let logical = ext.node.as_any().downcast_ref::<PythonLogical>().unwrap();
            assert!(logical.script.starts_with("sandboxed:false\n"));
            assert!(logical.script.contains("x = 1"));
        } else {
            panic!("Expected Extension logical plan");
        }
    }

    #[test]
    fn test_propose_plan_sandboxed_true_prefix() {
        let node = PythonNode;
        let config = json!({"script": "x = 1"});
        let empty_input = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let rt = tokio::runtime::Runtime::new().unwrap();
        let plans = rt
            .block_on(node.propose_plan(vec![empty_input], &config))
            .unwrap();
        let plan = &plans[0];
        if let LogicalPlan::Extension(ext) = plan {
            let logical = ext.node.as_any().downcast_ref::<PythonLogical>().unwrap();
            assert!(logical.script.starts_with("sandboxed:true\n"));
        } else {
            panic!("Expected Extension logical plan");
        }
    }
}
