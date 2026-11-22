//! Execution seat for plugin-package row transforms.
//!
//! The gap this closes: [`WasmPluginHost`] can run a plugin function over Arrow
//! IPC bytes and it enforces the sandbox (epoch timeout, memory cap), but no
//! engine node could reach it. `NodeBehavior::propose_plan` returns a
//! `LogicalPlan`; a plugin is described only as opaque bytes in / bytes out —
//! the two never met. "WASM plugins are supported" was therefore true of the
//! host and false of anything a workflow could schedule.
//!
//! The joining piece is a `UserDefinedLogicalNode`: its physical plan reads
//! batches from its input, hands each one to the host, and emits what comes
//! back. Everything the host guarantees then applies to *scheduled* runs, not
//! just to hand-driven tests.
//!
//! ## Schema, stated honestly
//!
//! The host never parses payloads (that is what lets a plugin be written in any
//! language), so it cannot report the output schema. Two modes:
//!
//! * `same_as_input` (default) — a row transform that keeps its columns: sort,
//!   filter, sample. Verified at run time: a batch that comes back with a
//!   different schema is an error, never a silent mismatch.
//! * an explicit `output_schema` list of `{name, type}` — for plugins that add
//!   or drop columns. Declaring it is the caller's claim; the same run-time
//!   check holds them to it.

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;
use serde_json::Value as JsonValue;
use tokio::sync::OnceCell;

use crate::core::kernel::plugin::{PluginHandle, PluginHost, PluginType};
use crate::core::kernel::plugin_wasm::WasmPluginHost;
use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// type_id of the node this module registers.
pub const TYPE_ID: &str = "core.plugin.wasm_transform";

// ── Logical plan node ──

/// Logical stand-in for "run this plugin function over these batches".
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PluginTransformLogical {
    pub plugin_dir: String,
    pub plugin: String,
    pub function: String,
    /// Schema the plugin is declared to produce. Carried on the node so the
    /// planner, `EXPLAIN` and the run-time check all see one answer.
    pub schema: DFSchemaRef,
    pub input: LogicalPlan,
}

impl PartialOrd for PluginTransformLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.plugin.as_str(), self.function.as_str())
            .partial_cmp(&(other.plugin.as_str(), other.function.as_str()))
    }
}

impl UserDefinedLogicalNodeCore for PluginTransformLogical {
    fn name(&self) -> &str {
        "PluginTransform"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "PluginTransform: plugin={}, function={}",
            self.plugin, self.function
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            plugin_dir: self.plugin_dir.clone(),
            plugin: self.plugin.clone(),
            function: self.function.clone(),
            schema: self.schema.clone(),
            input: inputs[0].clone(),
        })
    }
}

// ── Physical plan ──

/// One sandboxed host, shared by every batch and partition of a run.
///
/// [`crate::nodes::wasm`] compiles a fresh `Engine` + `Module` per batch. Fine
/// for a scratch UDF, wrong for a scheduled row transform: compilation would
/// dominate a wide scan. Here the host is built once and kept in a `OnceCell`,
/// so N partitions share one compiled module and one epoch clock.
pub struct PluginTransformExec {
    pub plugin_dir: String,
    pub plugin: String,
    pub function: String,
    pub schema: SchemaRef,
    pub input: Arc<dyn ExecutionPlan>,
    host: Arc<OnceCell<Arc<WasmPluginHost>>>,
    cache: PlanProperties,
}

impl fmt::Debug for PluginTransformExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginTransformExec")
            .field("plugin", &self.plugin)
            .field("function", &self.function)
            .finish()
    }
}

impl PluginTransformExec {
    pub fn new(
        plugin_dir: String,
        plugin: String,
        function: String,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            plugin_dir,
            plugin,
            function,
            schema,
            input,
            host: Arc::new(OnceCell::new()),
            cache,
        }
    }
}

impl DisplayAs for PluginTransformExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PluginTransformExec({})", self.function)
    }
}

impl ExecutionPlan for PluginTransformExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "PluginTransformExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        Ok(Arc::new(Self::new(
            self.plugin_dir.clone(),
            self.plugin.clone(),
            self.function.clone(),
            self.schema.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let run = Arc::new(WasmRun {
            plugin_dir: self.plugin_dir.clone(),
            plugin: self.plugin.clone(),
            function: self.function.clone(),
            expected: self.schema.clone(),
        });
        let declared = self.schema.clone();

        // `unfold` lets each step await the async host call, which a hand-rolled
        // `poll_next` cannot do without re-implementing a waker.
        let stream = futures::stream::unfold(
            (input, self.host.clone(), run),
            |(mut input, host, run)| async move {
                let batch = match input.next().await? {
                    Ok(b) => b,
                    Err(e) => return Some((Err(e), (input, host, run))),
                };
                let out = run.apply(&host, batch).await;
                Some((out, (input, host, run)))
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(declared, stream)))
    }
}

/// One batch round-trip. Separate from the Exec so it can move into the stream
/// without borrowing `self` for the stream's lifetime.
struct WasmRun {
    plugin_dir: String,
    plugin: String,
    function: String,
    expected: SchemaRef,
}

impl WasmRun {
    async fn apply(
        &self,
        cell: &Arc<OnceCell<Arc<WasmPluginHost>>>,
        batch: RecordBatch,
    ) -> DFResult<RecordBatch> {
        let host = init_host(cell, &self.plugin_dir, &self.plugin).await?;
        let handle = PluginHandle {
            name: self.plugin.clone(),
            plugin_type: PluginType::Wasm,
        };
        let input = to_ipc(&batch)?;
        let out = host
            .run(&handle, &self.function, &input)
            .await
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "plugin '{}' function '{}' failed on a {}x{} batch: {}",
                    self.plugin,
                    self.function,
                    batch.num_rows(),
                    batch.num_columns(),
                    e
                ))
            })?;
        let out = from_ipc(&out)?;

        // The host treats payloads as opaque, so this is the only place the
        // contract can be checked. A silent schema drift would corrupt every
        // downstream node while looking like success.
        let out_schema = out.schema();
        let want = self.expected.fields();
        let got = out_schema.fields();
        if want != got {
            return Err(DataFusionError::Execution(format!(
                "plugin '{}' returned schema [{}] but the node declares [{}] — \
                 set the node's `output_schema` if the transform changes columns",
                self.plugin,
                short_schema(&out.schema()),
                short_schema(&self.expected)
            )));
        }
        Ok(out)
    }
}

/// Scan + init exactly once per execution, then hand out the same host.
async fn init_host(
    cell: &Arc<OnceCell<Arc<WasmPluginHost>>>,
    dir: &str,
    name: &str,
) -> DFResult<Arc<WasmPluginHost>> {
    let host = cell
        .get_or_try_init(|| async {
            let host = WasmPluginHost::new();
            // Scan first: it is what proves the descriptor exists and is
            // readable, and a missing plugin must read as "not installed"
            // rather than as a failed run half a stage later.
            let found = host
                .scan(dir)
                .await
                .map_err(|e| DataFusionError::Execution(format!("plugin dir '{dir}': {e}")))?;
            if !found.iter().any(|d| d.name == name) {
                let seen = if found.is_empty() {
                    "none".to_string()
                } else {
                    found
                        .iter()
                        .map(|d| d.name.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                return Err(DataFusionError::Execution(format!(
                    "plugin '{name}' is not installed in '{dir}' (found: {seen})"
                )));
            }
            let handle = PluginHandle {
                name: name.to_string(),
                plugin_type: PluginType::Wasm,
            };
            let mut conf = HashMap::new();
            conf.insert("dir".to_string(), dir.to_string());
            host.init(&handle, conf).await.map_err(|e| {
                DataFusionError::Execution(format!("plugin '{name}' failed to load: {e}"))
            })?;
            Ok(Arc::new(host))
        })
        .await?;
    Ok(host.clone())
}

fn short_schema(s: &Schema) -> String {
    s.fields()
        .iter()
        .map(|f| format!("{}:{}", f.name(), f.data_type()))
        .collect::<Vec<_>>()
        .join(", ")
}

fn to_ipc(batch: &RecordBatch) -> DFResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| DataFusionError::Execution(format!("ipc write: {e}")))?;
        w.write(batch)
            .map_err(|e| DataFusionError::Execution(format!("ipc write: {e}")))?;
        w.finish()
            .map_err(|e| DataFusionError::Execution(format!("ipc write: {e}")))?;
    }
    Ok(buf)
}

fn from_ipc(bytes: &[u8]) -> DFResult<RecordBatch> {
    let reader = StreamReader::try_new(bytes, None)
        .map_err(|e| DataFusionError::Execution(format!("plugin returned unreadable IPC: {e}")))?;
    let mut batches = Vec::new();
    for b in reader {
        batches.push(
            b.map_err(|e| DataFusionError::Execution(format!("plugin returned bad batch: {e}")))?,
        );
    }
    batches
        .into_iter()
        .next()
        .ok_or_else(|| DataFusionError::Execution("plugin returned an empty IPC stream".into()))
}

/// Turn a declared schema (JSON) into an Arrow schema.
fn schema_from_json(items: &[JsonValue], type_id: &str) -> DFResult<SchemaRef> {
    let mut fields = Vec::with_capacity(items.len());
    for item in items {
        let name = item.get("name").and_then(|v| v.as_str()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{type_id}: `output_schema` entry missing string field `name`"
            ))
        })?;
        let ty = item.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "{type_id}: `output_schema` entry `{name}` missing string field `type`"
            ))
        })?;
        let dt = match ty.to_ascii_lowercase().as_str() {
            "int64" | "bigint" | "long" => DataType::Int64,
            "int32" | "int" => DataType::Int32,
            "float64" | "double" => DataType::Float64,
            "float32" | "float" => DataType::Float32,
            "bool" | "boolean" => DataType::Boolean,
            "utf8" | "string" | "text" => DataType::Utf8,
            "date" => DataType::Date32,
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{type_id}: `output_schema` entry `{name}` has unsupported type '{other}'"
                )))
            }
        };
        fields.push(Field::new(name, dt, true));
    }
    Ok(Arc::new(Schema::new(fields)))
}

// ── Node registration ──

/// A node that runs a row transform shipped as a plugin package.
pub struct PluginTransformNode;

#[async_trait]
impl NodeBehavior for PluginTransformNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: TYPE_ID.to_string(),
            display_name: "WASM Plugin Transform".to_string(),
            description: "Run a row transform from a plugin package (swanflow-plugin-1 ABI) — \
                          sandboxed, with a wall-clock and memory cap"
                .to_string(),
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
                    "plugin_dir": { "type": "string", "description": "Directory holding <plugin>.plugin.json" },
                    "plugin": { "type": "string", "description": "Plugin name, as declared in its descriptor" },
                    "function": { "type": "string", "description": "Exported function to call; defaults to the descriptor's `entry`" },
                    "output_schema": {
                        "type": "array",
                        "description": "Columns the plugin emits. Omit for a row transform that keeps its columns.",
                        "items": { "type": "object" }
                    }
                },
                "required": ["plugin_dir", "plugin"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<(), anyhow::Error> {
        cfg::required_string(config, "plugin_dir")?;
        cfg::required_string(config, "plugin")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> anyhow::Result<Vec<LogicalPlan>> {
        let plugin_dir = cfg::required_string(config, "plugin_dir")?;
        let plugin = cfg::required_string(config, "plugin")?;
        // Read the descriptor here, at plan time, so a typo fails with "that
        // plugin is not installed" instead of surfacing mid-scan.
        let manifest = crate::core::kernel::plugin_wasm::read_plugin_manifest(&plugin_dir, &plugin)
            .map_err(|e| {
                anyhow::anyhow!(
                    "{TYPE_ID}: plugin '{plugin}' in '{plugin_dir}' could not be read: {e}"
                )
            })?;

        let function = config
            .get("function")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| manifest.entry.clone());
        if !manifest.functions.is_empty() && !manifest.functions.contains(&function) {
            return Err(anyhow::anyhow!(
                "{TYPE_ID}: plugin '{plugin}' does not export '{function}' (it exports: {})",
                manifest.functions.join(", ")
            ));
        }

        let arrow_schema = match config.get("output_schema").and_then(|v| v.as_array()) {
            Some(items) => schema_from_json(items, TYPE_ID)?,
            None => {
                // same_as_input: take the input's columns as the declaration.
                let in_schema = inputs[0].schema();
                let fields = in_schema
                    .fields()
                    .iter()
                    .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable()))
                    .collect::<Vec<_>>();
                Arc::new(Schema::new(fields))
            }
        };
        let df_schema = Arc::new(DFSchema::try_from(arrow_schema.as_ref().clone())?);

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(PluginTransformLogical {
                plugin_dir,
                plugin,
                function,
                schema: df_schema,
                input: inputs[0].clone(),
            }),
        })])
    }
}
