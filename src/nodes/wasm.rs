use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::task::{Context, Poll};
use wasmtime::*;

use crate::nodes::config as cfg;
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

// ── Logical Plan Node ──

/// Logical plan node for WASM UDF execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WasmLogical {
    pub module_path: String,
    pub function_name: String,
    pub input: LogicalPlan,
}

impl PartialOrd for WasmLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.module_path.as_str(), self.function_name.as_str())
            .partial_cmp(&(other.module_path.as_str(), other.function_name.as_str()))
    }
}

impl UserDefinedLogicalNodeCore for WasmLogical {
    fn name(&self) -> &str {
        "WasmUdf"
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
        write!(
            f,
            "WasmUdf: module={}, function={}",
            self.module_path, self.function_name
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            module_path: self.module_path.clone(),
            function_name: self.function_name.clone(),
            input: inputs[0].clone(),
        })
    }
}

// ── Physical Execution Plan ──

/// Physical execution plan for WASM UDF execution.
#[derive(Debug)]
pub struct WasmExec {
    pub module_path: String,
    pub function_name: String,
    pub input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl WasmExec {
    pub fn new(
        module_path: String,
        function_name: String,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            input.properties().output_partitioning().clone(),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            module_path,
            function_name,
            input,
            cache,
        }
    }
}

impl DisplayAs for WasmExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "WasmExec({})", self.function_name)
    }
}

impl ExecutionPlan for WasmExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "WasmExec"
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
        Ok(Arc::new(Self::new(
            self.module_path.clone(),
            self.function_name.clone(),
            children[0].clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let module_path = self.module_path.clone();
        let function_name = self.function_name.clone();

        Ok(Box::pin(WasmStream {
            input: input_stream,
            module_path,
            function_name,
        }))
    }
}

// ── RecordBatch Stream ──

struct WasmStream {
    input: SendableRecordBatchStream,
    module_path: String,
    function_name: String,
}

impl RecordBatchStream for WasmStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

impl Stream for WasmStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = run_wasm_udf(
                    &self.module_path,
                    &self.function_name,
                    batch,
                );
                match result {
                    Ok(out_batch) => Poll::Ready(Some(Ok(out_batch))),
                    Err(e) => Poll::Ready(Some(Err(
                        datafusion::error::DataFusionError::Execution(e.to_string()),
                    ))),
                }
            }
            other => other,
        }
    }
}

// ── WASM Execution Engine ──

/// Runs a single RecordBatch through a WASM UDF.
///
/// Protocol:
///   - The WASM module exports `alloc(size: i32) -> i32` and `dealloc(ptr: i32, size: i32)`.
///   - The WASM module exports `transform(ptr: i32, len: i32) -> i32`.
///   - Input: Arrow IPC stream bytes placed in WASM linear memory at (ptr, len).
///   - Output: The transform function returns a pointer. The first 4 bytes at that
///     pointer contain the output length (i32), followed by the Arrow IPC stream bytes.
fn run_wasm_udf(
    module_path: &str,
    function_name: &str,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    // 1. Read WASM module bytes
    let wasm_bytes = std::fs::read(module_path).map_err(|e| {
        anyhow!("Failed to read WASM module '{}': {}", module_path, e)
    })?;

    // 2. Setup wasmtime engine, module, store, instance
    let engine = Engine::new(&Config::new())?;
    let module = Module::new(&engine, &wasm_bytes)?;
    let mut store = Store::new(&engine, ());

    // Link imports (none for a self-contained WASM module)
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &module)?;

    // 3. Get exported functions
    let alloc = instance
        .get_typed_func::<i32, i32>(&mut store, "alloc")
        .map_err(|_| anyhow!("WASM module must export 'alloc(size: i32) -> i32'"))?;

    let dealloc = instance
        .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
        .map_err(|_| anyhow!("WASM module must export 'dealloc(ptr: i32, size: i32)'"))?;

    let transform = instance
        .get_typed_func::<(i32, i32), i32>(&mut store, function_name)
        .map_err(|_| {
            anyhow!(
                "WASM module must export '{}'(ptr: i32, len: i32) -> i32",
                function_name
            )
        })?;

    // 4. Serialize RecordBatch to Arrow IPC stream bytes
    let input_bytes = serialize_record_batch_to_ipc(&batch)?;

    // 5. Allocate memory in WASM linear memory for input
    let input_len = input_bytes.len() as i32;
    let input_ptr = alloc.call(&mut store, input_len)?;

    // 6. Copy input bytes into WASM memory
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or_else(|| anyhow!("WASM module must export linear memory 'memory'"))?;
    let wasm_mem = memory.data_mut(&mut store);
    wasm_mem[input_ptr as usize..(input_ptr + input_len) as usize].copy_from_slice(&input_bytes);

    // 7. Call the WASM transform function
    let output_ptr = transform.call(&mut store, (input_ptr, input_len))?;

    // 8. Read output length from first 4 bytes
    let wasm_mem = memory.data(&store);
    let output_len_bytes: [u8; 4] = wasm_mem[output_ptr as usize..(output_ptr as usize + 4)]
        .try_into()
        .map_err(|_| anyhow!("Failed to read output length from WASM memory"))?;
    let output_len = i32::from_le_bytes(output_len_bytes);

    // 9. Read output bytes (after the length prefix)
    let output_start = (output_ptr + 4) as usize;
    let output_data = wasm_mem[output_start..(output_start + output_len as usize)].to_vec();

    // 10. Free WASM memory
    // We need &mut store for dealloc, so we must drop the immutable borrow from memory.data()
    drop(wasm_mem);
    dealloc.call(&mut store, (input_ptr, input_len))?;
    dealloc.call(&mut store, (output_ptr, output_len + 4))?;

    // 11. Deserialize Arrow IPC bytes back to RecordBatch
    let output_batch = deserialize_record_batch_from_ipc(&output_data)?;

    Ok(output_batch)
}

/// Serialize a RecordBatch to Arrow IPC stream format bytes.
fn serialize_record_batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let schema = batch.schema();
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Deserialize a RecordBatch from Arrow IPC stream format bytes.
fn deserialize_record_batch_from_ipc(data: &[u8]) -> Result<RecordBatch> {
    let reader = StreamReader::try_new(data, None)?;
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<Vec<_>, _>>()?;
    batches
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("Arrow IPC stream contained no record batches"))
}

// ── Node Registration ──

/// A node that executes a WASM module for data transformation.
pub struct WasmNode;

#[async_trait]
impl NodeBehavior for WasmNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.script.wasm".to_string(),
            display_name: "WASM UDF".to_string(),
            description: "Transform data using a WebAssembly module (wasmtime runtime) — 10-100x faster than Python UDFs".to_string(),
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
                    "module": { "type": "string", "description": "Path to the .wasm module file" },
                    "function": { "type": "string", "description": "Name of the exported transform function", "default": "transform" }
                },
                "required": ["module"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "module")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let module_path = cfg::required_string(config, "module")?;
        let function_name = config
            .get("function")
            .and_then(|v| v.as_str())
            .unwrap_or("transform")
            .to_string();

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(WasmLogical {
                module_path,
                function_name,
                input: inputs[0].clone(),
            }),
        })])
    }
}
