// Regression tests for the python script node fixes surfaced by the energy
// E2E workflow (wf7). Feature-gated: these tests embed a CPython interpreter
// (feature "python"); the default build has no Python nodes at all.
#![cfg(feature = "python")]
//
// * Bug 18 — the sandbox replaced __builtins__ with a dict holding only the
//   restricted __import__, so len/range/sum/Exception raised NameError in
//   every non-trivial script. The sandbox must restrict IMPORTS, not strip
//   builtins.
//
// * Limitation 19 — a python transform's output schema (new columns like
//   `zscore`/`flag`) was invisible to the planner. The advertised schema now
//   flows logical -> physical (PythonExec::with_schema) -> optimizer rebuilds
//   (with_new_children) -> stream (RecordBatchStream::schema). If any layer
//   drops it, DataFusion 52's extension-planner schema check (or downstream
//   materialization) breaks.
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{collect, ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use swanflow::nodes::script::python::PythonExec;

const SANDBOX_PREFIX: &str = "sandboxed:true\n";

fn make_input() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 15.0])),
        ],
    )
    .unwrap()
}

async fn physical_input() -> Arc<dyn ExecutionPlan> {
    let ctx = SessionContext::new();
    let table = MemTable::try_new(make_input().schema(), vec![vec![make_input()]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let logical = df.into_unoptimized_plan();
    ctx.state().create_physical_plan(&logical).await.unwrap()
}

fn script_body(extra: &str) -> String {
    format!(
        r#"
import pyarrow as pa
ids = batch.column(0).to_pylist()
vals = batch.column(1).to_pylist()
n = len(vals)
mean = sum(vals) / n
var = sum((v - mean) ** 2 for v in vals) / n
std = var ** 0.5
z = [round((v - mean) / std, 2) for v in vals]
flags = ["high" if zv > 1.5 else "normal" for zv in z]
out = pa.RecordBatch.from_arrays(
    [pa.array(ids), pa.array(vals), pa.array(z), pa.array(flags)],
    names=["id", "value", "zscore", "flag"])
{}
return out
"#,
        extra
    )
}

async fn run_stream(exec: &PythonExec) -> (Vec<RecordBatch>, SendableRecordBatchStream) {
    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = exec.execute(0, task_ctx).unwrap();
    let mut batches = Vec::new();
    while let Some(b) = stream.next().await {
        batches.push(b.unwrap());
    }
    (batches, exec.execute(0, Arc::new(TaskContext::default())).unwrap())
}

/// Bug 18: builtins (len/sum/range/round...) must be available inside the
/// sandbox. The script above uses len/sum/round — on the old sandbox it died
/// with NameError before producing any batch.
#[tokio::test]
async fn sandbox_keeps_builtins() {
    let exec = PythonExec::new(
        format!("{}{}", SANDBOX_PREFIX, script_body("")),
        physical_input().await,
    );
    let (batches, _) = run_stream(&exec).await;
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5, "script using len/sum/round must run under sandbox");
    assert_eq!(batches[0].num_columns(), 4, "transform adds zscore+flag columns");
}

/// Bug 18 (negative): the sandbox must still block dangerous imports.
#[tokio::test]
async fn sandbox_blocks_dangerous_imports() {
    let script = format!("{}{}", SANDBOX_PREFIX, "import os\nreturn batch\n");
    let exec = PythonExec::new(script, physical_input().await);
    let (_, mut stream) = run_stream_no_collect(&exec).await;
    let err = stream.next().await.expect("stream must end").is_err();
    assert!(err, "import os must fail under sandbox");
}

/// Limitation 19: the advertised (declared) schema must surface through
/// PythonExec::schema() — DataFusion's extension-planner check compares this
/// against the logical schema.
#[tokio::test]
async fn advertised_schema_surfaces_in_exec_schema() {
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("zscore", DataType::Float64, true),
        Field::new("flag", DataType::Utf8, true),
    ]));
    let exec = PythonExec::with_schema(
        format!("{}{}", SANDBOX_PREFIX, script_body("")),
        physical_input().await,
        out_schema,
    );
    let names: Vec<String> = exec.schema().fields().iter().map(|f| f.name().to_string()).collect();
    assert_eq!(
        names,
        vec!["id", "value", "zscore", "flag"],
        "exec.schema() must advertise the override, not the input schema"
    );
}

/// Limitation 19: the override must survive optimizer rebuilds —
/// with_new_children used to reconstruct via Self::new and drop the
/// advertised schema.
#[tokio::test]
async fn advertised_schema_survives_with_new_children() {
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("zscore", DataType::Float64, true),
        Field::new("flag", DataType::Utf8, true),
    ]));
    let exec = PythonExec::with_schema(
        format!("{}{}", SANDBOX_PREFIX, script_body("")),
        physical_input().await,
        out_schema,
    );
    let exec_arc: Arc<dyn ExecutionPlan> = Arc::new(exec);
    let child = exec_arc.children()[0].clone();
    let rebuilt = exec_arc.with_new_children(vec![child]).unwrap();
    let names: Vec<String> = rebuilt.schema().fields().iter().map(|f| f.name().to_string()).collect();
    assert_eq!(
        names,
        vec!["id", "value", "zscore", "flag"],
        "with_new_children must preserve the advertised schema"
    );
}

/// End-to-end: collect through the exec and verify both data correctness
/// (z-score math) and that the stream advertises the declared schema.
#[tokio::test]
async fn transform_end_to_end_math_and_stream_schema() {
    let out_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
        Field::new("zscore", DataType::Float64, true),
        Field::new("flag", DataType::Utf8, true),
    ]));
    let exec = PythonExec::with_schema(
        format!("{}{}", SANDBOX_PREFIX, script_body("")),
        physical_input().await,
        out_schema.clone(),
    );
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
    // stream schema = advertised schema
    let stream_fields: Vec<String> =
        stream.schema().fields().iter().map(|f| f.name().to_string()).collect();
    assert_eq!(stream_fields, vec!["id", "value", "zscore", "flag"]);

    let batches = collect(
        Arc::new(exec) as Arc<dyn ExecutionPlan>,
        Arc::new(TaskContext::default()),
    )
    .await
    .unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 5);
    let last = &batches[0];
    let flags: Vec<String> = last
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.unwrap().to_string())
        .collect();
    let z: Vec<f64> = last
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .iter()
        .map(|v| v.unwrap())
        .collect();
    // id=5 has value 15.0 — the planted outlier. With n=5 the single-outlier
    // z-score is bounded by sqrt(n-1)=2, hence the 1.5 threshold in the script.
    let outlier_idx = flags.iter().position(|f| f == "high").expect("one outlier");
    assert!(z[outlier_idx] > 1.5, "planted outlier must have z>1.5, got {}", z[outlier_idx]);
    assert_eq!(flags.iter().filter(|f| **f == "high").count(), 1);
}

// helper: execute without collecting (for error-path tests)
async fn run_stream_no_collect(exec: &PythonExec) -> (Vec<RecordBatch>, SendableRecordBatchStream) {
    let task_ctx = Arc::new(TaskContext::default());
    let mut stream = exec.execute(0, task_ctx).unwrap();
    let mut batches = Vec::new();
    while let Some(b) = stream.next().await {
        match b {
            Ok(batch) => batches.push(batch),
            Err(_) => break, // error path — caller checks batches.is_empty()
        }
    }
    let fresh = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
    (batches, fresh)
}
