//! P0 acceptance nail: a real `.wasm` plugin, driven by `WasmPluginHost`, must
//! produce exactly what SQL does.
//!
//! The comparison is against `ORDER BY` executed by DuckDB on the same rows, so
//! the assertion is about *semantics*, not about the plugin agreeing with itself.
//!
//! The plugin binary is not committed (2.7 MB of build output). Build it first:
//!
//! ```text
//! plugins/examples/sort/build.sh
//! cargo test -p swanflow --features wasm --test plugin_sort_equivalence
//! ```
//!
//! Without the artifact the test fails with the build command in the message —
//! a skip would let "plugin support" rot silently.

#![cfg(feature = "wasm")]

use std::collections::HashMap;
use std::path::PathBuf;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use swanflow::core::kernel::plugin::{PluginHandle, PluginHost, PluginType};
use swanflow::core::kernel::plugin_wasm::WasmPluginHost;

fn plugin_dir() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/swanflow; the example plugin lives at the
    // workspace root, two levels up.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("plugins")
        .join("examples")
        .join("sort")
}

fn to_ipc(batch: &RecordBatch) -> Vec<u8> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, &batch.schema()).expect("ipc writer");
        w.write(batch).expect("write batch");
        w.finish().expect("finish");
    }
    buf
}

fn from_ipc(bytes: &[u8]) -> RecordBatch {
    let reader = StreamReader::try_new(bytes, None).expect("ipc reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("batch")).collect();
    assert_eq!(batches.len(), 1, "expected exactly one batch back");
    batches.into_iter().next().unwrap()
}

/// Rows chosen so a stable-but-wrong implementation (e.g. sorting descending,
/// or sorting a string form) cannot accidentally match.
const AMOUNTS: [i64; 8] = [30, 5, 100, 5, 2, 77, 41, 9];
const IDS: [i64; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

fn input_batch() -> RecordBatch {
    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("amount", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(AMOUNTS.to_vec())),
            std::sync::Arc::new(Int64Array::from(IDS.to_vec())),
        ],
    )
    .expect("input batch")
}

fn sql_expected() -> (Vec<i64>, Vec<i64>) {
    let con = duckdb::Connection::open_in_memory().expect("duckdb");
    con.execute_batch(
        "CREATE TABLE t(amount BIGINT, id BIGINT);
         INSERT INTO t VALUES (30,1),(5,2),(100,3),(5,4),(2,5),(77,6),(41,7),(9,8);",
    )
    .expect("seed");
    // ORDER BY only on `amount`: ties keep insert order in DuckDB, and arrow's
    // lexsort is likewise stable, so this is a fair comparison.
    let mut stmt = con
        .prepare("SELECT amount, id FROM t ORDER BY amount ASC")
        .expect("prepare");
    let rows: Vec<(i64, i64)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("query")
        .map(|r| r.expect("row"))
        .collect();
    (
        rows.iter().map(|(a, _)| *a).collect(),
        rows.iter().map(|(_, i)| *i).collect(),
    )
}

fn column_i64(batch: &RecordBatch, idx: usize) -> Vec<i64> {
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column")
        .values()
        .to_vec()
}

#[tokio::test]
async fn a_real_wasm_plugin_sorts_exactly_like_sql_order_by() {
    let dir = plugin_dir();
    let module = dir.join("sort.wasm");
    assert!(
        module.exists(),
        "missing {} — build the plugin first: plugins/examples/sort/build.sh",
        module.display()
    );

    let host = WasmPluginHost::new();
    let found = host.scan(dir.to_str().unwrap()).await.expect("scan");
    assert_eq!(found.len(), 1, "expected one plugin, got {:?}", found);
    assert_eq!(found[0].name, "sort");
    assert_eq!(found[0].plugin_type, PluginType::Wasm);
    assert!(found[0].functions.contains(&"sort_table".to_string()));

    let handle = PluginHandle {
        name: "sort".into(),
        plugin_type: PluginType::Wasm,
    };
    let mut cfg = HashMap::new();
    cfg.insert("dir".to_string(), dir.to_str().unwrap().to_string());
    host.init(&handle, cfg).await.expect("init");

    let batch = input_batch();
    let out = host
        .run(&handle, "sort_table", &to_ipc(&batch))
        .await
        .expect("run");
    let sorted = from_ipc(&out);

    let (want_amounts, want_ids) = sql_expected();
    let got_amounts = column_i64(&sorted, 0);
    let got_ids = column_i64(&sorted, 1);

    assert_eq!(
        got_amounts, want_amounts,
        "the plugin's row order must match SQL ORDER BY amount"
    );
    assert_eq!(
        got_ids, want_ids,
        "tie-breaking must match too — otherwise the sorted table is a different table"
    );
    // Sanity: the input really was unsorted, so the assertion above means something.
    assert_ne!(AMOUNTS.to_vec(), got_amounts);
}

#[tokio::test]
async fn a_plugin_that_returns_garbage_is_reported_not_swallowed() {
    let dir = plugin_dir();
    let module = dir.join("sort.wasm");
    assert!(
        module.exists(),
        "missing {} — build the plugin first: plugins/examples/sort/build.sh",
        module.display()
    );

    let host = WasmPluginHost::new();
    let handle = PluginHandle {
        name: "sort".into(),
        plugin_type: PluginType::Wasm,
    };
    let mut cfg = HashMap::new();
    cfg.insert("dir".to_string(), dir.to_str().unwrap().to_string());
    host.init(&handle, cfg).await.expect("init");

    // Not Arrow IPC. The plugin aborts on its own parse failure, and the host
    // must surface that as an error rather than as an empty result set that
    // looks like "no rows matched".
    let err = host
        .run(&handle, "sort_table", b"definitely not arrow ipc")
        .await
        .expect_err("a plugin that cannot parse its input must not look like success");
    let msg = err.to_string();
    assert!(
        msg.contains("trapped") || msg.contains("null") || msg.contains("failed"),
        "error should point at the plugin's response, got: {msg}"
    );
    assert!(
        !msg.contains("no rows") && !msg.contains("0 rows"),
        "must not present a plugin failure as an empty result: {msg}"
    );
}
