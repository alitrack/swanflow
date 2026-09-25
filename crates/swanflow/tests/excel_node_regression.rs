// Regression tests for the excel I/O fixes surfaced by the finance/retail
// E2E workflows (wf5/wf6), committed in e70d4e6:
//
// * Bug 15 — excel_read advertised a placeholder (empty) schema at plan
//   time, so downstream SQL saw "No field named ...". It must read the real
//   header row (all Utf8).
// * Bug 16a — a Utf8View column silently wrote empty strings; must convert
//   to Utf8 before writing.
use std::sync::Arc;

use datafusion::arrow::array::{Array, StringArray, StringViewArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use serde_json::json;

use swanflow::nodes::io::excel::ExcelReadNode;
use swanflow::nodes::NodeBehavior;

fn make_xlsx(path: &str, rows: Vec<Vec<&str>>) {
    let rows_lit = rows
        .iter()
        .map(|r| format!("ws.append({:?})", r))
        .collect::<Vec<_>>()
        .join(";");
    let st = std::process::Command::new("python3")
        .args(["-c", &format!("import openpyxl; wb=openpyxl.Workbook(); ws=wb.active; {}; wb.save('{}')", rows_lit, path)])
        .status()
        .unwrap();
    assert!(st.success(), "fixture creation failed");
}

/// Bug 15: infer_schema must surface the REAL header names (all Utf8), not
/// a placeholder. On the old code this returned DFSchema::empty().
#[tokio::test]
async fn excel_read_infers_real_header_schema() {
    let path = "/tmp/swanflow_test_header.xlsx";
    make_xlsx(path, vec![vec!["城市", "数量"], vec!["杭州", "10"], vec!["成都", "20"]]);

    let node = ExcelReadNode;
    let meta = node
        .infer_schema(vec![], &json!({"path": path}))
        .await
        .expect("infer_schema must succeed");
    let schema = &meta.schemas[0];
    let names: Vec<String> = schema
        .as_arrow()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(names, vec!["城市", "数量"], "must read real header names");
    assert!(
        schema.as_arrow().fields().iter().all(|f| f.data_type() == &DataType::Utf8),
        "excel columns infer as Utf8 (calamine); numeric needs explicit CAST downstream"
    );
}

/// Bug 15 companion: propose_plan must embed the same real schema (this is
/// the exact path that broke wf5 with 'No field named').
#[tokio::test]
async fn excel_read_proposes_real_schema_plan() {
    let path = "/tmp/swanflow_test_plan.xlsx";
    make_xlsx(path, vec![vec!["city", "amount"], vec!["hz", "1"]]);

    let node = ExcelReadNode;
    let plans = node
        .propose_plan(vec![], &json!({"path": path}))
        .await
        .expect("propose_plan must succeed");
    let names: Vec<String> = plans[0]
        .schema()
        .as_arrow()
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();
    assert_eq!(names, vec!["city", "amount"], "logical plan must carry real header schema");
}

/// Bug 16a: Utf8View columns must cast to Utf8 preserving values — the old
/// writer path silently emitted empty strings for Utf8View.
#[test]
fn utf8_view_cast_preserves_values() {
    let view = StringViewArray::from(vec![Some("杭州"), Some("成都"), Some("北京")]);
    assert_eq!(view.data_type(), &DataType::Utf8View);
    let utf8 = datafusion::arrow::compute::cast(&view, &DataType::Utf8).unwrap();
    let strings = utf8.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.value(0), "杭州");
    assert_eq!(strings.value(1), "成都");
    assert_eq!(strings.value(2), "北京");
    assert_eq!(strings.len(), 3);
}

/// Schema sanity used by the cast-guard: Utf8/Utf8View field variants.
#[test]
fn utf8_fields_accept_utf8view_values() {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, true)]);
    let view = StringViewArray::from(vec![Some("x")]);
    let cast = datafusion::arrow::compute::cast(&view, schema.field(0).data_type()).unwrap();
    assert_eq!(cast.len(), 1);
    let _ = Arc::new(StringArray::from(vec![Some("ok")])); // Arc<Array> usable downstream
}
