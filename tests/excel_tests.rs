use rust_xlsxwriter::Workbook;
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::{NodeInstance, Workflow};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeState;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_excel_read_preview() {
    // 1. Create a dummy Excel file using rust_xlsxwriter
    let excel_path = "test_excel_demo.xlsx";
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.write(0, 0, "Product").unwrap();
    worksheet.write(0, 1, "Price").unwrap();
    worksheet.write(1, 0, "Apple").unwrap();
    worksheet.write(1, 1, 1.5).unwrap();
    worksheet.write(2, 0, "Banana").unwrap();
    worksheet.write(2, 1, 0.8).unwrap();
    workbook.save(excel_path).unwrap();

    // 2. Setup Engine
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 3. Build Workflow: Excel Read
    let mut workflow = Workflow::new("Excel Preview Test");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.excel_read".into(),
        config: json!({ "path": excel_path, "sample_rows": 5 }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
    });

    // 4. Infer Metadata (Preview)
    let metadata = engine.infer_metadata(&workflow).await.unwrap();

    // 5. Verify
    let schema = &metadata.schemas[&node_id][0];
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.fields()[0].name(), "Product");
    assert_eq!(schema.fields()[1].name(), "Price");

    let samples = &metadata.sample_data[&node_id];
    assert_eq!(samples[0].num_rows(), 2); // 2 data rows

    // Cleanup
    let _ = std::fs::remove_file(excel_path);
}
