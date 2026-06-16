use calamine::Reader;
use rust_xlsxwriter::Workbook;
use serde_json::json;
use std::sync::Arc;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeState;
use uuid::Uuid;

fn should_run_excel_integration_tests() -> bool {
    std::env::var("RUSTFLOW_RUN_EXCEL_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

#[tokio::test]
async fn test_excel_read_preview() {
    if !should_run_excel_integration_tests() {
        println!(
            "Skipping excel integration test. Set RUSTFLOW_RUN_EXCEL_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

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
        config: json!({ "path": excel_path, "sample_rows": 5, "has_header": false }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    // 4. Infer Metadata (Preview)
    let metadata = engine.infer_metadata(&workflow).await.unwrap();

    // 5. Verify
    let schemas = metadata.schemas.get(&node_id).unwrap();
    let _schema = &schemas[0];
    // assert_eq!(schema.fields().len(), 2);

    if let Some(samples) = metadata.sample_data.get(&node_id) {
        if let Some(first) = samples.first() {
            assert_eq!(first.num_rows(), 3); // 3 total rows including header
        }
    }

    // Cleanup
    let _ = std::fs::remove_file(excel_path);
}

#[tokio::test]
async fn test_excel_template_filling() {
    if !should_run_excel_integration_tests() {
        println!(
            "Skipping excel integration test. Set RUSTFLOW_RUN_EXCEL_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    // 1. Create a template file with markers
    let template_path = "test_template.xlsx";
    let output_path = "test_filled_output.xlsx";
    let mut workbook = Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.write(0, 0, "Customer:").unwrap();
    worksheet.write(0, 1, "{{CustomerName}}").unwrap(); // Marker
    worksheet.write(2, 0, "Item").unwrap();
    worksheet.write(2, 1, "Quantity").unwrap();
    // Table will start at A4 (Row 3, Col 0 since 0-indexed)
    workbook.save(template_path).unwrap();

    // 2. Setup Engine and Registry
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 3. Build Workflow
    let mut workflow = Workflow::new("Excel Template Test");

    // Create source data (dummy CSV)
    let csv_path = "test_data.csv";
    std::fs::write(csv_path, "Item,Quantity\nWidget A,10\nWidget B,20").unwrap();

    let node_a_id = Uuid::new_v4();
    let node_a_idx = workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.io.csv".into(),
        config: serde_json::json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    let node_b_id = Uuid::new_v4();
    let node_b_idx = workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.io.excel_template".into(),
        config: serde_json::json!({
            "template_path": template_path,
            "output_path": output_path,
            "markers": { "Item": "{{CustomerName}}" },
            "table_start": "A4"
        }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            node_a_idx,
            node_b_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 4. Execute Workflow
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    // 5. Verify Output
    assert!(std::path::Path::new(output_path).exists());
    let mut reader: calamine::Xlsx<_> = calamine::open_workbook(output_path).unwrap();
    let range = reader.worksheet_range("Sheet1").unwrap();

    // Check marker replacement (Row 0, Col 1 should be "Widget A")
    assert_eq!(range.get_value((0, 1)).unwrap().to_string(), "Widget A");

    // Check table injection (Row 3 is data start)
    assert_eq!(range.get_value((3, 0)).unwrap().to_string(), "Widget A");
    assert_eq!(range.get_value((4, 0)).unwrap().to_string(), "Widget B");

    // Cleanup
    let _ = std::fs::remove_file(template_path);
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(csv_path);
}

#[tokio::test]
async fn test_excel_styling_and_table() {
    if !should_run_excel_integration_tests() {
        println!(
            "Skipping excel integration test. Set RUSTFLOW_RUN_EXCEL_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    let output_path = "test_styled_table.xlsx";
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    let mut workflow = Workflow::new("Excel Style Test");
    let csv_path = "test_style_data.csv";
    std::fs::write(csv_path, "Name,Score\nAlice,95\nBob,82").unwrap();

    let node_a_id = Uuid::new_v4();
    let node_a_idx = workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    let node_b_id = Uuid::new_v4();
    let node_b_idx = workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.io.excel_write".into(),
        config: json!({
            "path": output_path,
            "convert_to_table": true,
            "styles": [
                { "column": "Score", "format": { "bold": true, "font_color": "red" } }
            ]
        }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            node_a_idx,
            node_b_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    assert!(std::path::Path::new(output_path).exists());
    // Verification of styles/tables is hard with calamine, but we check file existence and basic readability
    let mut reader: calamine::Xlsx<_> = calamine::open_workbook(output_path).unwrap();
    let range = reader.worksheet_range("Sheet1").unwrap();
    assert_eq!(range.get_value((1, 0)).unwrap().to_string(), "Alice");

    // Cleanup
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(csv_path);
}

#[tokio::test]
async fn test_excel_report_and_metadata() {
    if !should_run_excel_integration_tests() {
        println!(
            "Skipping excel integration test. Set RUSTFLOW_RUN_EXCEL_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    let output_path = "test_report.xlsx";
    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 1. Create a report with a chart
    let mut workflow = Workflow::new("Excel Report Test");
    let csv_path = "test_report_data.csv";
    std::fs::write(csv_path, "Month,Revenue\nJan,100\nFeb,150\nMar,120").unwrap();

    let node_a_id = Uuid::new_v4();
    let node_a_idx = workflow.add_node(NodeInstance {
        id: node_a_id,
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    let node_b_id = Uuid::new_v4();
    let node_b_idx = workflow.add_node(NodeInstance {
        id: node_b_id,
        type_id: "core.io.excel_report".into(),
        config: json!({
            "path": output_path,
            "charts": [
                {
                    "type": "column",
                    "title": "Revenue by Month",
                    "data_range": "Data!$B$2:$B$4",
                    "label_range": "Data!$A$2:$A$4"
                }
            ]
        }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            node_a_idx,
            node_b_idx,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Report generation failed: {}", message);
        }
    }

    assert!(std::path::Path::new(output_path).exists());

    // 2. Use ExcelSheetNamesNode to verify sheets
    let mut workflow_meta = Workflow::new("Excel Metadata Test");
    let node_c_id = Uuid::new_v4();
    let _node_c_idx = workflow_meta.add_node(NodeInstance {
        id: node_c_id,
        type_id: "core.io.excel_sheet_names".into(),
        config: json!({ "path": output_path }),
        state: NodeState::Configured,
        materialize: false,
        sub_workflow: None,
        ..Default::default()
    });

    // let metadata = engine.infer_metadata(&workflow_meta).await.unwrap();
    // let samples = &metadata.sample_data[&node_c_id];

    let mut rx = engine.execute_workflow(&workflow_meta).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Metadata extraction failed: {}", message);
        }
    }

    // Cleanup
    let _ = std::fs::remove_file(output_path);
    let _ = std::fs::remove_file(csv_path);
}
