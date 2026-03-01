use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::Workflow;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeEvent;
use std::sync::Arc;
use std::path::Path;

#[tokio::test]
async fn test_all_tutorial_workflows() {
    let registry = Arc::new(NodeRegistry::default());
    let cache_dir = "target/yaml_tutorial_test";
    let engine = SwanFlowEngine::with_cache_dir(registry, cache_dir.to_string());

    // 测试 Tutorial 09: Excel Reports
    run_yaml_workflow(&engine, "tutorials/09_excel_reports.yaml").await;
    
    // 测试 Tutorial 13: Lakehouse SQL Interop (Decoupled SQL)
    run_yaml_workflow(&engine, "tutorials/13_lakehouse_sql.yaml").await;

    // 测试 Tutorial 14: Conditional Formatting
    run_yaml_workflow(&engine, "tutorials/14_conditional_formatting.yaml").await;

    // 测试 Tutorial 15: Federated Lakehouse Join
    run_yaml_workflow(&engine, "tutorials/15_federated_lakehouse.yaml").await;

    // 测试 Tutorial 16: Dynamic Excel Exploration
    run_yaml_workflow(&engine, "tutorials/16_dynamic_excel_exploration.yaml").await;
    
    // 测试 Tutorial 12: Excel Templates (如果提供了模板文件，这里可以取消注释)
    // run_yaml_workflow(&engine, "tutorials/12_excel_templates.yaml").await;
}

async fn run_yaml_workflow(engine: &SwanFlowEngine, path: &str) {
    println!("Testing YAML Workflow: {}", path);
    let yaml_content = std::fs::read_to_string(path).expect("Failed to read YAML file");
    let workflow: Workflow = serde_yaml::from_str(&yaml_content).expect("Failed to parse YAML into Workflow");

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { node_id, message } = event {
            panic!("Workflow failed at node {}: {}", node_id, message);
        }
    }
    println!("✓ Workflow {} passed successfully", path);
}
