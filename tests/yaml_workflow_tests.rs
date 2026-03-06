use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::Workflow;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeEvent;
use std::sync::Arc;

fn should_run_engine_integration_tests() -> bool {
    std::env::var("RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

#[tokio::test]
async fn test_all_tutorial_workflows() {
    if !should_run_engine_integration_tests() {
        println!(
            "Skipping tutorial workflow integration test. Set RUSTFLOW_RUN_ENGINE_INTEGRATION_TESTS=1 to enable."
        );
        return;
    }

    let registry = Arc::new(NodeRegistry::default());

    // 测试 Tutorial 09: Excel Reports
    run_yaml_workflow_in_isolated_cache(
        registry.clone(),
        "tutorials/09_excel_reports.yaml",
        "target/yaml_tutorial_test_09",
    )
    .await;

    // 测试 Tutorial 13: Lakehouse SQL Interop (Decoupled SQL)
    run_yaml_workflow_in_isolated_cache(
        registry.clone(),
        "tutorials/13_lakehouse_sql.yaml",
        "target/yaml_tutorial_test_13",
    )
    .await;

    // 测试 Tutorial 14: Conditional Formatting
    run_yaml_workflow_in_isolated_cache(
        registry.clone(),
        "tutorials/14_conditional_formatting.yaml",
        "target/yaml_tutorial_test_14",
    )
    .await;

    // 测试 Tutorial 15: Federated Lakehouse Join
    run_yaml_workflow_in_isolated_cache(
        registry.clone(),
        "tutorials/15_federated_lakehouse.yaml",
        "target/yaml_tutorial_test_15",
    )
    .await;

    // 测试 Tutorial 16: Dynamic Excel Exploration
    run_yaml_workflow_in_isolated_cache(
        registry,
        "tutorials/16_dynamic_excel_exploration.yaml",
        "target/yaml_tutorial_test_16",
    )
    .await;

    // 测试 Tutorial 12: Excel Templates (如果提供了模板文件，这里可以取消注释)
    // run_yaml_workflow(&engine, "tutorials/12_excel_templates.yaml").await;
}

async fn run_yaml_workflow_in_isolated_cache(
    registry: Arc<NodeRegistry>,
    path: &str,
    cache_dir: &str,
) {
    let _ = std::fs::remove_dir_all(cache_dir);
    let engine = SwanFlowEngine::with_cache_dir(registry, cache_dir.to_string());
    run_yaml_workflow(&engine, path).await;
}

async fn run_yaml_workflow(engine: &SwanFlowEngine, path: &str) {
    println!("Testing YAML Workflow: {}", path);
    let yaml_content = std::fs::read_to_string(path).expect("Failed to read YAML file");
    let workflow: Workflow =
        serde_yaml::from_str(&yaml_content).expect("Failed to parse YAML into Workflow");

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let NodeEvent::Error { node_id, message } = event {
            panic!("Workflow failed at node {}: {}", node_id, message);
        }
    }
    println!("✓ Workflow {} passed successfully", path);
}
