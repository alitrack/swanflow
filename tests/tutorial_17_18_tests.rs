use std::fs;
use std::sync::Arc;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::Workflow;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeEvent;

#[tokio::test]
async fn test_tutorial_17_db_connector() {
    let registry = Arc::new(NodeRegistry::default());
    let cache_dir = "target/tutorial_17_test";
    let engine = SwanFlowEngine::with_cache_dir(registry, cache_dir.to_string());

    // 1. Setup standard SQLite db (not DuckDB format)
    let db_path = "users.db";
    fs::remove_file(db_path).ok();

    // We can use a simple SQL statement to create it if we have a sqlite driver
    // Or just use DuckDB to EXPORT to a sqlite file? No, DuckDB ATTACH (TYPE SQLITE)
    // Actually, I'll use SQLx to create it since it's already a dependency
    sqlx::any::install_default_drivers();
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(db_path)
        .create_if_missing(true);
    let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
    sqlx::query("CREATE TABLE users (id INTEGER, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // 2. Run Workflow
    run_yaml_workflow(&engine, "tutorials/17_database_connector.yaml").await;

    fs::remove_file(db_path).ok();
}

#[tokio::test]
async fn test_tutorial_18_json_glob() {
    let registry = Arc::new(NodeRegistry::default());
    let cache_dir = "target/tutorial_18_test";
    let engine = SwanFlowEngine::with_cache_dir(registry, cache_dir.to_string());

    // 1. Setup data/logs/
    let log_dir = "data/logs";
    fs::create_dir_all(log_dir).unwrap();
    // Use NDJSON (one object per line) for read_json_auto compatibility
    fs::write(
        format!("{}/1.json", log_dir),
        r#"{"id": 1, "event": "login"}"#,
    )
    .unwrap();
    fs::write(
        format!("{}/2.json", log_dir),
        r#"{"id": 2, "event": "logout"}"#,
    )
    .unwrap();

    // 2. Run Workflow
    run_yaml_workflow(&engine, "tutorials/18_universal_json_glob.yaml").await;

    fs::remove_dir_all("data").ok();
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
