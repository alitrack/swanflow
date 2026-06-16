use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeState;
use serde_json::json;
use std::fs;
use std::sync::Arc;
use uuid::Uuid;

async fn create_sqlite_db(path: &str, table_sql: &str, insert_sql: &str) {
    fs::remove_file(path).ok();
    sqlx::any::install_default_drivers();
    let opts = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
    sqlx::query(table_sql).execute(&pool).await.unwrap();
    sqlx::query(insert_sql).execute(&pool).await.unwrap();
    pool.close().await;
}

#[tokio::test]
async fn test_db_connector_sqlx_sqlite_basic() {
    let cache_dir = format!("target/connector_test_{}", Uuid::new_v4());
    let engine =
        SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    let sqlite_path = format!("{}/basic.db", cache_dir);
    fs::create_dir_all(&cache_dir).unwrap();
    create_sqlite_db(
        &sqlite_path,
        "CREATE TABLE users (id INTEGER, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    let mut workflow = Workflow::new("SQLx Basic");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.db_connector".into(),
        config: json!({
            "url": format!("sqlite3://{}", sqlite_path),
            "query": "SELECT * FROM users"
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    let table_name = format!("node_{}", &node_id.to_string()[..8]);
    let results = engine
        .ctx
        .sql(&format!("SELECT * FROM {}", table_name))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 2);
}

#[tokio::test]
async fn test_db_connector_sqlx_numeric_aggregation() {
    let cache_dir = format!("target/connector_test_{}", Uuid::new_v4());
    let engine =
        SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    let sqlite_path = format!("{}/agg.db", cache_dir);
    fs::create_dir_all(&cache_dir).unwrap();
    create_sqlite_db(
        &sqlite_path,
        "CREATE TABLE sales (id INTEGER, amount DOUBLE)",
        "INSERT INTO sales VALUES (1, 100.5), (2, 200.5)",
    )
    .await;

    let mut workflow = Workflow::new("SQLx Aggregation");

    let db_node_id = Uuid::new_v4();
    let db_node_idx = workflow.add_node(NodeInstance {
        id: db_node_id,
        type_id: "core.io.db_connector".into(),
        config: json!({
            "url": format!("sqlite3://{}", sqlite_path),
            "query": "SELECT * FROM sales"
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let sql_node_id = Uuid::new_v4();
    let sql_node_idx = workflow.add_node(NodeInstance {
        id: sql_node_id,
        type_id: "core.sql".into(),
        config: json!({
            "query": "SELECT SUM(amount) as total FROM input_0"
        }),
        state: NodeState::Configured,
        materialize: true, // MUST materialize to be queryable by caller via 'node_...' table
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            db_node_idx,
            sql_node_idx,
            swanflow::core::graph::EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        println!("DEBUG: Event: {:?}", event);
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    // Give materialization a moment to settle
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    // List all tables in DuckDB to see what's there
    if let Ok(conn) = duckdb::Connection::open(format!("{}/metadata.duckdb", cache_dir)) {
        let mut stmt = conn
            .prepare("SELECT table_name, parquet_path FROM flow_metadata")
            .unwrap();
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .unwrap();
        for row in rows {
            let (name, path) = row.unwrap();
            println!("DEBUG: flow_metadata entry: {} -> {}", name, path);
        }
    }

    let table_name = format!("node_{}", &sql_node_id.to_string()[..8]);
    let results = engine
        .ctx
        .sql(&format!("SELECT * FROM {}", table_name))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    if results.is_empty() || results[0].num_rows() == 0 {
        panic!("Aggregation table {} exists but is empty", table_name);
    }

    println!("DEBUG: Results batches:");
    datafusion::arrow::util::pretty::print_batches(&results).unwrap();

    let total_array = results[0].column(0);
    let s = datafusion::arrow::util::display::array_value_to_string(total_array, 0).unwrap();
    println!("DEBUG: Final Extracted total: '{}'", s);
    let total: f64 = s
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("Failed to parse '{}'", s));
    assert_eq!(total, 301.0);
}

#[tokio::test]
async fn test_sql_node_parameterization() {
    let cache_dir = format!("target/connector_test_{}", Uuid::new_v4());
    let engine =
        SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    let sqlite_path = format!("{}/params.db", cache_dir);
    fs::create_dir_all(&cache_dir).unwrap();
    create_sqlite_db(
        &sqlite_path,
        "CREATE TABLE users (id INTEGER, name TEXT)",
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
    )
    .await;

    let mut workflow = Workflow::new("SQL Params Test");

    let db_node_id = Uuid::new_v4();
    let db_node_idx = workflow.add_node(NodeInstance {
        id: db_node_id,
        type_id: "core.io.db_connector".into(),
        config: json!({
            "url": format!("sqlite3://{}", sqlite_path),
            "query": "SELECT * FROM users"
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let sql_node_id = Uuid::new_v4();
    let sql_node_idx = workflow.add_node(NodeInstance {
        id: sql_node_id,
        type_id: "core.sql".into(),
        config: json!({
            "query": "SELECT * FROM input_0 WHERE name = $1",
            "params": ["Alice"]
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            db_node_idx,
            sql_node_idx,
            swanflow::core::graph::EdgeInstance {
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

    // Give materialization a moment to settle
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let table_name = format!("node_{}", &sql_node_id.to_string()[..8]);
    let results = engine
        .ctx
        .sql(&format!("SELECT * FROM {}", table_name))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    assert_eq!(results[0].num_rows(), 1);
    let name =
        datafusion::arrow::util::display::array_value_to_string(results[0].column(1), 0).unwrap();
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_db_connector_sqlx_timestamps() {
    let cache_dir = format!("target/connector_test_{}", Uuid::new_v4());
    let engine =
        SwanFlowEngine::with_cache_dir(Arc::new(NodeRegistry::default()), cache_dir.to_string());

    let sqlite_path = format!("{}/ts.db", cache_dir);
    fs::create_dir_all(&cache_dir).unwrap();
    create_sqlite_db(
        &sqlite_path,
        "CREATE TABLE logs (id INTEGER, ts TEXT)",
        "INSERT INTO logs VALUES (1, '2026-03-02 12:00:00')",
    )
    .await;

    let mut workflow = Workflow::new("SQLx Timestamps");
    let node_id = Uuid::new_v4();
    workflow.add_node(NodeInstance {
        id: node_id,
        type_id: "core.io.db_connector".into(),
        config: json!({
            "url": format!("sqlite3://{}", sqlite_path),
            "query": "SELECT ts FROM logs"
        }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    while let Some(event) = rx.recv().await {
        if let swanflow::nodes::NodeEvent::Error { message, .. } = event {
            panic!("Workflow failed: {}", message);
        }
    }

    let table_name = format!("node_{}", &node_id.to_string()[..8]);
    let results = engine
        .ctx
        .sql(&format!("SELECT * FROM {}", table_name))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(results[0].num_rows(), 1);
}
