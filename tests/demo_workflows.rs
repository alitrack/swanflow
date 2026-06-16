use serde_json::json;
use std::sync::Arc;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::{EdgeInstance, NodeInstance, Workflow};
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeEvent, NodeState};
use uuid::Uuid;

fn should_run_demo_workflows() -> bool {
    std::env::var("RUSTFLOW_RUN_DEMO_WORKFLOW_TESTS")
        .ok()
        .as_deref()
        == Some("1")
}

async fn run_and_print_events(engine: &SwanFlowEngine, workflow: &Workflow, title: &str) {
    println!(
        "
=== Running Workflow: {} ===",
        title
    );
    let mut rx = engine.execute_workflow(workflow).await.unwrap();

    // 我们在这里监听“流量计”传回的实时信号
    while let Some(event) = rx.recv().await {
        match event {
            NodeEvent::StateChanged { node_id, state } => {
                let status_icon = match state {
                    NodeState::Running => "🟡 Running",
                    NodeState::Executed => "🟢 Executed",
                    NodeState::Error(e) => &format!("🔴 Error: {}", e),
                    _ => "⚪ Idle",
                };
                println!("Node[{}] -> {}", node_id, status_icon);
            }
            NodeEvent::Progress {
                node_id,
                percentage,
            } => {
                println!("Node[{}] -> Progress: {:.1}%", node_id, percentage);
            }
            NodeEvent::Materialized { node_id, .. } => {
                println!("Node[{}] -> Materialized", node_id);
            }
            NodeEvent::Error { node_id, message } => {
                println!("Node[{}] -> ERROR: {}", node_id, message);
            }
            _ => {} // wildcard required by #[non_exhaustive]
        }
    }
}

#[tokio::test]
async fn demo_standard_etl_pipeline() {
    if !should_run_demo_workflows() {
        println!("Skipping demo workflow test. Set RUSTFLOW_RUN_DEMO_WORKFLOW_TESTS=1 to enable.");
        return;
    }

    // 准备测试数据
    let csv_path = "demo_data.csv";
    std::fs::write(
        csv_path,
        "name,salary,dept
alice,5000,IT
bob,3000,HR
charlie,7000,IT
david,4500,Sales",
    )
    .unwrap();

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 构建图：Read -> SQL (加薪) -> Filter (高薪) -> Project (姓名)
    let mut workflow = Workflow::new("Standard ETL");

    let n_read = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.io.csv".into(),
        config: json!({ "path": csv_path }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let n_sql = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT name, salary * 1.1 as new_salary, dept FROM input_0" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    let n_filter = workflow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.transform.filter".into(),
        config: json!({ "expr": "new_salary > 5000" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });

    workflow
        .add_edge(
            n_read,
            n_sql,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    workflow
        .add_edge(
            n_sql,
            n_filter,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    run_and_print_events(&engine, &workflow, "ETL Pipeline Demo").await;
    let _ = std::fs::remove_file(csv_path);
}

#[tokio::test]
async fn demo_reusable_metanode() {
    if !should_run_demo_workflows() {
        println!("Skipping demo workflow test. Set RUSTFLOW_RUN_DEMO_WORKFLOW_TESTS=1 to enable.");
        return;
    }

    let registry = Arc::new(NodeRegistry::default());
    let engine = SwanFlowEngine::new(registry);

    // 1. 创建一个子图：专门处理“IT 部门过滤”
    let mut cleaner_sub = Workflow::new("IT_Cleaner_Sub");
    let sub_in = cleaner_sub.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.input".into(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });
    let sub_filter = cleaner_sub.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.transform.filter".into(),
        config: json!({ "expr": "dept = 'IT'" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });
    let sub_out = cleaner_sub.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.output".into(),
        config: json!({ "port_index": 0 }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
        ..Default::default()
    });
    cleaner_sub
        .add_edge(
            sub_in,
            sub_filter,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();
    cleaner_sub
        .add_edge(
            sub_filter,
            sub_out,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    // 2. 主流程：生成数据 -> 调用上述 MetaNode
    let mut main_flow = Workflow::new("Main_With_MetaNode");
    let n_gen = main_flow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.sql".into(),
        config: json!({ "query": "SELECT 'alice' as name, 'IT' as dept UNION ALL SELECT 'bob', 'HR'" }),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: None,
    ..Default::default()
    });
    let n_meta = main_flow.add_node(NodeInstance {
        id: Uuid::new_v4(),
        type_id: "core.metanode".into(),
        config: json!({}),
        state: NodeState::Configured,
        materialize: true,
        sub_workflow: Some(Box::new(cleaner_sub)),
        ..Default::default()
    });
    main_flow
        .add_edge(
            n_gen,
            n_meta,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )
        .unwrap();

    run_and_print_events(&engine, &main_flow, "MetaNode Reuse Demo").await;
}
