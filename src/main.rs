use std::path::PathBuf;
use std::sync::Arc;
use clap::{Parser, Subcommand};
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::Workflow;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeEvent;

#[derive(Parser)]
#[command(name = "swanflow")]
#[command(about = "SwanFlow: High-performance Rust-native Data Science Engine", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a workflow from a YAML or JSON file
    Run {
        /// Path to the workflow file
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { file } => {
            let extension = file.extension()
                .and_then(|s| s.to_str())
                .ok_or_else(|| anyhow::anyhow!("File must have .yaml or .json extension"))?;

            let content = std::fs::read_to_string(&file)?;
            
            // 1. 从文件反序列化工作流
            let workflow: Workflow = if extension == "yaml" || extension == "yml" {
                serde_yaml::from_str(&content)?
            } else {
                serde_json::from_str(&content)?
            };

            println!("🚀 Loading Workflow: {} (ID: {})", workflow.name, workflow.id);

            // 2. 初始化引擎和算子注册中心
            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);

            // 3. 执行
            let mut rx = engine.execute_workflow(&workflow).await?;

            println!("📊 Execution Started...");
            while let Some(event) = rx.recv().await {
                if let NodeEvent::StateChanged { node_id, state } = event {
                    println!("  Node[{}] -> {:?}", node_id, state);
                }
            }
            println!("✅ Execution Finished.");
        }
    }

    Ok(())
}
