use clap::{Parser, Subcommand};
use rustflow::core::engine::SwanFlowEngine;
use rustflow::core::graph::Workflow;
use rustflow::nodes::registry::NodeRegistry;
use rustflow::nodes::NodeEvent;
use std::path::PathBuf;
use std::sync::Arc;

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
    /// Inspect a workflow's inferred schemas without executing it
    Inspect {
        /// Path to the workflow file
        file: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { file } => {
            let workflow = load_workflow(file)?;
            println!(
                "🚀 Loading Workflow: {} (ID: {})",
                workflow.name, workflow.id
            );

            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);

            let mut rx = engine.execute_workflow(&workflow).await?;

            println!("📊 Execution Started...");
            while let Some(event) = rx.recv().await {
                if let NodeEvent::StateChanged { node_id, state } = event {
                    println!("  Node[{}] -> {:?}", node_id, state);
                }
            }
            println!("✅ Execution Finished.");
        }
        Commands::Inspect { file } => {
            let workflow = load_workflow(file)?;
            println!(
                "🔍 Inspecting Workflow: {} (ID: {})",
                workflow.name, workflow.id
            );

            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);

            let metadata = engine.infer_metadata(&workflow).await?;

            for (node_id, node_schemas) in metadata.schemas {
                println!("  Node[{}]:", node_id);
                for (i, schema) in node_schemas.iter().enumerate() {
                    println!(
                        "    Port {}: {:?}",
                        i,
                        schema
                            .fields()
                            .iter()
                            .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                            .collect::<Vec<_>>()
                    );
                }
                if let Some(samples) = metadata.sample_data.get(&node_id) {
                    println!(
                        "    Sample Rows: {}",
                        samples.iter().map(|b| b.num_rows()).sum::<usize>()
                    );
                }
            }
        }
    }

    Ok(())
}

fn load_workflow(file: PathBuf) -> anyhow::Result<Workflow> {
    let extension = file
        .extension()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("File must have .yaml or .json extension"))?;

    let content = std::fs::read_to_string(&file)?;

    let workflow: Workflow = if extension == "yaml" || extension == "yml" {
        serde_yaml::from_str(&content)?
    } else {
        serde_json::from_str(&content)?
    };
    Ok(workflow)
}
