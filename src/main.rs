use clap::{Parser, Subcommand};
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::Workflow;
use swanflow::mcp::client::McpClient;
use swanflow::mcp::server::McpServer;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::NodeEvent;
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
    /// MCP (Model Context Protocol) commands for interacting with MCP servers
    Mcp {
        #[command(subcommand)]
        command: McpCommands,
    },
}

#[derive(Subcommand)]
enum McpCommands {
    /// Start MCP server (stdio-based, for LangFlow / Claude Code / Dify integration)
    Serve,

    /// List tools from an external MCP server
    Tools {
        /// Command to start the MCP server (e.g., "python", "node")
        command: String,

        /// Arguments for the server command
        args: Vec<String>,
    },

    /// Call a tool on an external MCP server
    Call {
        /// Command to start the MCP server (e.g., "python", "node")
        command: String,

        /// Arguments for the server command
        args: Vec<String>,

        /// Name of the tool to call
        tool: String,

        /// JSON arguments for the tool
        tool_args: String,
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
                    let sample_vec: Vec<&datafusion::arrow::record_batch::RecordBatch> =
                        samples.iter().collect();
                    println!(
                        "    Sample Rows: {}",
                        sample_vec.iter().map(|b| b.num_rows()).sum::<usize>()
                    );
                }
                if let Some(diagnostics) = metadata.diagnostics.get(&node_id) {
                    for diagnostic in diagnostics {
                        println!("    Diagnostic: {}", diagnostic);
                    }
                }
            }
        }
        Commands::Mcp { command } => match command {
            McpCommands::Serve => {
                println!("🧩 Starting SwanFlow MCP Server (stdio)...");
                let registry = Arc::new(NodeRegistry::default());
                let engine = SwanFlowEngine::new(registry.clone());
                let server = McpServer::new(engine, registry);
                server.run().await?;
            }
            McpCommands::Tools { command, args } => {
                println!(
                    "🔧 Connecting to MCP server: {} {}",
                    command,
                    args.join(" ")
                );
                let mut client = McpClient::connect(&command, &args).await?;
                let tools = client.list_tools().await?;
                client.close().await?;

                println!("📋 Available tools ({}):", tools.len());
                for tool in &tools {
                    let desc = tool
                        .description
                        .as_deref()
                        .unwrap_or("(no description)");
                    println!("  - {}: {}", tool.name, desc);
                }
            }
            McpCommands::Call {
                command,
                args,
                tool,
                tool_args,
            } => {
                println!(
                    "🔧 Calling tool '{}' on MCP server: {} {}",
                    tool,
                    command,
                    args.join(" ")
                );

                let parsed_args: serde_json::Value =
                    serde_json::from_str(&tool_args).unwrap_or(serde_json::json!({}));

                let mut client = McpClient::connect(&command, &args).await?;
                let result = client.call_tool(&tool, parsed_args).await?;
                client.close().await?;

                println!("📬 Result:");
                println!("{}", serde_json::to_string_pretty(&result)?);
            }
        },
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
