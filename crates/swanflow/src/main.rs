use clap::{Parser, Subcommand};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use swanflow::core::connection_config::SwanFlowConnection;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::core::graph::Workflow;
use swanflow::mcp::client::McpClient;
use swanflow::mcp::server::McpServer;
use swanflow::nodes::registry::NodeRegistry;
use swanflow::nodes::{NodeEvent, NodeState};
use swanflow_knime::KnwfParser;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use uuid::Uuid;

#[derive(Parser)]
#[command(name = "swanflow")]
#[command(about = "SwanFlow: High-performance Rust-native Data Science Engine", long_about = None)]
struct Cli {
    /// Remote host for daemon commands (SSH target, e.g. sa@10.10.10.131)
    #[arg(short = 'r', long, global = true)]
    remote: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a workflow from a YAML or JSON file
    Run {
        /// Path to the workflow file
        file: PathBuf,

        /// Use the legacy step-engine (execute_yaml_workflow) which honors
        /// node mode annotations (loop/conditional/fan_out/collect + sub_workflow)
        #[arg(long)]
        legacy_yaml: bool,
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

    /// Manage database connections (DBX-compatible)
    Connect {
        #[command(subcommand)]
        command: ConnectCommands,
    },

    /// KNIME workflow import and conversion
    Knime {
        #[command(subcommand)]
        command: KnimeCommands,
    },

    /// Plugin management (Herdr-inspired)
    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },

    /// Daemon mode — start a persistent workflow server (Herdr-inspired)
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },

    /// AI Agent commands (generate + human-on-the-loop approval)
    Agent {
        #[command(subcommand)]
        command: AgentCommands,
    },

    /// Import external workflow formats into SwanFlow YAML
    Import {
        #[command(subcommand)]
        command: ImportCommands,
    },

    /// Materialization ledger (ADR-014 Stage 1): list what has been
    /// materialized and what is stale.
    Assets {
        #[command(subcommand)]
        command: AssetsCommands,
    },
}

#[derive(Subcommand)]
enum AssetsCommands {
    /// List recorded materializations (digest + last run time)
    List,
    /// Recompute digest chain for a workflow and classify every node
    /// against the ledger (fresh / stale-config / stale-upstream / never-run)
    Status {
        /// Workflow YAML/JSON file
        file: PathBuf,
    },
}

#[derive(Subcommand)]
enum ImportCommands {
    /// Convert a duckOrch task folder (one *.sql = one task) into a
    /// SwanFlow workflow YAML. Edges come from @inputs/@depends_on and
    /// are auto-inferred from SQL lineage when omitted.
    Duckorch {
        /// Path to the folder containing duckOrch *.sql task files
        dir: PathBuf,
        /// Output workflow YAML path (default: <dir>/<dirname>.yaml)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum AgentCommands {
    /// Review a workflow and approve execution (Human-on-the-Loop gate).
    ///
    /// Loads the workflow, prints a summary, and asks for an explicit
    /// yes/no before running it. Use --yes to skip the prompt (automation),
    /// --dry-run to preview without executing, --explain to also show the
    /// inferred per-node schemas.
    Approve {
        /// Path to the workflow file (.yaml / .json)
        file: PathBuf,
        /// Skip the interactive prompt and approve immediately.
        #[arg(long)]
        yes: bool,
        /// Show the summary and exit without executing.
        #[arg(long)]
        dry_run: bool,
        /// Also print the inferred schemas for each node.
        #[arg(long)]
        explain: bool,
    },

    /// Run the AI AnalystAgent against a natural-language request and emit a
    /// workflow. Uses the Plan→Verify→Fix loop (up to 3 attempts); the result
    /// is saved to YAML so it can be reviewed with `swanflow agent approve`.
    ///
    /// Defaults target the local OpenAI-compatible proxy (15721, key
    /// PROXY_MANAGED); override with --endpoint/--model/--api-key.
    Analyze {
        /// The natural-language analysis request for the agent.
        prompt: String,
        /// LLM model name.
        #[arg(long, default_value = "deepseek-v4-flash")]
        model: String,
        /// OpenAI-compatible endpoint (base URL).
        #[arg(long, default_value = "http://127.0.0.1:15721")]
        endpoint: String,
        /// API key sent as Bearer token.
        #[arg(long, default_value = "PROXY_MANAGED")]
        api_key: String,
        /// Save the generated workflow to this YAML path.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum KnimeCommands {
    /// Parse KNIME workflow and execute it with SwanFlow engine
    Run {
        /// Path to KNIME workflow file (.knwf or .knar)
        input: PathBuf,
        /// Execution engine: datafusion, duckdb, or auto (default: auto)
        #[arg(long, default_value = "auto")]
        engine: String,
        /// Print first 5 rows of the final output table
        #[arg(long)]
        print: bool,
        /// After execution, also push Shaper dashboard to SQLite
        #[arg(long)]
        push_shaper: bool,
        /// Override Shaper SQLite path (used with --push-shaper)
        #[arg(long, default_value_t = default_shaper_db())]
        shaper_db: String,
    },
    /// Convert KNIME workflow (.knwf/.knar) to Shaper SQL dashboard queries
    Shaper {
        /// Path to KNIME workflow file (.knwf or .knar)
        input: PathBuf,
        /// Output SQL file (optional, writes to stdout if omitted)
        #[arg(short = 'o', long)]
        output: Option<PathBuf>,
        /// Push directly to Shaper's SQLite database
        /// (default: ~/.shaper/shaper_internal.sqlite)
        #[arg(long)]
        push: bool,
        /// Override Shaper SQLite path (used with --push)
        #[arg(long, default_value_t = default_shaper_db())]
        shaper_db: String,
    },
    /// Scan KNIME workflow(s) and report factory type coverage
    Scan {
        /// Path(s) to KNIME workflow files (.knwf/.knar) or directories
        inputs: Vec<PathBuf>,
    },
    /// List all KNIME→SwanFlow factory mappings and their compatibility
    ListMappings,
}

#[derive(Subcommand)]
enum ConnectCommands {
    /// Import database connections from ~/.dbx/config/profiles/ (DBX format)
    ImportDbx,

    /// List imported connections
    List,

    /// Show connection details
    Show {
        /// Connection name or ID to show
        name: String,
    },
}

#[derive(Subcommand)]
enum PluginCommands {
    /// List all registered node types (built-in + plugins)
    List,
}

#[derive(Subcommand)]
enum DaemonCommands {
    /// Start the SwanFlow daemon (background workflow server)
    Start {
        /// Custom socket path (default: ~/.swanflow/daemon.sock)
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Enable embedded Quack server so any DuckDB client can ATTACH
        #[arg(long)]
        quack: bool,
        /// Quack server bind address (default: localhost:9494)
        #[arg(long, default_value = "localhost:9494")]
        quack_bind: String,
        /// Quack server auth token (default: auto-generated)
        #[arg(long)]
        quack_token: Option<String>,
        /// Internal: run in foreground (used when forking)
        #[arg(long, hide = true)]
        foreground: bool,
    },
    /// Stop a running SwanFlow daemon
    Stop {
        /// Custom socket path
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Check daemon health (ping)
    Ping {
        /// Custom socket path
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Submit a KNIME workflow to the daemon for background execution
    Submit {
        /// Path to KNIME workflow file (.knwf/.knar)
        file: PathBuf,
        /// Custom socket path
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// List running workflows
    List {
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Show status of a specific workflow
    Status {
        workflow_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Cancel a running workflow
    Cancel {
        workflow_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    /// Stream live events from a running workflow (like Herdr agent attach)
    Attach {
        workflow_id: String,
        #[arg(long)]
        socket: Option<PathBuf>,
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
    // Honor RUST_LOG (default info) so registry auto-registration and engine
    // diagnostics are actually visible — previously all tracing went nowhere
    // because no subscriber was ever installed. Logs go to stderr so stdout
    // stays clean for piped/JSON CLI output.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Commands::Run { file, legacy_yaml } => {
            let workflow = load_workflow(file)?;
            println!(
                "🚀 Loading Workflow: {} (ID: {})",
                workflow.name, workflow.id
            );

            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);

            let mut rx = if legacy_yaml {
                engine.execute_yaml_workflow(&workflow).await?
            } else {
                engine.execute_workflow(&workflow).await?
            };

            println!("📊 Execution Started...");
            let mut failed = false;
            while let Some(event) = rx.recv().await {
                match event {
                    NodeEvent::StateChanged { node_id, state } => {
                        println!("  Node[{}] -> {:?}", node_id, state);
                        if matches!(state, NodeState::Error(_)) {
                            failed = true;
                        }
                    }
                    NodeEvent::Error { message, .. } => {
                        eprintln!("  ❌ Error: {}", message);
                        failed = true;
                    }
                    _ => {}
                }
            }
            if failed {
                println!("❌ Execution Finished with errors.");
                std::process::exit(1);
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
        Commands::Agent { command } => match command {
            AgentCommands::Approve { file, yes, dry_run, explain } => {
                use swanflow::hotl::ApprovalGate;

                let workflow = load_workflow(file)?;
                let registry = Arc::new(NodeRegistry::default());
                let engine = SwanFlowEngine::new(registry);

                // Optional: show inferred schemas before the approval prompt.
                if explain {
                    if let Ok(metadata) = engine.infer_metadata(&workflow).await {
                        println!("📋 Inferred schemas:");
                        for (node_id, node_schemas) in metadata.schemas {
                            println!("  Node[{}]:", node_id);
                            for (i, schema) in node_schemas.iter().enumerate() {
                                let cols: Vec<String> = schema
                                    .fields()
                                    .iter()
                                    .map(|f| format!("{}: {:?}", f.name(), f.data_type()))
                                    .collect();
                                println!("    Port {}: [{}]", i, cols.join(", "));
                            }
                        }
                        if !metadata.diagnostics.is_empty() {
                            let all: Vec<&String> = metadata
                                .diagnostics
                                .values()
                                .flat_map(|v| v.iter())
                                .collect();
                            println!("⚠️  Diagnostics:");
                            for d in all {
                                println!("    - {d}");
                            }
                        }
                        println!();
                    }
                }

                if dry_run {
                    // Preview only: print the summary, never execute, never prompt.
                    println!("🔍 Dry-run summary (no execution):\n");
                    print!("{}", swanflow::hotl::render_summary(&workflow));
                    return Ok(());
                }

                // Interactive (or --yes) approval gate on stdout/stdin.
                let gate = if yes {
                    ApprovalGate::auto_approve()
                } else {
                    ApprovalGate::new()
                };
                let stdin = std::io::stdin();
                let stdout = std::io::stdout();
                let mut lock_in = stdin.lock();
                let mut lock_out = stdout.lock();
                let approved = gate.review(&workflow, &mut lock_in, &mut lock_out)?;
                drop(lock_in);
                drop(lock_out);

                if !approved {
                    println!("❌ Execution cancelled by human reviewer.");
                    return Ok(());
                }

                println!("✅ Approved — executing workflow: {} ({})", workflow.name, workflow.id);
                let mut rx = engine.execute_workflow(&workflow).await?;
                while let Some(event) = rx.recv().await {
                    if let NodeEvent::StateChanged { node_id, state } = event {
                        println!("  Node[{}] -> {:?}\n", node_id, state);
                    }
                }
                println!("✅ Execution Finished.");
            }
            AgentCommands::Analyze { prompt, model, endpoint, api_key, out } => {
                use swanflow::agent::AnalystAgent;
                use swanflow::nodes::ai::providers::{LlmConfig, OpenAiProvider, ProviderKind};

                // The local proxy port drifts (15721↔15722) as providers are
                // toggled; auto-fallback keeps the default command working.
                let endpoint = probe_local_llm_endpoint(&endpoint);

                println!("🤖 AnalystAgent — running Plan→Verify→Fix loop...\n");
                let config = LlmConfig {
                    provider: ProviderKind::OpenAI,
                    api_key: Some(api_key),
                    model,
                    endpoint: Some(endpoint),
                };
                let provider = OpenAiProvider::new(config)?;
                let registry = Arc::new(NodeRegistry::default());
                let engine = Arc::new(SwanFlowEngine::new(registry.clone()));
                let agent = AnalystAgent::new(Arc::new(provider), registry, engine);

                let workflow = agent.run_analysis(&prompt).await?;

                // Show the result.
                println!("\n📋 Generated workflow:\n");
                print!("{}", swanflow::hotl::render_summary(&workflow));

                // Persist to YAML so it can be reviewed/approved via `agent approve`.
                let path = out.unwrap_or_else(|| PathBuf::from("agent_workflow.yaml"));
                let yaml = serde_yaml::to_string(&workflow)?;
                std::fs::write(&path, &yaml)?;
                println!("\n💾 Saved to: {}", path.display());
                println!("   Next: swanflow agent approve {}", path.display());
            }
        },
        Commands::Import { command } => match command {
            ImportCommands::Duckorch { dir, output } => {
                use swanflow::nodes::import_duckorch as orch;

                println!("🦆 Importing duckOrch tasks from: {}", dir.display());
                let report = orch::import_folder(&dir)?;
                print!("{}", report);

                // Build the workflow: one core.sql.duckdb node per task,
                // edges from inputs/depends_on (task-name resolution).
                use petgraph::stable_graph::StableGraph;
                use swanflow_core::graph::{EdgeInstance, NodeInstance, Workflow};
                use swanflow_core::nodes::NodeState;

                let name_by_table: std::collections::HashMap<String, String> = report
                    .tasks
                    .iter()
                    .flat_map(|t| t.outputs.iter().map(|o| (o.clone(), t.name.clone())))
                    .collect();

                let mut graph: StableGraph<NodeInstance, EdgeInstance> = StableGraph::new();
                let mut idx_by_task: std::collections::HashMap<String, petgraph::graph::NodeIndex> =
                    std::collections::HashMap::new();

                for t in &report.tasks {
                    let idx = graph.add_node(NodeInstance {
                        id: orch::task_node_id(&t.name),
                        type_id: "core.sql.duckdb".to_string(),
                        config: serde_json::json!({ "query": t.sql }),
                        state: NodeState::Configured,
                        ..Default::default()
                    });
                    idx_by_task.insert(t.name.clone(), idx);
                }

                let mut added = std::collections::BTreeSet::<(String, String)>::new();
                let mut add_edge = |from: &str, to: &str,
                                    idx_by_task: &std::collections::HashMap<
                    String,
                    petgraph::graph::NodeIndex,
                >,
                                    graph: &mut StableGraph<NodeInstance, EdgeInstance>,
                                    added: &mut std::collections::BTreeSet<(String, String)>| {
                    if from == to {
                        return;
                    }
                    if let (Some(&a), Some(&b)) = (idx_by_task.get(from), idx_by_task.get(to)) {
                        if added.insert((from.to_string(), to.to_string())) {
                            graph.add_edge(
                                a,
                                b,
                                EdgeInstance { source_port: 0, target_port: 0 },
                            );
                        }
                    }
                };

                for t in &report.tasks {
                    let upstream_names: Vec<String> = t
                        .inputs
                        .iter()
                        .filter_map(|i| name_by_table.get(i).cloned())
                        .chain(t.depends_on.iter().cloned())
                        .collect();
                    for up in upstream_names {
                        add_edge(&up, &t.name, &idx_by_task, &mut graph, &mut added);
                    }
                    if t.inputs.is_empty() {
                        if let Some(inf) = report.inferred_inputs.get(&t.name) {
                            for up in inf {
                                add_edge(up, &t.name, &idx_by_task, &mut graph, &mut added);
                            }
                        }
                    }
                }

                let workflow = Workflow {
                    id: uuid::Uuid::new_v4(),
                    name: dir
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("duckorch-import")
                        .to_string(),
                    context: Default::default(),
                    graph,
                };

                let out_path = output.unwrap_or_else(|| {
                    dir.clone()
                });
                let out_path = if out_path.is_dir() {
                    let stem = dir
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("duckorch-import");
                    out_path.join(format!("{}.yaml", stem))
                } else {
                    out_path
                };
                let yaml = serde_yaml::to_string(&workflow)?;
                std::fs::write(&out_path, &yaml)?;
                println!("\n💾 Workflow saved to: {}", out_path.display());
                println!("   Next: swanflow inspect {}", out_path.display());
            }
        },
        Commands::Assets { command } => match command {
            AssetsCommands::List => {
                use swanflow::core::engine::assets::AssetLedger;

                let cache_dir = std::path::PathBuf::from("target/swanflow_cache");
                let ledger_path = cache_dir.join("assets.duckdb");
                if !ledger_path.exists() {
                    println!("📭 No materialization ledger yet ({} absent).", ledger_path.display());
                    println!("   Run a workflow to populate it (ADR-014 Stage 1).");
                    return Ok(());
                }
                let ledger = AssetLedger::open(&cache_dir)?;
                let rows = ledger.all()?;
                if rows.is_empty() {
                    println!("📭 Ledger is empty.");
                    return Ok(());
                }
                println!("📦 Materialization ledger ({}):", ledger_path.display());
                for (node_id, digest, at) in rows {
                    println!("  {}  digest {}  last run {}", node_id, &digest[..12], at);
                }
            }
            AssetsCommands::Status { file } => {
                use swanflow::core::engine::assets::{status_for_workflow, AssetLedger};

                let workflow = load_workflow(file)?;
                println!("🔎 Asset status for workflow: {} (ID: {})", workflow.name, workflow.id);

                let cache_dir = std::path::PathBuf::from("target/swanflow_cache");
                let ledger_path = cache_dir.join("assets.duckdb");
                if !ledger_path.exists() {
                    println!("📭 No materialization ledger yet ({} absent).", ledger_path.display());
                    println!("   Every node would be: never-run.");
                    return Ok(());
                }
                let ledger = AssetLedger::open(&cache_dir)?;
                let rows = status_for_workflow(&ledger, &workflow)?;
                let fresh = rows.iter().filter(|r| r.2 == swanflow::core::engine::assets::AssetVerdict::Fresh).count();
                println!("{} node(s): {} fresh, {} action needed\n", rows.len(), fresh, rows.len() - fresh);
                for (node_id, type_id, verdict) in rows {
                    let marker = match verdict {
                        swanflow::core::engine::assets::AssetVerdict::Fresh => "✅",
                        swanflow::core::engine::assets::AssetVerdict::NeverRun => "🌑",
                        _ => "⚠️ ",
                    };
                    println!("  {} {} [{}] {}", marker, node_id, verdict.label(), type_id);
                }
            }
        },
        Commands::Mcp { command } => match command {
            McpCommands::Serve => {
                // stdout is the JSON-RPC transport — all diagnostics to stderr,
                // or real MCP clients cannot parse our frames.
                eprintln!("Starting SwanFlow MCP Server (stdio)...");
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
                    let desc = tool.description.as_deref().unwrap_or("(no description)");
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
        Commands::Knime { command } => match command {
            KnimeCommands::Run { input, engine, print, push_shaper, shaper_db } => {
                println!("🧩 Parsing KNIME workflow: {:?}", input);
                let archive = KnwfParser::parse_file(&input)?;
                println!(
                    "   Nodes: {}  Edges: {}  Metanodes: {}",
                    archive.nodes.len(),
                    archive.edges.len(),
                    archive.metanodes.len(),
                );

                let core_wf = swanflow_knime::import_knwf(
                    input.to_str().unwrap_or("unknown"),
                )
                .await?;

                // Convert swanflow_core::Workflow → swanflow::Workflow via JSON
                let json = serde_json::to_value(&core_wf)?;
                let workflow: Workflow = serde_json::from_value(json)?;

                println!(
                    "🚀 Executing Workflow: {} ({} nodes) [engine={}]",
                    workflow.name,
                    workflow.graph.node_count(),
                    engine,
                );

                let registry = Arc::new(NodeRegistry::default());
                let mut swan_engine = SwanFlowEngine::new(registry);

                // Configure engine mode
                if engine == "duckdb" {
                    use swanflow::core::engine::DuckDBEngine;
                    let duckdb = DuckDBEngine::new_in_memory()
                        .map_err(|e| anyhow::anyhow!("DuckDB: {}", e))?;
                    swan_engine = swan_engine.with_engine(Arc::new(duckdb));
                } else if engine == "datafusion" {
                    swan_engine = swan_engine
                        .with_engine_mode(swanflow::core::engine::EngineMode::DataFusion);
                }
                // "auto" is default — no explicit mode needed

                let mut rx = swan_engine.execute_workflow(&workflow).await?;

                let mut executed = 0;
                let mut errors = 0;
                while let Some(event) = rx.recv().await {
                    match &event {
                        NodeEvent::StateChanged { node_id, state } => {
                            let short_id = &node_id.to_string()[..8];
                            match state {
                                swanflow::nodes::NodeState::Pending => {
                                    println!("  ⏳ Node[{}] Pending", short_id)
                                }
                                swanflow::nodes::NodeState::Running => {
                                    println!("  ▶  Node[{}] Running", short_id)
                                }
                                swanflow::nodes::NodeState::Waiting => {
                                    println!("  ⏸  Node[{}] Waiting (external I/O)", short_id)
                                }
                                swanflow::nodes::NodeState::Executed => {
                                    executed += 1;
                                    println!("  ✅ Node[{}] Executed", short_id)
                                }
                                swanflow::nodes::NodeState::Error(e) => {
                                    println!("  ❌ Node[{}] Error: {}", short_id, e)
                                }
                                _ => {}
                            }
                        }
                        NodeEvent::Error { node_id, message } => {
                            errors += 1;
                            println!("  ❌ Node[{}]: {}", &node_id.to_string()[..8], message);
                        }
                        NodeEvent::Materialized { node_id, table_name, .. } => {
                            println!(
                                "  💾 Node[{}] → table `{}`",
                                &node_id.to_string()[..8],
                                table_name
                            );
                        }
                        _ => {}
                    }
                }
                println!(
                    "✅ Done — {} executed, {} errors",
                    executed, errors
                );

                if print {
                    // Read the last materialized node's output from metadata DB
                    let cache_dir = "target/swanflow_cache";
                    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
                    if let Ok(conn) = duckdb::Connection::open(&metadata_path) {
                        let mut stmt = conn
                            .prepare("SELECT node_id, table_name, parquet_path FROM flow_metadata ORDER BY rowid DESC LIMIT 1")
                            .ok();
                        if let Some(stmt) = stmt.as_mut() {
                            if let Ok(rows) = stmt.query_map([], |row| {
                                Ok((
                                    row.get::<_, String>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, String>(2)?,
                                ))
                            }) {
                                for row in rows.flatten() {
                                    let (_node_id, table_name, parquet_path) = row;
                                    if !parquet_path.is_empty() {
                                        println!("\n📋 Output table: `{}`", table_name);
                                        // Read parquet with DuckDB and print first 5 rows
                                        if let Ok(pconn) = duckdb::Connection::open_in_memory() {
                                            if pconn.execute_batch(&format!(
                                                "CREATE VIEW output AS SELECT * FROM '{}';",
                                                parquet_path
                                            )).is_ok() {
                                                if let Ok(mut stmt) = pconn.prepare("SELECT * FROM output LIMIT 5") {
                                                    if let Ok(mut rows) = stmt.query([]) {
                                                        // Get columns from first row
                                                        let mut first = true;
                                                        while let Ok(Some(r)) = rows.next() {
                                                            let ncols = r.as_ref().column_count();
                                                            let vals: Vec<String> = (0..ncols)
                                                                .map(|i| {
                                                                    match r.get_ref(i) {
                                                                        Ok(duckdb::types::ValueRef::Null) => "NULL".into(),
                                                                        Ok(duckdb::types::ValueRef::Boolean(b)) => b.to_string(),
                                                                        Ok(duckdb::types::ValueRef::TinyInt(n)) => n.to_string(),
                                                                        Ok(duckdb::types::ValueRef::SmallInt(n)) => n.to_string(),
                                                                        Ok(duckdb::types::ValueRef::Int(n)) => n.to_string(),
                                                                        Ok(duckdb::types::ValueRef::BigInt(n)) => n.to_string(),
                                                                        Ok(duckdb::types::ValueRef::Float(f)) => format!("{:.4}", f),
                                                                        Ok(duckdb::types::ValueRef::Double(f)) => format!("{:.4}", f),
                                                                        Ok(duckdb::types::ValueRef::Text(s)) => {
                                                                            String::from_utf8_lossy(s).into_owned()
                                                                        }
                                                                        Ok(other) => format!("{:?}", other),
                                                                        Err(_) => "NULL".into(),
                                                                    }
                                                                })
                                                                .collect();
                                                            if first {
                                                                // Use column indices as headers
                                                                let headers: Vec<String> = (0..ncols)
                                                                    .map(|i| format!("col{}", i))
                                                                    .collect();
                                                                println!("  {}", headers.join(" | "));
                                                                println!("  {}", "-".repeat(headers.join(" | ").len()));
                                                                first = false;
                                                            }
                                                            println!("  {}", vals.join(" | "));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        println!(
                                            "\n📋 Output materialized as in-memory table `{}`",
                                            table_name
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                if push_shaper {
                    if errors > 0 {
                        println!("\n⚠️  {} execution errors — pushing dashboard anyway", errors);
                    }
                    println!("\n📊 Generating Shaper dashboard...");
                    let queries = swanflow_knime::shaper::generate_shaper_queries(&archive);
                    if queries.is_empty() {
                        println!("   No visualization nodes found — nothing pushed.");
                    } else {
                        let db = duckdb::Connection::open_in_memory()
                            .map_err(|e| anyhow::anyhow!("DuckDB: {}", e))?;
                        db.execute_batch("INSTALL sqlite; LOAD sqlite;")
                            .map_err(|e| anyhow::anyhow!("sqlite ext: {}", e))?;
                        db.execute_batch(&format!(
                            "ATTACH '{}' AS shaper (TYPE SQLITE);",
                            shaper_db
                        ))?;

                        let content: String = queries
                            .iter()
                            .map(|q| format!("-- [{}] {}\n{};\n", q.chart_type, q.node_name, q.sql))
                            .collect();
                        let id = uuid::Uuid::new_v4().simple().to_string();
                        let name = format!(
                            "SwanFlow {}",
                            archive.workflow_meta.name.as_deref().unwrap_or("Untitled")
                        );

                        db.execute(
                            "INSERT INTO shaper.apps (id, name, content, created_at, updated_at, type, visibility)
                             VALUES (?, ?, ?, now(), now(), 'dashboard', 'private')",
                            duckdb::params![id, name, content],
                        )?;

                        println!(
                            "✅ Pushed dashboard '{}' to Shaper SQLite ({} charts)",
                            name,
                            queries.len()
                        );
                    }
                }
            }
            KnimeCommands::Scan { inputs } => {
                println!("🔍 Scanning {} workflow(s) for factory types...", inputs.len());
                let mut all_factories: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
                for path in &inputs {
                    if let Ok(archive) = KnwfParser::parse_file(path) {
                        for node in &archive.nodes {
                            let factory = node.node_type.as_deref().unwrap_or("unknown");
                            *all_factories.entry(factory.to_string()).or_default() += 1;
                        }
                    }
                }
                println!("  Unique factory types: {}", all_factories.len());
                let mut sorted: Vec<_> = all_factories.into_iter().collect();
                sorted.sort_by(|a, b| b.1.cmp(&a.1));
                for (factory, count) in sorted.iter().take(20) {
                    println!("  {} ({} occurrences)", factory, count);
                }
            }
            KnimeCommands::ListMappings => {
                use swanflow_knime::KnimeNodeMapper;
                let mapper = KnimeNodeMapper::new();
                let mappings = mapper.list_all();
                println!("\"factory\",\"swanflow_type\",\"category\",\"compat\"");
                for m in &mappings {
                    println!("\"{}\",\"{}\",\"{}\",\"{}\"", m.factory, m.sw_type, m.category, m.compat);
                }
            }
            KnimeCommands::Shaper {
                input,
                output,
                push,
                shaper_db,
            } => {
                println!("\u{1f9e9} Parsing KNIME workflow: {:?}", input);
                let archive = KnwfParser::parse_file(&input)?;
                println!(
                    "   Nodes: {}  Edges: {}  Metanodes: {}",
                    archive.nodes.len(),
                    archive.edges.len(),
                    archive.metanodes.len(),
                );

                if push {
                    let queries = swanflow_knime::shaper::generate_shaper_queries(&archive);
                    if queries.is_empty() {
                        println!("   No visualization nodes found — nothing pushed.");
                    } else {
                        // Use DuckDB to ATTACH Shaper's SQLite and INSERT directly
                        let db = duckdb::Connection::open_in_memory()
                            .map_err(|e| anyhow::anyhow!("DuckDB: {}", e))?;
                        db.execute_batch("INSTALL sqlite; LOAD sqlite;")
                            .map_err(|e| anyhow::anyhow!("sqlite ext: {}", e))?;
                        db.execute_batch(&format!(
                            "ATTACH '{}' AS shaper (TYPE SQLITE);",
                            shaper_db
                        ))?;

                        let content: String = queries
                            .iter()
                            .map(|q| format!("-- [{}] {}\n{};\n", q.chart_type, q.node_name, q.sql))
                            .collect();
                        let id = uuid::Uuid::new_v4().simple().to_string();
                        let name = format!(
                            "SwanFlow {}",
                            archive.workflow_meta.name.as_deref().unwrap_or("Untitled")
                        );

                        db.execute(
                            "INSERT INTO shaper.apps (id, name, content, created_at, updated_at, type, visibility)
                             VALUES (?, ?, ?, now(), now(), 'dashboard', 'private')",
                            duckdb::params![id, name, content],
                        )?;

                        println!(
                            "✅ Pushed dashboard '{}' to Shaper SQLite",
                            name
                        );
                    }
                } else {
                    let queries = swanflow_knime::shaper::generate_shaper_queries(&archive);
                    if queries.is_empty() {
                        println!("   No visualization nodes found in this workflow.");
                    } else {
                        println!("   Generated {} Shaper queries.", queries.len());
                        if let Some(out) = output {
                            let out_path = out.display().to_string();
                            swanflow_knime::shaper::write_shaper_sql(&archive, &out_path)?;
                            println!("✅ SQL written to {:?}", out);
                        } else {
                            for q in &queries {
                                println!("\n-- [{}] {}", q.chart_type, q.node_name);
                                println!("{};", q.sql);
                            }
                        }
                    }
                }
            }
        },
        Commands::Connect { command } => match command {
            ConnectCommands::ImportDbx => {
                println!("🔌 Importing DBX profiles from ~/.dbx/config/profiles/...\n");
                let configs = SwanFlowConnection::load_dbx_profiles()?;
                if configs.is_empty() {
                    println!("  No DBX profiles found. (Is ~/.dbx/config/profiles/ empty?)");
                } else {
                    println!("  Found {} connection(s):\n", configs.len());
                    for cfg in &configs {
                        let port_str = if cfg.port > 0 {
                            format!(":{}", cfg.port)
                        } else {
                            String::new()
                        };
                        println!("  ├─ {} ({})", cfg.name, cfg.db_type);
                        println!(
                            "  │  {}@{}{}/{}",
                            cfg.user,
                            cfg.host,
                            port_str,
                            cfg.database.as_deref().unwrap_or("?")
                        );
                        let cc = cfg.to_conn_config();
                        if let Ok(cc) = cc {
                            println!("  │  URL: {}", cc.url);
                        }
                        println!();
                    }
                    println!("✅ To use a connection, reference it in your workflow YAML.");
                }
            }
            ConnectCommands::List => {
                let configs = SwanFlowConnection::load_dbx_profiles()?;
                if configs.is_empty() {
                    println!("No connections found.");
                } else {
                    println!("Available connections:\n");
                    for cfg in &configs {
                        println!(
                            "  {} ({}) — {}@{}",
                            cfg.name, cfg.db_type, cfg.user, cfg.host
                        );
                    }
                }
            }
            ConnectCommands::Show { name } => {
                let configs = SwanFlowConnection::load_dbx_profiles()?;
                let cfg = configs.iter().find(|c| c.name == name || c.id == name);
                match cfg {
                    Some(c) => {
                        println!("Connection: {}", c.name);
                        println!("  Type:     {}", c.db_type);
                        println!("  Host:     {}", c.host);
                        println!("  Port:     {}", c.port);
                        println!("  User:     {}", c.user);
                        println!("  Database: {}", c.database.as_deref().unwrap_or("(none)"));
                        println!("  SSL:      {}", c.ssl);
                        println!(
                            "  Timeout:  {}s connect / {}s query",
                            c.connect_timeout, c.query_timeout
                        );
                        println!("  Read-only: {}", c.read_only);
                    }
                    None => println!("Connection '{}' not found.", name),
                }
            }
        },
        Commands::Daemon { command } => {
            handle_daemon_command(command, cli.remote).await?;
        }
        Commands::Plugin { command } => match command {
            PluginCommands::List => {
                let registry = NodeRegistry::default();
                let specs = registry.list_specs();
                println!("📦 SwanFlow Node Registry — {} types\n", specs.len());
                for spec in &specs {
                    println!("  {} ({})", spec.type_id, spec.display_name);
                }
            }
        },
    }

    Ok(())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Daemon client helper — talk to the SwanFlow daemon over Unix socket
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn handle_daemon_command(cmd: DaemonCommands, remote: Option<String>) -> anyhow::Result<()> {
    if let Some(ref host) = remote {
        return handle_remote_daemon(cmd, host).await;
    }

    match cmd {
        DaemonCommands::Start { socket, quack, quack_bind, quack_token, foreground } => {
            if foreground {
                // Forked child — run daemon in foreground
                let registry = Arc::new(NodeRegistry::default());
                let engine = SwanFlowEngine::new(registry);
                let mut daemon = swanflow::core::engine::daemon::Daemon::new(engine);
                if let Some(path) = socket {
                    daemon = daemon.with_socket_path(path);
                }
                if quack {
                    let mut qc = swanflow::core::engine::daemon::QuackConfig::default();
                    qc.bind = quack_bind;
                    if let Some(t) = quack_token {
                        qc.token = t;
                    }
                    daemon = daemon.with_quack(qc);
                }
                daemon.start().await
                    .map_err(|e| anyhow::anyhow!("Daemon error: {}", e))?;
                return Ok(());
            }

            let socket_path = socket.clone()
                .unwrap_or_else(swanflow::core::engine::daemon::default_socket_path);
            let pid_path = swanflow::core::engine::daemon::pid_path();

            // Fork to background
            let exe = std::env::current_exe()?;
            let mut child = std::process::Command::new(exe);
            child.arg("daemon")
                .arg("start")
                .arg("--foreground")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .stdin(std::process::Stdio::null());
            if let Some(ref p) = socket {
                child.arg("--socket").arg(p);
            }
            if quack {
                child.arg("--quack").arg("--quack-bind").arg(&quack_bind);
                if let Some(ref t) = quack_token {
                    child.arg("--quack-token").arg(t);
                }
            }
            let mut child = child.spawn()?;
            let pid = child.id();
            // Don't wait — let it run
            std::thread::sleep(std::time::Duration::from_millis(500));

            // Check if daemon is alive
            match tokio::net::UnixStream::connect(&socket_path).await {
                Ok(_) => {
                    println!("✅ SwanFlow daemon started (PID: {})", pid);
                    println!("   Socket: {}", socket_path.display());
                    if quack {
                        let token_display = quack_token.as_deref().unwrap_or("<auto-generated>");
                        println!("   Quack:  quack://{}/ (token: {})", quack_bind, token_display);
                    }
                    println!("   Submit: swanflow daemon submit <file>");
                    println!("   Attach: swanflow daemon attach <workflow_id>");
                }
                Err(_) => {
                    println!("⚠ Daemon process started (PID: {}) but not yet ready. Try 'swanflow daemon ping'.", pid);
                }
            }
            Ok(())
        }
        DaemonCommands::Stop { socket } => {
            // Try to kill by PID first
            let pid_path = swanflow::core::engine::daemon::pid_path();
            if let Ok(pid_str) = std::fs::read_to_string(&pid_path) {
                if let Ok(pid) = pid_str.trim().parse::<i32>() {
                    let _ = std::process::Command::new("kill")
                        .arg(pid.to_string())
                        .output();
                    let _ = std::fs::remove_file(&pid_path);
                }
            }
            // Also try socket stop as fallback
            let _ = send_daemon_request(&socket, "daemon.stop", serde_json::json!({})).await;
            println!("🛑 Daemon stopped.");
            Ok(())
        }
        DaemonCommands::Ping { socket } => {
            send_daemon_request(&socket, "daemon.ping", serde_json::json!({})).await
        }
        DaemonCommands::Submit { file, socket } => {
            let path = file.to_string_lossy().to_string();
            let resp = send_daemon_raw(&socket, "workflow.submit", serde_json::json!({"path": path})).await?;
            println!("{}", resp);
            Ok(())
        }
        DaemonCommands::List { socket } => {
            let resp = send_daemon_raw(&socket, "workflow.list", serde_json::json!({})).await?;
            println!("{}", resp);
            Ok(())
        }
        DaemonCommands::Status { workflow_id, socket } => {
            let resp = send_daemon_raw(&socket, "workflow.status", serde_json::json!({"workflow_id": workflow_id})).await?;
            println!("{}", resp);
            Ok(())
        }
        DaemonCommands::Cancel { workflow_id, socket } => {
            let resp = send_daemon_raw(&socket, "workflow.cancel", serde_json::json!({"workflow_id": workflow_id})).await?;
            println!("{}", resp);
            Ok(())
        }
        DaemonCommands::Attach { workflow_id, socket } => {
            attach_workflow(&socket, &workflow_id).await
        }
    }
}

async fn send_daemon_request(
    socket_override: &Option<PathBuf>,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<()> {
    let resp = send_daemon_raw(socket_override, method, params).await?;
    let v: serde_json::Value = serde_json::from_str(&resp)?;
    if v.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        println!("✅ {}", method);
        Ok(())
    } else {
        let err = v.get("error").and_then(|v| v.as_str()).unwrap_or("unknown");
        anyhow::bail!("❌ {} failed: {}", method, err)
    }
}

async fn send_daemon_raw(
    socket_override: &Option<PathBuf>,
    method: &str,
    params: serde_json::Value,
) -> anyhow::Result<String> {
    let socket_path = socket_override
        .clone()
        .unwrap_or_else(swanflow::core::engine::daemon::default_socket_path);

    let stream = tokio::net::UnixStream::connect(&socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = tokio::io::BufReader::new(reader);

    let req = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "method": method,
        "params": params,
    });
    writer
        .write_all(serde_json::to_string(&req)?.as_bytes())
        .await?;
    writer.write_all(b"\n").await?;
    writer.shutdown().await?;

    let mut resp = String::new();
    tokio::io::AsyncReadExt::read_to_string(&mut buf_reader, &mut resp).await?;
    Ok(resp.trim().to_string())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Remote daemon — SSH wrapper (Herdr --remote inspired)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn handle_remote_daemon(cmd: DaemonCommands, host: &str) -> anyhow::Result<()> {
    use std::process::Command;

    match cmd {
        DaemonCommands::Start { .. } => {
            let status = Command::new("ssh").arg(host)
                .arg("swanflow").arg("daemon").arg("start")
                .status()?;
            anyhow::ensure!(status.success(), "Remote daemon start failed");
        }
        DaemonCommands::Stop { .. } => {
            let status = Command::new("ssh").arg(host)
                .arg("swanflow").arg("daemon").arg("stop")
                .status()?;
        }
        DaemonCommands::Ping { .. } => {
            run_ssh(host, &["swanflow", "daemon", "ping"])?;
        }
        DaemonCommands::List { .. } => {
            run_ssh(host, &["swanflow", "daemon", "list"])?;
        }
        DaemonCommands::Status { workflow_id, .. } => {
            run_ssh(host, &["swanflow", "daemon", "status", &workflow_id])?;
        }
        DaemonCommands::Cancel { workflow_id, .. } => {
            run_ssh(host, &["swanflow", "daemon", "cancel", &workflow_id])?;
        }
        DaemonCommands::Submit { file, .. } => {
            let fname = file.file_name().and_then(|s| s.to_str()).unwrap_or("workflow.knwf");
            let remote_path = format!("/tmp/{}", fname);
            // SCP the file
            let status = Command::new("scp")
                .arg(file.to_string_lossy().as_ref())
                .arg(format!("{}:{}", host, remote_path))
                .status()?;
            anyhow::ensure!(status.success(), "SCP upload failed");
            // Submit on remote
            run_ssh(host, &["swanflow", "daemon", "submit", &remote_path])?;
        }
        DaemonCommands::Attach { workflow_id, .. } => {
            // Use ssh -t for PTY — live streaming needs terminal
            let status = Command::new("ssh").arg("-t").arg(host)
                .arg("swanflow").arg("daemon").arg("attach").arg(&workflow_id)
                .status()?;
            anyhow::ensure!(status.success(), "Remote attach failed");
        }
    }
    Ok(())
}

/// Probe a list of local LLM proxy ports and return the first one that
/// accepts a TCP connection. Returns the given default if none are up.
///
/// The local cc-switch proxy is known to drift between 15720-15722 as the
/// user toggles providers, so a hard-coded default can go stale. This keeps
/// `swanflow agent analyze` working with no flags.
fn probe_local_llm_endpoint(default: &str) -> String {
    // Only auto-probe for clearly-local endpoints; remote hosts are used as-is.
    let host = default
        .strip_prefix("http://")
        .or_else(|| default.strip_prefix("https://"))
        .unwrap_or(default)
        .split('/')
        .next()
        .unwrap_or(default)
        .split(':')
        .next()
        .unwrap_or(default);
    if !matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]") {
        return default.to_string();
    }
    let scheme = if default.starts_with("https") { "https" } else { "http" };

    // Candidate ports: the requested one FIRST, then the known cc-switch drift
    // set (it moves between these as providers are toggled).
    let mut candidates: Vec<u16> = Vec::new();
    if let Some(port_str) = default.rsplit_once(':').map(|(_, p)| p) {
        if let Ok(p) = port_str.parse::<u16>() {
            candidates.push(p);
        }
    }
    for p in [15721u16, 15722, 15720, 15723] {
        if !candidates.contains(&p) {
            candidates.push(p);
        }
    }

    for port in candidates {
        if std::net::TcpStream::connect((host, port)).is_ok() {
            let chosen = format!("{}://{}:{}", scheme, host, port);
            if chosen != default {
                eprintln!(
                    "[agent] note: {} down; using live local proxy {}",
                    default, chosen
                );
            }
            return chosen;
        }
    }
    default.to_string()
}

fn run_ssh(host: &str, args: &[&str]) -> anyhow::Result<()> {
    let mut cmd = std::process::Command::new("ssh");
    cmd.arg(host);
    for a in args { cmd.arg(a); }
    let output = cmd.output()?;
    if !output.stdout.is_empty() {
        println!("{}", String::from_utf8_lossy(&output.stdout));
    }
    if !output.stderr.is_empty() {
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    }
    anyhow::ensure!(output.status.success(), "SSH command failed");
    Ok(())
}

#[cfg(test)]
mod main_tests {
    use super::*;

    #[test]
    fn probe_remote_endpoint_is_returned_verbatim() {
        // Remote endpoints must never be probed/rewritten.
        let remote = "https://api.openai.com/v1";
        assert_eq!(probe_local_llm_endpoint(remote), remote);
        let remote2 = "http://10.0.0.5:8080/v1";
        assert_eq!(probe_local_llm_endpoint(remote2), remote2);
    }

    #[test]
    fn probe_keeps_a_live_requested_local_port() {
        // Bind a real listener on an ephemeral port, request it, and expect
        // it to be returned unchanged (requested local port up → used first).
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let req = format!("http://127.0.0.1:{}", port);
        // Keep the listener alive for the duration of the probe.
        let out = probe_local_llm_endpoint(&req);
        assert_eq!(out, req);
        drop(listener);
    }

    #[test]
    fn probe_is_a_noop_for_non_local_hosts() {
        // A non-local host string with a port is untouched even if the port
        // would be in the drift set.
        let out = probe_local_llm_endpoint("http://my-proxy:15721");
        assert_eq!(out, "http://my-proxy:15721");
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Workflow attach — stream live events from daemon (Herdr-style)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn attach_workflow(socket_override: &Option<PathBuf>, workflow_id: &str) -> anyhow::Result<()> {
    let socket_path = socket_override
        .clone()
        .unwrap_or_else(swanflow::core::engine::daemon::default_socket_path);

    let stream = tokio::net::UnixStream::connect(&socket_path).await?;
    let (reader, mut writer) = stream.into_split();
    let mut lines = tokio::io::BufReader::new(reader).lines();

    let req = serde_json::json!({
        "id": uuid::Uuid::new_v4().to_string(),
        "method": "workflow.events",
        "params": {"workflow_id": workflow_id},
    });
    writer.write_all(serde_json::to_string(&req)?.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    writer.shutdown().await?;

    println!("📡 Attached to workflow {}. Streaming events...\n", workflow_id);
    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() { continue; }
        if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
            let evt_type = event["event"].as_str().unwrap_or("");
            match evt_type {
                "workflow.subscribed" => continue,
                "workflow.status" => {
                    let name = event["name"].as_str().unwrap_or("");
                    let state = event["state"].as_str().unwrap_or("");
                    let done = event["nodes_done"].as_u64().unwrap_or(0);
                    let total = event["nodes_total"].as_u64().unwrap_or(0);
                    let errs = event["nodes_error"].as_u64().unwrap_or(0);
                    let icon = match state { "running" => "▶", "completed" => "✅", "error" => "❌", "cancelled" => "⏹", _ => "·" };
                    println!("  {} [{}/{}] {} — {} done, {} errors", icon, done, total, name, done, errs);
                }
                "workflow.stream_end" => {
                    println!("\n✅ Workflow stream ended.");
                    break;
                }
                "workflow.not_found" => {
                    println!("❌ Workflow not found.");
                    break;
                }
                _ => {}
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

fn default_shaper_db() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".into());
    format!("{home}/.shaper/shaper_internal.sqlite")
}
