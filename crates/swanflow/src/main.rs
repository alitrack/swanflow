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
use swanflow::nodes::NodeEvent;
use swanflow_knime::KnwfParser;
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

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

    /// Daemon mode — start a persistent workflow server (Herdr-inspired)
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
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
enum DaemonCommands {
    /// Start the SwanFlow daemon (background workflow server)
    Start {
        /// Custom socket path (default: ~/.swanflow/daemon.sock)
        #[arg(long)]
        socket: Option<PathBuf>,
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
            handle_daemon_command(command).await?;
        }
    }

    Ok(())
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Daemon client helper — talk to the SwanFlow daemon over Unix socket
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn handle_daemon_command(cmd: DaemonCommands) -> anyhow::Result<()> {
    match cmd {
        DaemonCommands::Start { socket } => {
            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);
            let mut daemon = swanflow::core::engine::daemon::Daemon::new(engine);
            if let Some(path) = socket {
                daemon = daemon.with_socket_path(path);
            }
            println!("🔄 Starting SwanFlow daemon...");
            println!("   Socket: {}", swanflow::core::engine::daemon::default_socket_path().display());
            println!("   Submit workflows with: swanflow daemon submit <file>");
            daemon.start().await
                .map_err(|e| anyhow::anyhow!("Daemon error: {}", e))?;
            Ok(())
        }
        DaemonCommands::Stop { socket } => {
            send_daemon_request(&socket, "daemon.stop", serde_json::json!({})).await
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
