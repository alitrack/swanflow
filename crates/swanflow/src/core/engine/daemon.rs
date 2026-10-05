//! SwanFlow daemon — Unix socket server for remote workflow control.
//!
//! Inspired by Herdr's socket API: a long-running server that accepts
//! workflow submissions, streams events, and keeps workflows alive
//! after the client detaches.
//!
//! Protocol: newline-delimited JSON over Unix domain socket.
//!   → Request:  {"id":"req1","method":"workflow.submit","params":{...}}
//!   ← Response: {"id":"req1","ok":true,"workflow_id":"..."}
//!   ← Event:    {"event":"node.state_changed","node_id":"...","state":"Running"}

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::Workflow;
use crate::nodes::{NodeEvent, NodeState};

/// Default socket path for the daemon.
pub fn default_socket_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".swanflow").join("daemon.sock")
}

/// PID file path for tracking the daemon process.
pub fn pid_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".swanflow").join("daemon.pid")
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Protocol types
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[derive(Debug, Deserialize)]
struct DaemonRequest {
    id: String,
    method: String,
    #[serde(default)]
    params: JsonValue,
}

#[derive(Debug, Serialize)]
struct DaemonResponse {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflows: Option<Vec<WorkflowInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<WorkflowStatus>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkflowInfo {
    id: String,
    name: String,
    nodes_total: usize,
    nodes_done: usize,
    nodes_error: usize,
    state: String, // "running" | "completed" | "error" | "cancelled"
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkflowStatus {
    id: String,
    name: String,
    state: String,
    nodes: Vec<NodeStatusInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NodeStatusInfo {
    node_id: String,
    node_type: String,
    state: String,
}

#[derive(Debug, Serialize)]
struct DaemonEvent {
    event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nodes_executed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    errors: Option<usize>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Running workflow handle
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

struct RunningWorkflow {
    id: Uuid,
    name: String,
    cancel_token: CancellationToken,
    state: Arc<Mutex<WorkflowRunState>>,
}

#[derive(Clone)]
struct WorkflowRunState {
    nodes_total: usize,
    nodes_done: usize,
    nodes_error: usize,
    run_state: String, // "running" | "completed" | "error" | "cancelled"
    node_states: HashMap<Uuid, String>,
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Daemon
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Configuration for the Quack server embedded in the daemon.
#[derive(Clone)]
pub struct QuackConfig {
    /// Host:port to bind (e.g. "0.0.0.0:9494")
    pub bind: String,
    /// Auth token (clients must present this to connect)
    pub token: String,
    /// Path to the persistent DuckDB database file backing the Quack server
    pub db_path: String,
}

impl Default for QuackConfig {
    fn default() -> Self {
        Self {
            bind: "localhost:9494".into(),
            token: Uuid::new_v4().to_string(),
            db_path: default_db_path().to_string_lossy().to_string(),
        }
    }
}

fn default_db_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".swanflow").join("quack_server.duckdb")
}

pub struct Daemon {
    socket_path: PathBuf,
    engine: Arc<SwanFlowEngine>,
    workflows: Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
    quack_config: Option<QuackConfig>,
    /// ADR-014 Stage 3: workflows registered for automation sensor ticks.
    watched: Arc<Mutex<Vec<WatchedWorkflow>>>,
    /// Sensor tick period (default 30s). Exposed for tests.
    tick_secs: u64,
}

/// A workflow registered with the daemon's automation sensor.
pub struct WatchedWorkflow {
    pub workflow: Workflow,
    /// Per-node last-ready timestamps (gates on_cron / on_interval
    /// re-firing); persisted nowhere yet — daemon lifetime only.
    pub last_ready: HashMap<Uuid, chrono::DateTime<chrono::Utc>>,
}

impl Daemon {
    pub fn new(engine: SwanFlowEngine) -> Self {
        Self {
            socket_path: default_socket_path(),
            engine: Arc::new(engine),
            workflows: Arc::new(Mutex::new(HashMap::new())),
            quack_config: None,
            watched: Arc::new(Mutex::new(Vec::new())),
            tick_secs: 30,
        }
    }

    /// Override the sensor tick period (ADR-014 Stage 3).
    pub fn with_tick_secs(mut self, secs: u64) -> Self {
        self.tick_secs = secs;
        self
    }

    pub fn with_socket_path(mut self, path: PathBuf) -> Self {
        self.socket_path = path;
        self
    }

    /// Enable the embedded Quack server so any DuckDB client can ATTACH.
    pub fn with_quack(mut self, config: QuackConfig) -> Self {
        self.quack_config = Some(config);
        self
    }

    /// Start the daemon — listens on Unix socket indefinitely.
    /// Optionally starts an embedded Quack server for DuckDB client access.
    /// Writes PID to `~/.swanflow/daemon.pid` on startup.
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Ensure parent directory exists
        if let Some(parent) = self.socket_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Remove stale socket file
        let _ = tokio::fs::remove_file(&self.socket_path).await;

        // Write PID file
        let pid = std::process::id();
        tokio::fs::write(pid_path(), pid.to_string()).await?;

        // ── Start embedded Quack server ──
        if let Some(ref qc) = self.quack_config {
            let bind = qc.bind.clone();
            let token = qc.token.clone();
            let db_path = qc.db_path.clone();
            std::thread::spawn(move || {
                // Use a dedicated DuckDB connection for the Quack server
                if let Ok(db) = duckdb::Connection::open(&db_path) {
                    if db.execute_batch("INSTALL quack; LOAD quack;").is_ok() {
                        tracing::info!("Quack server starting on {} (db: {})", bind, db_path);
                        let _ = db.execute_batch(&format!(
                            "CALL quack_serve('quack:{}', token => '{}');",
                            bind, token
                        ));
                    } else {
                        tracing::error!("Failed to load quack extension");
                    }
                } else {
                    tracing::error!("Failed to open Quack db: {}", db_path);
                }
            });
        }

        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!("SwanFlow daemon listening on {}", self.socket_path.display());

        // ── ADR-014 Stage 3: automation sensor loop ──
        // Every tick, evaluate registered workflows' automation conditions
        // (stateless evaluator + daemon-owned last_ready state) and execute
        // any workflow with a ready node. Runs in the daemon task — no new
        // thread, per the ADR.
        {
            let engine = self.engine.clone();
            let watched = self.watched.clone();
            let tick = std::time::Duration::from_secs(self.tick_secs.max(1));
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(tick);
                loop {
                    interval.tick().await;
                    sensor_tick(&engine, &watched).await;
                }
            });
        }

        loop {
            let (stream, addr) = listener.accept().await?;
            // P2-4: keep a peer label so write failures on a connection are identifiable.
            let peer = addr
                .as_pathname()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "unnamed-client".to_string());
            let engine = self.engine.clone();
            let workflows = self.workflows.clone();
            let watched = self.watched.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, engine, workflows, watched, peer).await {
                    tracing::warn!("Client error: {}", e);
                }
            });
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ADR-014 Stage 3 — automation sensor
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// One sensor pass: for every watched workflow, find nodes whose
/// `automation` condition evaluates ready; if any, execute the whole
/// workflow (fresh engine run — same semantics as backfill) and stamp
/// last_ready for the nodes that fired.
async fn sensor_tick(
    engine: &Arc<SwanFlowEngine>,
    watched: &Arc<Mutex<Vec<WatchedWorkflow>>>,
) {
    use crate::core::engine::automation::{evaluate, parse_automation, EvalContext};
    use chrono::Utc;

    let now = Utc::now();
    // Snapshot decisions under the lock, execute outside it (runs can be
    // long; we must not block watch registration on an execution).
    let mut to_run: Vec<(usize, Workflow, Vec<Uuid>)> = Vec::new();
    {
        let mut w = watched.lock().await;
        for (i, ww) in w.iter_mut().enumerate() {
            let mut ready_nodes = Vec::new();
            for idx in ww.workflow.graph.node_indices() {
                let node = &ww.workflow.graph[idx];
                let Some(dsl) = node.config.get("automation").and_then(|v| v.as_str()) else {
                    continue;
                };
                let Ok(cond) = parse_automation(dsl) else {
                    continue; // parse errors surface via `assets status`
                };
                // Freshness input from the ledger — open per tick (short
                // lived; the engine's record hook owns the write window).
                let mat = crate::core::engine::assets::AssetLedger::open(std::path::Path::new(
                    &crate::core::engine::default_cache_dir(),
                ))
                .ok()
                .and_then(|l| l.last_materialized_at(node.id).ok().flatten());

                let ctx = EvalContext {
                    now,
                    last_materialized_at: mat,
                    last_eval_ready_at: ww.last_ready.get(&node.id).copied(),
                };
                if evaluate(&cond, &ctx) {
                    ready_nodes.push(node.id);
                }
            }
            if !ready_nodes.is_empty() {
                for id in &ready_nodes {
                    ww.last_ready.insert(*id, now);
                }
                to_run.push((i, ww.workflow.clone(), ready_nodes));
            }
        }
    }

    for (_, workflow, ready_nodes) in to_run {
        tracing::info!(
            "automation sensor: {} node(s) ready in '{}' — executing",
            ready_nodes.len(),
            workflow.name
        );
        match engine.execute_workflow(&workflow).await {
            Ok(rx) => {
                let mut rx = rx;
                let mut errs = 0usize;
                let mut first_err = String::new();
                while let Some(ev) = rx.recv().await {
                    if let crate::core::engine::NodeEvent::Error { message, node_id, .. } = &ev {
                        errs += 1;
                        if first_err.is_empty() {
                            first_err = format!("node {node_id}: {message}");
                        }
                    }
                }
                if errs > 0 {
                    tracing::warn!(
                        "automation run of '{}' finished with {errs} error(s); first: {first_err}",
                        workflow.name
                    );
                } else {
                    tracing::info!("automation run of '{}' completed", workflow.name);
                }
            }
            Err(e) => {
                tracing::error!("automation run of '{}' failed to start: {e}", workflow.name);
            }
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Client handler
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn handle_client(
    stream: UnixStream,
    engine: Arc<SwanFlowEngine>,
    workflows: Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
    watched: Arc<Mutex<Vec<WatchedWorkflow>>>,
    peer: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.trim().is_empty() {
            continue;
        }

        let req: DaemonRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = DaemonResponse {
                    id: "unknown".to_string(),
                    ok: Some(false),
                    error: Some(format!("Invalid JSON: {}", e)),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
                if let Err(e) = write_response(&mut writer, &resp).await {
                    eprintln!("[daemon] WARNING: write response to client {} failed: {}", peer, e);
                }
                continue;
            }
        };

        // workflow.events — switch to streaming mode
        if req.method == "workflow.events" {
            let wf_id = req
                .params
                .get("workflow_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());
            if let Some(id) = wf_id {
                stream_workflow_events(&mut writer, id, &workflows, &peer).await;
            } else {
                let resp = DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing or invalid workflow_id".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
                if let Err(e) = write_response(&mut writer, &resp).await {
                    eprintln!("[daemon] WARNING: write response to client {} failed: {}", peer, e);
                }
            }
            return Ok(());
        }

        let resp = handle_method(&req, &engine, &workflows, &watched).await;
        if let Err(e) = write_response(&mut writer, &resp).await {
            eprintln!("[daemon] WARNING: write response to client {} failed: {}", peer, e);
        }
    }

    Ok(())
}

async fn handle_method(
    req: &DaemonRequest,
    engine: &Arc<SwanFlowEngine>,
    workflows: &Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
    watched: &Arc<Mutex<Vec<WatchedWorkflow>>>,
) -> DaemonResponse {
    match req.method.as_str() {
        // ADR-014 Stage 3: register a workflow file with the automation
        // sensor. Params: {"path": "<workflow.yaml>"} (swanflow YAML/JSON,
        // NOT knwf).
        "automation.register" => {
            let path = req.params.get("path").and_then(|v| v.as_str()).unwrap_or("");
            if path.is_empty() {
                return DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing 'path' parameter".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
            }
            // Reuse the CLI loader (YAML or JSON workflow).
            let loaded = crate::load_workflow_file(std::path::Path::new(path));
            let workflow = match loaded {
                Ok(w) => w,
                Err(e) => {
                    return DaemonResponse {
                        id: req.id.clone(),
                        ok: Some(false),
                        error: Some(format!("Failed to load workflow: {e}")),
                        workflow_id: None,
                        workflows: None,
                        status: None,
                    }
                }
            };
            let has_automation = workflow
                .graph
                .node_indices()
                .any(|i| workflow.graph[i].config.get("automation").is_some());
            if !has_automation {
                return DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Workflow has no nodes with an 'automation' condition".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
            }
            let wf_id = workflow.id;
            let n = workflow.name.clone();
            let mut w = watched.lock().await;
            w.push(WatchedWorkflow {
                workflow,
                last_ready: HashMap::new(),
            });
            tracing::info!("automation sensor now watching '{n}' ({wf_id})");
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: Some(wf_id.to_string()),
                workflows: None,
                status: None,
            }
        }

        "daemon.ping" => DaemonResponse {
            id: req.id.clone(),
            ok: Some(true),
            error: None,
            workflow_id: None,
            workflows: None,
            status: None,
        },

        "daemon.stop" => {
            tracing::info!("Daemon stop requested — shutting down");
            // Allow graceful shutdown — just acknowledge
            let resp = DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: None,
                workflows: None,
                status: None,
            };
            tokio::spawn(async {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                std::process::exit(0);
            });
            resp
        }

        "workflow.submit" => {
            let path = req
                .params
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // Optional params merged into workflow.context (call-side wins).
            let params = req.params.get("params").cloned();

            if path.is_empty() {
                return DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing 'path' parameter".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
            }

            match submit_workflow(path, params.as_ref(), engine, workflows).await {
                Ok(wf_id) => DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(true),
                    error: None,
                    workflow_id: Some(wf_id.to_string()),
                    workflows: None,
                    status: None,
                },
                Err(e) => DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some(e.to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                },
            }
        }

        "workflow.list" => {
            let wfs = workflows.lock().await;
            let list: Vec<WorkflowInfo> = wfs
                .iter()
                .map(|(id, wf)| {
                    let state = wf.state.blocking_lock();
                    WorkflowInfo {
                        id: id.to_string(),
                        name: wf.name.clone(),
                        nodes_total: state.nodes_total,
                        nodes_done: state.nodes_done,
                        nodes_error: state.nodes_error,
                        state: state.run_state.clone(),
                    }
                })
                .collect();
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: None,
                workflows: Some(list),
                status: None,
            }
        }

        "workflow.status" => {
            let wf_id = req
                .params
                .get("workflow_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());

            match wf_id {
                Some(id) => {
                    let wfs = workflows.lock().await;
                    if let Some(wf) = wfs.get(&id) {
                        let state = wf.state.blocking_lock();
                        DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(true),
                            error: None,
                            workflow_id: None,
                            workflows: None,
                            status: Some(WorkflowStatus {
                                id: id.to_string(),
                                name: wf.name.clone(),
                                state: state.run_state.clone(),
                                nodes: state
                                    .node_states
                                    .iter()
                                    .map(|(nid, ns)| NodeStatusInfo {
                                        node_id: nid.to_string(),
                                        node_type: String::new(),
                                        state: ns.clone(),
                                    })
                                    .collect(),
                            }),
                        }
                    } else {
                        DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(false),
                            error: Some(format!("Workflow {} not found", id)),
                            workflow_id: None,
                            workflows: None,
                            status: None,
                        }
                    }
                }
                None => DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing or invalid 'workflow_id'".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                },
            }
        }

        "workflow.cancel" => {
            let wf_id = req
                .params
                .get("workflow_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());

            match wf_id {
                Some(id) => {
                    let wfs = workflows.lock().await;
                    if let Some(wf) = wfs.get(&id) {
                        wf.cancel_token.cancel();
                        DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(true),
                            error: None,
                            workflow_id: Some(id.to_string()),
                            workflows: None,
                            status: None,
                        }
                    } else {
                        DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(false),
                            error: Some(format!("Workflow {} not found", id)),
                            workflow_id: None,
                            workflows: None,
                            status: None,
                        }
                    }
                }
                None => DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing or invalid 'workflow_id'".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                },
            }
        }

        _ => DaemonResponse {
            id: req.id.clone(),
            ok: Some(false),
            error: Some(format!("Unknown method: {}", req.method)),
            workflow_id: None,
            workflows: None,
            status: None,
        },
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Workflow submission
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async fn submit_workflow(
    path: &str,
    params: Option<&serde_json::Value>,
    engine: &Arc<SwanFlowEngine>,
    workflows: &Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
) -> Result<Uuid, Box<dyn std::error::Error + Send + Sync>> {
    let wf_id = Uuid::new_v4();
    let cancel_token = CancellationToken::new();
    let cancel = cancel_token.clone();

    // Load workflow from KNIME file
    let core_wf = swanflow_knime::import_knwf(path)
        .await
        .map_err(|e| format!("Failed to import KNIME workflow: {}", e))?;
    let mut workflow: Workflow = {
        let workflow_json = serde_json::to_value(&core_wf)
            .map_err(|e| format!("Serialization error: {}", e))?;
        serde_json::from_value(workflow_json)
            .map_err(|e| format!("Deserialization error: {}", e))?
    };
    // Merge optional params into workflow.context — call-side values win.
    if let Some(map) = params.and_then(|p| p.as_object()) {
        for (k, v) in map {
            workflow.context.insert(k.clone(), v.clone());
        }
    }
    let wf_name = workflow.name.clone();

    let node_count = workflow.graph.node_count();
    let run_state = Arc::new(Mutex::new(WorkflowRunState {
        nodes_total: node_count,
        nodes_done: 0,
        nodes_error: 0,
        run_state: "running".to_string(),
        node_states: HashMap::new(),
    }));

    // Register the running workflow
    {
        let mut wfs = workflows.lock().await;
        wfs.insert(
            wf_id,
            RunningWorkflow {
                id: wf_id,
                name: wf_name.clone(),
                cancel_token: cancel_token.clone(),
                state: run_state.clone(),
            },
        );
    }

    let engine_clone = engine.clone();
    let state_clone = run_state.clone();
    let wf_id_clone = wf_id;
    let workflows_clone = workflows.clone();
    let wf_name_clone = wf_name.clone();

    tokio::spawn(async move {
        let result =
            tokio::select! {
                r = engine_clone.execute_workflow(&workflow) => r,
                _ = cancel.cancelled() => {
                    let mut st = state_clone.blocking_lock();
                    st.run_state = "cancelled".to_string();
                    return;
                }
            };

        match result {
            Ok(mut rx) => {
                let mut nodes_done = 0usize;
                let mut nodes_error = 0usize;
                while let Some(event) = rx.recv().await {
                    match &event {
                        NodeEvent::StateChanged { node_id, state } => {
                            let state_str = node_state_to_string(state);
                            state_clone
                                .blocking_lock()
                                .node_states
                                .insert(*node_id, state_str);
                            if matches!(state, NodeState::Executed) {
                                nodes_done += 1;
                            }
                            if matches!(state, NodeState::Error(_)) {
                                nodes_error += 1;
                            }
                        }
                        _ => {}
                    }
                    // Update run state
                    let mut st = state_clone.blocking_lock();
                    st.nodes_done = nodes_done;
                    st.nodes_error = nodes_error;
                }
                let mut st = state_clone.blocking_lock();
                st.run_state = if nodes_error > 0 {
                    "error".to_string()
                } else {
                    "completed".to_string()
                };
                st.nodes_done = nodes_done;
                st.nodes_error = nodes_error;
            }
            Err(e) => {
                let mut st = state_clone.blocking_lock();
                st.run_state = "error".to_string();
                tracing::error!("Workflow {} ({}) failed: {}", wf_id_clone, wf_name_clone, e);
            }
        }

        // Keep workflow in registry for 5 minutes after completion for status queries
        tokio::time::sleep(std::time::Duration::from_secs(300)).await;
        workflows_clone.lock().await.remove(&wf_id_clone);
    });

    Ok(wf_id)
}

fn node_state_to_string(state: &NodeState) -> String {
    match state {
        NodeState::Unconfigured => "unconfigured".into(),
        NodeState::Configured => "configured".into(),
        NodeState::Pending => "pending".into(),
        NodeState::Running => "running".into(),
        NodeState::Waiting => "waiting".into(),
        NodeState::Executed => "executed".into(),
        NodeState::Error(_) => "error".into(),
    }
}

/// Stream live workflow events over a persistent connection.
/// Sends newline-delimited JSON events every 500ms until the workflow completes.
async fn stream_workflow_events(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    wf_id: Uuid,
    workflows: &Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
    peer: &str,
) {
    // Send initial ack
    let ack = serde_json::json!({"event":"workflow.subscribed","workflow_id":wf_id.to_string()});
    if let Err(e) = writer.write_all(format!("{}\n", ack).as_bytes()).await {
        eprintln!(
            "[daemon] WARNING: write to client {} failed (workflow {}), dropping stream: {}",
            peer, wf_id, e
        );
        return;
    }

    loop {
        let done = {
            let wfs = workflows.lock().await;
            if let Some(wf) = wfs.get(&wf_id) {
                let state = wf.state.blocking_lock();
                let event = serde_json::json!({
                    "event": "workflow.status",
                    "workflow_id": wf_id.to_string(),
                    "name": wf.name,
                    "state": state.run_state,
                    "nodes_done": state.nodes_done,
                    "nodes_error": state.nodes_error,
                    "nodes_total": state.nodes_total,
                });
                if let Err(e) = writer
                    .write_all(format!("{}\n", serde_json::to_string(&event).unwrap_or_default()).as_bytes())
                    .await
                {
                    // P2-4: write failed -> peer gone; warn and stop streaming this connection.
                    eprintln!(
                        "[daemon] WARNING: write to client {} failed (workflow {}), dropping stream: {}",
                        peer, wf_id, e
                    );
                    break;
                }

                state.run_state == "completed" || state.run_state == "error" || state.run_state == "cancelled"
            } else {
                let event = serde_json::json!({
                    "event": "workflow.not_found",
                    "workflow_id": wf_id.to_string(),
                });
                if let Err(e) = writer
                    .write_all(format!("{}\n", serde_json::to_string(&event).unwrap_or_default()).as_bytes())
                    .await
                {
                    // P2-4: write failed -> peer gone; warn and stop streaming this connection.
                    eprintln!(
                        "[daemon] WARNING: write to client {} failed (workflow {}), dropping stream: {}",
                        peer, wf_id, e
                    );
                    break;
                }
                true
            }
        };

        if done {
            let done_event = serde_json::json!({"event":"workflow.stream_end","workflow_id":wf_id.to_string()});
            if let Err(e) = writer.write_all(format!("{}\n", done_event).as_bytes()).await {
                // P2-4: connection already gone when delivering the end event.
                eprintln!(
                    "[daemon] WARNING: write to client {} failed (workflow {}), stream end not delivered: {}",
                    peer, wf_id, e
                );
            }
            break;
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn write_response(
    writer: &mut (impl tokio::io::AsyncWrite + Unpin),
    resp: &DaemonResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string(resp)?;
    writer.write_all(json.as_bytes()).await?;
    writer.write_all(b"\n").await?;
    Ok(())
}
