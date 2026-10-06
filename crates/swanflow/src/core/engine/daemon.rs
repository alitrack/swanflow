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
    /// re-firing); persisted to watched.json so daemon restarts restore it.
    pub last_ready: HashMap<Uuid, chrono::DateTime<chrono::Utc>>,
    /// Absolute path of the workflow file (persisted; enables re-loading
    /// on daemon restart). `None` only for programmatic registrations
    /// without a file (tests).
    pub path: Option<std::path::PathBuf>,
    /// Monotonic registration epoch. sensor_tick snapshots the workflow
    /// under the lock and executes outside it; if the entry is
    /// unregistered (or re-registered with a new definition) in between,
    /// the generation no longer matches and the stale run is dropped
    /// instead of executing a workflow nobody asked for.
    pub generation: u64,
}

/// Serializable form of one watched-workflow entry in watched.json.
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct WatchedEntry {
    path: String,
    workflow_id: String,
    /// node-id -> ISO-8601 UTC timestamp of the last ready evaluation.
    last_ready: std::collections::BTreeMap<String, chrono::DateTime<chrono::Utc>>,
}

/// Monotonic counter minting fresh generations on every register /
/// unregister / re-register (see WatchedWorkflow::generation).
static WATCH_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Location of the watched-workflow registry file: `<cache_dir>/watched.json`
/// (same directory the asset ledger and extfn catalog live in — cache_dir
/// env override applies). Returns None when no HOME is set.
fn watched_registry_path() -> Option<std::path::PathBuf> {
    Some(std::path::PathBuf::from(
        crate::core::engine::default_cache_dir(),
    ))
    .map(|p| p.join("watched.json"))
}

/// Load persisted watch entries from watched.json (best-effort: missing
/// file, parse errors, and trailing garbage all yield an empty vec —
/// persistence must never prevent the daemon from starting).
fn load_watched_entries() -> Vec<WatchedEntry> {
    let Some(p) = watched_registry_path() else {
        return Vec::new();
    };
    let Ok(text) = std::fs::read_to_string(&p) else {
        return Vec::new();
    };
    match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("watched.json unreadable ({e}) — starting with no watches");
            Vec::new()
        }
    }
}

/// Serialize watched.json persistence across concurrent callers (sensor
/// tick racing register/unregister). Held for snapshot-check → write →
/// rename so no stale snapshot can overwrite a newer persisted state.
static SAVE_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Monotonic counter for unique temp-file names: two concurrent saves
/// must never share a `.tmp` path (one's fsync+rename could act on the
/// other's half-written file even with SAVE_MUTEX, e.g. across processes).
static TMP_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Snapshot the watched list into its serializable form.
async fn snapshot_entries(watched: &Arc<Mutex<Vec<WatchedWorkflow>>>) -> Vec<WatchedEntry> {
    let w = watched.lock().await;
    w.iter()
        .filter_map(|ww| {
            let path = ww.path.as_ref()?;
            Some(WatchedEntry {
                path: path.to_string_lossy().to_string(),
                workflow_id: ww.workflow.id.to_string(),
                last_ready: ww
                    .last_ready
                    .iter()
                    .map(|(k, v)| (k.to_string(), *v))
                    .collect(),
            })
        })
        .collect()
}

/// Persist the watched list to watched.json (best-effort — a write failure
/// is logged, never fatal).
///
/// codex-review P1 (r2): two races fixed here.
/// 1. Fixed tmp name: concurrent savers shared `watched.json.tmp`; one
///    could fsync/rename the other's half-written file. Now unique per
///    call (pid + counter).
/// 2. Stale overwrite: caller A snapshots, caller B mutates+saves, A
///    renames its older snapshot over B's newer state. SAVE_MUTEX
///    serializes the whole write; additionally the snapshot is re-taken
///    under the file lock, and re-checked right before rename — if the
///    live list moved on, we persist the NEWER state instead.
async fn save_watched_entries(watched: &Arc<Mutex<Vec<WatchedWorkflow>>>) {
    let Some(p) = watched_registry_path() else {
        return;
    };
    let _file_lock = SAVE_MUTEX.lock().await;
    let mut entries = snapshot_entries(watched).await;
    if let Some(parent) = p.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let tmp = p.with_extension(format!(
        "json.tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    for _attempt in 0..3 {
        match serde_json::to_string_pretty(&entries) {
            Ok(text) => {
                // Atomic replace: write temp + fsync + rename. A crash mid-write
                // must never leave a truncated watched.json (it would silently
                // drop every watch on daemon restart).
                match tokio::fs::write(&tmp, &text).await {
                    Ok(()) => {
                        if let Ok(f) = tokio::fs::File::open(&tmp).await {
                            drop(f.sync_all().await);
                        }
                        // Stale-overwrite guard: if the live list changed since
                        // our snapshot, persist the newer state instead of
                        // renaming ours over it.
                        let latest = snapshot_entries(watched).await;
                        if latest != entries {
                            tracing::debug!(
                                "watched.json: list changed during save — persisting newer state"
                            );
                            entries = latest;
                            continue; // rewrite tmp with the newer snapshot
                        }
                        if let Err(e) = tokio::fs::rename(&tmp, &p).await {
                            tracing::warn!("failed to persist watched.json: {e}");
                        }
                        return;
                    }
                    Err(e) => {
                        tracing::warn!("failed to persist watched.json: {e}");
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::warn!("failed to serialize watched.json: {e}");
                return;
            }
        }
    }
    // Loop exhausted: three snapshot churns in a row — give up rather
    // than spin; next mutation will save again.
    let _ = tokio::fs::remove_file(&tmp).await;
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

        // ── ADR-014 Stage 3: restore persisted watches from watched.json ──
        // Reloads each workflow file from disk (config edits since the last
        // registration are picked up); entries whose file no longer exists
        // or no longer parses are dropped with a WARN — never fatal.
        {
            let mut w = self.watched.lock().await;
            for e in load_watched_entries() {
                let p = std::path::PathBuf::from(&e.path);
                match crate::load_workflow_file(&p) {
                    Ok(workflow) => {
                        let mut last_ready = HashMap::new();
                        for (k, v) in &e.last_ready {
                            if let Ok(id) = uuid::Uuid::parse_str(k) {
                                last_ready.insert(id, *v);
                            }
                        }
                        tracing::info!(
                            "restored watch '{}' ({}) from watched.json",
                            workflow.name,
                            e.path
                        );
                        w.push(WatchedWorkflow {
                            workflow,
                            last_ready,
                            path: Some(p),
                            generation: WATCH_GENERATION.fetch_add(
                                1,
                                std::sync::atomic::Ordering::Relaxed,
                            ),
                        });
                    }
                    Err(err) => {
                        tracing::warn!(
                            "dropped persisted watch {}: file missing or unreadable ({err})",
                            e.path
                        );
                    }
                }
            }
        }

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
    // long; we must not block watch registration on an execution). Each
    // snapshot carries the entry's (workflow id, generation) — re-checked
    // before executing so an unregister / re-watch that happened while we
    // were busy drops the stale run instead of executing it.
    let mut to_run: Vec<(uuid::Uuid, u64, Workflow, Vec<Uuid>)> = Vec::new();
    {
        let mut w = watched.lock().await;
        for ww in w.iter_mut() {
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
                // A failed open/read is tri-stated as "unavailable" (not
                // "never ran") so FreshnessViolated skips instead of
                // firing on missing data.
                let mat_res = crate::core::engine::assets::AssetLedger::open(
                    std::path::Path::new(&crate::core::engine::default_cache_dir()),
                )
                .and_then(|l| l.last_materialized_at(node.id));
                let (mat, ledger_unavailable) = match mat_res {
                    Ok(v) => (v, false),
                    Err(_) => (None, true),
                };

                let ctx = EvalContext {
                    now,
                    last_materialized_at: mat,
                    last_eval_ready_at: ww.last_ready.get(&node.id).copied(),
                    ledger_unavailable,
                };
                if evaluate(&cond, &ctx) {
                    ready_nodes.push(node.id);
                }
            }
            if !ready_nodes.is_empty() {
                for id in &ready_nodes {
                    ww.last_ready.insert(*id, now);
                }
                to_run.push((ww.workflow.id, ww.generation, ww.workflow.clone(), ready_nodes));
            }
        }
    }

    for (wf_id, generation, workflow, ready_nodes) in to_run {
        // Re-check under the lock: still watched, same registration epoch?
        let still_current = {
            let w = watched.lock().await;
            w.iter()
                .any(|ww| ww.workflow.id == wf_id && ww.generation == generation)
        };
        if !still_current {
            tracing::info!(
                "automation sensor: dropping stale run for '{n}' (unwatched/re-watched mid-tick)",
                n = workflow.name
            );
            continue;
        }
        // Ready timestamps were stamped under the lock above — flush them
        // so a daemon restart doesn't re-fire these intervals.
        save_watched_entries(watched).await;
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
            // Validate every automation expression NOW — a parse error
            // would otherwise be silently skipped every tick (invisible
            // misconfiguration). Quoted forms like on_cron("*/5 * * * *")
            // are supported.
            for i in workflow.graph.node_indices() {
                if let Some(dsl) = workflow.graph[i].config.get("automation").and_then(|v| v.as_str()) {
                    if let Err(e) = crate::core::engine::automation::parse_automation(dsl) {
                        return DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(false),
                            error: Some(format!(
                                "node {} has an invalid automation condition: {e}",
                                workflow.graph[i].id
                            )),
                            workflow_id: None,
                            workflows: None,
                            status: None,
                        };
                    }
                }
            }
            let wf_id = workflow.id;
            let n = workflow.name.clone();
            let mut w = watched.lock().await;
            // Dedupe on workflow id: re-watching an already-watched file
            // refreshes its definition (config edits) while PRESERVING
            // last_ready (otherwise on_interval would fire immediately).
            let prev_last_ready = w
                .iter()
                .find(|ww| ww.workflow.id == wf_id)
                .map(|ww| ww.last_ready.clone())
                .unwrap_or_default();
            w.retain(|ww| ww.workflow.id != wf_id);
            w.push(WatchedWorkflow {
                workflow,
                last_ready: prev_last_ready,
                path: Some(std::path::PathBuf::from(path)),
                generation: WATCH_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            });
            tracing::info!("automation sensor now watching '{n}' ({wf_id})");
            drop(w);
            save_watched_entries(watched).await;
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

        // ADR-014 follow-up: remove a workflow from the automation sensor.
        // Params: {"path": "..."} or {"workflow_id": "..."} (path resolves
        // by canonicalization + id match, matching how `daemon watch` sends
        // absolute paths). Idempotent — unregistering an unknown workflow
        // returns ok with removed=false rather than an error.
        "automation.unregister" => {
            let wf_id: Option<uuid::Uuid> = req
                .params
                .get("workflow_id")
                .and_then(|v| v.as_str())
                .and_then(|s| uuid::Uuid::parse_str(s).ok());
            let path = req.params.get("path").and_then(|v| v.as_str());
            if wf_id.is_none() && path.is_none() {
                return DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Need 'path' or 'workflow_id' parameter".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
            }
            let mut w = watched.lock().await;
            let before = w.len();
            // Path matching: canonicalize (like `daemon watch` does) and
            // compare both raw and canonical forms — a watch registered
            // from one symlink/relative spelling must still unregister.
            let canon = path.and_then(|p| std::fs::canonicalize(p).ok());
            w.retain(|ww| {
                let id_match = wf_id.map(|id| ww.workflow.id == id).unwrap_or(false);
                if id_match {
                    return false;
                }
                if let (Some(p), Some(wwp)) = (path, ww.path.as_ref()) {
                    if wwp.to_string_lossy() == p {
                        return false;
                    }
                    if let Some(c) = &canon {
                        // canonicalize the stored path lazily; entries whose
                        // file vanished still match by raw string above.
                        if std::fs::canonicalize(wwp)
                            .map(|cp| cp == *c)
                            .unwrap_or(false)
                        {
                            return false;
                        }
                    }
                }
                true
            });
            let removed = before - w.len();
            drop(w);
            if removed > 0 {
                save_watched_entries(watched).await;
            }
            match wf_id {
                Some(id) => tracing::info!(
                    "automation sensor: unregistered {removed} watch(es) for {id}"
                ),
                None => tracing::info!(
                    "automation sensor: unregistered {removed} watch(es) for path {path:?}"
                ),
            }
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(removed > 0),
                error: if removed > 0 {
                    None
                } else {
                    Some("no matching watch".to_string())
                },
                workflow_id: wf_id.map(|id| id.to_string()),
                workflows: None,
                status: None,
            }
        }

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

#[cfg(test)]
mod watched_persistence_tests {
    use super::*;

    fn sample_entry(path: &str, wf_id: &str) -> WatchedEntry {
        let mut last_ready = std::collections::BTreeMap::new();
        last_ready.insert(
            "11111111-2222-3333-4444-555555555555".to_string(),
            chrono::DateTime::parse_from_rfc3339("2026-10-06T01:02:03Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        );
        WatchedEntry {
            path: path.to_string(),
            workflow_id: wf_id.to_string(),
            last_ready,
        }
    }

    /// The persisted JSON shape must round-trip: field names, nested map,
    /// RFC3339 timestamps — any drift breaks restarts of older daemons.
    #[test]
    fn watched_entry_roundtrips_through_json() {
        let e = sample_entry("/tmp/wf.yaml", "aaaaaaaa-0000-0000-0000-000000000001");
        let text = serde_json::to_string_pretty(&e).unwrap();
        let back: WatchedEntry = serde_json::from_str(&text).unwrap();
        assert_eq!(back.path, "/tmp/wf.yaml");
        assert_eq!(
            back.workflow_id,
            "aaaaaaaa-0000-0000-0000-000000000001"
        );
        let ts = back
            .last_ready
            .get("11111111-2222-3333-4444-555555555555")
            .unwrap();
        assert_eq!(ts.to_rfc3339(), "2026-10-06T01:02:03+00:00");
    }

    /// load_watched_entries must tolerate a corrupt/garbage registry file —
    /// persistence is best-effort and must never block daemon startup.
    /// NOTE: merged with the save round-trip into ONE test because both
    /// mutate the process-global SWANFLOW_CACHE_DIR — parallel #[test]s
    /// would race on it (observed as flaky FAILED when run filtered).
    #[tokio::test]
    async fn load_save_roundtrip_tolerates_garbage_and_persists() {
        let dir = std::env::temp_dir()
            .join(format!("swanflow-watched-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("watched.json"), "{not valid json").unwrap();
        let prev = std::env::var("SWANFLOW_CACHE_DIR").ok();
        std::env::set_var("SWANFLOW_CACHE_DIR", &dir);

        // 1. Garbage file -> empty list, no panic.
        assert!(load_watched_entries().is_empty());

        // 2. Valid file -> loads.
        let e = sample_entry("/tmp/wf.yaml", "aaaaaaaa-0000-0000-0000-000000000001");
        std::fs::write(
            dir.join("watched.json"),
            serde_json::to_string_pretty(&vec![e]).unwrap(),
        )
        .unwrap();
        let entries = load_watched_entries();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "/tmp/wf.yaml");

        // 3. save_watched_entries persists path + last_ready for entries
        //    with a file path and skips programmatic (path-less) entries.
        let node_id = uuid::Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let ts = chrono::DateTime::parse_from_rfc3339("2026-10-06T01:02:03Z")
            .unwrap()
            .with_timezone(&chrono::Utc);

        let watched: Arc<Mutex<Vec<WatchedWorkflow>>> =
            Arc::new(Mutex::new(Vec::new()));
        let mut wf = Workflow::default();
        wf.id = uuid::Uuid::parse_str("aaaaaaaa-0000-0000-0000-000000000001").unwrap();
        wf.name = "persisted".into();
        let mut last_ready = HashMap::new();
        last_ready.insert(node_id, ts);
        watched.lock().await.push(WatchedWorkflow {
            workflow: wf,
            last_ready,
            path: Some(std::path::PathBuf::from("/tmp/wf.yaml")),
            generation: 1,
        });
        let mut wf2 = Workflow::default();
        wf2.id = uuid::Uuid::new_v4();
        wf2.name = "ephemeral".into();
        watched.lock().await.push(WatchedWorkflow {
            workflow: wf2,
            last_ready: HashMap::new(),
            path: None,
            generation: 2,
        });

        save_watched_entries(&watched).await;
        let entries = load_watched_entries();
        assert_eq!(entries.len(), 1, "path-less entry must be skipped");
        assert_eq!(entries[0].path, "/tmp/wf.yaml");
        assert_eq!(
            entries[0].workflow_id,
            "aaaaaaaa-0000-0000-0000-000000000001"
        );
        let restored = entries[0]
            .last_ready
            .get("11111111-2222-3333-4444-555555555555")
            .unwrap();
        assert_eq!(*restored, ts);

        match prev {
            Some(v) => std::env::set_var("SWANFLOW_CACHE_DIR", v),
            None => std::env::remove_var("SWANFLOW_CACHE_DIR"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_saves_never_lose_state() {
        // codex-review P1 (r2): concurrent save_watched_entries callers
        // used to share one .tmp path and could rename a STALE snapshot
        // over a newer persisted state. With SAVE_MUTEX + unique tmp
        // names + rename-time re-check, racing savers must converge to
        // the NEWEST list. Here: 8 tasks each add their own workflow
        // then save concurrently; the file must finally contain all 8.
        let dir = std::env::temp_dir().join(format!("swanflow_watched_race_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let prev = std::env::var("SWANFLOW_CACHE_DIR").ok();
        unsafe { std::env::set_var("SWANFLOW_CACHE_DIR", dir.to_str().unwrap()) };

        let watched: Arc<Mutex<Vec<WatchedWorkflow>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let w = watched.clone();
            handles.push(tokio::spawn(async move {
                let mut wf = Workflow::default();
                wf.id = uuid::Uuid::new_v4();
                wf.name = format!("racer-{i}");
                w.lock().await.push(WatchedWorkflow {
                    workflow: wf,
                    last_ready: HashMap::new(),
                    path: Some(std::path::PathBuf::from(format!("/tmp/racer-{i}.yaml"))),
                    generation: i as u64 + 1,
                });
                save_watched_entries(&w).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        // Final ground truth: file must reflect the FULL in-memory list.
        save_watched_entries(&watched).await;
        let entries = load_watched_entries();
        assert_eq!(entries.len(), 8, "all 8 watches must survive: {entries:?}");
        // No leftover tmp files.
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp files left behind: {leftovers:?}");

        match prev {
            Some(v) => std::env::set_var("SWANFLOW_CACHE_DIR", v),
            None => std::env::remove_var("SWANFLOW_CACHE_DIR"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn sensor_drops_stale_generation_run() {
        // A sensor tick snapshots (id, generation) under the lock; if the
        // entry is unwatched or re-watched before the run starts, the
        // stale run must be dropped. We can't easily pause sensor_tick
        // mid-flight, so we verify the mechanism it relies on: after
        // unregister + re-register, the entry carries a NEW generation
        // and the old (id, old-gen) pair no longer matches.
        let watched: Arc<Mutex<Vec<WatchedWorkflow>>> = Arc::new(Mutex::new(Vec::new()));
        let wf_id = uuid::Uuid::new_v4();
        let mk_wf = |id: uuid::Uuid| {
            let mut wf = Workflow::default();
            wf.id = id;
            wf.name = "gen_test".into();
            wf
        };
        watched.lock().await.push(WatchedWorkflow {
            workflow: mk_wf(wf_id),
            last_ready: HashMap::new(),
            path: Some(std::path::PathBuf::from("/tmp/gen_wf.yaml")),
            generation: 10,
        });

        // Snapshot as sensor_tick would.
        let (snap_id, snap_gen) = {
            let w = watched.lock().await;
            let ww = &w[0];
            (ww.workflow.id, ww.generation)
        };
        assert_eq!((snap_id, snap_gen), (wf_id, 10));

        // Unwatch (retain) then re-watch with a fresh generation — exactly
        // what automation.register does on re-watch.
        {
            let mut w = watched.lock().await;
            w.retain(|ww| ww.workflow.id != wf_id);
            w.push(WatchedWorkflow {
                workflow: mk_wf(wf_id),
                last_ready: HashMap::new(),
                path: Some(std::path::PathBuf::from("/tmp/gen_wf.yaml")),
                generation: WATCH_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            });
        }

        // The sensor's re-check predicate: entry must exist with the SAME
        // generation. The old snapshot no longer matches -> run dropped.
        let still_current = {
            let w = watched.lock().await;
            w.iter()
                .any(|ww| ww.workflow.id == snap_id && ww.generation == snap_gen)
        };
        assert!(!still_current, "stale (id, generation) must not match");
    }
}
