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
// Daemon process identity — PID-reuse guard for start/stop
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Parse raw `/proc/<pid>/cmdline` bytes into argv.
///
/// The kernel stores argv NUL-separated (with a trailing NUL); this
/// splits on NUL, drops empty chunks, and tolerates non-UTF8 args.
fn parse_cmdline(raw: &[u8]) -> Vec<String> {
    raw.split(|&b| b == 0)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| String::from_utf8_lossy(chunk).into_owned())
        .collect()
}

/// Returns true if `args` (a process argv) identifies a running
/// swanflow daemon: both the `daemon` subcommand and `--foreground`
/// are present. Exact-argument matching, so a path like
/// `/tmp/daemon.sock` cannot false-positive. A dead daemon's recycled
/// PID will have a different argv, which is what keeps us from
/// killing (or deferring to) an innocent process.
fn cmdline_is_daemon(args: &[String]) -> bool {
    let has_subcommand = args.iter().any(|a| a == "daemon");
    let has_foreground = args.iter().any(|a| a == "--foreground");
    has_subcommand && has_foreground
}

/// Returns true if `pid` is alive and is a swanflow daemon process.
///
/// On Linux this verifies process identity by reading
/// `/proc/<pid>/cmdline` and requiring both the `daemon` subcommand
/// and `--foreground` (the forked background daemon's argv), so a
/// recycled PID re-assigned to an innocent process is never treated
/// as the daemon. On other platforms there is no `/proc`, so this
/// degrades to a liveness probe (`kill -0 <pid>`).
pub fn is_swanflow_daemon(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    #[cfg(target_os = "linux")]
    {
        let raw = match std::fs::read(format!("/proc/{pid}/cmdline")) {
            Ok(raw) => raw,
            // Process is gone (or /proc unavailable) — not a daemon.
            Err(_) => return false,
        };
        cmdline_is_daemon(&parse_cmdline(&raw))
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No /proc on non-Linux: degrade to a liveness probe.
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
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
    watched: Option<Vec<WatchedInfo>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    logs: Option<Vec<String>>,
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

/// One watched-workflow row for `automation.list` (daemon watched).
#[derive(Debug, Serialize, Deserialize)]
struct WatchedInfo {
    workflow_id: String,
    name: String,
    /// Source file the entry was registered from (None = programmatic).
    path: Option<String>,
    /// Registration generation — bumps on re-watch; stale runs are dropped.
    generation: u64,
    /// True while an unregister is deferred behind a running sensor claim.
    pending_remove: bool,
    /// True while a sensor run holds this entry's claim.
    running: bool,
    /// node_id -> last time its automation evaluated ready.
    last_ready: std::collections::BTreeMap<String, String>,
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
    /// Claim slot (codex-review P1 r2): a run in flight holds a claim
    /// (wf_id, generation) here. An unregister racing the run marks
    /// `pending_remove` instead of removing — the entry must survive
    /// until the sensor clears the claim, so post-run bookkeeping
    /// (last_ready stamps) lands on a live entry and the removal takes
    /// effect atomically once the run finishes.
    pub running_claim: Option<(Uuid, u64)>,
    /// Set by unregister while a claim is held; the claim-holder removes
    /// the entry after the run completes.
    pub pending_remove: bool,
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

/// Location of the daemon event log: `<cache_dir>/daemon.log`. Plain
/// append-only text (one event per line, RFC3339 timestamp) so it tails
/// well over SSH and survives daemon restarts. Rolling: when the file
/// passes [`DAEMON_LOG_MAX_BYTES`] it is renamed to daemon.log.1 (any
/// previous .1 is dropped) — bounded disk usage, newest events always
/// win.
const DAEMON_LOG_MAX_BYTES: u64 = 1024 * 1024;

fn daemon_log_path() -> Option<std::path::PathBuf> {
    Some(std::path::PathBuf::from(
        crate::core::engine::default_cache_dir(),
    ))
    .map(|p| p.join("daemon.log"))
}

/// Serialize appends across sensor ticks and RPC handlers.
static DAEMON_LOG_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Append one event line to `<cache_dir>/daemon.log` (best-effort: log
/// failures must never break the daemon — fall back to tracing).
fn daemon_log(event: &str) {
    let Some(path) = daemon_log_path() else { return };
    let _g = DAEMON_LOG_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
    // Roll when oversized: rename current -> .1 (replacing any old .1).
    if let Ok(meta) = std::fs::metadata(&path) {
        if meta.len() > DAEMON_LOG_MAX_BYTES {
            let rolled = path.with_extension("log.1");
            let _ = std::fs::remove_file(&rolled);
            let _ = std::fs::rename(&path, &rolled);
        }
    }
    let line = format!(
        "{} {}\n",
        chrono::Utc::now().to_rfc3339(),
        event.trim()
    );
    use std::io::Write;
    let res = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .and_then(|mut f| f.write_all(line.as_bytes()));
    if let Err(e) = res {
        tracing::warn!("daemon.log append failed: {e}");
    }
}

/// Tail the daemon event log: last `tail` lines, oldest first. Missing
/// file yields an empty vec (a daemon that never logged is not an error).
fn daemon_log_tail(tail: usize) -> Vec<String> {
    let Some(path) = daemon_log_path() else {
        return Vec::new();
    };
    let Ok(text) = std::fs::read_to_string(&path) else {
        return Vec::new();
    };
    let lines: Vec<&str> = text.lines().collect();
    let skip = lines.len().saturating_sub(tail);
    lines[skip..].iter().map(|s| s.to_string()).collect()
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
                            running_claim: None,
                            pending_remove: false,
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
                match evaluate(&cond, &ctx) {
                    // Fire only on definite true. Unknown (ledger unreadable
                    // etc.) skips this tick and retries — never fires blind.
                    Some(true) => ready_nodes.push(node.id),
                    Some(false) => {}
                    None => tracing::debug!(
                        "automation sensor: '{}' node {n} Unknown — skipping this tick",
                        ww.workflow.name, n = node.id
                    ),
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
        // Claim + re-check under ONE lock acquisition (codex-review P1 r2):
        // the old code checked (id, generation) and released the lock, then
        // `save_watched_entries().await` yielded — an unregister landing in
        // that window removed the entry while the run proceeded with the
        // old definition anyway, and its post-run last_ready stamps were
        // lost. Now: verify AND set running_claim atomically; unregister
        // sees the claim and defers removal (pending_remove) until we
        // clear the claim after the run.
        let claimed = {
            let mut w = watched.lock().await;
            match w
                .iter_mut()
                .find(|ww| ww.workflow.id == wf_id && ww.generation == generation)
            {
                None => false,
                Some(ww) => {
                    if ww.pending_remove {
                        // Unregister already requested before we claimed — honor it.
                        false
                    } else {
                        ww.running_claim = Some((wf_id, generation));
                        true
                    }
                }
            }
        };
        if !claimed {
            tracing::info!(
                "automation sensor: dropping stale run for '{n}' (unwatched/re-watched mid-tick)",
                n = workflow.name
            );
            continue;
        }
        // Ready timestamps were stamped under the lock above — flush them
        // so a daemon restart doesn't re-fire these intervals.
        save_watched_entries(watched).await;
        // save() awaits — close the window: honor a pending_remove that
        // landed while we were persisting.
        let aborted = {
            let w = watched.lock().await;
            w.iter().any(|ww| {
                ww.workflow.id == wf_id && ww.generation == generation && ww.pending_remove
            })
        };
        if aborted {
            tracing::info!(
                "automation sensor: aborted run for '{n}' before execute (unregistered during save)",
                n = workflow.name
            );
            sensor_clear_claim(watched, wf_id, generation).await;
            continue;
        }
        tracing::info!(
            "automation sensor: {} node(s) ready in '{}' — executing",
            ready_nodes.len(), workflow.name
        );
        daemon_log(&format!(
            "run '{}' ({wf_id}) gen={generation} nodes={}",
            workflow.name,
            ready_nodes.len()
        ));
        let run_started = std::time::Instant::now();
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
                    daemon_log(&format!(
                        "run-done '{}' ({wf_id}) errors={errs} elapsed={:?} first={first_err}",
                        workflow.name,
                        run_started.elapsed()
                    ));
                } else {
                    tracing::info!("automation run of '{}' completed", workflow.name);
                    daemon_log(&format!(
                        "run-done '{}' ({wf_id}) errors=0 elapsed={:?}",
                        workflow.name,
                        run_started.elapsed()
                    ));
                }
            }
            Err(e) => {
                tracing::error!("automation run of '{}' failed to start: {e}", workflow.name);
                daemon_log(&format!(
                    "run-fail '{}' ({wf_id}) start-error={e}",
                    workflow.name
                ));
            }
        }
        // Run finished (or failed to start): release the claim and honor
        // any unregister that was deferred while we held it.
        sensor_clear_claim(watched, wf_id, generation).await;
    }
}

/// Clear a sensor-run claim; if an unregister was deferred behind it,
/// remove the entry now and persist (codex-review P1 r2).
async fn sensor_clear_claim(
    watched: &Arc<Mutex<Vec<WatchedWorkflow>>>,
    wf_id: Uuid,
    generation: u64,
) {
    let remove_now = {
        let mut w = watched.lock().await;
        let mut remove = false;
        for ww in w.iter_mut() {
            if ww.running_claim == Some((wf_id, generation)) {
                ww.running_claim = None;
                if ww.pending_remove {
                    ww.pending_remove = false;
                    remove = true;
                }
                break;
            }
        }
        if remove {
            w.retain(|ww| !(ww.workflow.id == wf_id && ww.generation == generation));
        }
        remove
    };
    if remove_now {
        save_watched_entries(watched).await;
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
                    watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
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
                        watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
                    status: None,
                };
            }
            // A non-string `automation` value is a misconfiguration, not a
            // missing condition — reject loudly instead of silently skipping
            // the node every tick (codex-review P1 r2).
            for i in workflow.graph.node_indices() {
                if let Some(v) = workflow.graph[i].config.get("automation") {
                    if v.as_str().is_none() {
                        return DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(false),
                            error: Some(format!(
                                "node {} has a non-string 'automation' value ({v}) — use a quoted DSL string",
                                workflow.graph[i].id
                            )),
                            workflow_id: None,
                            workflows: None,
                            watched: None,
                logs: None,
                            status: None,
                        };
                    }
                }
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
                            watched: None,
                logs: None,
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
            // Re-watch replacing an old entry: if a sensor run is in flight
            // on the OLD entry, defer its removal (pending_remove) — the
            // claim-holder finalizes it. The new entry starts unclaimed.
            for old in w.iter_mut() {
                if old.workflow.id == wf_id && old.running_claim.is_some() {
                    old.pending_remove = true;
                }
            }
            w.retain(|ww| !(ww.workflow.id == wf_id && !ww.pending_remove));
            w.push(WatchedWorkflow {
                workflow,
                last_ready: prev_last_ready,
                path: Some(std::path::PathBuf::from(path)),
                generation: WATCH_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                running_claim: None,
                pending_remove: false,
            });
            tracing::info!("automation sensor now watching '{n}' ({wf_id})");
            daemon_log(&format!("watch '{n}' ({wf_id}) path={path}"));
            drop(w);
            save_watched_entries(watched).await;
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: Some(wf_id.to_string()),
                workflows: None,
                watched: None,
                logs: None,
                status: None,
            }
        }

        "daemon.ping" => DaemonResponse {
            id: req.id.clone(),
            ok: Some(true),
            error: None,
            workflow_id: None,
            workflows: None,
            watched: None,
            logs: None,
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
                    watched: None,
                logs: None,
                    status: None,
                };
            }
            let mut w = watched.lock().await;
            let before = w.len();
            // Path matching: canonicalize (like `daemon watch` does) and
            // compare both raw and canonical forms — a watch registered
            // from one symlink/relative spelling must still unregister.
            let canon = path.and_then(|p| std::fs::canonicalize(p).ok());
            let mut deferred = 0usize;
            w.retain_mut(|ww| {
                let id_match = wf_id.map(|id| ww.workflow.id == id).unwrap_or(false);
                let path_match = path
                    .zip(ww.path.as_ref())
                    .map(|(p, wwp)| {
                        wwp.to_string_lossy() == p
                            || canon
                                .as_ref()
                                .map(|c| {
                                    std::fs::canonicalize(wwp).map(|cp| cp == *c).unwrap_or(false)
                                })
                                .unwrap_or(false)
                    })
                    .unwrap_or(false);
                if id_match || path_match {
                    if ww.running_claim.is_some() {
                        // A sensor run is in flight on this entry — removing
                        // it now would strand the run's post-run bookkeeping.
                        // Defer: the claim-holder removes it on completion.
                        ww.pending_remove = true;
                        deferred += 1;
                        true // keep for now
                    } else {
                        false
                    }
                } else {
                    true
                }
            });
            let removed = before - w.len();
            drop(w);
            if removed > 0 || deferred > 0 {
                save_watched_entries(watched).await;
            }
            match wf_id {
                Some(id) => {
                    tracing::info!(
                        "automation sensor: unregistered {removed} watch(es) for {id}"
                    );
                    daemon_log(&format!("unwatch id={id} removed={removed} deferred={deferred}"));
                }
                None => {
                    tracing::info!(
                        "automation sensor: unregistered {removed} watch(es) for path {path:?}"
                    );
                    daemon_log(&format!(
                        "unwatch path={path:?} removed={removed} deferred={deferred}"
                    ));
                }
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
                watched: None,
                logs: None,
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
                watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
                    status: None,
                },
                Err(e) => DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some(e.to_string()),
                    workflow_id: None,
                    workflows: None,
                    watched: None,
                logs: None,
                    status: None,
                },
            }
        }

        // ADR-014 Stage 3: list watched (sensor-registered) workflows with
        // their generation, claim/pending_remove state and last_ready
        // timestamps. No params.
        "automation.list" => {
            let w = watched.lock().await;
            let list: Vec<WatchedInfo> = w
                .iter()
                .map(|e| WatchedInfo {
                    workflow_id: e.workflow.id.to_string(),
                    name: e.workflow.name.clone(),
                    path: e.path.as_ref().map(|p| p.display().to_string()),
                    generation: e.generation,
                    pending_remove: e.pending_remove,
                    running: e.running_claim.is_some(),
                    last_ready: e
                        .last_ready
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_rfc3339()))
                        .collect(),
                })
                .collect();
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: None,
                workflows: None,
                watched: Some(list),
                logs: None,
                status: None,
            }
        }

        "automation.logs" => {
            // Tail the persistent event log. Default 50 lines; the CLI
            // passes an explicit tail. Missing log is not an error.
            let tail = req
                .params
                .get("tail")
                .and_then(|v| v.as_u64())
                .map(|v| v.clamp(1, 10_000) as usize)
                .unwrap_or(50);
            DaemonResponse {
                id: req.id.clone(),
                ok: Some(true),
                error: None,
                workflow_id: None,
                workflows: None,
                watched: None,
                logs: Some(daemon_log_tail(tail)),
                status: None,
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
                watched: None,
                logs: None,
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
                            watched: None,
                logs: None,
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
                            watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
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
                            watched: None,
                logs: None,
                            status: None,
                        }
                    } else {
                        DaemonResponse {
                            id: req.id.clone(),
                            ok: Some(false),
                            error: Some(format!("Workflow {} not found", id)),
                            workflow_id: None,
                            workflows: None,
                            watched: None,
                logs: None,
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
                    watched: None,
                logs: None,
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
            watched: None,
            logs: None,
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

    /// Serializes tests that mutate the process-global SWANFLOW_CACHE_DIR.
    /// Parallel #[test]s racing on set_var made load_watched_entries read
    /// the WRONG dir (observed flaky); a module-level mutex keeps them
    /// exclusive without new deps.
    static ENV_MUX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// RAII guard: hold the env lock, set the dir, restore on drop.
    struct EnvDir<'a> {
        prev: Option<String>,
        _g: std::sync::MutexGuard<'a, ()>,
    }

    impl<'a> EnvDir<'a> {
        fn set(dir: &str) -> Self {
            let g = ENV_MUX.lock().unwrap_or_else(|p| p.into_inner());
            let prev = std::env::var("SWANFLOW_CACHE_DIR").ok();
            std::env::set_var("SWANFLOW_CACHE_DIR", dir);
            Self { prev, _g: g }
        }
    }

    impl Drop for EnvDir<'_> {
        fn drop(&mut self) {
            match self.prev.clone() {
                Some(v) => std::env::set_var("SWANFLOW_CACHE_DIR", v),
                None => std::env::remove_var("SWANFLOW_CACHE_DIR"),
            }
        }
    }

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
        let _env = EnvDir::set(dir.to_str().unwrap());

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
            running_claim: None,
            pending_remove: false,
        });
        let mut wf2 = Workflow::default();
        wf2.id = uuid::Uuid::new_v4();
        wf2.name = "ephemeral".into();
        watched.lock().await.push(WatchedWorkflow {
            workflow: wf2,
            last_ready: HashMap::new(),
            path: None,
            generation: 2,
            running_claim: None,
            pending_remove: false,
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

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_saves_never_lose_state() {
        let dir = std::env::temp_dir().join(format!("swanflow_watched_race_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // codex-review P1 (r2): concurrent save_watched_entries callers
        // used to share one .tmp path and could rename a STALE snapshot
        // over a newer persisted state. With SAVE_MUTEX + unique tmp
        // names + rename-time re-check, racing savers must converge to
        // the NEWEST list. Here: 8 tasks each add their own workflow
        // then save concurrently; the file must finally contain all 8.
        let _env = EnvDir::set(dir.to_str().unwrap());

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
                    running_claim: None,
                    pending_remove: false,
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
            running_claim: None,
            pending_remove: false,
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
                running_claim: None,
                pending_remove: false,
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

    #[tokio::test]
    async fn unregister_during_run_defers_until_claim_released() {
        // codex-review P1 (r2): the sensor used to verify (id, generation)
        // and release the lock, then await save + execute — an unregister
        // in that window removed the entry while the stale run proceeded
        // and its post-run bookkeeping was lost. With claims: unregister
        // marks pending_remove, the entry survives until the claim is
        // cleared, THEN is removed and persisted.
        let dir = std::env::temp_dir().join(format!(
            "swanflow_claim_e2e_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let _env = EnvDir::set(dir.to_str().unwrap());

        let watched: Arc<Mutex<Vec<WatchedWorkflow>>> = Arc::new(Mutex::new(Vec::new()));
        let wf_id = uuid::Uuid::new_v4();
        let mut wf = Workflow::default();
        wf.id = wf_id;
        wf.name = "claim_test".into();
        watched.lock().await.push(WatchedWorkflow {
            workflow: wf,
            last_ready: HashMap::new(),
            path: Some(std::path::PathBuf::from("/tmp/claim_wf.yaml")),
            generation: 42,
            running_claim: None,
            pending_remove: false,
        });

        // 1) Sensor claims the run (same shape as sensor_tick).
        {
            let mut w = watched.lock().await;
            let ww = w
                .iter_mut()
                .find(|ww| ww.workflow.id == wf_id && ww.generation == 42)
                .unwrap();
            ww.running_claim = Some((wf_id, 42));
        }

        // 2) Unregister arrives mid-run: entry must survive, flagged.
        {
            let mut w = watched.lock().await;
            w.retain_mut(|ww| {
                if ww.workflow.id == wf_id {
                    if ww.running_claim.is_some() {
                        ww.pending_remove = true;
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            });
        }
        {
            let w = watched.lock().await;
            assert_eq!(w.len(), 1, "claimed entry must survive unregister");
            assert!(w[0].pending_remove, "removal must be deferred");
        }

        // 3) Run finishes -> claim cleared -> deferred removal lands.
        sensor_clear_claim(&watched, wf_id, 42).await;
        {
            let w = watched.lock().await;
            assert!(w.is_empty(), "entry must be gone after claim release");
        }
        // And the persisted file reflects the removal (empty list).
        let entries = load_watched_entries();
        assert!(entries.is_empty(), "watched.json must show removal: {entries:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn daemon_log_append_and_tail() {
        // Events append as timestamped lines; tail returns the last N,
        // oldest first; a missing log is empty (not an error).
        let dir = std::env::temp_dir().join(format!("sf_daemon_log_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let _g = EnvDir::set(dir.to_str().unwrap());

        // No log yet -> empty tail.
        assert!(daemon_log_tail(10).is_empty());

        daemon_log("watch 'wf' (abc) path=/tmp/wf.yaml");
        daemon_log("run 'wf' (abc) gen=1 nodes=2");
        daemon_log("run-done 'wf' (abc) errors=0");

        let all = daemon_log_tail(10);
        assert_eq!(all.len(), 3, "all three events: {all:?}");
        assert!(all[0].contains("watch 'wf'"), "oldest first: {all:?}");
        assert!(all[0].contains('T'), "RFC3339 timestamp prefix: {}", all[0]);
        assert!(all[2].contains("run-done"), "newest last: {all:?}");

        // Tail smaller than the log -> only the last lines.
        let last2 = daemon_log_tail(2);
        assert_eq!(last2.len(), 2);
        assert!(last2[0].contains("run 'wf'"), "tail window: {last2:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PID-identity guard tests (start/stop double-daemon + PID-reuse fix)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod daemon_pid_identity_tests {
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_cmdline_splits_on_nul_and_drops_trailing() {
        // Typical /proc/<pid>/cmdline: NUL-separated, with a trailing NUL.
        let raw = b"swanflow\0daemon\0start\0--foreground\0";
        let args = parse_cmdline(raw);
        assert_eq!(args, vec!["swanflow", "daemon", "start", "--foreground"]);
    }

    #[test]
    fn parse_cmdline_handles_missing_trailing_nul() {
        let raw = b"swanflow\0daemon\0start\0--foreground";
        let args = parse_cmdline(raw);
        assert_eq!(args, vec!["swanflow", "daemon", "start", "--foreground"]);
    }

    #[test]
    fn parse_cmdline_empty_input() {
        assert!(parse_cmdline(b"").is_empty());
    }

    #[test]
    fn cmdline_is_daemon_requires_both_args() {
        // A forked background daemon child: subcommand + --foreground.
        assert!(cmdline_is_daemon(&argv(&[
            "swanflow",
            "daemon",
            "start",
            "--foreground"
        ])));
        // Missing --foreground (e.g. the interactive `daemon start` parent):
        assert!(!cmdline_is_daemon(&argv(&["swanflow", "daemon", "start"])));
        // Missing the `daemon` subcommand (an innocent process that
        // happens to take a --foreground flag):
        assert!(!cmdline_is_daemon(&argv(&["bash", "--foreground"])));
        // Innocent process whose arg list merely contains the word daemon:
        assert!(!cmdline_is_daemon(&argv(&["vim", "daemon"])));
        // Empty argv:
        assert!(!cmdline_is_daemon(&argv(&[])));
    }

    #[test]
    fn cmdline_is_daemon_full_forked_argv() {
        // Realistic argv of the forked daemon child, incl. extra flags.
        let args = argv(&[
            "/usr/bin/swanflow",
            "daemon",
            "start",
            "--foreground",
            "--socket",
            "/home/u/.swanflow/daemon.sock",
            "--tick",
            "5",
        ]);
        assert!(cmdline_is_daemon(&args));
    }

    #[test]
    fn cmdline_is_daemon_no_substring_false_positive() {
        // "daemon" appears only as a substring of a path arg — exact
        // matching must NOT count it.
        let args = argv(&["/usr/bin/swanflow", "run", "/tmp/daemon.sock"]);
        assert!(!cmdline_is_daemon(&args));
    }

    #[test]
    fn parse_then_decide_roundtrip() {
        // Simulate the full /proc read: raw cmdline bytes -> argv -> verdict.
        let raw = b"/usr/bin/swanflow\0daemon\0start\0--foreground\0--socket\0/tmp/x.sock\0";
        assert!(cmdline_is_daemon(&parse_cmdline(raw)));
        let innocent = b"/bin/bash\0-c\0swanflow run x.json\0";
        assert!(!cmdline_is_daemon(&parse_cmdline(innocent)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn is_swanflow_daemon_rejects_nonpositive_and_dead_pid() {
        // pid <= 0 short-circuits; an out-of-range PID has no /proc entry.
        assert!(!is_swanflow_daemon(0));
        assert!(!is_swanflow_daemon(-1));
        assert!(!is_swanflow_daemon(i32::MAX));
    }
}
