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

pub struct Daemon {
    socket_path: PathBuf,
    engine: Arc<SwanFlowEngine>,
    workflows: Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
}

impl Daemon {
    pub fn new(engine: SwanFlowEngine) -> Self {
        Self {
            socket_path: default_socket_path(),
            engine: Arc::new(engine),
            workflows: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_socket_path(mut self, path: PathBuf) -> Self {
        self.socket_path = path;
        self
    }

    /// Start the daemon — listens on Unix socket indefinitely.
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

        let listener = UnixListener::bind(&self.socket_path)?;
        tracing::info!("SwanFlow daemon listening on {}", self.socket_path.display());

        loop {
            let (stream, _addr) = listener.accept().await?;
            let engine = self.engine.clone();
            let workflows = self.workflows.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_client(stream, engine, workflows).await {
                    tracing::warn!("Client error: {}", e);
                }
            });
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
                let _ = write_response(&mut writer, &resp).await;
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
                stream_workflow_events(&mut writer, id, &workflows).await;
            } else {
                let resp = DaemonResponse {
                    id: req.id.clone(),
                    ok: Some(false),
                    error: Some("Missing or invalid workflow_id".to_string()),
                    workflow_id: None,
                    workflows: None,
                    status: None,
                };
                let _ = write_response(&mut writer, &resp).await;
            }
            return Ok(());
        }

        let resp = handle_method(&req, &engine, &workflows).await;
        let _ = write_response(&mut writer, &resp).await;
    }

    Ok(())
}

async fn handle_method(
    req: &DaemonRequest,
    engine: &Arc<SwanFlowEngine>,
    workflows: &Arc<Mutex<HashMap<Uuid, RunningWorkflow>>>,
) -> DaemonResponse {
    match req.method.as_str() {
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

            match submit_workflow(path, engine, workflows).await {
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
    let workflow_json = serde_json::to_value(&core_wf)
        .map_err(|e| format!("Serialization error: {}", e))?;
    let workflow: Workflow = serde_json::from_value(workflow_json)
        .map_err(|e| format!("Deserialization error: {}", e))?;
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
) {
    // Send initial ack
    let ack = serde_json::json!({"event":"workflow.subscribed","workflow_id":wf_id.to_string()});
    let _ = writer.write_all(format!("{}\n", ack).as_bytes()).await;

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
                let _ = writer
                    .write_all(format!("{}\n", serde_json::to_string(&event).unwrap_or_default()).as_bytes())
                    .await;

                state.run_state == "completed" || state.run_state == "error" || state.run_state == "cancelled"
            } else {
                let event = serde_json::json!({
                    "event": "workflow.not_found",
                    "workflow_id": wf_id.to_string(),
                });
                let _ = writer
                    .write_all(format!("{}\n", serde_json::to_string(&event).unwrap_or_default()).as_bytes())
                    .await;
                true
            }
        };

        if done {
            let done_event = serde_json::json!({"event":"workflow.stream_end","workflow_id":wf_id.to_string()});
            let _ = writer.write_all(format!("{}\n", done_event).as_bytes()).await;
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
