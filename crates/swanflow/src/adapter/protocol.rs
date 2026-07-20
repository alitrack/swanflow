use serde::{Deserialize, Serialize};

use crate::core::graph::Workflow;
use crate::nodes::{NodeEvent, NodeState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterFlowRevision {
    pub workflow_id: String,
    pub revision_id: String,
    pub workflow: Workflow,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AdapterRequest {
    Inspect {
        request_id: String,
        flow_revision: AdapterFlowRevision,
    },
    Execute {
        request_id: String,
        run_id: String,
        flow_revision: AdapterFlowRevision,
    },
    Cancel {
        request_id: String,
        run_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdapterDiagnostic {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdapterSchemaSummary {
    pub node_id: String,
    pub ports: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[non_exhaustive]
pub enum CancellationStatus {
    #[serde(rename = "accepted")]
    Accepted,
    #[serde(rename = "not_found")]
    NotFound,
    #[serde(rename = "unsupported")]
    Unsupported,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AdapterExecutionEvent {
    NodeStateChanged {
        node_id: String,
        state: String,
    },
    Progress {
        node_id: String,
        percentage: f32,
    },
    Error {
        node_id: String,
        message: String,
    },
    RunCompleted {
        status: String,
        terminal_output_refs: Vec<String>,
    },
}

impl AdapterExecutionEvent {
    pub fn from_node_event(event: NodeEvent) -> Self {
        match event {
            NodeEvent::StateChanged { node_id, state } => Self::NodeStateChanged {
                node_id: node_id.to_string(),
                state: serialize_node_state(state),
            },
            NodeEvent::Progress {
                node_id,
                percentage,
            } => Self::Progress {
                node_id: node_id.to_string(),
                percentage,
            },
            NodeEvent::Error { node_id, message } => Self::Error {
                node_id: node_id.to_string(),
                message,
            },
            NodeEvent::Materialized { .. } => {
                // materialization events are not forwarded through the adapter protocol
                return Self::Progress {
                    node_id: String::new(),
                    percentage: 100.0,
                };
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[non_exhaustive]
pub enum AdapterMessage {
    InspectResult {
        request_id: String,
        diagnostics: Vec<AdapterDiagnostic>,
        schema_summary: Vec<AdapterSchemaSummary>,
    },
    ExecuteAccepted {
        request_id: String,
        run_id: String,
    },
    ExecutionEvent {
        request_id: String,
        run_id: String,
        event: AdapterExecutionEvent,
    },
    CancelResult {
        request_id: String,
        run_id: String,
        status: CancellationStatus,
    },
    Error {
        request_id: Option<String>,
        code: String,
        message: String,
    },
}

fn serialize_node_state(state: NodeState) -> String {
    match state {
        NodeState::Unconfigured => "unconfigured".to_owned(),
        NodeState::Configured => "configured".to_owned(),
        NodeState::Pending => "pending".to_owned(),
        NodeState::Running => "running".to_owned(),
        NodeState::Waiting => "waiting".to_owned(),
        NodeState::Executed => "executed".to_owned(),
        NodeState::Error(message) => format!("error:{message}"),
    }
}
