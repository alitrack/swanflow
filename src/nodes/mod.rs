use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;
use anyhow::Result;

pub mod registry;
pub mod common;
pub mod sql;
pub mod io;

/// Port identification and description.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortSpec {
    pub id: u32,
    pub name: String,
    pub description: String,
}

/// Metadata and specification for a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSpec {
    pub type_id: String,
    pub display_name: String,
    pub description: String,
    pub input_ports: Vec<PortSpec>,
    pub output_ports: Vec<PortSpec>,
    pub config_schema: JsonValue,
}

/// Execution state of a node (The "Traffic Lights").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum NodeState {
    Unconfigured,
    Configured,
    Running,
    Executed,
    Error(String),
}

/// Event emitted by a node during its lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeEvent {
    StateChanged {
        node_id: Uuid,
        state: NodeState,
    },
    Progress {
        node_id: Uuid,
        percentage: f32,
    },
}

/// The core interface for all nodes in SwanFlow.
/// Nodes implement this trait to describe their behavior and how they
/// modify the DataFusion logical plan.
#[async_trait]
pub trait NodeBehavior: Send + Sync {
    /// Returns the static specification of the node.
    fn spec(&self) -> NodeSpec;

    /// Validates the provided configuration against the node's schema.
    fn validate(&self, config: &JsonValue) -> Result<()>;

    /// Build a DataFusion LogicalPlan based on input plans and configuration.
    /// This is where the core logic of the node is defined.
    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>>;
}
