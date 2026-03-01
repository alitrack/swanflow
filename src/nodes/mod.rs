use anyhow::Result;
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

pub mod ai;
pub mod boundary;
pub mod common;
pub mod io;
pub mod registry;
pub mod script;
pub mod sql;
pub mod transform;

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
    StateChanged { node_id: Uuid, state: NodeState },
    Progress { node_id: Uuid, percentage: f32 },
    Error { node_id: Uuid, message: String },
}

/// Metadata inferred before full execution.
pub struct InferredMetadata {
    pub schemas: Vec<DFSchemaRef>,
    /// Optional sample data for each output port.
    pub sample_data: Option<Vec<RecordBatch>>,
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
    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>>;

    /// Predict the output schema(s) and optionally provide a data sample.
    async fn infer_schema(
        &self,
        input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        use datafusion::logical_expr::EmptyRelation;

        let inputs = input_schemas
            .into_iter()
            .map(|s| {
                LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: s,
                })
            })
            .collect();

        let output_plans = self.propose_plan(inputs, config).await?;
        Ok(InferredMetadata {
            schemas: output_plans
                .into_iter()
                .map(|p| p.schema().clone())
                .collect(),
            sample_data: None,
        })
    }
}
