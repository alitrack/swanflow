use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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
#[non_exhaustive]
pub enum NodeState {
    Unconfigured,
    Configured,
    Running,
    Executed,
    Error(String),
}

/// Capability model for nodes — inspired by Nika's read-XOR-write model.
///
/// - `ReadOnly`: transforms data in-memory, no external side effects
/// - `ReadWrite`: may read from or write to external systems
/// - `WriteOnly`: sends data out (e.g. email, webhook), no output port
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[non_exhaustive]
pub enum NodeCapability {
    #[default]
    ReadWrite,
    ReadOnly,
    WriteOnly,
}

/// Event emitted by a node during its lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum NodeEvent {
    StateChanged {
        node_id: Uuid,
        state: NodeState,
    },
    Materialized {
        node_id: Uuid,
        table_name: String,
        parquet_path: String,
    },
    Progress {
        node_id: Uuid,
        percentage: f32,
    },
    Error {
        node_id: Uuid,
        message: String,
    },
}

/// Error classes used for deterministic routing and policy decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeErrorClass {
    Validation,
    Migration,
    Planning,
    Execution,
}

impl NodeErrorClass {
    pub fn is_recoverable(&self) -> bool {
        matches!(self, Self::Planning | Self::Execution)
    }
}

/// Structured node-scoped diagnostic payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeDiagnostic {
    pub node_id: Uuid,
    pub node_type: String,
    pub class: NodeErrorClass,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failed_step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remediation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<JsonValue>,
}

impl NodeDiagnostic {
    pub fn new(
        node_id: Uuid,
        node_type: impl Into<String>,
        class: NodeErrorClass,
        message: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            node_type: node_type.into(),
            class,
            message: message.into(),
            source_version: None,
            target_version: None,
            failed_step: None,
            remediation: None,
            metadata: None,
        }
    }

    pub fn with_versions(mut self, source_version: u32, target_version: u32) -> Self {
        self.source_version = Some(source_version);
        self.target_version = Some(target_version);
        self
    }

    pub fn with_failed_step(mut self, failed_step: impl Into<String>) -> Self {
        self.failed_step = Some(failed_step.into());
        self
    }

    pub fn with_remediation(mut self, remediation: impl Into<String>) -> Self {
        self.remediation = Some(remediation.into());
        self
    }

    pub fn with_metadata(mut self, metadata: JsonValue) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn to_event_message(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            json!({
                "node_id": self.node_id,
                "node_type": self.node_type,
                "class": format!("{:?}", self.class).to_lowercase(),
                "message": self.message,
            })
            .to_string()
        })
    }
}

/// Metadata inferred before full execution.
pub struct InferredMetadata {
    pub schemas: Vec<DFSchemaRef>,
    /// Optional sample data for each output port.
    pub sample_data: Option<Vec<RecordBatch>>,
}

/// Standard Graph Data Protocol
pub struct GraphSchemas;

impl GraphSchemas {
    pub fn node_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("properties", DataType::Utf8, true), // JSON string
        ]))
    }

    pub fn edge_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Int64, false),
            Field::new("target_id", DataType::Int64, false),
            Field::new("type", DataType::Utf8, true),
            Field::new("weight", DataType::Float64, true),
        ]))
    }
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

    /// Current implementation version for this node.
    fn impl_version(&self) -> u32 {
        1
    }

    /// Current config schema version for this node.
    fn config_version(&self) -> u32 {
        1
    }

    /// Returns the capability model for this node.
    ///
    /// - `ReadOnly`: transforms data in-memory, no external side effects
    /// - `ReadWrite`: may read from or write to external systems
    /// - `WriteOnly`: sends data to external systems (e.g. email, webhook)
    ///
    /// Default is ReadWrite. Pure transform nodes (filter, project, aggregate)
    /// should override to ReadOnly for safety gating.
    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadWrite
    }

    /// Migrate config from one schema version to the next.
    fn migrate_config(
        &self,
        config: JsonValue,
        from_version: u32,
        to_version: u32,
    ) -> Result<JsonValue> {
        if to_version != from_version + 1 {
            return Err(anyhow!(
                "Migration steps must be incremental ({} -> {})",
                from_version,
                to_version
            ));
        }
        Ok(config)
    }

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

/// Typed extension of NodeBehavior with compile-time port-count guarantees.
///
/// Unlike the base `NodeBehavior` trait which accepts `Vec<LogicalPlan>`,
/// `TypedNodeBehavior` enforces fixed input/output port counts at the type
/// level. The engine can validate arity before proposing plans.
///
/// Inspired by FluxFlow's `FlowNode<TIn, TOut>` generics, adapted for
/// DataFusion's dynamic-schema architecture where type-level Arrow types
/// are infeasible.
#[async_trait]
pub trait TypedNodeBehavior: NodeBehavior {
    /// Fixed number of input ports.
    const INPUT_COUNT: usize;
    /// Fixed number of output ports.
    const OUTPUT_COUNT: usize;

    /// Validate that the actual input count matches the expected count.
    fn validate_input_count(actual: usize) -> Result<()> {
        if actual != Self::INPUT_COUNT {
            Err(anyhow!(
                "Node expects {} inputs but received {}",
                Self::INPUT_COUNT,
                actual
            ))
        } else {
            Ok(())
        }
    }

    /// Build designer metadata from the node's spec + type-level constants.
    fn designer_metadata(&self) -> DesignerMetadata {
        let spec = self.spec();
        DesignerMetadata {
            type_id: spec.type_id.clone(),
            display_name: spec.display_name.clone(),
            description: spec.description.clone(),
            category: NodeCategory::Other("uncategorized".into()),
            icon: None,
            input_count: Self::INPUT_COUNT,
            output_count: Self::OUTPUT_COUNT,
            inputs: spec
                .input_ports
                .iter()
                .map(|p| PortDesignMetadata {
                    name: p.name.clone(),
                    description: p.description.clone(),
                    data_type: DataType::Utf8,
                })
                .collect(),
            outputs: spec
                .output_ports
                .iter()
                .map(|p| PortDesignMetadata {
                    name: p.name.clone(),
                    description: p.description.clone(),
                    data_type: DataType::Utf8,
                })
                .collect(),
            options: vec![],
            config_schema: spec.config_schema,
            capability: self.capability(),
            impl_version: self.impl_version(),
            config_version: self.config_version(),
        }
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Designer Metadata — standalone metadata for visual editor consumption.
// Separated from NodeBehavior so the editor can enumerate node types
// without constructing node instances.
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/// Category for organizing nodes in the visual editor palette.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeCategory {
    IO,
    Transform,
    Analysis,
    Quality,
    Script,
    AI,
    Flow,
    Connector,
    Other(String),
}

/// Per-option metadata for the visual editor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionDesignMetadata {
    /// The config key this option maps to.
    pub key: String,
    /// Human-readable display name.
    pub display_name: String,
    /// Description / help text.
    pub description: String,
    /// Expected Arrow type or JSON schema type.
    pub value_type: OptionValueType,
    /// Default value (JSON).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<JsonValue>,
    /// Whether this option is required.
    #[serde(default)]
    pub required: bool,
    /// Valid choices (for enum-style options).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub choices: Vec<OptionChoice>,
}

/// A valid choice for an option.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionChoice {
    pub value: String,
    pub label: String,
}

/// The type hint for an option value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OptionValueType {
    String,
    Integer,
    Float,
    Boolean,
    Json,
    ColumnRef,
    Expression,
    Path,
    Url,
    Secret,
}

/// Per-port metadata for the visual editor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortDesignMetadata {
    pub name: String,
    pub description: String,
    /// Arrow DataType hint (serialized as string).
    #[serde(with = "serde_datatype")]
    pub data_type: DataType,
}

/// Standalone designer metadata for a node type.
/// This can be constructed without instantiating a NodeBehavior,
/// enabling the visual editor to enumerate available node types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DesignerMetadata {
    pub type_id: String,
    pub display_name: String,
    pub description: String,
    pub category: NodeCategory,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon: Option<String>,
    /// Number of input ports (fixed for most nodes).
    pub input_count: usize,
    /// Number of output ports (fixed for most nodes).
    pub output_count: usize,
    pub inputs: Vec<PortDesignMetadata>,
    pub outputs: Vec<PortDesignMetadata>,
    pub options: Vec<OptionDesignMetadata>,
    /// Config JSON schema for validation.
    pub config_schema: JsonValue,
    /// Capability hint for safety gating.
    #[serde(default)]
    pub capability: NodeCapability,
    /// Implementation version for migration tracking.
    #[serde(default = "default_version")]
    pub impl_version: u32,
    /// Config schema version.
    #[serde(default = "default_version")]
    pub config_version: u32,
}

fn default_version() -> u32 {
    1
}

/// Serde module for DataType ↔ string conversion.
mod serde_datatype {
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &DataType, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        format!("{:?}", dt).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DataType, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "Utf8" | "LargeUtf8" => DataType::Utf8,
            "Int32" => DataType::Int32,
            "Int64" => DataType::Int64,
            "Float32" => DataType::Float32,
            "Float64" => DataType::Float64,
            "Boolean" => DataType::Boolean,
            "Date32" => DataType::Date32,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(TimeUnit::Nanosecond, None),
            _ => DataType::Utf8,
        })
    }
}
