use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug)]
pub struct MissingNode {
    pub node_id: Uuid,
    pub original_type_id: String,
}

impl MissingNode {
    pub fn new(node_id: Uuid, original_type_id: impl Into<String>) -> Self {
        Self {
            node_id,
            original_type_id: original_type_id.into(),
        }
    }
}

#[async_trait]
impl NodeBehavior for MissingNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.missing.placeholder".to_string(),
            display_name: "Missing Node Placeholder".to_string(),
            description: "Represents a node type that is not installed in current runtime."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Passthrough schema anchor".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Unavailable output".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "additionalProperties": true
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let schema = if inputs.is_empty() {
            Arc::new(DFSchema::empty())
        } else {
            inputs[0].schema().clone()
        };
        let remediation = format!(
            "missing node implementation for '{}'; install extension or migrate node type",
            self.original_type_id
        );

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(MissingNodeLogical {
                node_id: self.node_id,
                original_type_id: self.original_type_id.clone(),
                remediation,
                inputs,
                schema,
            }),
        })])
    }
}

#[derive(Debug, Clone)]
pub struct MissingNodeLogical {
    pub node_id: Uuid,
    pub original_type_id: String,
    pub remediation: String,
    pub inputs: Vec<LogicalPlan>,
    pub schema: DFSchemaRef,
}

impl PartialEq for MissingNodeLogical {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
            && self.original_type_id == other.original_type_id
            && self.remediation == other.remediation
            && self.inputs == other.inputs
    }
}

impl Eq for MissingNodeLogical {}

impl std::hash::Hash for MissingNodeLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
        self.original_type_id.hash(state);
        self.remediation.hash(state);
        self.inputs.hash(state);
    }
}

impl PartialOrd for MissingNodeLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for MissingNodeLogical {
    fn name(&self) -> &str {
        "MissingNodePlaceholder"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MissingNodePlaceholder(node_id={}, original_type_id={})",
            self.node_id, self.original_type_id
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            inputs,
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct MissingNodeExec {
    schema: datafusion::arrow::datatypes::SchemaRef,
    message: String,
    cache: PlanProperties,
}

impl MissingNodeExec {
    pub fn new(logical: &MissingNodeLogical) -> Self {
        let schema = Arc::new(logical.schema.as_arrow().clone());
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            schema,
            message: format!(
                "missing node implementation for '{}' (node_id={}); {}",
                logical.original_type_id, logical.node_id, logical.remediation
            ),
            cache,
        }
    }
}

impl DisplayAs for MissingNodeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "MissingNodeExec")
    }
}

impl ExecutionPlan for MissingNodeExec {
    fn name(&self) -> &str {
        "MissingNodeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        Err(DataFusionError::Execution(self.message.clone()))
    }
}
