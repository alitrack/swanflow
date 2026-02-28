use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;
use anyhow::Result;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

/// Input boundary node for sub-workflows.
/// It acts as a pass-through for plans injected by the parent workflow.
pub struct InputNode;

#[async_trait]
impl NodeBehavior for InputNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.input".to_string(),
            display_name: "Input Port".to_string(),
            description: "Entry point for data into a sub-workflow".to_string(),
            input_ports: vec![], // Injected externally by compiler
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Injected Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "port_index": { "type": "integer", "default": 0 }
                }
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
        // Input nodes usually receive their plan from the 'injected_inputs'
        // which the compiler passes as 'inputs' here.
        Ok(inputs)
    }
}

/// Output boundary node for sub-workflows.
/// It marks the exit point for data to be returned to the parent workflow.
pub struct OutputNode;

#[async_trait]
impl NodeBehavior for OutputNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.output".to_string(),
            display_name: "Output Port".to_string(),
            description: "Exit point for data from a sub-workflow".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Result Data".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "port_index": { "type": "integer", "default": 0 }
                }
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
        // Returns the plan to the compiler to be mapped back to MetaNode outputs.
        Ok(inputs)
    }
}
