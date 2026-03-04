use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationItem {
    pub source_id: String,
    pub snippet: String,
    pub score: f32,
    pub start: usize,
    pub end: usize,
}

pub struct StructuredIoContractNode;

#[async_trait]
impl NodeBehavior for StructuredIoContractNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.structured_io_contract".to_string(),
            display_name: "Structured IO Contract".to_string(),
            description: "Validate structured output contract requirements.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Validated output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "required_fields": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                },
                "required": ["required_fields"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let required_fields = config["required_fields"]
            .as_array()
            .ok_or_else(|| anyhow!("required_fields must be an array"))?;

        if required_fields.is_empty() {
            return Err(anyhow!("required_fields must not be empty"));
        }

        for field in required_fields {
            if field.as_str().is_none() {
                return Err(anyhow!("required_fields must contain only strings"));
            }
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!(
                "Structured IO Contract requires one input plan"
            ));
        }
        Ok(vec![inputs[0].clone()])
    }
}
