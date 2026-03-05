pub mod hook;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::{NodeInstance, Workflow};
use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeState;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentToolCall {
    pub name: String,
    #[serde(default)]
    pub arguments: JsonValue,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AgentResponse {
    #[serde(default)]
    pub thought: Option<String>,
    #[serde(default)]
    pub tool_calls: Vec<AgentToolCall>,
    #[serde(default)]
    pub final_answer: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AddNodeArguments {
    pub type_id: String,
    #[serde(default)]
    pub config: JsonValue,
}

pub struct AnalystAgent {
    provider: Arc<dyn LlmProvider>,
    registry: Arc<NodeRegistry>,
    #[allow(dead_code)]
    engine: Arc<SwanFlowEngine>,
}

impl AnalystAgent {
    pub fn new(
        provider: Arc<dyn LlmProvider>,
        registry: Arc<NodeRegistry>,
        engine: Arc<SwanFlowEngine>,
    ) -> Self {
        Self {
            provider,
            registry,
            engine,
        }
    }

    pub async fn run_analysis(&self, prompt: &str) -> Result<Workflow> {
        let completion = self.provider.complete(prompt).await?;
        let response = self.parse_response(&completion)?;
        self.build_workflow_from_response(response)
    }

    fn parse_response(&self, completion: &str) -> Result<AgentResponse> {
        let json_block =
            crate::nodes::ai::ConfigWizard::extract_json_from_response(completion).unwrap_or(completion);
        serde_json::from_str::<AgentResponse>(json_block.trim())
            .with_context(|| format!("failed to parse agent response JSON: {}", completion))
    }

    fn build_workflow_from_response(&self, response: AgentResponse) -> Result<Workflow> {
        let mut workflow = Workflow::new("agent-analysis");

        for call in response.tool_calls {
            match call.name.as_str() {
                "add_node" => {
                    let args: AddNodeArguments = serde_json::from_value(call.arguments)
                        .context("invalid add_node arguments")?;
                    self.registry
                        .get_node(&args.type_id)
                        .with_context(|| format!("unknown node type requested by agent: {}", args.type_id))?;

                    workflow.add_node(NodeInstance {
                        id: Uuid::new_v4(),
                        type_id: args.type_id,
                        config: args.config,
                        state: NodeState::Configured,
                        materialize: false,
                        sub_workflow: None,
                    });
                }
                _ => {}
            }
        }

        Ok(workflow)
    }
}
