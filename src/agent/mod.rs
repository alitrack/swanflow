use std::sync::Arc;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use crate::nodes::ai::providers::LlmProvider;
use crate::core::graph::Workflow;
use crate::nodes::registry::NodeRegistry;

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentToolCall {
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentResponse {
    pub thought: String,
    pub tool_calls: Vec<AgentToolCall>,
    pub final_answer: Option<String>,
}

pub struct AnalystAgent {
    provider: Arc<dyn LlmProvider>,
    registry: Arc<NodeRegistry>,
}

impl AnalystAgent {
    pub fn new(provider: Arc<dyn LlmProvider>, registry: Arc<NodeRegistry>) -> Self {
        Self { provider, registry }
    }

    /// The core reasoning loop, adapted from ZeroClaw.
    pub async fn run_loop(&self, user_prompt: &str, workflow: &mut Workflow) -> Result<String> {
        let mut history = format!("User: {}\n", user_prompt);
        let system_prompt = self.build_system_prompt();

        for _iteration in 0..10 {
            let full_prompt = format!("System: {}\n{}", system_prompt, history);
            let response_text = self.provider.complete(&full_prompt).await?;
            
            // Simplified parsing: assume JSON response for MVP
            let response: AgentResponse = serde_json::from_str(&response_text)
                .map_err(|e| anyhow::anyhow!("Failed to parse agent response: {}. Response was: {}", e, response_text))?;

            println!("🤖 Thought: {}", response.thought);

            if let Some(answer) = response.final_answer {
                return Ok(answer);
            }

            for call in response.tool_calls {
                let result = self.execute_tool(&call, workflow).await?;
                history.push_str(&format!("\nTool Result ({}): {}\n", call.name, result));
            }
        }

        anyhow::bail!("Exceeded maximum iterations")
    }

    fn build_system_prompt(&self) -> String {
        let node_specs = self.registry.list_specs();
        let specs_json = serde_json::to_string_pretty(&node_specs).unwrap();

        format!(
            "You are the SwanFlow Analyst Agent. Your goal is to build a data science workflow to solve the user's request.\n\
            Available Nodes:\n{}\n\n\
            You can use the following tools:\n\
            - add_node(type_id, config): Add a node to the DAG.\n\
            - connect(source_id, target_id, source_port, target_port): Connect two nodes.\n\
            - inspect_schema(node_id): Get the output schema of a node.\n\n\
            Respond in JSON format like this: {{ \"thought\": \"...\", \"tool_calls\": [], \"final_answer\": null }}",
            specs_json
        )
    }

    async fn execute_tool(&self, call: &AgentToolCall, _workflow: &mut Workflow) -> Result<String> {
        match call.name.as_str() {
            "add_node" => {
                let type_id = call.arguments["type_id"].as_str().unwrap();
                let _config = call.arguments["config"].clone();
                // ... logic to add node to workflow ...
                Ok(format!("Added node of type {}", type_id))
            }
            "connect" => {
                // ... logic to connect nodes ...
                Ok("Connected nodes".to_string())
            }
            _ => anyhow::bail!("Unknown tool: {}", call.name),
        }
    }
}
