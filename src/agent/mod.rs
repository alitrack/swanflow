use std::sync::Arc;
use anyhow::{Result, Context as _};
use serde::{Deserialize, Serialize};
use crate::nodes::ai::providers::LlmProvider;
use crate::core::graph::{Workflow, NodeInstance, EdgeInstance};
use crate::core::engine::SwanFlowEngine;
use crate::nodes::registry::NodeRegistry;
use petgraph::prelude::NodeIndex;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AgentToolCall {
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentResponse {
    #[serde(alias = "explanation")]
    pub thought: String,
    #[serde(alias = "actions")]
    pub tool_calls: Option<Vec<serde_json::Value>>,
    pub final_answer: Option<String>,
}

pub struct AnalystAgent {
    provider: Arc<dyn LlmProvider>,
    registry: Arc<NodeRegistry>,
    engine: Arc<SwanFlowEngine>,
}

impl AnalystAgent {
    pub fn new(
        provider: Arc<dyn LlmProvider>, 
        registry: Arc<NodeRegistry>,
        engine: Arc<SwanFlowEngine>
    ) -> Self {
        Self { provider, registry, engine }
    }

    pub async fn run_analysis(&self, user_prompt: &str) -> Result<Workflow> {
        let mut workflow = Workflow::new("Agent Planned Workflow");
        let mut history = format!("User Request: {}\n", user_prompt);
        let system_prompt = self.build_system_prompt();

        for i in 0..5 {
            let full_prompt = format!("System: {}\n{}", system_prompt, history);
            let response_text = self.provider.complete(&full_prompt).await?;
            let response: AgentResponse = self.parse_response(&response_text)?;

            println!("🤖 Thought: {}", response.thought);

            if let Some(calls) = response.tool_calls {
                for raw_call in calls {
                    let tool_call = self.normalize_tool_call(raw_call)?;
                    let tool_result = self.execute_tool(&tool_call, &mut workflow).await;
                    match tool_result {
                        Ok(msg) => {
                            println!("  ✅ Tool [{}]: {}", tool_call.name, msg);
                            history.push_str(&format!("\nTool Result ({}): {}\n", tool_call.name, msg));
                        }
                        Err(e) => {
                            println!("  ❌ Tool [{}] Error: {}", tool_call.name, e);
                            history.push_str(&format!("\nTool Error ({}): {}\n", tool_call.name, e));
                        }
                    }
                }
                
                match self.engine.infer_metadata(&workflow).await {
                    Ok(metadata) => {
                        let schema_summary = self.summarize_metadata(&metadata);
                        history.push_str(&format!("\nValidation Success! Current schemas are valid. You can now provide the final_answer or continue adding nodes.\nSchemas:\n{}\n", schema_summary));
                    }
                    Err(e) => {
                        history.push_str(&format!("\nValidation Error: {}. Fix the workflow using patch_node_config or by adjusting connections.\n", e));
                    }
                }
            }

            if let Some(answer) = response.final_answer {
                println!("🎯 Final Answer: {}", answer);
                return Ok(workflow);
            }
            
            // If LLM didn't provide a final answer but built a valid graph, consider it done if iteration count > 1
            if i > 0 && !workflow.graph.node_count() == 0 {
                 println!("🎯 Inferring completion based on valid graph.");
                 return Ok(workflow);
            }
        }
        Ok(workflow)
    }

    fn normalize_tool_call(&self, val: serde_json::Value) -> Result<AgentToolCall> {
        // Handle both {"name": "...", "arguments": {...}} and {"type": "...", ...fields...}
        if let Some(name) = val.get("name").and_then(|v| v.as_str()) {
            let args = val.get("arguments").cloned().unwrap_or(serde_json::json!({}));
            Ok(AgentToolCall { name: name.to_string(), arguments: args })
        } else if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
            Ok(AgentToolCall { name: t.to_string(), arguments: val.clone() })
        } else {
            anyhow::bail!("Invalid tool call format: {}", val)
        }
    }

    fn build_system_prompt(&self) -> String {
        let node_specs = self.registry.list_specs();
        let specs_json = serde_json::to_string_pretty(&node_specs).unwrap();

        format!(
            "You are the SwanFlow Analyst Agent. You build data science workflows.\n\n\
            AVAILABLE NODES:\n{}\n\n\
            YOUR TOOLS:\n\
            - add_node(type_id, config): Add a node. Config must match the JSON schema of the node type.\n\
            - connect(source_idx, target_idx, source_port, target_port): Connect nodes by their creation order indices (0, 1, 2...).\n\n\
            OUTPUT FORMAT:\n\
            Return a JSON object with: {{ \"explanation\": \"...\", \"actions\": [{{ \"type\": \"add_node\", \"type_id\": \"...\", \"config\": {{...}} }}, ...], \"final_answer\": \"...\" }}\n\
            Always create a complete workflow. Start with a data reader (CSV/Excel), then analytical nodes, ending with a writer.",
            specs_json
        )
    }

    async fn execute_tool(&self, call: &AgentToolCall, workflow: &mut Workflow) -> Result<String> {
        match call.name.as_str() {
            "add_node" => {
                let type_id = call.arguments["type_id"].as_str().context("Missing type_id")?;
                let config = call.arguments["config"].clone();
                let instance = NodeInstance {
                    id: uuid::Uuid::new_v4(),
                    type_id: type_id.to_string(),
                    config,
                    state: crate::nodes::NodeState::Configured,
                    materialize: false,
                    sub_workflow: None,
                };
                let idx = workflow.add_node(instance);
                Ok(format!("Added node {} at index {:?}", type_id, idx.index()))
            }
            "connect" => {
                let src = call.arguments["source_idx"].as_u64().context("Missing source_idx")? as usize;
                let dst = call.arguments["target_idx"].as_u64().context("Missing target_idx")? as usize;
                let src_p = call.arguments["source_port"].as_u64().unwrap_or(0) as u32;
                let dst_p = call.arguments["target_port"].as_u64().unwrap_or(0) as u32;
                workflow.add_edge(NodeIndex::new(src), NodeIndex::new(dst), EdgeInstance { source_port: src_p, target_port: dst_p })?;
                Ok(format!("Connected node {} to {}", src, dst))
            }
            _ => anyhow::bail!("Unknown tool: {}", call.name),
        }
    }

    fn parse_response(&self, text: &str) -> Result<AgentResponse> {
        let json_str = if text.contains("```json") {
            text.split("```json").nth(1).unwrap().split("```").next().unwrap()
        } else if text.contains("```") {
            text.split("```").nth(1).unwrap()
        } else {
            text
        };
        serde_json::from_str(json_str.trim()).map_err(|e| anyhow::anyhow!("JSON Parse Error: {}. Text was: {}", e, text))
    }

    fn summarize_metadata(&self, metadata: &crate::core::engine::WorkflowMetadata) -> String {
        let mut summary = String::new();
        for (id, schemas) in &metadata.schemas {
            if !schemas.is_empty() {
                summary.push_str(&format!("Node[{}]: {:?}\n", id, schemas[0].fields().iter().map(|f| f.name()).collect::<Vec<_>>()));
            }
        }
        summary
    }
}
