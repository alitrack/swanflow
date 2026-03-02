use crate::core::engine::SwanFlowEngine;
use crate::core::graph::{EdgeInstance, NodeInstance, Workflow};
use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::registry::NodeRegistry;
use anyhow::{Context as _, Result};
use petgraph::prelude::NodeIndex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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
        engine: Arc<SwanFlowEngine>,
    ) -> Self {
        Self {
            provider,
            registry,
            engine,
        }
    }

    pub async fn run_analysis(&self, user_prompt: &str) -> Result<Workflow> {
        let mut workflow = Workflow::new("Agent Planned Workflow");
        let mut history = format!("User Request: {}\n", user_prompt);
        let system_prompt = self.build_system_prompt();

        for _i in 0..5 {
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
                            history.push_str(&format!(
                                "\nTool Result ({}): {}\n",
                                tool_call.name, msg
                            ));
                        }
                        Err(e) => {
                            println!("  ❌ Tool [{}] Error: {}", tool_call.name, e);
                            history
                                .push_str(&format!("\nTool Error ({}): {}\n", tool_call.name, e));
                        }
                    }
                }

                match self.engine.infer_metadata(&workflow).await {
                    Ok(metadata) => {
                        let schema_summary = self.summarize_metadata(&metadata);
                        history.push_str(&format!(
                            "\nValidation Success! Schemas:\n{}\n",
                            schema_summary
                        ));
                    }
                    Err(e) => {
                        history
                            .push_str(&format!("\nValidation Error: {}. Fix the workflow.\n", e));
                    }
                }
            }

            if let Some(answer) = response.final_answer {
                println!("🎯 Final Answer: {}", answer);
                return Ok(workflow);
            }

            // Return immediately if workflow has nodes - we have a valid solution
            if workflow.graph.node_count() > 0 {
                return Ok(workflow);
            }
        }
        Ok(workflow)
    }

    fn normalize_tool_call(&self, val: serde_json::Value) -> Result<AgentToolCall> {
        // AI sometimes nests everything under "config" or "params"
        let mut target = val.clone();
        if let Some(inner) = target.get("config").filter(|v| {
            v.is_object() && (v.get("type_id").is_some() || v.get("source_idx").is_some())
        }) {
            target = inner.clone();
        } else if let Some(inner) = target.get("params").filter(|v| v.is_object()) {
            target = inner.clone();
        }

        if let Some(name) = val.get("name").and_then(|v| v.as_str()) {
            let args = val
                .get("arguments")
                .cloned()
                .unwrap_or(serde_json::json!({}));
            Ok(AgentToolCall {
                name: name.to_string(),
                arguments: args,
            })
        } else if let Some(method) = val.get("method").and_then(|v| v.as_str()) {
            let params = val.get("params").cloned().unwrap_or(serde_json::json!({}));
            Ok(AgentToolCall {
                name: method.to_string(),
                arguments: params,
            })
        } else if let Some(t) = val.get("type").and_then(|v| v.as_str()) {
            Ok(AgentToolCall {
                name: t.to_string(),
                arguments: target,
            })
        } else {
            anyhow::bail!("Invalid tool call format: {}", val)
        }
    }

    fn build_system_prompt(&self) -> String {
        let node_specs = self.registry.list_specs();
        let specs_json = serde_json::to_string_pretty(&node_specs).unwrap();

        format!(
            "You are the SwanFlow Analyst Agent. You build data science workflows.\n\n\
            VARIABLE TEMPLATING:\n\
            You can use variables in node configurations using the syntax: ${{context.key_name}}.\n\
            Variables are GLOBAL and do NOT require explicit connections or Input nodes.\n\
            For example: \"expr\": \"dept = '${{context.target_dept}}'\"\n\n\
            AVAILABLE NODES:\n{}\n\n\
            EXCEL DASHBOARDING:\n\
            - 'core.io.excel_write' supports multiple input ports. Map each port to a sheet using the 'sheets' array config.\n\
            - It automatically applies professional styling (blue headers, thin borders).\n\
            - It automatically generates a Column Chart for the first sheet.\n\n\
            PYTHON CAPABILITIES:\n\
            The 'core.script.python' node is HIGH PERFORMANCE (zero-copy via Arrow FFI).\n\
            Use it for complex math, specialized data cleaning, or libraries like Scikit-learn.\n\
            The input batch is available as 'batch' (convert via pa.record_batch(batch)).\n\n\
            YOUR TOOLS:\n\
            - add_node(type_id, config): Add a node.\n\
            - connect(source_idx, target_idx, source_port, target_port): Connect nodes.\n\n\
            OUTPUT FORMAT:\n\
            Return a JSON object with: {{ \"explanation\": \"...\", \"actions\": [{{ \"type\": \"add_node\", \"type_id\": \"...\", \"config\": {{...}} }}, ...], \"final_answer\": \"...\" }}",
            specs_json
        )
    }

    async fn execute_tool(&self, call: &AgentToolCall, workflow: &mut Workflow) -> Result<String> {
        match call.name.as_str() {
            "add_node" => {
                let type_id = call
                    .arguments
                    .get("type_id")
                    .and_then(|v| v.as_str())
                    .context(format!("Missing type_id in arguments: {}", call.arguments))?;
                let config = call
                    .arguments
                    .get("config")
                    .cloned()
                    .unwrap_or(serde_json::json!({}));
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
                let src = call
                    .arguments
                    .get("source_idx")
                    .and_then(|v| v.as_u64())
                    .or_else(|| call.arguments.get("source").and_then(|v| v.as_u64()))
                    .context(format!(
                        "Missing source_idx in arguments: {}",
                        call.arguments
                    ))? as usize;
                let dst = call
                    .arguments
                    .get("target_idx")
                    .and_then(|v| v.as_u64())
                    .or_else(|| call.arguments.get("target").and_then(|v| v.as_u64()))
                    .context(format!(
                        "Missing target_idx in arguments: {}",
                        call.arguments
                    ))? as usize;
                let src_p = call.arguments["source_port"].as_u64().unwrap_or(0) as u32;
                let dst_p = call.arguments["target_port"].as_u64().unwrap_or(0) as u32;
                workflow.add_edge(
                    NodeIndex::new(src),
                    NodeIndex::new(dst),
                    EdgeInstance {
                        source_port: src_p,
                        target_port: dst_p,
                    },
                )?;
                Ok(format!("Connected node {} to {}", src, dst))
            }
            _ => anyhow::bail!("Unknown tool: {}", call.name),
        }
    }

    fn parse_response(&self, text: &str) -> Result<AgentResponse> {
        let json_str = if text.contains("```json") {
            text.split("```json")
                .nth(1)
                .unwrap()
                .split("```")
                .next()
                .unwrap()
        } else if text.contains("```") {
            text.split("```").nth(1).unwrap()
        } else {
            text
        };
        serde_json::from_str(json_str.trim())
            .map_err(|e| anyhow::anyhow!("JSON Parse Error: {}. Text was: {}", e, text))
    }

    fn summarize_metadata(&self, metadata: &crate::core::engine::WorkflowMetadata) -> String {
        let mut summary = String::new();
        for (id, schemas) in &metadata.schemas {
            if !schemas.is_empty() {
                summary.push_str(&format!(
                    "Node[{}]: {:?}\n",
                    id,
                    schemas[0]
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                ));
            }
        }
        summary
    }
}
