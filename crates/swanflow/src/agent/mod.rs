pub mod hook;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

use crate::core::engine::SwanFlowEngine;
use crate::core::graph::{EdgeInstance, NodeInstance, Workflow};
use crate::nodes::ai::providers::LlmProvider;
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeState;

// ---------------------------------------------------------------------------
// Existing data structures (kept for backward compatibility)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// New: AgentTool enum for internal dispatch
// ---------------------------------------------------------------------------

/// Tools the AI Agent can invoke during Plan→Verify→Fix→Execute cycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum AgentTool {
    /// Macro-planning: generate a complete YAML workflow from natural language.
    GenerateWorkflowYaml { description: String },
    /// Atomic fix: modify a single node's configuration.
    PatchNodeConfig { node_id: String, config: JsonValue },
    /// Add a new node to the workflow.
    AddNode {
        type_id: String,
        #[serde(default)]
        config: JsonValue,
    },
    /// Connect two nodes with an edge.
    Connect {
        from_node_id: String,
        to_node_id: String,
    },
    /// Infer schema/metadata for the current workflow.
    InferMetadata {},
}

// ---------------------------------------------------------------------------
// AnalystAgent
// ---------------------------------------------------------------------------

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

    // ── Public entry-point with verification loop ──────────────────────

    /// Run analysis with Plan→Verify→Fix→Execute closed loop.
    /// Up to 3 retry attempts when verification fails.
    pub async fn run_analysis(&self, prompt: &str) -> Result<Workflow> {
        let system_prompt = self.build_agent_system_prompt();
        let mut workflow = Workflow::new("agent-analysis");
        let mut last_errors: Vec<String> = Vec::new();
        let mut max_attempts = 3u32;

        loop {
            // Build context-aware prompt
            let error_context = if last_errors.is_empty() {
                String::new()
            } else {
                format!(
                    "\n[VERIFICATION ERRORS — please fix these]\n{}\n",
                    last_errors.join("\n")
                )
            };

            let full_prompt = format!(
                "{}\n\nUser Request: {}\n{}",
                system_prompt, prompt, error_context
            );

            // 1. PLAN: Call LLM
            let completion = self.provider.complete(&full_prompt).await?;
            let response = self.parse_response(&completion)?;

            // 2. EXECUTE tool calls
            self.apply_tool_calls(&response, &mut workflow).await?;

            // 3. VERIFY: compile-workflow inference
            max_attempts -= 1;

            // Skip verification for InferMetadata-only responses
            let has_infer = response
                .tool_calls
                .iter()
                .any(|c| c.name == "infer_metadata");
            if has_infer && max_attempts == 0 {
                break;
            }

            match self.engine.infer_metadata(&workflow).await {
                Ok(metadata) => {
                    let errors: Vec<String> = metadata
                        .diagnostics
                        .values()
                        .flat_map(|v| v.iter().cloned())
                        .collect();

                    if errors.is_empty() {
                        // 4. EXECUTE: all good, break out
                        println!(
                            "[Agent] Workflow verified OK — {} nodes, {} edges",
                            workflow.graph.node_count(),
                            workflow.graph.edge_count()
                        );
                        break;
                    }

                    println!(
                        "[Agent] Verification found {} error(s), attempts remaining: {}",
                        errors.len(),
                        max_attempts
                    );
                    last_errors = errors;

                    if max_attempts == 0 {
                        // Log but still return the last workflow
                        eprintln!(
                            "[Agent] Max retries reached. Returning last workflow with errors."
                        );
                        break;
                    }
                }
                Err(e) => {
                    let err_msg = format!("Verification error: {}", e);
                    println!("[Agent] {}", err_msg);
                    last_errors.push(err_msg);

                    if max_attempts == 0 {
                        break;
                    }
                }
            }
        }

        Ok(workflow)
    }

    // ── System prompt builder ──────────────────────────────────────────

    fn build_agent_system_prompt(&self) -> String {
        let spec_list = self.registry.list_specs();
        let node_types: Vec<String> = spec_list
            .iter()
            .map(|s| format!("- {} ({}): {}", s.type_id, s.display_name, s.description))
            .collect();

        format!(
            r#"You are SwanFlow AI Analyst Agent. You build data workflow graphs by issuing tool calls.

AVAILABLE TOOLS (output as JSON array in the "tool_calls" field):
1. generate_workflow_yaml {{ "description": "..." }}
   - Macro planner: generate a complete YAML workflow from a natural-language description.
   - Always use this FIRST to create the initial workflow.

2. add_node {{ "type_id": "...", "config": {{...}} }}
   - Add a single node. type_id must be one of the registered types below.

3. patch_node_config {{ "node_id": "...", "config": {{...}} }}
   - Modify an existing node's config. node_id is the node's type_id string.

4. connect {{ "from_node_id": "...", "to_node_id": "..." }}
   - Connect two nodes by their type_id strings (finds the most recent instance).

5. infer_metadata {{}}
   - Ask the engine to infer schema metadata for the current workflow (for validation).

REGISTERED NODE TYPES:
{}

WORKFLOW: The graph is a DAG. Nodes process data in topological order.
Node configs use the schema defined by each type. Use the generate_workflow_yaml
tool first, then use patch_node_config/add_node/connect to fix any issues.

OUTPUT FORMAT — respond with a JSON object:
{{
    "thought": "your reasoning",
    "tool_calls": [
        {{ "name": "generate_workflow_yaml", "arguments": {{ "description": "..." }} }}
    ],
    "final_answer": "summary (optional)"
}}"#,
            node_types.join("\n")
        )
    }

    // ── Response parsing ──────────────────────────────────────────────

    fn parse_response(&self, completion: &str) -> Result<AgentResponse> {
        let json_block = crate::nodes::ai::ConfigWizard::extract_json_from_response(completion)
            .unwrap_or(completion);
        // Try as an array of tool calls first (alternate format)
        if json_block.trim().starts_with('[') {
            let calls: Vec<AgentToolCall> = serde_json::from_str(json_block.trim())
                .with_context(|| format!("failed to parse agent tool array: {}", completion))?;
            return Ok(AgentResponse {
                thought: None,
                tool_calls: calls,
                final_answer: None,
            });
        }
        serde_json::from_str::<AgentResponse>(json_block.trim())
            .with_context(|| format!("failed to parse agent response JSON: {}", completion))
    }

    // ── Tool call dispatch ─────────────────────────────────────────────

    async fn apply_tool_calls(
        &self,
        response: &AgentResponse,
        workflow: &mut Workflow,
    ) -> Result<()> {
        for call in &response.tool_calls {
            match call.name.as_str() {
                "generate_workflow_yaml" => {
                    let desc = call.arguments["description"]
                        .as_str()
                        .unwrap_or("build workflow");
                    self.generate_workflow_yaml(desc, workflow).await?;
                }
                "add_node" => {
                    let args: AddNodeArguments = serde_json::from_value(call.arguments.clone())
                        .context("invalid add_node arguments")?;
                    self.add_node(&args.type_id, args.config, workflow)?;
                }
                "patch_node_config" => {
                    let node_id = call.arguments["node_id"].as_str().unwrap_or("").to_string();
                    let config = call.arguments["config"].clone();
                    self.patch_node_config(&node_id, config, workflow)?;
                }
                "connect" => {
                    let from = call.arguments["from_node_id"].as_str().unwrap_or("");
                    let to = call.arguments["to_node_id"].as_str().unwrap_or("");
                    self.connect(from, to, workflow)?;
                }
                "infer_metadata" => {
                    // No-op at apply time; the run_analysis loop handles this
                    println!("[Agent] InferMetadata requested by LLM");
                }
                other => {
                    println!("[Agent] Unknown tool call: {}", other);
                }
            }
        }
        Ok(())
    }

    // ── Tool implementations ───────────────────────────────────────────

    /// Macro Planner: generate a complete YAML workflow from natural language.
    async fn generate_workflow_yaml(
        &self,
        description: &str,
        workflow: &mut Workflow,
    ) -> Result<()> {
        let prompt = format!(
            r#"You are a SwanFlow YAML workflow generator.

Convert the following natural language description into a valid SwanFlow workflow in YAML format.

The YAML structure is:
```yaml
name: <workflow-name>
nodes:
  - id: <uuid>
    type_id: <registered-node-type>
    config:
      key: value
edges:
  - source: <source-type_id>
    target: <target-type_id>
    source_port: 0
    target_port: 0
```

Available node types (with their type_id and config schema):
{}

Description: {}
"#,
            self.registry
                .list_specs()
                .iter()
                .map(|s| { format!("- {}: config_schema={}", s.type_id, s.config_schema) })
                .collect::<Vec<_>>()
                .join("\n"),
            description
        );

        let completion = self.provider.complete(&prompt).await?;
        let yaml_str = crate::nodes::ai::ConfigWizard::extract_json_from_response(&completion)
            .unwrap_or(&completion);

        // Parse YAML into a temporary workflow structure
        // The LLM may output YAML directly or inside a code block
        let raw = yaml_str.trim();
        let parsed: Workflow = if raw.starts_with('{') {
            serde_json::from_str(raw)
                .with_context(|| format!("failed to parse workflow JSON: {}", raw))?
        } else {
            // Try YAML, strip any markdown fences if present
            let cleaned = raw
                .strip_prefix("```yaml")
                .or_else(|| raw.strip_prefix("```yml"))
                .or_else(|| raw.strip_prefix("```"))
                .map(|s| s.strip_suffix("```").unwrap_or(s))
                .unwrap_or(raw)
                .trim();
            serde_yaml::from_str(cleaned)
                .with_context(|| format!("failed to parse workflow YAML: {}", cleaned))?
        };

        // Merge: replace workflow content with the parsed one
        workflow.name = parsed.name;
        workflow.graph = parsed.graph;
        workflow.context = parsed.context;

        println!(
            "[Agent] Generated workflow '{}' with {} nodes, {} edges",
            workflow.name,
            workflow.graph.node_count(),
            workflow.graph.edge_count()
        );

        Ok(())
    }

    /// Atomic Corrector: modify a single node's configuration.
    fn patch_node_config(
        &self,
        node_id_str: &str,
        config: JsonValue,
        workflow: &mut Workflow,
    ) -> Result<()> {
        // Try by node_id UUID first, then by type_id
        let target_uuid = Uuid::parse_str(node_id_str).ok();
        let target_idx = workflow.graph.node_indices().find(|&idx| {
            let node = &workflow.graph[idx];
            match target_uuid {
                Some(uuid) => node.id == uuid,
                None => node.type_id == node_id_str,
            }
        });

        match target_idx {
            Some(idx) => {
                if let Some(node) = workflow.graph.node_weight_mut(idx) {
                    node.config = config;
                    node.state = NodeState::Unconfigured;
                    println!(
                        "[Agent] Patched config for node {} (type: {})",
                        node.id, node.type_id
                    );
                    Ok(())
                } else {
                    anyhow::bail!("patch_node_config: node index {} vanished", idx.index())
                }
            }
            None => anyhow::bail!(
                "patch_node_config: node '{}' not found in workflow",
                node_id_str
            ),
        }
    }

    /// Add a new node to the workflow.
    fn add_node(&self, type_id: &str, config: JsonValue, workflow: &mut Workflow) -> Result<()> {
        // Validate that the type exists in the registry
        self.registry
            .get_node(type_id)
            .with_context(|| format!("unknown node type requested by agent: {}", type_id))?;

        let mut node = NodeInstance::new(Uuid::new_v4(), type_id, config, NodeState::Configured);
        node.materialize = false;
        let idx = workflow.add_node(node);
        let node = &workflow.graph[idx];
        println!("[Agent] Added node {} (type: {})", node.id, node.type_id);
        Ok(())
    }

    /// Connect two nodes by finding them and adding an edge.
    fn connect(&self, from_node_id: &str, to_node_id: &str, workflow: &mut Workflow) -> Result<()> {
        let from_uuid = Uuid::parse_str(from_node_id).ok();
        let to_uuid = Uuid::parse_str(to_node_id).ok();

        let from_idx = workflow.graph.node_indices().find(|&idx| {
            let node = &workflow.graph[idx];
            match from_uuid {
                Some(uuid) => node.id == uuid,
                None => node.type_id == from_node_id,
            }
        });
        let to_idx = workflow.graph.node_indices().find(|&idx| {
            let node = &workflow.graph[idx];
            match to_uuid {
                Some(uuid) => node.id == uuid,
                None => node.type_id == to_node_id,
            }
        });

        let from = from_idx
            .ok_or_else(|| anyhow::anyhow!("connect: source node '{}' not found", from_node_id))?;
        let to = to_idx
            .ok_or_else(|| anyhow::anyhow!("connect: target node '{}' not found", to_node_id))?;

        workflow.add_edge(
            from,
            to,
            EdgeInstance {
                source_port: 0,
                target_port: 0,
            },
        )?;

        println!("[Agent] Connected {} → {}", from_node_id, to_node_id);

        Ok(())
    }

    // ── Legacy entry point (delegates to run_analysis) ─────────────────

    #[deprecated(
        since = "0.2.0",
        note = "use run_analysis() which includes the verification loop"
    )]
    pub async fn run_analysis_legacy(&self, prompt: &str) -> Result<Workflow> {
        let completion = self.provider.complete(prompt).await?;
        let response = self.parse_response(&completion)?;
        self.build_workflow_from_response_legacy(response)
    }

    fn build_workflow_from_response_legacy(&self, response: AgentResponse) -> Result<Workflow> {
        let mut workflow = Workflow::new("agent-analysis");

        for call in response.tool_calls {
            if call.name.as_str() == "add_node" {
                let args: AddNodeArguments =
                    serde_json::from_value(call.arguments).context("invalid add_node arguments")?;
                self.registry.get_node(&args.type_id).with_context(|| {
                    format!("unknown node type requested by agent: {}", args.type_id)
                })?;

                let mut node = NodeInstance::new(
                    Uuid::new_v4(),
                    args.type_id,
                    args.config,
                    NodeState::Configured,
                );
                node.materialize = false;
                workflow.add_node(node);
            }
        }

        Ok(workflow)
    }
}
