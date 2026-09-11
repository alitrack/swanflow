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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CreateLuaLibArguments {
    pub lib_name: String,
    pub source: String,
    #[serde(default)]
    pub category: Option<String>,
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
    /// Self-evolution: write a new Lua library into duckdb-luajit-libs and
    /// register it in the INDEX so it becomes a luajit.<name> canvas node.
    CreateLuaLib {
        lib_name: String,
        source: String,
        #[serde(default)]
        category: Option<String>,
    },
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

        // Plain (non-format) string: braces are literal, so we can document
        // ${context.key} / {{var}} / %%var%% without format! escaping.
        let guidance = r#"VARIABLES & CONFIG INTERPOLATION
Node config string values are interpolated at run time. Use:
- ${context.key}         read a value from the workflow/step context (preferred for cross-node data passing)
- {{var}} / {{var:default}}   flow variable; the default is used if the var is missing
- %%var%% / %%var:default%%   KNIME-compatible flow variable form
Always prefer a form with a safe default (e.g. {{var:default}}) so a missing variable cannot break the run.

PERFORMANCE & REPORTING GUIDANCE
- Heavy numeric / ML / stats work (outlier detection, scaling, clustering) -> use core.script.python.
  Python runs zero-copy over the Arrow batch and is the high-performance path; do NOT chain many
  SQL steps where one Python transform does the job. Use core.script.wasm only when you need 10-100x UDF speed.
- Dashboards & multi-sheet reports -> use core.io.excel_report (auto charts) or core.io.excel_write.
- Spread a column into one column per distinct value (pivot) -> use core.transform.pivot
  (row + column + value + aggregator).
- Pivot results feed straight into an Excel writer node for formatted export."#;

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

6. create_lua_lib {{ "lib_name": "...", "source": "<lua code>", "category": "udf|parser|datasource|network|ffi|etl|export|mcp" }}
   - SELF-EVOLUTION: write a NEW Lua library into duckdb-luajit-libs and register it in the INDEX.
   - Use this when no existing node type expresses what you need (new parser, new UDF, new data source).
   - After creation, use it immediately via core.luajit with mode/table + source='<lib_name>',
     or restart to get a dedicated luajit.<lib_name> node.
   - The lib runs in the luajit extension's trusted sandbox; io/os/ffi need explicit approval.

REGISTERED NODE TYPES:
{0}

{1}

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
            node_types.join("\n"),
            guidance
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
                "create_lua_lib" => {
                    let args: CreateLuaLibArguments = serde_json::from_value(call.arguments.clone())
                        .context("invalid create_lua_lib arguments")?;
                    self.create_lua_lib(&args.lib_name, &args.source, args.category.as_deref())?;
                }
                other => {
                    println!("[Agent] Unknown tool call: {}", other);
                }
            }
        }
        Ok(())
    }

    // ── Tool implementations ───────────────────────────────────────────

    /// Self-evolution: write a new Lua library into the duckdb-luajit-libs
    /// repo and register it in the INDEX. After this, the lib can be used via
    /// the generic `core.luajit` node immediately, and (on next startup) a
    /// dedicated `luajit.<name>` node is auto-registered.
    ///
    /// Write location priority:
    /// 1. `SWANFLOW_LUAJIT_LIBS_DIR` — explicit libs repo root
    /// 2. local dev clone `/mnt/d/wsl2/duckdb-luajit-libs`
    /// 3. `~/.duckdb/luajit-libs` — extension offline cache (INDEX-less write)
    fn create_lua_lib(
        &self,
        lib_name: &str,
        source: &str,
        category: Option<&str>,
    ) -> Result<()> {
        // Sanitize lib name: ascii alnum + underscore (like sigil's tool names).
        let clean: String = lib_name
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let clean = clean.trim_matches('_');
        if clean.is_empty() {
            anyhow::bail!("lib_name must contain at least one alphanumeric character");
        }
        if source.trim().is_empty() {
            anyhow::bail!("source must not be empty");
        }

        // Resolve the libs repo root.
        let root: std::path::PathBuf = if let Ok(dir) = std::env::var("SWANFLOW_LUAJIT_LIBS_DIR") {
            if !dir.is_empty() {
                std::path::PathBuf::from(dir)
            } else {
                resolve_libs_root()?
            }
        } else {
            resolve_libs_root()?
        };

        let cat = category.unwrap_or("udf");
        let dir = root.join("libs").join(cat);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create libs/{cat} dir"))?;
        let file_path = dir.join(format!("{clean}.lua"));
        std::fs::write(&file_path, source)
            .with_context(|| format!("failed to write {}", file_path.display()))?;

        // Update INDEX (name|path, append if not present).
        let index_path = root.join("INDEX");
        if index_path.exists() {
            let index = std::fs::read_to_string(&index_path)?;
            let entry = format!("{clean}|libs/{cat}/{clean}.lua");
            if !index.lines().any(|l| l.trim().starts_with(&format!("{clean}|"))) {
                let mut updated = index.trim_end().to_string();
                updated.push('\n');
                updated.push_str(&entry);
                updated.push('\n');
                std::fs::write(&index_path, updated)
                    .with_context(|| format!("failed to update {}", index_path.display()))?;
            }
        } else {
            // No INDEX yet (offline cache): write one.
            std::fs::write(&index_path, format!("{clean}|libs/{cat}/{clean}.lua\n"))?;
        }

        println!(
            "[Agent] create_lua_lib: wrote {} (category={cat}), INDEX updated. \
             Use it now via core.luajit with source='{clean}', or restart to get \
             a dedicated luajit.{clean} node.",
            file_path.display()
        );
        Ok(())
    }

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

/// Resolve the duckdb-luajit-libs repo root for `create_lua_lib` writes.
/// Priority: env var > dev clone > extension offline cache dir.
fn resolve_libs_root() -> Result<std::path::PathBuf> {
    if let Ok(dir) = std::env::var("SWANFLOW_LUAJIT_LIBS_DIR") {
        if !dir.is_empty() {
            return Ok(std::path::PathBuf::from(dir));
        }
    }
    const CANDIDATES: &[&str] = &[
        "/mnt/d/wsl2/duckdb-luajit-libs",
        "/home/lhy/.duckdb/luajit-libs",
    ];
    for c in CANDIDATES {
        if std::path::Path::new(c).exists() {
            return Ok(std::path::PathBuf::from(c));
        }
    }
    anyhow::bail!(
        "no duckdb-luajit-libs root found (checked {}); set SWANFLOW_LUAJIT_LIBS_DIR",
        CANDIDATES.join(", ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_prompt_renders_variable_and_perf_guidance() {
        struct StubProvider;
        #[async_trait::async_trait]
        impl LlmProvider for StubProvider {
            async fn complete(&self, _prompt: &str) -> Result<String> {
                Ok("{}".into())
            }
        }
        let agent = AnalystAgent {
            provider: Arc::new(StubProvider),
            registry: Arc::new(NodeRegistry::new()),
            engine: Arc::new(SwanFlowEngine::new(Arc::new(NodeRegistry::new()))),
        };
        let prompt = agent.build_agent_system_prompt();

        // Interpolation syntax must survive format! verbatim (braces not eaten).
        assert!(prompt.contains("${context.key}"), "context.key syntax missing");
        assert!(prompt.contains("{{var:default}}"), "flow-var default syntax missing");
        assert!(prompt.contains("%%var:default%%"), "knime var syntax missing");

        // Performance / reporting guidance must be present.
        assert!(prompt.contains("PERFORMANCE & REPORTING GUIDANCE"));
        assert!(prompt.contains("core.script.python"));
        assert!(prompt.contains("core.script.wasm"));
        assert!(prompt.contains("core.io.excel_report"));
        assert!(prompt.contains("core.transform.pivot"));

        // JSON output block braces survived format! escaping.
        assert!(prompt.contains("\"tool_calls\""));
    }

    #[test]
    fn test_create_lua_lib_sanitizes_name_and_writes() {
        let tmp = std::env::temp_dir().join("swanflow_agent_lib_test");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("INDEX"), "base64|libs/udf/base64.lua\n").unwrap();

        // Use the module-level resolver via env override.
        std::env::set_var("SWANFLOW_LUAJIT_LIBS_DIR", tmp.to_str().unwrap());
        struct StubProvider;
        #[async_trait::async_trait]
        impl LlmProvider for StubProvider {
            async fn complete(&self, _prompt: &str) -> Result<String> {
                Ok("{}".into())
            }
        }
        let agent = AnalystAgent {
            provider: Arc::new(StubProvider),
            registry: Arc::new(NodeRegistry::new()),
            engine: Arc::new(SwanFlowEngine::new(Arc::new(NodeRegistry::new()))),
        };
        agent
            .create_lua_lib("my cool-lib", "return function(x) return x end", Some("udf"))
            .unwrap();

        // File written at libs/udf/my_cool_lib.lua
        let file = tmp.join("libs/udf/my_cool_lib.lua");
        assert!(file.exists(), "lib file should exist");
        assert!(
            std::fs::read_to_string(&file).unwrap().contains("return function"),
            "source content should be preserved"
        );
        // INDEX updated
        let index = std::fs::read_to_string(tmp.join("INDEX")).unwrap();
        assert!(index.contains("my_cool_lib|libs/udf/my_cool_lib.lua"));
        // no duplicate on second write
        agent
            .create_lua_lib("my_cool_lib", "return 1", Some("udf"))
            .unwrap();
        let index2 = std::fs::read_to_string(tmp.join("INDEX")).unwrap();
        assert_eq!(
            index2.matches("my_cool_lib|").count(),
            1,
            "INDEX should not duplicate entries"
        );

        std::env::remove_var("SWANFLOW_LUAJIT_LIBS_DIR");
        std::fs::remove_dir_all(&tmp).ok();
    }
}
