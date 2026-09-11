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

    /// Extract the top-level JSON object/array from a raw LLM completion,
    /// being AWARE of JSON string literals (so braces inside `"..."` don't
    /// corrupt the balance count). This is more robust than the shared
    /// `ConfigWizard::extract_json_from_response` helper, which does naive
    /// brace counting and truncates responses whose strings contain braces
    /// or fences (e.g. a thought mentioning "```").
    fn extract_top_level_json<'a>(completion: &'a str) -> Option<&'a str> {
        // Pick the opening bracket: prefer a leading `[` or `{`; otherwise
        // use whichever of `{`/`[` appears first in the text.
        let open: char = if completion.trim_start().starts_with('[') {
            '['
        } else if completion.trim_start().starts_with('{') {
            '{'
        } else {
            let (b, k) = (completion.find('{'), completion.find('['));
            match (b, k) {
                (Some(b), Some(k)) => {
                    if b <= k {
                        '{'
                    } else {
                        '['
                    }
                }
                (Some(_), None) => '{',
                (None, Some(_)) => '[',
                _ => return None,
            }
        };
        let close: char = if open == '{' {
            '}'
        } else {
            ']'
        };
        let start = completion.find(open)?;

        let mut depth: i32 = 0;
        let mut in_string = false;
        let mut escaped = false;

        for (i, c) in completion[start..].char_indices() {
            if in_string {
                if escaped {
                    escaped = false;
                } else if c == '\\' {
                    escaped = true;
                } else if c == '"' {
                    in_string = false;
                }
                continue;
            }
            match c {
                '"' => in_string = true,
                c if c == open => depth += 1,
                c if c == close => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(&completion[start..start + i + 1]);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn parse_response(&self, completion: &str) -> Result<AgentResponse> {
        // Prefer the robust string-aware extractor; fall back to the shared
        // helper (which handles ```json fences) if it finds nothing.
        let json_block = match Self::extract_top_level_json(completion) {
            Some(s) => s.trim().to_string(),
            None => crate::nodes::ai::ConfigWizard::extract_json_from_response(completion)
                .unwrap_or(completion)
                .trim()
                .to_string(),
        };

        // Array-of-tool-calls alternate format.
        if json_block.starts_with('[') {
            let calls: Vec<AgentToolCall> = serde_json::from_str(&json_block)
                .with_context(|| format!("failed to parse agent tool array: {}", completion))?;
            return Ok(AgentResponse {
                thought: None,
                tool_calls: calls,
                final_answer: None,
            });
        }
        serde_json::from_str::<AgentResponse>(&json_block)
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
        /// A node as described in the LLM's flat YAML (before conversion
        /// into a real petgraph NodeInstance).
        struct FlatNode {
            type_id: String,
            uuid: String,
            config: JsonValue,
        }

        let prompt = format!(
            r#"You are a SwanFlow YAML workflow generator.

Convert the description into a valid SwanFlow workflow.

RULES:
- Output ONLY the YAML document, starting with `name:` on the first line.
- Do NOT add a language tag, comment, or any non-YAML prefix line.
- Use each node's `id` (UUID) in the edges, NOT the type_id.

YAML structure:
name: <workflow-name>
nodes:
  - id: <uuid>
    type_id: <registered-node-type>
    config:
      key: value
edges:
  - source: <source-node-uuid>
    target: <target-node-uuid>
    source_port: 0
    target_port: 0

Available node types:
{}

Description: {}
"#,
            self.registry
                .list_specs()
                .iter()
                .map(|s| format!("- {}: config_schema={}", s.type_id, s.config_schema))
                .collect::<Vec<_>>()
                .join("\n"),
            description
        );

        let completion = self.provider.complete(&prompt).await?;
        let yaml_str = crate::nodes::ai::ConfigWizard::extract_json_from_response(&completion)
            .unwrap_or(&completion);

        // Strip markdown fences and any stray "yaml"/"yml" prefix line that
        // the LLM sometimes emits before the actual YAML document.
        let raw = yaml_str.trim();
        let cleaned = raw
            .strip_prefix("```yaml")
            .or_else(|| raw.strip_prefix("```yml"))
            .or_else(|| raw.strip_prefix("```"))
            .map(|s| s.strip_suffix("```").unwrap_or(s))
            .unwrap_or(raw)
            .trim();
        let cleaned = {
            let first_nl = cleaned.find('\n').unwrap_or(cleaned.len());
            let first_line = cleaned[..first_nl].trim().to_lowercase();
            if first_line == "yaml" || first_line == "yml" {
                cleaned[first_nl..]
                    .trim_start_matches(|c: char| c == '\n' || c == '\r')
            } else {
                cleaned
            }
        };

        // Parse into a JSON Value. The LLM outputs a FLAT structure
        // (name/nodes[]/edges[] at the top level), NOT the nested
        // Workflow serde shape (name/id/context/graph{nodes,edges}).
        let flat: JsonValue = if cleaned.starts_with('{') {
            serde_json::from_str(cleaned)
                .with_context(|| format!("failed to parse workflow JSON: {}", cleaned))?
        } else {
            let yaml_val: serde_yaml::Value = serde_yaml::from_str(cleaned)
                .with_context(|| format!("failed to parse workflow YAML:\n{}", cleaned))?;
            serde_json::to_value(&yaml_val)
                .with_context(|| "failed to convert YAML→JSON")?
        };

        // Build a real Workflow from the flat structure.
        let mut wf = Workflow::new(
            flat["name"].as_str().unwrap_or("agent-workflow"),
        );

        // Phase 1: collect flat node descriptors.
        let flat_nodes: Vec<FlatNode> = flat["nodes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|n| {
                        let type_id = n["type_id"].as_str()?.to_string();
                        let uuid = n["id"]
                            .as_str()
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| Uuid::new_v4().to_string());
                        Some(FlatNode {
                            type_id,
                            uuid,
                            config: n["config"].clone(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Phase 2: add nodes to the graph, recording each NodeIndex by position.
        let mut graph_idx: Vec<_> = Vec::with_capacity(flat_nodes.len());
        for node in &flat_nodes {
            let ni = NodeInstance::new(
                Uuid::parse_str(&node.uuid).unwrap_or_else(|_| Uuid::new_v4()),
                node.type_id.clone(),
                node.config.clone(),
                NodeState::Configured,
            );
            graph_idx.push(wf.graph.add_node(ni));
        }

        // Phase 3: add edges. Resolve source/target by UUID first, then type_id.
        fn find_node_pos<'a>(flat_nodes: &'a [FlatNode], key: &str) -> Option<usize> {
            flat_nodes.iter().position(|d| d.uuid == key || d.type_id == key)
        }

        if let Some(edges_arr) = flat["edges"].as_array() {
            for e in edges_arr {
                let src = e["source"].as_str().unwrap_or("");
                let dst = e["target"].as_str().unwrap_or("");
                if src.is_empty() || dst.is_empty() {
                    continue;
                }
                let from_pos = find_node_pos(&flat_nodes, src)
                    .ok_or_else(|| anyhow::anyhow!("edge source '{}' not found in nodes list", src))?;
                let to_pos = find_node_pos(&flat_nodes, dst)
                    .ok_or_else(|| anyhow::anyhow!("edge target '{}' not found in nodes list", dst))?;
                let sp = e["source_port"].as_u64().unwrap_or(0) as u32;
                let tp = e["target_port"].as_u64().unwrap_or(0) as u32;
                wf.graph.add_edge(
                    graph_idx[from_pos],
                    graph_idx[to_pos],
                    EdgeInstance {
                        source_port: sp,
                        target_port: tp,
                    },
                );
            }
        }

        // Merge into the passed-in workflow (in-place, so run_analysis sees it).
        workflow.name = wf.name;
        workflow.graph = wf.graph;
        workflow.context = wf.context;

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

    /// Unit tests for the robust JSON extractor.
    #[test]
    fn test_extract_top_level_json_handles_braces_in_strings() {
        // Braces inside a JSON string must NOT corrupt the balance count.
        let input = r#"Thought first. Then: {"thought":"has {braces} inside","tool_calls":[{"name":"x","arguments":{}}],"final_answer":""}"#;
        let s = AnalystAgent::extract_top_level_json(input).unwrap();
        assert!(s.starts_with('{'));
        assert!(s.ends_with('}'));
        let v: serde_json::Value = serde_json::from_str(s).unwrap();
        assert_eq!(v["thought"], "has {braces} inside");
    }

    #[test]
    fn test_extract_top_level_json_handles_array_of_tool_calls() {
        let input = "Here is the plan:\n[{\"name\":\"add_node\",\"arguments\":{\"type_id\":\"a\"}}]";
        let s = AnalystAgent::extract_top_level_json(input).unwrap();
        assert!(s.starts_with('['));
        let arr: Vec<AgentToolCall> = serde_json::from_str(s).unwrap();
        assert_eq!(arr[0].name, "add_node");
    }

    #[test]
    fn test_parse_response_recovers_from_truncated_fence() {
        // Simulates the real failure mode: a response whose naive brace
        // counting would truncate (string contains braces).
        struct StubProvider;
        #[async_trait::async_trait]
        impl LlmProvider for StubProvider {
            async fn complete(&self, _prompt: &str) -> Result<String> {
                Ok("{}".into())
            }
        }
        let agent: &AnalystAgent = &AnalystAgent {
            provider: Arc::new(StubProvider),
            registry: Arc::new(NodeRegistry::new()),
            engine: Arc::new(SwanFlowEngine::new(Arc::new(NodeRegistry::new()))),
        };
        let completion = r#"{"thought":"the user wants {a} filter","tool_calls":[{"name":"generate_workflow_yaml","arguments":{"description":"read csv"}}],"final_answer":""}"#;
        let resp = agent.parse_response(completion).unwrap();
        assert_eq!(resp.tool_calls.len(), 1);
        assert_eq!(resp.tool_calls[0].name, "generate_workflow_yaml");
    }

    #[test]
    fn test_generate_workflow_yaml_parses_flat_llm_yaml() {
        struct StubProvider;
        #[async_trait::async_trait]
        impl LlmProvider for StubProvider {
            async fn complete(&self, _prompt: &str) -> Result<String> {
                // Exactly the shape the real LLM emitted (with a stray "yaml"
                // prefix line and markdown-style edges referencing UUIDs).
                Ok("yaml\nname: users_csv_filter_to_excel\nnodes:\n  - id: 11111111-1111-4111-8111-111111111111\n    type_id: core.io.csv_read\n    config:\n      path: users.csv\n  - id: 22222222-2222-4222-8222-222222222222\n    type_id: core.transform.filter\n    config:\n      expr: \"age > 30\"\n  - id: 33333333-3333-4333-8333-333333333333\n    type_id: core.io.excel_write\n    config:\n      path: adults.xlsx\nedges:\n  - source: 11111111-1111-4111-8111-111111111111\n    target: 22222222-2222-4222-8222-222222222222\n    source_port: 0\n    target_port: 0\n  - source: 22222222-2222-4222-8222-222222222222\n    target: 33333333-3333-4333-8333-333333333333\n    source_port: 0\n    target_port: 0\n".into())
            }
        }
        let agent = AnalystAgent {
            provider: Arc::new(StubProvider),
            registry: Arc::new(NodeRegistry::new()),
            engine: Arc::new(SwanFlowEngine::new(Arc::new(NodeRegistry::new()))),
        };
        let mut wf = Workflow::new("before");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(agent.generate_workflow_yaml("desc", &mut wf))
            .unwrap();
        assert_eq!(wf.name, "users_csv_filter_to_excel");
        assert_eq!(wf.graph.node_count(), 3);
        assert_eq!(wf.graph.edge_count(), 2);
        // The graph must be a real chain: csv_read -> filter -> excel_write.
        let mut types: Vec<&str> = wf.graph.node_weights().map(|n| n.type_id.as_str()).collect();
        types.sort();
        assert_eq!(types, ["core.io.csv_read", "core.io.excel_write", "core.transform.filter"]);
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
