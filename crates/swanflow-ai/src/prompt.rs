//! Modular prompt builder — assembles AI prompts from structured context.
//!
//! Inspired by OpenFang's `prompt_builder.rs` modular context assembly pattern.
//! Each context field comes from a separate source (NodeSpec, error diagnostic,
//! tool registry), NOT from a monolithic 200KB embedded string.
//!
//! Uses section-based assembly: each section is built by a dedicated method.
//! Empty sections are automatically skipped.

use serde_json::Value;

/// Column info for schema context.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Data type (e.g. "Int64", "Utf8", "Float64").
    pub data_type: String,
    /// Whether this column is nullable.
    pub nullable: bool,
}

/// Information about an upstream (parent) node in the DAG.
#[derive(Debug, Clone)]
pub struct UpstreamNode {
    /// Node identifier.
    pub node_id: String,
    /// Node type (e.g. "FileSource", "Filter").
    pub node_type: String,
    /// Port/connection name to this node.
    pub port_name: String,
}

/// Information about a downstream (child) node in the DAG.
#[derive(Debug, Clone)]
pub struct DownstreamNode {
    /// Node identifier.
    pub node_id: String,
    /// Node type (e.g. "FileSource", "Filter").
    pub node_type: String,
    /// Port/connection name from this node.
    pub port_name: String,
    /// Whether this downstream node also has errors.
    pub also_broken: bool,
}

/// Record of a previous fix attempt.
#[derive(Debug, Clone)]
pub struct FixRecord {
    /// Fix attempt number (1-based).
    pub attempt: u32,
    /// The fix that was attempted (description or config diff).
    pub attempted_fix: String,
    /// Whether the fix succeeded.
    pub success: bool,
    /// Error message after the fix (if failed).
    pub result_error: Option<String>,
}

/// Context for assembling a node fix prompt.
#[derive(Debug, Clone)]
pub struct PromptContext {
    /// Node type identifier (e.g. "FileSource").
    pub node_type: String,
    /// Human-readable node description from NodeSpec.
    pub node_description: String,
    /// Current node configuration as JSON.
    pub current_config: Value,
    /// The error message that triggered the fix attempt.
    pub error_message: String,
    /// Available tools the LLM can suggest using.
    pub available_tools: Vec<String>,
    /// Domain-specific fix instructions.
    pub fix_instructions: String,
    /// Expected output schema for this node.
    pub expected_schema: Vec<ColumnInfo>,
    /// Upstream (parent) nodes in the DAG.
    pub upstream_nodes: Vec<UpstreamNode>,
    /// Downstream (child) nodes in the DAG.
    pub downstream_nodes: Vec<DownstreamNode>,
    /// Domain hints (e.g. "financial data", "IoT sensor", "web logs").
    pub domain_hints: Vec<String>,
    /// History of previous fix attempts.
    pub fix_history: Vec<FixRecord>,
    /// Full DAG topology around the failed node, as "source→target" edge
    /// strings (used by INSERT_PREREQ / REWIRE operators).
    pub graph_topology: Vec<String>,
}

/// Builds AI prompts from modular context using section-based assembly.
pub struct PromptBuilder {
    /// Maximum prompt length in characters (default: 4096).
    max_length: usize,
}

impl Default for PromptBuilder {
    fn default() -> Self {
        Self { max_length: 4096 }
    }
}

impl PromptBuilder {
    pub fn new(max_length: usize) -> Self {
        Self { max_length }
    }

    // ── main builder ────────────────────────────────────────────

    /// Build a fix prompt from the given context.
    ///
    /// Sections are assembled in order; empty sections are automatically skipped.
    pub fn build_fix_prompt(&self, ctx: &PromptContext) -> String {
        let config_str =
            serde_json::to_string_pretty(&ctx.current_config).unwrap_or_else(|_| "{}".into());
        let config_truncated = truncate(&config_str, 2000);

        let tools_str = if ctx.available_tools.is_empty() {
            "none".into()
        } else {
            ctx.available_tools.join(", ")
        };

        let mut sections: Vec<String> = Vec::new();

        // Header section (always present)
        sections.push(format!(
            "You are fixing a data pipeline node.\n\n\
             Node: {node_type} — {description}\n\
             Error: {error}",
            node_type = ctx.node_type,
            description = ctx.node_description,
            error = ctx.error_message,
        ));

        // Optional sections
        if let Some(s) = self.build_schema_section(ctx) {
            sections.push(s);
        }
        if let Some(s) = self.build_topology_section(ctx) {
            sections.push(s);
        }
        if let Some(s) = self.build_domain_hints_section(ctx) {
            sections.push(s);
        }
        if let Some(s) = self.build_fix_history_section(ctx) {
            sections.push(s);
        }

        // Config section (always present)
        sections.push(format!(
            "\nCurrent config:\n```json\n{config}\n```",
            config = config_truncated,
        ));

        // Topology section (only when provided by caller)
        if let Some(s) = self.build_graph_topology_section(ctx) {
            sections.push(s);
        }

        // Tools and instructions (always present)
        sections.push(format!(
            "Available tools: {tools}\n\n\
             Instructions: {instructions}\n\n\
             Return ONLY a JSON object describing a repair OPERATOR. Choose the \\\n\
             minimal operator that fixes the failure:\n\
             - REBIND (default, when only parameters need fixing): \\\
               {{\"operator\":\"REBIND\",\"fixed_config\":{{...}}}} — include ALL \\\
               fields of the node config, with the broken ones corrected.\n\
             - INSERT_PREREQ (when a missing precondition, e.g. a table/view/\\\n\
               file, must be created upstream): \\\
               {{\"operator\":\"INSERT_PREREQ\",\"insert_node\":{{\"type_id\":\"...\",\\\
               \"config\":{{...}}}},\"insert_before\":\"<failed-node-id>\"}}\n\
             - SUBSTITUTE (when the node type itself is wrong): \\\
               {{\"operator\":\"SUBSTITUTE\",\"new_type\":\"...\",\"new_config\":{{...}}}}\n\
             - REWIRE (when the failure is a wrong edge between nodes): \\\
               {{\"operator\":\"REWIRE\",\"add_edges\":[{{\"source\":\"a\",\"target\":\"b\"}}],\\\
               \"remove_edges\":[{{\"source\":\"a\",\"target\":\"b\"}}]}}\n\
             - BYPASS (when the node can be skipped, downstream already satisfied): \\\
               {{\"operator\":\"BYPASS\"}}\n\
             Use the graph topology section for node ids. Never invent node ids.",
            tools = tools_str,
            instructions = ctx.fix_instructions,
        ));

        let prompt = sections.join("\n\n");
        truncate(&prompt, self.max_length)
    }

    // ── section builders ────────────────────────────────────────

    /// Build the expected schema section. Returns `None` if no schema columns are defined.
    fn build_schema_section(&self, ctx: &PromptContext) -> Option<String> {
        if ctx.expected_schema.is_empty() {
            return None;
        }

        let mut lines = vec!["Expected output schema:".to_string()];
        for col in &ctx.expected_schema {
            let null_marker = if col.nullable { " (nullable)" } else { "" };
            lines.push(format!(
                "  - {}: {}{}",
                col.name, col.data_type, null_marker
            ));
        }

        Some(lines.join("\n"))
    }

    /// Build the DAG topology section. Returns `None` if there are no upstream or downstream nodes.
    fn build_topology_section(&self, ctx: &PromptContext) -> Option<String> {
        if ctx.upstream_nodes.is_empty() && ctx.downstream_nodes.is_empty() {
            return None;
        }

        let mut lines: Vec<String> = Vec::new();
        lines.push("DAG context:".to_string());

        if !ctx.upstream_nodes.is_empty() {
            lines.push("  Upstream (parents):".to_string());
            for u in &ctx.upstream_nodes {
                lines.push(format!(
                    "    - {} ({}) via port \"{}\"",
                    u.node_id, u.node_type, u.port_name
                ));
            }
        }

        if !ctx.downstream_nodes.is_empty() {
            lines.push("  Downstream (children):".to_string());
            for d in &ctx.downstream_nodes {
                let broken = if d.also_broken { " [ALSO BROKEN]" } else { "" };
                lines.push(format!(
                    "    - {} ({}) via port \"{}\"{}",
                    d.node_id, d.node_type, d.port_name, broken
                ));
            }
        }

        Some(lines.join("\n"))
    }

    /// Build the full graph topology section (edges around the failed node).
    /// Returns `None` when no topology was provided by the caller.
    fn build_graph_topology_section(&self, ctx: &PromptContext) -> Option<String> {
        if ctx.graph_topology.is_empty() {
            return None;
        }
        let mut lines = vec!["Graph topology (node ids):".to_string()];
        for edge in &ctx.graph_topology {
            lines.push(format!("  - {edge}"));
        }
        Some(lines.join("\n"))
    }

    /// Build the domain hints section. Returns `None` if no hints are provided.
    fn build_domain_hints_section(&self, ctx: &PromptContext) -> Option<String> {
        if ctx.domain_hints.is_empty() {
            return None;
        }

        let mut lines = vec!["Domain context:".to_string()];
        for hint in &ctx.domain_hints {
            lines.push(format!("  - {}", hint));
        }

        Some(lines.join("\n"))
    }

    /// Build the fix history section. Returns `None` if there are no previous fix attempts.
    fn build_fix_history_section(&self, ctx: &PromptContext) -> Option<String> {
        if ctx.fix_history.is_empty() {
            return None;
        }

        let mut lines = vec!["Previous fix attempts:".to_string()];
        for record in &ctx.fix_history {
            let status = if record.success { "SUCCESS" } else { "FAILED" };
            let error_note = record
                .result_error
                .as_ref()
                .map(|e| format!(" — {}", e))
                .unwrap_or_default();
            lines.push(format!(
                "  Attempt {}: {} {}{}",
                record.attempt, status, record.attempted_fix, error_note
            ));
        }
        lines.push("Please learn from past failures and try a different approach.".to_string());

        Some(lines.join("\n"))
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let mut truncated = s[..max_len - 20].to_string();
        truncated.push_str("...(truncated)");
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builds_basic_prompt() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "FileSource".into(),
            node_description: "Reads CSV/Parquet files".into(),
            current_config: json!({"path": "/data/missing.csv", "format": "csv"}),
            error_message: "File not found: /data/missing.csv".into(),
            available_tools: vec!["read_file".into(), "list_files".into()],
            fix_instructions: "Check if the file path is correct.".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("FileSource"));
        assert!(prompt.contains("File not found"));
        assert!(prompt.contains("fixed_config"));
        assert!(prompt.contains("read_file"));
        assert!(prompt.len() <= 4096);
    }

    #[test]
    fn truncates_long_config() {
        let builder = PromptBuilder::new(500);
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({"data": "x".repeat(3000)}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.len() <= 500);
        assert!(prompt.contains("(truncated)"));
    }

    #[test]
    fn schema_section_appears_when_columns_present() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: "Utf8".into(),
                    nullable: true,
                },
            ],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("Expected output schema"));
        assert!(prompt.contains("id: Int64"));
        assert!(prompt.contains("name: Utf8 (nullable)"));
    }

    #[test]
    fn schema_section_skipped_when_empty() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(!prompt.contains("Expected output schema"));
    }

    #[test]
    fn topology_section_shows_upstream_and_downstream() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Filter".into(),
            node_description: "Filters rows".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![UpstreamNode {
                node_id: "src1".into(),
                node_type: "FileSource".into(),
                port_name: "output".into(),
            }],
            downstream_nodes: vec![DownstreamNode {
                node_id: "sink1".into(),
                node_type: "FileSink".into(),
                port_name: "input".into(),
                also_broken: true,
            }],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("DAG context"));
        assert!(prompt.contains("src1 (FileSource)"));
        assert!(prompt.contains("sink1 (FileSink)"));
        assert!(prompt.contains("ALSO BROKEN"));
    }

    #[test]
    fn topology_section_skipped_when_empty() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(!prompt.contains("DAG context"));
    }

    #[test]
    fn domain_hints_section_appears() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec!["financial data".into(), "time-series".into()],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("Domain context"));
        assert!(prompt.contains("financial data"));
        assert!(prompt.contains("time-series"));
    }

    #[test]
    fn fix_history_section_appears() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![
                FixRecord {
                    attempt: 1,
                    attempted_fix: "Changed path to /data/real.csv".into(),
                    success: false,
                    result_error: Some("Still file not found".into()),
                },
                FixRecord {
                    attempt: 2,
                    attempted_fix: "Changed format to parquet".into(),
                    success: false,
                    result_error: None,
                },
            ],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("Previous fix attempts"));
        assert!(prompt.contains("FAILED"));
        assert!(prompt.contains("Changed path to /data/real.csv"));
        assert!(prompt.contains("learn from past failures"));
    }

    #[test]
    fn fix_history_skipped_when_empty() {
        let builder = PromptBuilder::default();
        let ctx = PromptContext {
            node_type: "Test".into(),
            node_description: "Test".into(),
            current_config: json!({}),
            error_message: "test".into(),
            available_tools: vec![],
            fix_instructions: "test".into(),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(!prompt.contains("Previous fix attempts"));
    }

    #[test]
    fn full_prompt_with_all_sections() {
        let builder = PromptBuilder::new(8192);
        let ctx = PromptContext {
            node_type: "Join".into(),
            node_description: "Joins two streams on a key".into(),
            current_config: json!({"left_key": "id", "right_key": "user_id"}),
            error_message: "Column 'user_id' not found in right stream".into(),
            available_tools: vec!["list_columns".into(), "rename_column".into()],
            fix_instructions: "Fix the join key to match the actual schema.".into(),
            expected_schema: vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "name".into(),
                    data_type: "Utf8".into(),
                    nullable: true,
                },
            ],
            upstream_nodes: vec![
                UpstreamNode {
                    node_id: "src_left".into(),
                    node_type: "FileSource".into(),
                    port_name: "left".into(),
                },
                UpstreamNode {
                    node_id: "src_right".into(),
                    node_type: "ApiSource".into(),
                    port_name: "right".into(),
                },
            ],
            downstream_nodes: vec![DownstreamNode {
                node_id: "agg".into(),
                node_type: "Aggregate".into(),
                port_name: "input".into(),
                also_broken: false,
            }],
            domain_hints: vec!["e-commerce orders".into()],
            fix_history: vec![FixRecord {
                attempt: 1,
                attempted_fix: "Changed right_key to 'uid'".into(),
                success: false,
                result_error: Some("Column 'uid' not found".into()),
            }],
            graph_topology: vec![],
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.contains("Expected output schema"));
        assert!(prompt.contains("DAG context"));
        assert!(prompt.contains("Domain context"));
        assert!(prompt.contains("Previous fix attempts"));
        assert!(prompt.len() <= 8192);
    }
}
