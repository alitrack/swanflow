//! Modular prompt builder — assembles AI prompts from structured context.
//!
//! Inspired by OpenFang's `prompt_builder.rs` modular context assembly pattern.
//! Each context field comes from a separate source (NodeSpec, error diagnostic,
//! tool registry), NOT from a monolithic 200KB embedded string.

use serde_json::Value;

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
}

/// Builds AI prompts from modular context.
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

    /// Build a fix prompt from the given context.
    pub fn build_fix_prompt(&self, ctx: &PromptContext) -> String {
        let config_str = serde_json::to_string_pretty(&ctx.current_config)
            .unwrap_or_else(|_| "{}".into());
        let config_truncated = truncate(&config_str, 2000);

        let tools_str = if ctx.available_tools.is_empty() {
            "none".into()
        } else {
            ctx.available_tools.join(", ")
        };

        let prompt = format!(
            "You are fixing a data pipeline node.\n\n\
             Node: {node_type} — {description}\n\
             Error: {error}\n\n\
             Current config:\n```json\n{config}\n```\n\n\
             Available tools: {tools}\n\n\
             Instructions: {instructions}\n\n\
             Return ONLY a JSON object: {{\"fixed_config\": {{...}}}}\n\
             Only change the fields that need fixing. Keep all other fields unchanged.",
            node_type = ctx.node_type,
            description = ctx.node_description,
            error = ctx.error_message,
            config = config_truncated,
            tools = tools_str,
            instructions = ctx.fix_instructions,
        );

        truncate(&prompt, self.max_length)
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
        };

        let prompt = builder.build_fix_prompt(&ctx);
        assert!(prompt.len() <= 500);
        assert!(prompt.contains("(truncated)"));
    }
}
