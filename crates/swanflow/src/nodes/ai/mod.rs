pub mod contracts;
pub mod graph;
pub mod mcp;
pub mod privacy;
pub mod providers;
pub mod retrieval;
pub mod runtime;
pub mod tool_executor;
pub mod verification;

use self::providers::LlmProvider;
use crate::nodes::NodeSpec;
use anyhow::Result;
use std::sync::Arc;

pub struct ConfigWizard {
    provider: Arc<dyn LlmProvider>,
}

impl ConfigWizard {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self { provider }
    }

    /// Generates a configuration for a specific node based on a natural language prompt.
    pub async fn suggest_config(
        &self,
        node_spec: &NodeSpec,
        prompt: &str,
    ) -> Result<serde_json::Value> {
        let system_prompt = format!(
            "You are a configuration expert for SwanFlow. Generate a JSON configuration for a node of type '{}'.

            Node Description: {}

            Configuration Schema: {}

            Return ONLY the JSON object, no explanation.",
            node_spec.type_id,
            node_spec.description,
            node_spec.config_schema
        );

        let full_prompt = format!(
            "{}
User Request: {}",
            system_prompt, prompt
        );
        let result = self.provider.complete(&full_prompt).await?;

        // Safe extraction of JSON from response - handles markdown code blocks
        let json_str = Self::extract_json_from_response(&result)?;

        let config: serde_json::Value = serde_json::from_str(json_str.trim()).map_err(|e| {
            anyhow::anyhow!(
                "Failed to parse LLM config response: {}. Response: {}",
                e,
                json_str
            )
        })?;
        Ok(config)
    }

    /// Safely extracts JSON from LLM response, handling various markdown formats.
    /// Made pub for testing.
    pub fn extract_json_from_response(response: &str) -> Result<&str> {
        // Try ```json ... ``` format first
        if let Some(start) = response.find("```json") {
            let after_json_start = &response[start + 7..]; // Skip "```json"
            if let Some(end) = after_json_start.find("```") {
                return Ok(&after_json_start[..end]);
            }
            // If no closing ```, try to find any ```
            if let Some(end) = after_json_start.find("`") {
                return Ok(&after_json_start[..end]);
            }
            // No closing marker - try anyway with rest of string
            return Ok(after_json_start.trim());
        }

        // Try generic ``` ... ``` format
        if let Some(start) = response.find("```") {
            let after_first_tick = &response[start + 3..];
            if let Some(end) = after_first_tick.find("```") {
                return Ok(&after_first_tick[..end]);
            }
            // No closing - try anyway
            return Ok(after_first_tick.trim());
        }

        // No code blocks - try to find JSON object directly
        // Look for { ... }
        if let Some(start) = response.find('{') {
            let mut brace_count = 0;
            // Use char_indices so `end` is a BYTE offset — chars().enumerate() yields
            // char positions which are NOT byte offsets for multi-byte UTF-8
            // (Chinese text: 3 bytes/char), slicing at a char index truncates the JSON.
            for (i, c) in response[start..].char_indices() {
                match c {
                    '{' => brace_count += 1,
                    '}' => {
                        brace_count -= 1;
                        if brace_count == 0 {
                            return Ok(&response[start..start + i + c.len_utf8()]);
                        }
                    }
                    _ => {}
                }
            }
        }

        // Fallback: return as-is
        Ok(response.trim())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_json_from_response_with_json_block() {
        let response = r#"```json
{"key": "value"}
```"#;
        let result = ConfigWizard::extract_json_from_response(response).unwrap();
        assert!(result.contains("key"));
    }

    #[test]
    fn test_extract_json_from_response_with_generic_block() {
        let response = r#"```
{"key": "value"}
```"#;
        let result = ConfigWizard::extract_json_from_response(response).unwrap();
        assert!(result.contains("key"));
    }

    #[test]
    fn test_extract_json_from_response_no_block() {
        let response = r#"{"key": "value"}"#;
        let result = ConfigWizard::extract_json_from_response(response).unwrap();
        assert!(result.contains("key"));
    }

    #[test]
    fn test_extract_json_chinese_multibyte_not_truncated() {
        // Regression (Bug 13): chars().enumerate() yields CHAR indices but the slice
        // used BYTE offsets — Chinese JSON (3 bytes/char) got deterministically
        // truncated at "EOF while parsing a string". ASCII tests all passed.
        let response = r#"{"nodes": [{"name": "患者0001", "label": "心内科"}], "edges": [{"source": "患者0001", "target": "心内科", "type": "就诊于"}]}"#;
        let result = ConfigWizard::extract_json_from_response(response).unwrap();
        assert_eq!(result, response, "multi-byte JSON must extract complete, not truncated");
    }

    #[test]
    fn test_extract_json_from_response_invalid_json() {
        let response = "not json at all";
        // Should return trimmed response as fallback
        let result = ConfigWizard::extract_json_from_response(response).unwrap();
        assert_eq!(result, "not json at all");
    }
}
