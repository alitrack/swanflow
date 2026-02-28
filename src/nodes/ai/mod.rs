pub mod providers;
pub mod mcp;

use std::sync::Arc;
use anyhow::Result;
use crate::nodes::NodeSpec;
use self::providers::LlmProvider;

pub struct ConfigWizard {
    provider: Arc<dyn LlmProvider>,
}

impl ConfigWizard {
    pub fn new(provider: Arc<dyn LlmProvider>) -> Self {
        Self { provider }
    }

    /// Generates a configuration for a specific node based on a natural language prompt.
    pub async fn suggest_config(&self, node_spec: &NodeSpec, prompt: &str) -> Result<serde_json::Value> {
        let system_prompt = format!(
            "You are a configuration expert for SwanFlow. Generate a JSON configuration for a node of type '{}'.

            Node Description: {}

            Configuration Schema: {}

            Return ONLY the JSON object, no explanation.",
            node_spec.type_id,
            node_spec.description,
            node_spec.config_schema
        );

        let full_prompt = format!("{}
User Request: {}", system_prompt, prompt);
        let result = self.provider.complete(&full_prompt).await?;
        
        // Basic extraction of JSON from response
        let json_str = if result.contains("```json") {
            result.split("```json").nth(1).unwrap().split("```").next().unwrap()
        } else if result.contains("```") {
            result.split("```").nth(1).unwrap()
        } else {
            &result
        };

        let config: serde_json::Value = serde_json::from_str(json_str.trim())?;
        Ok(config)
    }
}
