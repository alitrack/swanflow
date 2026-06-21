//! OpenAiHook — LLM-powered IntelligenceHook implementation.
//!
//! Uses ModelRouter + PromptBuilder + LlmClient to automatically attempt
//! error recovery when a node execution fails.

use crate::client::LlmClient;
use crate::prompt::{PromptBuilder, PromptContext};
use crate::router::{ErrorKind, ErrorSignal, ModelRouter};
use crate::KeyProvider;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use tracing::warn;

/// Expected response format from the LLM fix prompt.
#[derive(Debug, Deserialize)]
struct FixResponse {
    fixed_config: Value,
}

/// Maps a generic error message to an ErrorKind for routing.
fn classify_error(error: &str) -> ErrorKind {
    let lower = error.to_lowercase();
    if lower.contains("not found") || lower.contains("missing") || lower.contains("invalid parameter") {
        ErrorKind::Config
    } else if lower.contains("schema") || lower.contains("column") || lower.contains("type mismatch") {
        ErrorKind::Schema
    } else if lower.contains("null") || lower.contains("overflow") || lower.contains("cast") {
        ErrorKind::Data
    } else {
        ErrorKind::Topology
    }
}

/// LLM-powered hook that attempts to fix node execution errors.
pub struct OpenAiHook {
    client: LlmClient,
    router: ModelRouter,
    prompt_builder: PromptBuilder,
}

impl OpenAiHook {
    pub fn new(
        key_provider: Arc<dyn KeyProvider>,
        router_config: crate::router::RouterConfig,
    ) -> Self {
        Self {
            client: LlmClient::new(key_provider),
            router: ModelRouter::new(router_config),
            prompt_builder: PromptBuilder::default(),
        }
    }

    /// Attempt to fix a node error using an LLM.
    ///
    /// Returns `Some(fixed_config)` if the LLM successfully proposed a fix,
    /// or `None` if the fix attempt failed or timed out.
    pub async fn try_fix(
        &self,
        node_type: &str,
        node_description: &str,
        config: &Value,
        error: &str,
    ) -> Option<Value> {
        let signal = ErrorSignal {
            error_kind: classify_error(error),
            affected_nodes: 1,
            schema_mismatch: error.to_lowercase().contains("schema"),
        };

        let model = self.router.route(&signal);
        let ctx = PromptContext {
            node_type: node_type.to_string(),
            node_description: node_description.to_string(),
            current_config: config.clone(),
            error_message: error.to_string(),
            available_tools: vec![],
            fix_instructions: format!(
                "The node '{}' failed with the above error. \
                 Suggest a fix by modifying the config. \
                 Error type: {:?}",
                node_type, signal.error_kind
            ),
        };

        let prompt = self.prompt_builder.build_fix_prompt(&ctx);

        match timeout(
            Duration::from_secs(model.timeout_secs),
            self.client.complete(&model, &prompt),
        )
        .await
        {
            Ok(Ok(response)) => match serde_json::from_str::<FixResponse>(&response) {
                Ok(fix) => Some(fix.fixed_config),
                Err(e) => {
                    warn!("Failed to parse LLM fix response: {}", e);
                    None
                }
            },
            Ok(Err(e)) => {
                warn!("LLM fix request failed: {}", e);
                None
            }
            Err(_) => {
                warn!("LLM fix request timed out");
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_config_error() {
        assert_eq!(
            classify_error("File not found: /data/missing.csv"),
            ErrorKind::Config
        );
        assert_eq!(
            classify_error("Missing parameter 'delimiter'"),
            ErrorKind::Config
        );
    }

    #[test]
    fn classify_schema_error() {
        assert_eq!(
            classify_error("Schema mismatch: expected Int64, got Utf8"),
            ErrorKind::Schema
        );
        assert_eq!(
            classify_error("Column 'age' not found in schema"),
            ErrorKind::Schema
        );
    }

    #[test]
    fn classify_data_error() {
        assert_eq!(
            classify_error("Null value in non-nullable column"),
            ErrorKind::Data
        );
        assert_eq!(
            classify_error("Integer overflow: value 99999999999999999999"),
            ErrorKind::Data
        );
    }

    #[test]
    fn classify_topology_error() {
        assert_eq!(
            classify_error("Unknown error in execution"),
            ErrorKind::Topology
        );
    }
}
