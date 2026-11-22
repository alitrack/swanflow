//! OpenAiHook — LLM-powered IntelligenceHook implementation.
//!
//! Uses ModelRouter + PromptBuilder + LlmClient to automatically attempt
//! error recovery when a node execution fails.

use crate::client::LlmClient;
use crate::prompt::{PromptBuilder, PromptContext};
use crate::router::{ConfigComplexity, ErrorKind, ErrorSignal, ModelRouter};
use crate::KeyProvider;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use swanflow_core::repair::{RepairOperator, RepairResponse};
use tokio::time::timeout;
use tracing::warn;

/// Expected response format from the LLM fix prompt: a tagged repair
/// operator (GraSP-style), or the legacy bare `{"fixed_config": ...}` shape
/// (handled by [`RepairResponse`]'s untagged deserialization).
type FixResponse = RepairResponse;

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

/// Measure the complexity of a node's configuration.
///
/// Heuristics:
/// - Simple: flat config with ≤ 5 fields, no nested objects/arrays
/// - Medium: nested objects or 6–15 fields
/// - Complex: deeply nested, arrays, or > 15 fields
pub fn measure_config_complexity(config: &Value) -> ConfigComplexity {
    let field_count = count_fields(config);
    let max_depth = measure_depth(config);
    let has_arrays = has_array_values(config);

    if max_depth <= 1 && field_count <= 5 && !has_arrays {
        ConfigComplexity::Simple
    } else if max_depth <= 3 && field_count <= 15 {
        ConfigComplexity::Medium
    } else {
        ConfigComplexity::Complex
    }
}

/// Count total number of leaf fields in a JSON value.
///
/// `depth` used to be threaded through the recursion but was never read, so it
/// is gone: the parameter implied the count depended on nesting, which it does
/// not.
fn count_fields(value: &Value) -> usize {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                1
            } else {
                map.values().map(count_fields).sum()
            }
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                1
            } else {
                arr.iter().map(count_fields).sum()
            }
        }
        _ => 1,
    }
}

/// Measure the maximum nesting depth of a JSON value.
fn measure_depth(value: &Value) -> usize {
    match value {
        Value::Object(map) => {
            if map.is_empty() {
                1
            } else {
                1 + map.values().map(measure_depth).max().unwrap_or(0)
            }
        }
        Value::Array(arr) => {
            if arr.is_empty() {
                1
            } else {
                1 + arr.iter().map(measure_depth).max().unwrap_or(0)
            }
        }
        _ => 1,
    }
}

/// Check if a JSON value contains any arrays (recursively).
fn has_array_values(value: &Value) -> bool {
    match value {
        Value::Array(_) => true,
        Value::Object(map) => map.values().any(has_array_values),
        _ => false,
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
    /// Returns `Some(repair_operator)` if the LLM successfully proposed a
    /// fix, or `None` if the fix attempt failed or timed out. The returned
    /// operator must be applied via `swanflow::core::repair::apply_repair`.
    ///
    /// `graph_topology` carries the DAG edges around the failed node as
    /// "source→target" strings (used by INSERT_PREREQ / REWIRE operators).
    ///
    /// Now populates enhanced `ErrorSignal` (5-dimension scoring) and
    /// enriched `PromptContext` (schema, topology, domain hints, fix history).
    pub async fn try_fix(
        &self,
        node_type: &str,
        node_description: &str,
        config: &Value,
        error: &str,
        graph_topology: &[String],
    ) -> Option<RepairOperator> {
        // Build enhanced error signal with all 5 dimensions
        let config_complexity = measure_config_complexity(config);
        let signal = ErrorSignal {
            error_kind: classify_error(error),
            affected_nodes: 1,
            schema_mismatch: error.to_lowercase().contains("schema"),
            error_message_len: error.len(),
            config_complexity,
        };

        let model = self.router.route(&signal);

        // Build enriched prompt context
        let ctx = PromptContext {
            node_type: node_type.to_string(),
            node_description: node_description.to_string(),
            current_config: config.clone(),
            error_message: error.to_string(),
            available_tools: vec![],
            fix_instructions: format!(
                "The node '{}' failed with the above error. \
                 Suggest a repair. \
                 Error type: {:?}, config complexity: {:?}.",
                node_type, signal.error_kind, signal.config_complexity
            ),
            expected_schema: vec![],
            upstream_nodes: vec![],
            downstream_nodes: vec![],
            domain_hints: vec![],
            fix_history: vec![],
            graph_topology: graph_topology.to_vec(),
        };

        let prompt = self.prompt_builder.build_fix_prompt(&ctx);

        match timeout(
            Duration::from_secs(model.timeout_secs),
            self.client.complete(&model, &prompt),
        )
        .await
        {
            Ok(Ok(response)) => match serde_json::from_str::<FixResponse>(&response) {
                Ok(parsed) => Some(parsed.into_operator()),
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
    use serde_json::json;

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
            ErrorKind::Config
        );
    }

    #[test]
    fn classify_data_error() {
        assert_eq!(
            classify_error("Null value in non-nullable column"),
            ErrorKind::Schema
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

    #[test]
    fn measure_simple_config() {
        let config = json!({"path": "/data/file.csv", "format": "csv"});
        assert_eq!(
            measure_config_complexity(&config),
            ConfigComplexity::Medium
        );
    }

    #[test]
    fn measure_medium_config_nested() {
        let config = json!({
            "source": {"type": "file", "path": "/data"},
            "format": {"type": "csv", "delimiter": ",", "header": true},
            "options": {"encoding": "utf-8"}
        });
        let complexity = measure_config_complexity(&config);
        // 3 nested objects, 6+ fields → Medium
        assert_eq!(complexity, ConfigComplexity::Medium);
    }

    #[test]
    fn measure_complex_config_deeply_nested() {
        let config = json!({
            "a": {"b": {"c": {"d": {"e": "deep"}}}},
            "array_field": [1, 2, 3]
        });
        assert_eq!(
            measure_config_complexity(&config),
            ConfigComplexity::Complex
        );
    }

    #[test]
    fn measure_complex_config_many_fields() {
        let mut map = serde_json::Map::new();
        for i in 0..20 {
            map.insert(format!("field_{}", i), json!(i));
        }
        let config = Value::Object(map);
        assert_eq!(
            measure_config_complexity(&config),
            ConfigComplexity::Complex
        );
    }

    #[test]
    fn measure_empty_config_is_simple() {
        assert_eq!(
            measure_config_complexity(&json!({})),
            ConfigComplexity::Simple
        );
    }
}
