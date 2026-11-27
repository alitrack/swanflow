//! Schema-driven config validation — the first consumer of the config contract.
//!
//! Borrowed pattern from Forge (github.com/nihalashetty/Forge, MIT, research
//! #2026081102): a single JSON Schema drives validation, compilation, and UI
//! form generation so the frontend and backend cannot drift. SwanFlow has no
//! GUI; the "form generation" corner is served by exposing `config_schema`
//! through the adapter protocol. This module is the "validator" corner:
//! `NodeSpec.config_schema` becomes the single source of truth for config
//! checks, replacing per-node hand-written validation as the default.
//!
//! Errors carry a JSON Pointer path (`/config/sandboxed`) so external editors
//! can highlight the offending field directly.

use std::collections::HashMap;

use serde_json::Value as JsonValue;

use crate::error::{CoreResult, ErrorCategory, ErrorCode, SwanFlowError};
use crate::registry::NodeRegistry;

/// Schema-driven config validator.
///
/// Built from a [`NodeRegistry`]: every registered node's `config_schema` is
/// indexed by `type_id`. Validation compiles the schema once (jsonschema
/// caches internally) and checks the instance against it.
#[derive(Debug, Default, Clone)]
pub struct SchemaValidator {
    schemas: HashMap<String, JsonValue>,
}

impl SchemaValidator {
    /// Collect every registered node's `config_schema`, keyed by `type_id`.
    pub fn from_registry(registry: &NodeRegistry) -> Self {
        let mut schemas = HashMap::new();
        for spec in registry.list_specs() {
            schemas.insert(spec.type_id.clone(), spec.config_schema.clone());
        }
        Self { schemas }
    }

    /// Validate `config` against the schema registered for `type_id`.
    ///
    /// Errors:
    /// - `CONFIG_ERROR` (Semantic) when the node type is unknown or has no
    ///   schema — never a silent pass.
    /// - `CONFIG_ERROR` (Semantic) when the instance fails validation; the
    ///   message embeds the JSON Pointer path (e.g. `/config/sandboxed`).
    pub fn validate_config(&self, type_id: &str, config: &JsonValue) -> CoreResult<()> {
        let schema = self.schemas.get(type_id).ok_or_else(|| {
            SwanFlowError::new(
                ErrorCategory::Semantic,
                ErrorCode::ConfigError,
                format!("Node type '{type_id}' has no config_schema in registry"),
            )
        })?;
        Self::validate_single(type_id, schema, config)
    }

    /// Validate `config` against an explicit schema (no registry lookup).
    ///
    /// This is the static entry point used by the `NodeBehavior::validate`
    /// default implementation: a node validates its own spec's schema without
    /// needing a registry reference.
    pub fn validate_single(
        type_id: &str,
        schema: &JsonValue,
        config: &JsonValue,
    ) -> CoreResult<()> {
        let validator = jsonschema::validator_for(schema).map_err(|e| {
            SwanFlowError::new(
                ErrorCategory::Semantic,
                ErrorCode::ConfigError,
                format!("Invalid JSON Schema for node type '{type_id}': {e}"),
            )
        })?;
        validator.validate(config).map_err(|e| {
            SwanFlowError::new(
                ErrorCategory::Semantic,
                ErrorCode::ConfigError,
                format!(
                    "Config validation failed for '{type_id}' at {}: {}",
                    e.instance_path(),
                    e
                ),
            )
        })
    }

    /// Return the raw schema for `type_id`, if registered.
    pub fn schema_for(&self, type_id: &str) -> Option<&JsonValue> {
        self.schemas.get(type_id)
    }

    /// All registered type ids (for debugging / adapter exposure).
    pub fn known_types(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }
}

/// A compiled validator for *instance* data (as opposed to node config).
///
/// First consumer: `core.ai.structured_io_contract`, which checks every
/// element of an LLM's JSON output against the workflow author's schema
/// (UA 4-layer pipeline, layer 3: per-element validation; bad elements are
/// dropped and counted instead of failing the whole batch). Keeping the
/// `jsonschema` types private to swanflow-core means the swanflow crate
/// never has to name the dependency itself (no new dependencies allowed in
/// the UA port batches — jsonschema 0.49 already lives here).
pub struct InstanceValidator {
    validator: jsonschema::Validator,
}

impl std::fmt::Debug for InstanceValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceValidator").finish()
    }
}

impl InstanceValidator {
    /// Compile an instance schema. Fails with a human-readable message when
    /// the schema itself is not a valid JSON Schema.
    pub fn compile(schema: &serde_json::Value) -> Result<Self, String> {
        jsonschema::validator_for(schema)
            .map(|validator| Self { validator })
            .map_err(|e| format!("invalid JSON Schema: {e}"))
    }

    /// Validate one instance. `Ok(())` = element passes; `Err` carries a
    /// human-readable reason suitable for a `rejected` cell or diagnostic.
    pub fn validate(&self, instance: &serde_json::Value) -> Result<(), String> {
        self.validator
            .validate(instance)
            .map_err(|e| format!("{e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_schema() -> JsonValue {
        json!({
            "type": "object",
            "properties": {
                "script": { "type": "string" },
                "sandboxed": { "type": "boolean" }
            },
            "required": ["script"]
        })
    }

    #[test]
    fn valid_config_passes() {
        let config = json!({"script": "return batch", "sandboxed": false});
        assert!(SchemaValidator::validate_single("test.node", &sample_schema(), &config).is_ok());
    }

    #[test]
    fn missing_required_field_errors_with_pointer() {
        let config = json!({"sandboxed": true});
        let err = SchemaValidator::validate_single("test.node", &sample_schema(), &config)
            .expect_err("missing required field must fail");
        let msg = err.to_string();
        assert!(msg.contains("script"), "error should name the field: {msg}");
        // jsonschema 0.49: required-errors point at the whole object (empty
        // instance path) and name the missing field in the message. Type/format
        // errors carry the JSON Pointer (covered by wrong_type_errors_with_pointer).
        assert!(msg.contains("required"), "should say required: {msg}");
    }

    #[test]
    fn wrong_type_errors_with_pointer() {
        let config = json!({"script": 42});
        let err = SchemaValidator::validate_single("test.node", &sample_schema(), &config)
            .expect_err("wrong type must fail");
        assert!(err.to_string().contains("/script"));
    }

    #[test]
    fn unknown_type_id_reports_explicit_error() {
        let v = SchemaValidator::default();
        let err = v
            .validate_config("no_such_type", &json!({}))
            .expect_err("unknown type must fail loudly");
        assert!(err.to_string().contains("no_such_type"));
        assert!(err.to_string().contains("no config_schema"));
    }

    #[test]
    fn invalid_schema_reports_schema_error() {
        let bad_schema = json!({"type": 42});
        let err = SchemaValidator::validate_single("broken.node", &bad_schema, &json!({}))
            .expect_err("invalid schema must fail");
        assert!(err.to_string().contains("broken.node"));
    }

    #[test]
    fn empty_config_ok_when_no_required() {
        let schema = json!({
            "type": "object",
            "properties": { "name": { "type": "string" } }
        });
        assert!(SchemaValidator::validate_single("t", &schema, &json!({})).is_ok());
    }

    #[test]
    fn from_registry_indexes_specs() {
        use crate::nodes::{NodeBehavior, NodeSpec};
        use anyhow::Result;
        use datafusion::logical_expr::LogicalPlan;
        use std::sync::Arc;

        #[derive(Debug)]
        struct Dummy;
        #[async_trait::async_trait]
        impl NodeBehavior for Dummy {
            fn spec(&self) -> NodeSpec {
                NodeSpec {
                    type_id: "test.dummy".into(),
                    display_name: "Dummy".into(),
                    description: String::new(),
                    input_ports: vec![],
                    output_ports: vec![],
                    config_schema: json!({"type": "object"}),
                }
            }
            async fn propose_plan(
                &self,
                _inputs: Vec<LogicalPlan>,
                _config: &JsonValue,
            ) -> Result<Vec<LogicalPlan>> {
                Ok(vec![])
            }
        }

        let mut reg = NodeRegistry::new();
        reg.register_node(Arc::new(Dummy));
        let v = SchemaValidator::from_registry(&reg);
        assert!(v.schema_for("test.dummy").is_some());
        assert!(v.validate_config("test.dummy", &json!({})).is_ok());
    }

    #[test]
    fn instance_validator_compiles_and_rejects_with_reason() {
        let v = InstanceValidator::compile(&json!({
            "type": "object",
            "properties": { "kind": { "enum": ["organization", "person"] } },
            "required": ["kind"]
        }))
        .expect("schema must compile");
        assert!(v.validate(&json!({ "kind": "person" })).is_ok());
        let err = v
            .validate(&json!({ "kind": "星系" }))
            .expect_err("enum violation must fail");
        assert!(
            err.contains("organization"),
            "reason should name the allowed values: {err}"
        );
    }

    #[test]
    fn instance_validator_compile_error_is_human_readable() {
        let err = InstanceValidator::compile(&json!({ "type": 42 }))
            .expect_err("bad schema must not compile");
        assert!(err.starts_with("invalid JSON Schema"));
    }

    #[test]
    fn instance_validator_is_send_sync() {
        // InstanceValidator crosses thread boundaries inside the contract
        // node's streaming Exec — pin that down at compile time.
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<InstanceValidator>();
    }

    #[test]
    fn default_validate_uses_config_schema() {
        // A node that does NOT override validate() gets schema-driven checks
        // from the trait default — this is the P0 contract-triangle payoff.
        use crate::nodes::{NodeBehavior, NodeSpec};
        use anyhow::Result;
        use datafusion::logical_expr::LogicalPlan;

        #[derive(Debug)]
        struct TypedDummy;
        #[async_trait::async_trait]
        impl NodeBehavior for TypedDummy {
            fn spec(&self) -> NodeSpec {
                NodeSpec {
                    type_id: "test.typed".into(),
                    display_name: "Typed".into(),
                    description: String::new(),
                    input_ports: vec![],
                    output_ports: vec![],
                    // schema requires `query` string
                    config_schema: json!({
                        "type": "object",
                        "properties": { "query": { "type": "string" } },
                        "required": ["query"]
                    }),
                }
            }
            async fn propose_plan(
                &self,
                _inputs: Vec<LogicalPlan>,
                _config: &JsonValue,
            ) -> Result<Vec<LogicalPlan>> {
                Ok(vec![])
            }
        }

        let node = TypedDummy;
        // no validate() override → trait default runs schema validation
        assert!(node.validate(&json!({"query": "SELECT 1"})).is_ok());
        assert!(
            node.validate(&json!({"query": 42})).is_err(),
            "wrong type must fail"
        );
        assert!(
            node.validate(&json!({})).is_err(),
            "missing required must fail"
        );
    }
}
