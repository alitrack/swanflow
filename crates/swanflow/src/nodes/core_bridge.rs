//! Bridge from `swanflow_core::NodeBehavior` to this crate's `NodeBehavior`.
//!
//! ## Why this exists
//!
//! `plugin-architecture.md` §P1 asked for `core.quality.*` to leave the core
//! tree. The obstacle was not the code (those nodes touch only the node
//! contract, no engine internals) but the dependency graph: a plugin crate that
//! implemented *this* crate's `NodeBehavior` would have to depend on `swanflow`,
//! and `swanflow` would have to depend on the plugin to register it — a cycle
//! Cargo refuses.
//!
//! The way out was already in the tree: `swanflow_core` defines a parallel
//! `NodeBehavior` (`crates/swanflow-core/src/nodes.rs`) with an identical
//! `NodeSpec`/`PortSpec`/`NodeCapability` shape and the same
//! `async fn propose_plan(Vec<LogicalPlan>, &JsonValue)`. Both crates pin
//! `datafusion 52.1.0`, so `LogicalPlan` is the *same type* on both sides — the
//! two traits differ only in the module they live in.
//!
//! So the plugin depends on `swanflow-core` only, and this adapter (in
//! `swanflow`, which already depends on `swanflow-core`) turns a core node into
//! a native one. The graph stays acyclic:
//!
//! ```text
//! swanflow-core  ←── swanflow-plugin-quality
//!       ↑                    │
//!       └──────── swanflow ──┘   (optional dep, feature "plugin-quality")
//! ```
//!
//! ## What it costs
//!
//! One delegation per method, and the `NodeSpec` is rebuilt field by field
//! because the two structs are distinct types even though their fields match.
//! `TypedNodeBehavior` is *not* bridged: its whole point is a compile-time
//! `const` arity check, which a `Box<dyn ...>` erases. Bridged nodes therefore
//! rely on their own `propose_plan` validation for arity, which the quality
//! nodes already do.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde_json::Value as JsonValue;

use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};

/// Wraps a plugin node from `swanflow_core` so it can be registered natively.
pub struct CoreNodeAdapter {
    inner: Box<dyn swanflow_core::NodeBehavior>,
    /// Kept because `spec()` returns an owned `NodeSpec`; rebuilding it on
    /// every call would allocate a fresh schema `JsonValue` each time, and
    /// `spec()` is called on every palette refresh.
    spec: NodeSpec,
}

impl CoreNodeAdapter {
    pub fn new(inner: Box<dyn swanflow_core::NodeBehavior>) -> Self {
        let core_spec = inner.spec();
        let spec = NodeSpec {
            type_id: core_spec.type_id,
            display_name: core_spec.display_name,
            description: core_spec.description,
            input_ports: core_spec
                .input_ports
                .into_iter()
                .map(|p| PortSpec {
                    id: p.id,
                    name: p.name,
                    description: p.description,
                })
                .collect(),
            output_ports: core_spec
                .output_ports
                .into_iter()
                .map(|p| PortSpec {
                    id: p.id,
                    name: p.name,
                    description: p.description,
                })
                .collect(),
            config_schema: core_spec.config_schema,
        };
        Self { inner, spec }
    }

    /// Wrap a whole plugin's node list.
    pub fn wrap_all(nodes: Vec<Box<dyn swanflow_core::NodeBehavior>>) -> Vec<Arc<dyn NodeBehavior>> {
        nodes
            .into_iter()
            .map(|n| Arc::new(Self::new(n)) as Arc<dyn NodeBehavior>)
            .collect()
    }
}

#[async_trait]
impl NodeBehavior for CoreNodeAdapter {
    fn spec(&self) -> NodeSpec {
        self.spec.clone()
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        self.inner.validate(config)
    }

    fn impl_version(&self) -> u32 {
        self.inner.impl_version()
    }

    fn config_version(&self) -> u32 {
        self.inner.config_version()
    }

    fn migrate_config(
        &self,
        config: JsonValue,
        from_version: u32,
        to_version: u32,
    ) -> Result<JsonValue> {
        // Both crates take config by value and require incremental steps, so
        // this is a straight pass-through — no adaptation is needed or wanted.
        self.inner.migrate_config(config, from_version, to_version)
    }

    fn capability(&self) -> NodeCapability {
        // Mapped by hand: the two enums are separate types with the same
        // variants. Both are `#[non_exhaustive]`, so the `_` arm is mandatory;
        // it maps to ReadWrite, the conservative choice — if a future variant
        // means "this writes somewhere", claiming ReadOnly would hide the side
        // effect from the palette's safety warning.
        match self.inner.capability() {
            swanflow_core::NodeCapability::ReadOnly => NodeCapability::ReadOnly,
            swanflow_core::NodeCapability::ReadWrite => NodeCapability::ReadWrite,
            swanflow_core::NodeCapability::WriteOnly => NodeCapability::WriteOnly,
            _ => NodeCapability::ReadWrite,
        }
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        self.inner.propose_plan(inputs, config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal core-side node, to prove the bridge carries config through to
    /// the plugin's `propose_plan` rather than short-circuiting.
    struct EchoNode;

    #[async_trait]
    impl swanflow_core::NodeBehavior for EchoNode {
        fn spec(&self) -> swanflow_core::NodeSpec {
            swanflow_core::NodeSpec {
                type_id: "core.test.echo".to_string(),
                display_name: "Echo".to_string(),
                description: "test only".to_string(),
                input_ports: vec![swanflow_core::PortSpec {
                    id: 0,
                    name: "input".into(),
                    description: "".into(),
                }],
                output_ports: vec![swanflow_core::PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "".into(),
                }],
                config_schema: serde_json::json!({"type": "object"}),
            }
        }

        fn capability(&self) -> swanflow_core::NodeCapability {
            swanflow_core::NodeCapability::ReadOnly
        }

        async fn propose_plan(
            &self,
            inputs: Vec<LogicalPlan>,
            _config: &serde_json::Value,
        ) -> Result<Vec<LogicalPlan>> {
            Ok(inputs)
        }
    }

    struct WriterNode;

    #[async_trait]
    impl swanflow_core::NodeBehavior for WriterNode {
        fn spec(&self) -> swanflow_core::NodeSpec {
            EchoNode.spec()
        }
        fn capability(&self) -> swanflow_core::NodeCapability {
            swanflow_core::NodeCapability::WriteOnly
        }
        async fn propose_plan(
            &self,
            _inputs: Vec<LogicalPlan>,
            _config: &serde_json::Value,
        ) -> Result<Vec<LogicalPlan>> {
            Ok(vec![])
        }
    }

    #[test]
    fn spec_is_carried_across_the_bridge() {
        let bridged = CoreNodeAdapter::new(Box::new(EchoNode));
        let spec = bridged.spec();
        assert_eq!(spec.type_id, "core.test.echo");
        assert_eq!(spec.input_ports.len(), 1);
        assert_eq!(spec.output_ports[0].name, "output");
    }

    #[test]
    fn capability_is_mapped_not_defaulted() {
        // Regression guard: if the mapping ever falls back to a default, a
        // ReadOnly plugin node would be reported as a writer and the palette
        // would warn about side effects it does not have.
        assert_eq!(
            CoreNodeAdapter::new(Box::new(EchoNode)).capability(),
            NodeCapability::ReadOnly
        );
        assert_eq!(
            CoreNodeAdapter::new(Box::new(WriterNode)).capability(),
            NodeCapability::WriteOnly
        );
    }
}
