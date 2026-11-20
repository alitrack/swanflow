use crate::nodes::{
    DesignerMetadata, NodeBehavior, NodeCapability, NodeSpec,
};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// A thread-safe registry for node implementations.
/// Node types are indexed by their unique type_id.
///
/// The basic registry provides CRUD for NodeBehavior implementations.
/// Concrete node registration happens via trait extensions or the
/// `swanflow-nodes` crate's `register_core_nodes()` function.
pub struct NodeRegistry {
    pub nodes: HashMap<String, Arc<dyn NodeBehavior>>,
}

impl NodeRegistry {
    /// Create a new, empty node registry.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Register a node implementation.
    pub fn register_node(&mut self, behavior: Arc<dyn NodeBehavior>) {
        let type_id = behavior.spec().type_id;
        self.nodes.insert(type_id, behavior);
    }

    /// Retrieve a node implementation by its type identifier.
    pub fn get_node(&self, type_id: &str) -> Result<Arc<dyn NodeBehavior>> {
        self.nodes
            .get(type_id)
            .cloned()
            .ok_or_else(|| anyhow!("Node type '{}' not found in registry", type_id))
    }

    /// List all registered node specifications.
    pub fn list_specs(&self) -> Vec<NodeSpec> {
        self.nodes.values().map(|n| n.spec()).collect()
    }

    /// List all registered node type IDs.
    pub fn list_type_ids(&self) -> Vec<&str> {
        self.nodes.keys().map(|s| s.as_str()).collect()
    }

    /// Returns true if a node type is registered.
    pub fn contains(&self, type_id: &str) -> bool {
        self.nodes.contains_key(type_id)
    }

    // ── Capability queries ──────────────────────────────────────────

    /// Find all nodes with a specific capability.
    pub fn find_by_capability(&self, capability: NodeCapability) -> Vec<&str> {
        self.nodes
            .iter()
            .filter(|(_, node)| node.capability() == capability)
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Check if a node type is safe for read-only execution (no side effects).
    pub fn is_read_only(&self, type_id: &str) -> Result<bool> {
        let node = self.get_node(type_id)?;
        Ok(matches!(node.capability(), NodeCapability::ReadOnly))
    }

    /// Validate that a node is safe to execute in the current context.
    /// Returns an error if the node requires external access that isn't available.
    pub fn validate_capability_gate(
        &self,
        type_id: &str,
        allow_external: bool,
    ) -> Result<()> {
        let node = self.get_node(type_id)?;
        if !allow_external {
            match node.capability() {
                NodeCapability::ReadOnly => Ok(()),
                NodeCapability::ReadWrite | NodeCapability::WriteOnly => {
                    Err(anyhow!(
                        "Node '{}' requires external access (capability: {:?}) but external access is disabled",
                        type_id,
                        node.capability()
                    ))
                }
            }
        } else {
            Ok(())
        }
    }

    // ── Metadata export (for visual editor consumption) ─────────────

    /// Build a `DesignerMetadata` for a node type without instantiating it.
    ///
    /// If the node implements `TypedNodeBehavior`, uses the typed metadata
    /// generation which includes port-count guarantees. Otherwise falls
    /// back to `NodeSpec`-only metadata.
    pub fn designer_metadata_for(&self, type_id: &str) -> Result<DesignerMetadata> {
        let node = self.get_node(type_id)?;
        // Try downcasting to TypedNodeBehavior for richer metadata.
        // Since Rust doesn't support trait object downcasting on non-'static
        // traits, we generate metadata from the spec for untyped nodes.
        let spec = node.spec();
        Ok(DesignerMetadata {
            type_id: spec.type_id,
            display_name: spec.display_name,
            description: spec.description,
            category: crate::nodes::NodeCategory::Other("uncategorized".into()),
            icon: None,
            input_count: spec.input_ports.len(),
            output_count: spec.output_ports.len(),
            inputs: spec
                .input_ports
                .iter()
                .map(|p| crate::nodes::PortDesignMetadata {
                    name: p.name.clone(),
                    description: p.description.clone(),
                    data_type: datafusion::arrow::datatypes::DataType::Utf8,
                })
                .collect(),
            outputs: spec
                .output_ports
                .iter()
                .map(|p| crate::nodes::PortDesignMetadata {
                    name: p.name.clone(),
                    description: p.description.clone(),
                    data_type: datafusion::arrow::datatypes::DataType::Utf8,
                })
                .collect(),
            options: vec![],
            config_schema: spec.config_schema,
            capability: node.capability(),
            impl_version: node.impl_version(),
            config_version: node.config_version(),
        })
    }

    /// Export the complete designer metadata catalog as JSON.
    /// This is the primary API for visual editors to discover available nodes.
    pub fn export_metadata_catalog(&self) -> Vec<DesignerMetadata> {
        self.nodes
            .keys()
            .filter_map(|type_id| self.designer_metadata_for(type_id).ok())
            .collect()
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Meta column conventions — per ArkFlow's __meta_* pattern
// ═══════════════════════════════════════════════════════════════════════════

/// Standard metadata column names injected by the engine into every output.
/// Enables SQL-level data lineage and debugging without polluting user columns.
pub mod meta {
    /// The node id that produced this row.
    pub const SOURCE: &str = "__meta_source";
    /// Row number within the node's output.
    pub const ROW: &str = "__meta_row";
    /// Timestamp when this row was computed.
    pub const TIMESTAMP: &str = "__meta_timestamp";
    /// Input partition that produced this row (streaming sources).
    pub const PARTITION: &str = "__meta_partition";
}
