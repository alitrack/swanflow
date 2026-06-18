use crate::nodes::NodeBehavior;
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
    pub fn list_specs(&self) -> Vec<crate::nodes::NodeSpec> {
        self.nodes.values().map(|n| n.spec()).collect()
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        Self::new()
    }
}
