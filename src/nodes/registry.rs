use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use crate::nodes::NodeBehavior;

/// A thread-safe registry for node implementations.
/// Node types are indexed by their unique type_id.
pub struct NodeRegistry {
    nodes: HashMap<String, Arc<dyn NodeBehavior>>,
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

    /// Register all fundamental SwanFlow nodes.
    pub fn register_core_nodes(&mut self) {
        use crate::nodes::sql::SqlNode;
        use crate::nodes::io::{CsvReadNode, ParquetReadNode, ExcelReadNode, ExcelWriteNode, DuckDBReadNode, DuckDBWriteNode, HttpFetchNode};
        use crate::nodes::boundary::{InputNode, OutputNode};
        use crate::nodes::transform::{FilterNode, ProjectNode, AggregateNode, WindowNode, ConditionNode};
        use crate::nodes::script::python::PythonNode;

        self.register_node(Arc::new(SqlNode));
        self.register_node(Arc::new(CsvReadNode));
        self.register_node(Arc::new(ParquetReadNode));
        self.register_node(Arc::new(ExcelReadNode));
        self.register_node(Arc::new(ExcelWriteNode));
        self.register_node(Arc::new(DuckDBReadNode));
        self.register_node(Arc::new(DuckDBWriteNode));
        self.register_node(Arc::new(HttpFetchNode));
        self.register_node(Arc::new(InputNode));
        self.register_node(Arc::new(OutputNode));
        self.register_node(Arc::new(FilterNode));
        self.register_node(Arc::new(ProjectNode));
        self.register_node(Arc::new(AggregateNode));
        self.register_node(Arc::new(WindowNode));
        self.register_node(Arc::new(ConditionNode));
        self.register_node(Arc::new(PythonNode));
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut registry = Self::new();
        registry.register_core_nodes();
        registry
    }
}
