use crate::nodes::NodeState;
use anyhow::{anyhow, Result};
use petgraph::stable_graph::{NodeIndex, StableGraph};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use uuid::Uuid;

/// An instance of a node in a workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInstance {
    pub id: Uuid,
    pub type_id: String,
    pub config: JsonValue,
    pub state: NodeState,
    /// Whether this node's output should be materialized/cached.
    #[serde(default)]
    pub materialize: bool,
    /// For MetaNodes, this holds the internal subgraph.
    pub sub_workflow: Option<Box<Workflow>>,
}

/// A connection between two node ports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeInstance {
    pub source_port: u32,
    pub target_port: u32,
}

/// A directed acyclic graph representing a data processing workflow.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Workflow {
    pub id: Uuid,
    pub name: String,
    /// Global context/variables available to all nodes via templating.
    #[serde(default)]
    pub context: HashMap<String, JsonValue>,
    /// The logical graph using petgraph's StableGraph.
    pub graph: StableGraph<NodeInstance, EdgeInstance>,
}

impl Workflow {
    /// Create a new, empty workflow with a random ID.
    pub fn new(name: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: name.to_string(),
            context: HashMap::new(),
            graph: StableGraph::new(),
        }
    }

    /// Add a node instance to the workflow.
    pub fn add_node(&mut self, node: NodeInstance) -> NodeIndex {
        self.graph.add_node(node)
    }

    /// Remove a node instance and all its connected edges.
    pub fn remove_node(&mut self, node_idx: NodeIndex) -> Option<NodeInstance> {
        self.graph.remove_node(node_idx)
    }

    /// Add an edge between two node instances.
    pub fn add_edge(
        &mut self,
        source: NodeIndex,
        target: NodeIndex,
        edge: EdgeInstance,
    ) -> Result<()> {
        let mut test_graph = self.graph.clone();
        test_graph.add_edge(source, target, edge.clone());
        if petgraph::algo::is_cyclic_directed(&test_graph) {
            return Err(anyhow!("Adding this edge would create a cycle"));
        }
        self.graph.add_edge(source, target, edge);
        Ok(())
    }

    /// Retrieve a node instance by its index.
    pub fn get_node(&self, node_idx: NodeIndex) -> Option<&NodeInstance> {
        self.graph.node_weight(node_idx)
    }

    /// Update a node's configuration.
    pub fn update_config(&mut self, node_idx: NodeIndex, config: JsonValue) -> Result<()> {
        if let Some(node) = self.graph.node_weight_mut(node_idx) {
            node.config = config;
            node.state = NodeState::Unconfigured;
            Ok(())
        } else {
            Err(anyhow!("Node index not found"))
        }
    }
}
