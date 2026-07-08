use std::collections::HashMap;
use std::path::PathBuf;

/// Parsed representation of a KNIME workflow (.knwf).
#[derive(Debug, Clone)]
pub struct KnwfArchive {
    /// Top-level workflow metadata from workflow.knime
    pub workflow_meta: WorkflowMeta,
    /// All nodes in the workflow (top-level + nested metanode children)
    pub nodes: Vec<KnimeNode>,
    /// Connections between nodes
    pub edges: Vec<KnimeEdge>,
    /// Nested metanode workflows, keyed by metanode internal ID
    pub metanodes: HashMap<String, KnwfArchive>,
}

/// Metadata from workflow.knime.
#[derive(Debug, Clone, Default)]
pub struct WorkflowMeta {
    pub name: Option<String>,
    pub version: Option<String>,
    pub created_by: Option<String>,
}

/// A single KNIME node instance.
#[derive(Debug, Clone)]
pub struct KnimeNode {
    /// Internal KNIME node ID (integer string)
    pub id: String,
    /// Display name
    pub name: Option<String>,
    /// Node type identifier (e.g., "org.knime.base.node.io.filehandling.CSVReaderNodeFactory")
    pub node_type: Option<String>,
    /// Node state (IDLE, CONFIGURED, EXECUTED)
    pub state: Option<String>,
    /// Parsed settings from settings.xml → model config JSON
    pub settings: serde_json::Value,
    /// Whether this node is a metanode (contains sub-workflow)
    pub is_metanode: bool,
    /// Position on canvas
    pub x: Option<f64>,
    pub y: Option<f64>,
    /// Number of input ports
    pub num_inputs: Option<u32>,
    /// Number of output ports
    pub num_outputs: Option<u32>,
}

/// A connection (edge) between two KNIME nodes.
#[derive(Debug, Clone)]
pub struct KnimeEdge {
    pub source_id: String,
    pub source_port: u32,
    pub target_id: String,
    pub target_port: u32,
}

/// Intermediate representation of a parsed .knwf before mapping.
#[derive(Debug, Clone)]
pub struct ParsedKnwf {
    pub archive: KnwfArchive,
    pub source_path: PathBuf,
}

impl ParsedKnwf {
    pub fn new(archive: KnwfArchive, source_path: PathBuf) -> Self {
        Self {
            archive,
            source_path,
        }
    }

    /// Get all nodes flattened (top-level + metanode children).
    pub fn all_nodes(&self) -> Vec<&KnimeNode> {
        let mut nodes: Vec<&KnimeNode> = self.archive.nodes.iter().collect();
        for meta in self.archive.metanodes.values() {
            nodes.extend(meta.nodes.iter());
        }
        nodes
    }
}
