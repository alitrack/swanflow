//! SwanFlow Plugin SDK
//!
//! Build custom node types and register them into the SwanFlow engine.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use swanflow_plugin_sdk::*;
//! use swanflow_core::{NodeBehavior, NodeSpec, PortSpec};
//!
//! struct MyPlugin;
//!
//! impl Plugin for MyPlugin {
//!     fn name(&self) -> &str { "my-plugin" }
//!     fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }
//!
//!     fn node_types(&self) -> Vec<Box<dyn NodeBehavior>> {
//!         vec![Box::new(MyCustomNode::default())]
//!     }
//! }
//! ```
//!
//! Then register in your SwanFlow config or at engine startup:
//!
//! ```rust,ignore
//! registry.load_plugin(Box::new(MyPlugin));
//! ```

pub use swanflow_core::{
    NodeBehavior, NodeCapability, NodeCategory, NodeSpec, PortSpec,
    TypedNodeBehavior, DesignerMetadata, InferredMetadata,
};

/// A SwanFlow plugin that provides custom node types.
///
/// Implements Herdr-inspired plugin pattern: each plugin is a self-contained
/// bundle of node types that can be loaded into the engine at runtime.
pub trait Plugin: Send + Sync {
    /// Unique plugin identifier (e.g., "my-etl-nodes").
    fn name(&self) -> &str;

    /// Semantic version string.
    fn version(&self) -> &str;

    /// Human-readable description.
    fn description(&self) -> &str {
        ""
    }

    /// Node types provided by this plugin. Each `NodeBehavior`
    /// will be registered in the engine's `NodeRegistry`.
    fn node_types(&self) -> Vec<Box<dyn NodeBehavior>> {
        vec![]
    }
}

/// Metadata about a loaded plugin.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub node_count: usize,
}
