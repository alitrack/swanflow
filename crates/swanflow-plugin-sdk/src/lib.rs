//! SwanFlow Plugin SDK
//!
//! Re-exports and helpers for building SwanFlow plugins.
//! The `Plugin` trait lives in `swanflow_core::Plugin`.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use swanflow_core::{Plugin, NodeBehavior, NodeSpec, PortSpec};
//!
//! struct MyPlugin;
//!
//! impl Plugin for MyPlugin {
//!     fn name(&self) -> &str { "my-plugin" }
//!     fn version(&self) -> &str { env!("CARGO_PKG_VERSION") }
//!     fn node_types(&self) -> Vec<Box<dyn NodeBehavior>> {
//!         vec![Box::new(MyCustomNode::default())]
//!     }
//! }
//! ```

pub use swanflow_core::Plugin;

pub use swanflow_core::{
    NodeBehavior, NodeCapability, NodeCategory, NodeSpec, PortSpec,
    TypedNodeBehavior, DesignerMetadata, InferredMetadata,
};

/// Metadata about a loaded plugin.
#[derive(Debug, Clone)]
pub struct PluginInfo {
    pub name: String,
    pub version: String,
    pub description: String,
    pub node_count: usize,
}
