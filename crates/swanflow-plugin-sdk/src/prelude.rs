// Re-exports for convenient `use swanflow_plugin_sdk::prelude::*;`
pub use crate::manifest::PluginManifest;
pub use crate::version::{SwanFlowVersion, VersionReq};
pub use crate::PluginNode;
pub use async_trait::async_trait;
pub use swanflow_core::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};
