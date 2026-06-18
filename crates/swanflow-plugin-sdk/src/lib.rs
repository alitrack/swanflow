// swanflow-plugin-sdk — build custom SwanFlow nodes as external crates
//
// This crate provides the minimum API surface for third-party plugin
// development. Plugins depend only on `swanflow-plugin-sdk` (not the
// full `swanflow` crate), keeping compile times low and API surface
// small.
//
// ## Quick start
// ```ignore
// use swanflow_plugin_sdk::prelude::*;
//
// pub struct MyCustomNode;
//
// #[async_trait]
// impl PluginNode for MyCustomNode {
//     fn manifest() -> PluginManifest { ... }
//     fn spec(&self) -> NodeSpec { ... }
//     // ...
// }
//
// swanflow_plugin_sdk::export_plugin!(MyCustomNode);
// ```

pub mod manifest;
pub mod prelude;
pub mod version;

pub use manifest::PluginManifest;
pub use version::{check_compatibility, SwanFlowVersion, VersionReq};

use async_trait::async_trait;
use swanflow_core::nodes::{NodeBehavior, NodeSpec};

/// The extended trait for plugin nodes.
///
/// Plugin nodes implement `NodeBehavior` (from swanflow-core) plus
/// provide a static manifest describing the plugin package.
#[async_trait]
pub trait PluginNode: NodeBehavior + Send + Sync + 'static {
    /// Return the plugin manifest describing this plugin package.
    fn manifest() -> PluginManifest
    where
        Self: Sized;

    /// The minimum SwanFlow engine version required by this plugin.
    fn min_swanflow_version() -> SwanFlowVersion
    where
        Self: Sized,
    {
        SwanFlowVersion::new(0, 1, 0)
    }

    /// Create a boxed instance of this plugin node.
    fn create() -> Box<dyn PluginNode>
    where
        Self: Sized + Default,
    {
        Box::<Self>::default()
    }
}

/// Macro to export a plugin node type.
///
/// Generates the required `#[no_mangle]` entry point that SwanFlow's
/// plugin loader discovers at runtime.
///
/// ## Usage
/// ```ignore
/// swanflow_plugin_sdk::export_plugin!(MyAwesomeNode);
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($plugin_type:ty) => {
        #[no_mangle]
        pub extern "C" fn __swanflow_plugin_manifest() -> *const std::ffi::c_char {
            let manifest = <$plugin_type as $crate::PluginNode>::manifest();
            let json = serde_json::to_string(&manifest).unwrap_or_default();
            let c_str = std::ffi::CString::new(json).unwrap_or_default();
            c_str.into_raw()
        }

        #[no_mangle]
        pub extern "C" fn __swanflow_plugin_create() -> *mut dyn $crate::PluginNode {
            let plugin: Box<dyn $crate::PluginNode> = <$plugin_type as $crate::PluginNode>::create();
            Box::into_raw(plugin)
        }

        #[no_mangle]
        pub extern "C" fn __swanflow_plugin_min_version() -> u64 {
            let v = <$plugin_type as $crate::PluginNode>::min_swanflow_version();
            v.to_u64()
        }
    };
}
