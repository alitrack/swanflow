//! Plugin lifecycle kernel traits — PluginHost, WasmExec.
//!
//! Follows Nika's kernel-plugin pattern (PluginHost, WASM).
//! SwanFlow plugins are WASM-based or native .so/.dylib shared libraries
//! that extend the DAG engine with custom nodes, data sources, or transforms.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::CoreResult;

// ---------------------------------------------------------------------------
// PluginHost
// ---------------------------------------------------------------------------

/// Plugin lifecycle management — load, init, run, cleanup.
///
/// A plugin is a self-contained unit of extension code (WASM or native).
/// The host manages scanning, loading, initialization, and teardown.
#[async_trait]
pub trait PluginHost: Send + Sync {
    /// Scan a directory for available plugins. Returns plugin descriptors.
    async fn scan(&self, dir: &str) -> CoreResult<Vec<PluginDescriptor>>;

    /// Load a plugin by name. Returns a handle for execution.
    async fn load(&self, name: &str) -> CoreResult<PluginHandle>;

    /// Initialize a loaded plugin with optional config.
    async fn init(&self, handle: &PluginHandle, config: HashMap<String, String>) -> CoreResult<()>;

    /// Run a plugin function by name.
    async fn run(&self, handle: &PluginHandle, function: &str, args: &[u8]) -> CoreResult<Vec<u8>>;

    /// Unload a plugin and free resources.
    async fn unload(&self, handle: &PluginHandle) -> CoreResult<()>;

    /// List all currently loaded plugins.
    fn loaded(&self) -> Vec<String>;
}

/// Metadata describing a discovered plugin.
#[derive(Debug, Clone)]
pub struct PluginDescriptor {
    pub name: String,
    pub version: String,
    pub plugin_type: PluginType,
    pub description: String,
    pub functions: Vec<String>,
}

/// The runtime type of a plugin.
#[derive(Debug, Clone, PartialEq)]
pub enum PluginType {
    Wasm,
    Native,
}

/// An opaque handle to a loaded plugin instance.
#[derive(Debug, Clone)]
pub struct PluginHandle {
    pub name: String,
    pub plugin_type: PluginType,
}

// ---------------------------------------------------------------------------
// WasmExec
// ---------------------------------------------------------------------------

/// WASM bytecode execution — compile and run WASM modules.
///
/// Production impls wrap wasmtime or wasmer.
/// Used for user-defined functions (UDFs) and plugin sandbox execution.
#[async_trait]
pub trait WasmExec: Send + Sync {
    /// Compile WASM bytecode into a module.
    async fn compile(&self, bytes: &[u8]) -> CoreResult<WasmModule>;

    /// Execute a function export from a compiled module.
    async fn execute(
        &self,
        module: &WasmModule,
        function: &str,
        args: &[u8],
    ) -> CoreResult<Vec<u8>>;

    /// Check if a module has a specific function export.
    fn has_function(&self, module: &WasmModule, function: &str) -> bool;
}

/// A compiled WASM module reference.
#[derive(Debug, Clone)]
pub struct WasmModule {
    pub name: String,
    pub exports: Vec<String>,
}
