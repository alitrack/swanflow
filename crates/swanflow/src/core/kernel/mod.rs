//! Kernel traits — abstract side effects for testability.
//!
//! Organized into sub-modules:
//!
//! - `core`     — DbRead, DbWrite, FsRead, HttpFetch, KeyStore, Clock, BlobStore,
//!   ShellExec, ProcessManager, Logger, Notification, Cache,
//!   Telemetry, EventBus, SchemaRegistry, AuthProvider
//! - `ai`       — LLMInfer, Embedding, Tokenize, Moderation
//! - `runtime`  — CancellationToken, Timeout, Progress
//! - `plugin`   — PluginHost, WasmExec
//! - `mock`     — All mock implementations for in-memory testing
//!
//! Production impls wrap DuckDB / reqwest / std::fs / etc.
//! Mock impls enable full in-memory testing without external dependencies.

pub mod ai;
pub mod core;
pub mod mock;
pub mod plugin;
#[cfg(feature = "wasm")]
pub mod plugin_wasm;
pub mod runtime;

// Re-exports for backward compatibility — existing `use crate::core::kernel::*` still works.
pub use ai::*;
pub use core::*;
pub use mock::*;
pub use plugin::*;
pub use runtime::*;
