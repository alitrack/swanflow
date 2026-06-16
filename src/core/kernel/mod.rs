//! Kernel traits — abstract side effects for testability.
//!
//! Inspired by Nika's `nika-kernel` (L0.5 layer with ~50 atomic traits).
//! Organized into four sub-modules following Nika's pattern:
//!
//! - `core`   — Core I/O & system traits (DbRead, FsRead, HttpFetch, KeyStore, Clock, BlobStore, ShellExec)
//! - `ai`     — AI inference & embedding traits (LLMInfer, Embedding, Tokenize)
//! - `runtime` — Runtime lifecycle traits (CancellationToken, Timeout, Progress)
//! - `mock`   — All mock implementations for in-memory testing
//!
//! Production impls wrap DuckDB / reqwest / std::fs / etc.
//! Mock impls enable full in-memory testing without external dependencies.

pub mod ai;
pub mod core;
pub mod mock;
pub mod runtime;

// Re-exports for backward compatibility — existing `use crate::core::kernel::*` still works.
pub use core::*;
pub use ai::*;
pub use runtime::*;
pub use mock::*;
