//! Core I/O kernel traits — DbRead, DbWrite, FsRead, HttpFetch, KeyStore, Clock, BlobStore, ShellExec.
//!
//! Originally 4 traits from Nika inspiration; expanded to 8 for broader effect abstraction.
//! All traits are `Send + Sync` and use `CoreResult` for error propagation.

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::error::{CoreResult, SwanFlowError};

// ---------------------------------------------------------------------------
// DbRead / DbWrite
// ---------------------------------------------------------------------------

/// Read-only database access — query, list tables, get schema.
#[async_trait]
pub trait DbRead: Send + Sync {
    /// Execute a SQL query, returning an Arrow record batch stream.
    async fn query(&self, sql: &str) -> CoreResult<SendableRecordBatchStream>;

    /// List available tables / relations.
    async fn list_tables(&self) -> CoreResult<Vec<String>>;
}

/// Write-only database access — execute DML / DDL.
#[async_trait]
pub trait DbWrite: Send + Sync {
    /// Execute a SQL statement and return affected row count.
    async fn execute(&self, sql: &str) -> CoreResult<u64>;
}

// ---------------------------------------------------------------------------
// FsRead
// ---------------------------------------------------------------------------

/// Read-only filesystem access.
#[async_trait]
pub trait FsRead: Send + Sync {
    /// Read an entire file into a string.
    async fn read_to_string(&self, path: &str) -> CoreResult<String>;

    /// Check if a path exists.
    async fn exists(&self, path: &str) -> bool;

    /// List files in a directory matching a glob pattern.
    async fn ls(&self, dir: &str, pattern: &str) -> CoreResult<Vec<String>>;
}

// ---------------------------------------------------------------------------
// HttpFetch
// ---------------------------------------------------------------------------

/// HTTP fetch — get and post.
#[async_trait]
pub trait HttpFetch: Send + Sync {
    /// GET a URL, return the response body as text.
    async fn get(&self, url: &str) -> CoreResult<String>;

    /// POST JSON to a URL, return the response body as text.
    async fn post_json(&self, url: &str, body: &str) -> CoreResult<String>;
}

// ---------------------------------------------------------------------------
// KeyStore
// ---------------------------------------------------------------------------

/// Key-value storage abstraction for secrets, config, env vars.
///
/// Follows Nika's pattern of abstracting all ambient state behind traits.
/// Production impl: env-var backed, encrypted file, or vault integration.
#[async_trait]
pub trait KeyStore: Send + Sync {
    /// Get a value by key.
    async fn get(&self, key: &str) -> CoreResult<Option<String>>;

    /// Set a value by key.
    async fn set(&self, key: &str, value: &str) -> CoreResult<()>;

    /// Delete a key.
    async fn delete(&self, key: &str) -> CoreResult<()>;

    /// List all keys matching an optional prefix.
    async fn list_keys(&self, prefix: &str) -> CoreResult<Vec<String>>;
}

// ---------------------------------------------------------------------------
// Clock
// ---------------------------------------------------------------------------

/// Time abstraction — now, sleep, elapsed.
///
/// Wraps std::time / tokio::time behind a trait so tests can use a
/// deterministic mock clock.
#[async_trait]
pub trait Clock: Send + Sync {
    /// Current wall-clock time as Unix epoch seconds (with sub-second precision).
    fn now_unix_secs(&self) -> f64;

    /// Sleep for the given duration in seconds.
    async fn sleep(&self, secs: f64);

    /// Measure elapsed time since a start timestamp (in Unix secs).
    fn elapsed_secs(&self, since: f64) -> f64 {
        self.now_unix_secs() - since
    }
}

// ---------------------------------------------------------------------------
// BlobStore
// ---------------------------------------------------------------------------

/// Binary blob storage — read, write, delete large objects.
///
/// Production impl: local filesystem, S3-compatible, or DuckDB's native blob support.
/// Used by data nodes that need to materialize intermediate results.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Write bytes to a blob identified by key.
    async fn put(&self, key: &str, data: Vec<u8>) -> CoreResult<()>;

    /// Read bytes from a blob identified by key.
    async fn get(&self, key: &str) -> CoreResult<Option<Vec<u8>>>;

    /// Delete a blob.
    async fn delete(&self, key: &str) -> CoreResult<()>;

    /// Check if a blob exists.
    async fn exists(&self, key: &str) -> bool;
}

// ---------------------------------------------------------------------------
// ShellExec
// ---------------------------------------------------------------------------

/// Shell command execution — run a command, capture output.
///
/// All commands run in a controlled environment. The `capture` flag controls
/// whether stdout/stderr are returned (true = read-only, false = fire-and-forget).
#[async_trait]
pub trait ShellExec: Send + Sync {
    /// Run a command and return its exit status + captured output.
    async fn run(&self, command: &str, args: &[String]) -> CoreResult<ShellOutput>;
}

/// Result of a shell command execution.
#[derive(Debug, Clone)]
pub struct ShellOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

// ---------------------------------------------------------------------------
// Process (lightweight)
// ---------------------------------------------------------------------------

/// Process lifecycle abstraction — list, signal, poll.
#[async_trait]
pub trait ProcessManager: Send + Sync {
    /// List running processes (filter by name prefix).
    async fn list_processes(&self, name_filter: &str) -> CoreResult<Vec<ProcessInfo>>;

    /// Send a signal to a process by PID (Unix signal number).
    async fn signal(&self, pid: u32, signal: i32) -> CoreResult<()>;
}

/// Information about a running process.
#[derive(Debug, Clone)]
pub struct ProcessInfo {
    pub pid: u32,
    pub name: String,
    pub command: String,
}
