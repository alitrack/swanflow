//! Core I/O kernel traits — DbRead, DbWrite, FsRead, HttpFetch, KeyStore, Clock, BlobStore, ShellExec.
//!
//! Originally 4 traits from Nika inspiration; expanded to 8 for broader effect abstraction.
//! All traits are `Send + Sync` and use `CoreResult` for error propagation.

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::core::error::CoreResult;

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

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

/// Structured logging abstraction — log with levels and structured fields.
///
/// All kernel-level operations log through this trait rather than
/// printing to stdout/stderr directly, enabling testable log assertions.
#[async_trait]
pub trait Logger: Send + Sync {
    /// Log a message at the given level with optional structured fields.
    async fn log(&self, level: LogLevel, message: &str, fields: Vec<(String, String)>);

    /// Convenience: log at DEBUG level.
    async fn debug(&self, message: &str) {
        self.log(LogLevel::Debug, message, vec![]).await;
    }

    /// Convenience: log at INFO level.
    async fn info(&self, message: &str) {
        self.log(LogLevel::Info, message, vec![]).await;
    }

    /// Convenience: log at WARN level.
    async fn warn(&self, message: &str) {
        self.log(LogLevel::Warn, message, vec![]).await;
    }

    /// Convenience: log at ERROR level.
    async fn error(&self, message: &str) {
        self.log(LogLevel::Error, message, vec![]).await;
    }
}

/// Standard log severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

// ---------------------------------------------------------------------------
// Notification
// ---------------------------------------------------------------------------

/// Send notifications — alerts, webhooks, messages.
///
/// Used by DAG engine to notify users of workflow completion, failure,
/// or when a node requires human-in-the-loop approval.
#[async_trait]
pub trait Notification: Send + Sync {
    /// Send a notification to a channel (e.g., "stdout", "slack:#alerts", "webhook:url").
    async fn send(&self, channel: &str, title: &str, body: &str) -> CoreResult<()>;

    /// Send an alert (high-priority notification that demands attention).
    async fn alert(&self, title: &str, body: &str) -> CoreResult<()>;
}

// ---------------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------------

/// TTL-based key-value cache — get, set, invalidate.
///
/// Separate from `KeyStore` which is for persistent secrets/config.
/// Cache is ephemeral, auto-evicts entries older than TTL.
#[async_trait]
pub trait Cache: Send + Sync {
    /// Get a cached value by key.
    async fn get(&self, key: &str) -> CoreResult<Option<String>>;

    /// Set a value with a TTL in seconds.
    async fn set(&self, key: &str, value: &str, ttl_secs: u64) -> CoreResult<()>;

    /// Invalidate (delete) a cache entry.
    async fn invalidate(&self, key: &str) -> CoreResult<()>;

    /// Check if a key exists and is not expired.
    async fn exists(&self, key: &str) -> bool;
}

// ---------------------------------------------------------------------------
// Telemetry
// ---------------------------------------------------------------------------

/// Metrics and observability — counters, histograms, gauges.
///
/// Production impls wrap OpenTelemetry or prometheus client.
/// Used by the DAG engine to instrument node execution times,
/// data throughput, and error rates.
#[async_trait]
pub trait Telemetry: Send + Sync {
    /// Increment a counter metric.
    async fn counter_inc(&self, name: &str, value: u64, labels: Vec<(String, String)>);

    /// Record a histogram observation.
    async fn histogram_observe(&self, name: &str, value: f64, labels: Vec<(String, String)>);

    /// Set a gauge value.
    async fn gauge_set(&self, name: &str, value: f64, labels: Vec<(String, String)>);
}

// ---------------------------------------------------------------------------
// EventBus
// ---------------------------------------------------------------------------

/// In-process publish/subscribe event bus for DAG runtime events.
///
/// Enables decoupled communication between DAG nodes without
/// requiring explicit data-flow connections. Used for:
/// - Completion notifications (node A done → node B can start)
/// - Error propagation (node C failed → downstream nodes notified)
/// - Progress broadcasting (pipeline state updates)
#[async_trait]
pub trait EventBus: Send + Sync {
    /// Publish an event to a topic.
    async fn publish(&self, topic: &str, payload: &str) -> CoreResult<()>;

    /// Subscribe to a topic with a callback. Returns a subscription ID.
    async fn subscribe<F>(&self, topic: &str, callback: F) -> String
    where
        F: Fn(&str, &str) + Send + 'static;

    /// Unsubscribe a subscription by ID.
    async fn unsubscribe(&self, subscription_id: &str) -> CoreResult<()>;
}

// ---------------------------------------------------------------------------
// SchemaRegistry
// ---------------------------------------------------------------------------

/// Data schema registry — register and look up column schemas and entity types.
///
/// SwanFlow-specific trait (not in Nika's kernel). Bridges DAG nodes
/// and the ontology layer by providing a centralized registry of
/// known column names, types, and their business entity associations.
#[async_trait]
pub trait SchemaRegistry: Send + Sync {
    /// Register a schema for a table-like entity.
    async fn register_schema(&self, name: &str, columns: Vec<SchemaColumn>) -> CoreResult<()>;

    /// Look up a schema by name.
    async fn get_schema(&self, name: &str) -> CoreResult<Option<Vec<SchemaColumn>>>;

    /// Find schemas that contain a column with the given name.
    async fn find_by_column(&self, column_name: &str) -> CoreResult<Vec<String>>;
}

/// A column descriptor in a schema.
#[derive(Debug, Clone)]
pub struct SchemaColumn {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub entity_type: Option<String>,
}

// ---------------------------------------------------------------------------
// AuthProvider
// ---------------------------------------------------------------------------

/// Authentication/authorization — verify identity and permissions.
///
/// SwanFlow-specific trait. Used when accessing external data sources
/// (DB connections, API endpoints) or enforcing node-level access control.
#[async_trait]
pub trait AuthProvider: Send + Sync {
    /// Authenticate a request with credentials. Returns a token on success.
    async fn authenticate(&self, credentials: AuthCredentials) -> CoreResult<AuthToken>;

    /// Check if a token has the required permission.
    async fn authorize(&self, token: &AuthToken, permission: &str) -> CoreResult<bool>;

    /// Validate a token (check expiry, signature).
    async fn validate(&self, token: &AuthToken) -> CoreResult<bool>;
}

/// Credentials for authentication.
#[derive(Debug, Clone)]
pub struct AuthCredentials {
    pub username: Option<String>,
    pub password: Option<String>,
    pub api_key: Option<String>,
}

/// An authentication token.
#[derive(Debug, Clone)]
pub struct AuthToken {
    pub value: String,
    pub expires_at: Option<f64>,
    pub permissions: Vec<String>,
}
