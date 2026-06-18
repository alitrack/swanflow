//! Core error types with deterministic routing.
//!
//! Inspired by Nika's `is_transient()` / `spec_code()` error model.
//! Every engine-level error carries a category and an error code,
//! enabling structured workflow-level error policies like `retry:` / `on_error:`.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Broad category for routing decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ErrorCategory {
    /// Recoverable — retrying may succeed (network blips, transient DB failures).
    Transient,
    /// Non-recoverable — same inputs will always fail (syntax error, missing column).
    Permanent,
    /// The operation is mis-specified or semantically wrong (wrong table name, bad config).
    Semantic,
}

impl ErrorCategory {
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient)
    }
}

/// Fine-grained error code for programmatic matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ErrorCode {
    /// Connection refused / timeout / reset
    ConnectionFailed,
    /// Authentication failure
    AuthenticationFailed,
    /// SQL syntax error
    SqlSyntax,
    /// Column not found
    ColumnNotFound,
    /// Table or relation not found
    TableNotFound,
    /// I/O error (file not found, read/write failure)
    IoError,
    /// Data source configuration error
    ConfigError,
    /// Serialization / deserialization error
    SerdeError,
    /// Arrow / DataFusion internal error
    DataFusionError,
    /// WASM execution error
    WasmError,
    /// Timeout
    Timeout,
    /// Content moderation
    Moderation,
    /// Plugin execution
    PluginError,
    /// Cache error
    CacheError,
    /// Notification delivery failure
    NotificationError,
    /// Authorization failure
    AuthorizationFailed,
    /// Unclassified
    Unknown,
}

impl ErrorCode {
    /// Stable string identifier for spec_code().
    pub fn spec_code(&self) -> &'static str {
        match self {
            Self::ConnectionFailed => "CONNECTION_FAILED",
            Self::AuthenticationFailed => "AUTH_FAILED",
            Self::SqlSyntax => "SQL_SYNTAX",
            Self::ColumnNotFound => "COLUMN_NOT_FOUND",
            Self::TableNotFound => "TABLE_NOT_FOUND",
            Self::IoError => "IO_ERROR",
            Self::ConfigError => "CONFIG_ERROR",
            Self::SerdeError => "SERDE_ERROR",
            Self::DataFusionError => "DATAFUSION_ERROR",
            Self::WasmError => "WASM_ERROR",
            Self::Timeout => "TIMEOUT",
            Self::Moderation => "MODERATION",
            Self::PluginError => "PLUGIN_ERROR",
            Self::CacheError => "CACHE_ERROR",
            Self::NotificationError => "NOTIFICATION_ERROR",
            Self::AuthorizationFailed => "AUTHZ_FAILED",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// The core SwanFlow error type.
///
/// Every error carries:
/// - `category` — for transient / permanent policy routing
/// - `code` — for programmatic matching
/// - `message` — human-readable description
/// - `source` — optional inner error
#[derive(Debug)]
pub struct SwanFlowError {
    pub category: ErrorCategory,
    pub code: ErrorCode,
    pub message: String,
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl SwanFlowError {
    pub fn new(category: ErrorCategory, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            category,
            code,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    pub fn is_transient(&self) -> bool {
        self.category.is_transient()
    }

    pub fn spec_code(&self) -> &'static str {
        self.code.spec_code()
    }

    // ── convenience constructors ──

    pub fn connection_failed(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Transient, ErrorCode::ConnectionFailed, msg)
    }

    pub fn sql_syntax(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permanent, ErrorCode::SqlSyntax, msg)
    }

    pub fn column_not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permanent, ErrorCode::ColumnNotFound, msg)
    }

    pub fn table_not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Semantic, ErrorCode::TableNotFound, msg)
    }

    pub fn io_error(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permanent, ErrorCode::IoError, msg)
    }

    pub fn config_error(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Semantic, ErrorCode::ConfigError, msg)
    }

    pub fn unknown(msg: impl Into<String>) -> Self {
        Self::new(ErrorCategory::Permanent, ErrorCode::Unknown, msg)
    }
}

impl fmt::Display for SwanFlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {}: {}",
            self.code.spec_code(),
            self.message,
            self.source
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_default()
        )
    }
}

impl std::error::Error for SwanFlowError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|b| &**b as &(dyn std::error::Error + 'static))
    }
}

// ── From impls for common error types ──

impl From<std::io::Error> for SwanFlowError {
    fn from(e: std::io::Error) -> Self {
        SwanFlowError::io_error(e.to_string()).with_source(e)
    }
}

impl From<serde_json::Error> for SwanFlowError {
    fn from(e: serde_json::Error) -> Self {
        SwanFlowError::new(
            ErrorCategory::Permanent,
            ErrorCode::SerdeError,
            e.to_string(),
        )
        .with_source(e)
    }
}

impl From<datafusion::arrow::error::ArrowError> for SwanFlowError {
    fn from(e: datafusion::arrow::error::ArrowError) -> Self {
        SwanFlowError::new(
            ErrorCategory::Permanent,
            ErrorCode::DataFusionError,
            e.to_string(),
        )
        .with_source(e)
    }
}

// ── helper: convenience Result alias ──

/// Convenience alias for engine-level operations.
pub type CoreResult<T> = Result<T, SwanFlowError>;
