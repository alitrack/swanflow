//! Kernel traits — abstract side effects for testability.
//!
//! Inspired by Nika's `nika-kernel` (L0.5 layer with ~50 atomic traits).
//! We start with the four we actually need: DbRead, DbWrite, FsRead, HttpFetch.
//!
//! Production impls wrap DuckDB / reqwest / std::fs.
//! Mock impls enable full in-memory testing without external dependencies.

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::core::error::{CoreResult, SwanFlowError};

// ---------------------------------------------------------------------------
// Traits
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

/// HTTP fetch — get and post.
#[async_trait]
pub trait HttpFetch: Send + Sync {
    /// GET a URL, return the response body as text.
    async fn get(&self, url: &str) -> CoreResult<String>;

    /// POST JSON to a URL, return the response body as text.
    async fn post_json(&self, url: &str, body: &str) -> CoreResult<String>;
}

// ---------------------------------------------------------------------------
// Mock implementations
// ---------------------------------------------------------------------------

/// In-memory mock for DbRead + DbWrite.
/// Stores a list of SQL statements that were "executed" and
/// returns pre-configured results on query.
pub struct MockDb {
    pub tables: std::collections::HashMap<String, Vec<String>>,
    pub executed: std::sync::Mutex<Vec<String>>,
}

impl MockDb {
    pub fn new() -> Self {
        Self {
            tables: std::collections::HashMap::new(),
            executed: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn with_table(mut self, name: &str, rows: Vec<String>) -> Self {
        self.tables.insert(name.to_string(), rows);
        self
    }
}

#[async_trait]
impl DbRead for MockDb {
    async fn query(&self, _sql: &str) -> CoreResult<SendableRecordBatchStream> {
        // Return an empty stream — callers get EOF immediately.
        let schema = std::sync::Arc::new(datafusion::arrow::datatypes::Schema::empty());
        use datafusion::physical_plan::stream::RecordBatchReceiverStream;
        let mut builder = RecordBatchReceiverStream::builder(schema, 4);
        drop(builder.tx());
        Ok(builder.build())
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }
}

/// In-memory mock for FsRead.
pub struct MockFs {
    pub files: std::collections::HashMap<String, String>,
}

impl MockFs {
    pub fn new() -> Self {
        Self {
            files: std::collections::HashMap::new(),
        }
    }

    pub fn with_file(mut self, path: &str, content: &str) -> Self {
        self.files.insert(path.to_string(), content.to_string());
        self
    }
}

#[async_trait]
impl FsRead for MockFs {
    async fn read_to_string(&self, path: &str) -> CoreResult<String> {
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| SwanFlowError::io_error(format!("file not found: {path}")))
    }

    async fn exists(&self, path: &str) -> bool {
        self.files.contains_key(path)
    }

    async fn ls(&self, dir: &str, _pattern: &str) -> CoreResult<Vec<String>> {
        let keys: Vec<String> = self
            .files
            .keys()
            .filter(|k| k.starts_with(dir))
            .cloned()
            .collect();
        Ok(keys)
    }
}

/// In-memory mock for HttpFetch.
pub struct MockHttp {
    pub responses: std::collections::HashMap<String, String>,
}

impl MockHttp {
    pub fn new() -> Self {
        Self {
            responses: std::collections::HashMap::new(),
        }
    }

    pub fn with_response(mut self, url: &str, body: &str) -> Self {
        self.responses.insert(url.to_string(), body.to_string());
        self
    }
}

#[async_trait]
impl HttpFetch for MockHttp {
    async fn get(&self, url: &str) -> CoreResult<String> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| SwanFlowError::unknown(format!("no mock response for: {url}")))
    }

    async fn post_json(&self, url: &str, _body: &str) -> CoreResult<String> {
        self.responses
            .get(url)
            .cloned()
            .ok_or_else(|| SwanFlowError::unknown(format!("no mock response for: {url}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_db_list_tables() {
        let db = MockDb::new().with_table("users", vec!["alice".into(), "bob".into()]);
        let tables = db.list_tables().await.unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[tokio::test]
    async fn test_mock_fs_read() {
        let fs = MockFs::new().with_file("/tmp/hello.txt", "hello world");
        assert!(fs.exists("/tmp/hello.txt").await);
        let content = fs.read_to_string("/tmp/hello.txt").await.unwrap();
        assert_eq!(content, "hello world");
        assert!(fs.read_to_string("/tmp/missing.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_mock_http_get() {
        let http = MockHttp::new().with_response("https://example.com", "{\"ok\":true}");
        let body = http.get("https://example.com").await.unwrap();
        assert_eq!(body, "{\"ok\":true}");
        assert!(http.get("https://missing.com").await.is_err());
    }
}
