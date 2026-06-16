//! Mock implementations of all kernel traits.
//!
//! Follows Nika's `nika-kernel-mock` pattern: 1:1 mock for every trait,
//! enabling full in-memory testing without external dependencies.
//!
//! All mocks use `std::sync::Mutex` for interior mutability and
//! `std::collections::HashMap` for storage.

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::core::error::{CoreResult, SwanFlowError};
use crate::core::kernel::ai::*;
use crate::core::kernel::core::*;
use crate::core::kernel::runtime::*;

// ---------------------------------------------------------------------------
// MockDb — DbRead + DbWrite
// ---------------------------------------------------------------------------

/// In-memory mock for DbRead + DbWrite.
pub struct MockDb {
    pub tables: HashMap<String, Vec<String>>,
    pub executed: Mutex<Vec<String>>,
}

impl MockDb {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            executed: Mutex::new(Vec::new()),
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
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        use datafusion::physical_plan::stream::RecordBatchReceiverStream;
        let mut builder = RecordBatchReceiverStream::builder(schema, 4);
        drop(builder.tx());
        Ok(builder.build())
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }
}

#[async_trait]
impl DbWrite for MockDb {
    async fn execute(&self, sql: &str) -> CoreResult<u64> {
        self.executed.lock().unwrap().push(sql.to_string());
        Ok(1)
    }
}

// ---------------------------------------------------------------------------
// MockFs — FsRead
// ---------------------------------------------------------------------------

/// In-memory mock for FsRead.
pub struct MockFs {
    pub files: HashMap<String, String>,
}

impl MockFs {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
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
        Ok(self
            .files
            .keys()
            .filter(|k| k.starts_with(dir))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockHttp — HttpFetch
// ---------------------------------------------------------------------------

/// In-memory mock for HttpFetch.
pub struct MockHttp {
    pub responses: HashMap<String, String>,
}

impl MockHttp {
    pub fn new() -> Self {
        Self {
            responses: HashMap::new(),
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
// MockKeyStore — KeyStore
// ---------------------------------------------------------------------------

/// In-memory mock for KeyStore.
pub struct MockKeyStore {
    store: Mutex<HashMap<String, String>>,
}

impl MockKeyStore {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_entry(mut self, key: &str, value: &str) -> Self {
        self.store.lock().unwrap().insert(key.to_string(), value.to_string());
        self
    }
}

#[async_trait]
impl KeyStore for MockKeyStore {
    async fn get(&self, key: &str) -> CoreResult<Option<String>> {
        Ok(self.store.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> CoreResult<()> {
        self.store
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn delete(&self, key: &str) -> CoreResult<()> {
        self.store.lock().unwrap().remove(key);
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> CoreResult<Vec<String>> {
        Ok(self
            .store
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }
}

// ---------------------------------------------------------------------------
// MockClock — Clock
// ---------------------------------------------------------------------------

/// Deterministic mock for Clock — time only advances when told to.
pub struct MockClock {
    now: Mutex<f64>,
}

impl MockClock {
    pub fn new() -> Self {
        Self {
            now: Mutex::new(0.0),
        }
    }

    /// Advance the clock by `secs` seconds.
    pub fn advance(&self, secs: f64) {
        let mut now = self.now.lock().unwrap();
        *now += secs;
    }
}

#[async_trait]
impl Clock for MockClock {
    fn now_unix_secs(&self) -> f64 {
        *self.now.lock().unwrap()
    }

    async fn sleep(&self, secs: f64) {
        self.advance(secs);
    }
}

/// Real system clock.
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now_unix_secs(&self) -> f64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    }

    async fn sleep(&self, secs: f64) {
        tokio::time::sleep(tokio::time::Duration::from_secs_f64(secs)).await;
    }
}

// ---------------------------------------------------------------------------
// MockBlobStore — BlobStore
// ---------------------------------------------------------------------------

/// In-memory mock for BlobStore.
pub struct MockBlobStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl MockBlobStore {
    pub fn new() -> Self {
        Self {
            blobs: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_blob(mut self, key: &str, data: Vec<u8>) -> Self {
        self.blobs.lock().unwrap().insert(key.to_string(), data);
        self
    }
}

#[async_trait]
impl BlobStore for MockBlobStore {
    async fn put(&self, key: &str, data: Vec<u8>) -> CoreResult<()> {
        self.blobs.lock().unwrap().insert(key.to_string(), data);
        Ok(())
    }

    async fn get(&self, key: &str) -> CoreResult<Option<Vec<u8>>> {
        Ok(self.blobs.lock().unwrap().get(key).cloned())
    }

    async fn delete(&self, key: &str) -> CoreResult<()> {
        self.blobs.lock().unwrap().remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        self.blobs.lock().unwrap().contains_key(key)
    }
}

// ---------------------------------------------------------------------------
// MockShellExec — ShellExec
// ---------------------------------------------------------------------------

/// Mock for ShellExec — returns pre-configured outputs.
pub struct MockShellExec {
    results: Mutex<HashMap<String, (CoreResult<ShellOutput>,)>>,
}

impl MockShellExec {
    pub fn new() -> Self {
        Self {
            results: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_result(self, command: &str, output: ShellOutput) -> Self {
        self.results
            .lock()
            .unwrap()
            .insert(command.to_string(), (Ok(output),));
        self
    }

    pub fn with_error(self, command: &str, error: SwanFlowError) -> Self {
        self.results
            .lock()
            .unwrap()
            .insert(command.to_string(), (Err(error),));
        self
    }
}

#[async_trait]
impl ShellExec for MockShellExec {
    async fn run(&self, command: &str, _args: &[String]) -> CoreResult<ShellOutput> {
        let results = self.results.lock().unwrap();
        match results.get(command) {
            Some((Ok(out),)) => Ok(ShellOutput {
                exit_code: out.exit_code,
                stdout: out.stdout.clone(),
                stderr: out.stderr.clone(),
            }),
            Some((Err(e),)) => {
                Err(SwanFlowError::new(e.category, e.code, &e.message))
            }
            None => Ok(ShellOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// MockCancellationToken — CancellationToken
// ---------------------------------------------------------------------------

/// Mock for CancellationToken — default: not cancelled until told so.
pub struct MockCancellationToken {
    cancelled: Mutex<bool>,
}

impl MockCancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Mutex::new(false),
        }
    }
}

#[async_trait]
impl CancellationToken for MockCancellationToken {
    fn is_cancelled(&self) -> bool {
        *self.cancelled.lock().unwrap()
    }

    async fn wait_cancelled(&self) {
        // Spin-wait until cancelled (fine for tests).
        while !self.is_cancelled() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    fn cancel(&self) {
        *self.cancelled.lock().unwrap() = true;
    }
}

// ---------------------------------------------------------------------------
// MockTimeout — Timeout
// ---------------------------------------------------------------------------

/// Mock for Timeout — never times out.
pub struct MockTimeout;

#[async_trait]
impl Timeout for MockTimeout {
    async fn with_timeout<F, T>(&self, _deadline_secs: f64, future: F) -> CoreResult<T>
    where
        F: std::future::Future<Output = CoreResult<T>> + Send,
    {
        future.await
    }

    fn remaining_secs(&self) -> f64 {
        60.0
    }
}

// ---------------------------------------------------------------------------
// MockProgress — Progress
// ---------------------------------------------------------------------------

/// Mock for Progress — records all reports in memory.
pub struct MockProgress {
    reports: Mutex<Vec<(f64, String)>>,
}

impl MockProgress {
    pub fn new() -> Self {
        Self {
            reports: Mutex::new(Vec::new()),
        }
    }

    pub fn reports(&self) -> Vec<(f64, String)> {
        self.reports.lock().unwrap().clone()
    }
}

#[async_trait]
impl Progress for MockProgress {
    async fn report(&self, fraction: f64, message: Option<&str>) {
        self.reports
            .lock()
            .unwrap()
            .push((fraction, message.unwrap_or_default().to_string()));
    }

    fn last_reported(&self) -> Option<(f64, String)> {
        self.reports.lock().unwrap().last().cloned()
    }

    async fn complete(&self, message: Option<&str>) {
        self.reports
            .lock()
            .unwrap()
            .push((1.0, message.unwrap_or("complete").to_string()));
    }
}

// ---------------------------------------------------------------------------
// MockLLM — LLMInfer
// ---------------------------------------------------------------------------

/// Mock for LLMInfer — returns pre-configured responses.
pub struct MockLLM {
    responses: Mutex<Vec<String>>,
    index: Mutex<usize>,
}

impl MockLLM {
    pub fn new() -> Self {
        Self {
            responses: Mutex::new(Vec::new()),
            index: Mutex::new(0),
        }
    }

    pub fn with_response(self, content: &str) -> Self {
        self.responses.lock().unwrap().push(content.to_string());
        self
    }
}

#[async_trait]
impl LLMInfer for MockLLM {
    async fn complete(&self, _request: LlmRequest) -> CoreResult<LlmResponse> {
        let mut idx = self.index.lock().unwrap();
        let responses = self.responses.lock().unwrap();
        let content = responses.get(*idx).cloned().unwrap_or_default();
        *idx += 1;
        Ok(LlmResponse {
            content,
            structured: None,
            tool_calls: Vec::new(),
            usage: None,
        })
    }
}

// ---------------------------------------------------------------------------
// MockEmbedding — Embedding
// ---------------------------------------------------------------------------

/// Mock for Embedding — returns deterministic vectors based on text hash.
pub struct MockEmbedding {
    dim: usize,
}

impl MockEmbedding {
    pub fn new(dim: usize) -> Self {
        Self { dim }
    }
}

#[async_trait]
impl Embedding for MockEmbedding {
    async fn embed(&self, text: &str) -> CoreResult<Vec<f32>> {
        // Deterministic: use the text's byte values to generate a vector.
        let mut vec = vec![0.0f32; self.dim];
        for (i, b) in text.bytes().enumerate() {
            vec[i % self.dim] += b as f32 / 255.0;
        }
        Ok(vec)
    }

    async fn embed_batch(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>> {
        let mut results = Vec::with_capacity(texts.len());
        for t in texts {
            results.push(self.embed(t).await?);
        }
        Ok(results)
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── DbRead / DbWrite ──

    #[tokio::test]
    async fn test_mock_db_list_tables() {
        let db = MockDb::new().with_table("users", vec!["alice".into(), "bob".into()]);
        let tables = db.list_tables().await.unwrap();
        assert_eq!(tables, vec!["users"]);
    }

    #[tokio::test]
    async fn test_mock_db_execute_logs() {
        let db = MockDb::new();
        db.execute("DELETE FROM users").await.unwrap();
        let log = db.executed.lock().unwrap();
        assert_eq!(log[0], "DELETE FROM users");
    }

    // ── FsRead ──

    #[tokio::test]
    async fn test_mock_fs_read() {
        let fs = MockFs::new().with_file("/tmp/hello.txt", "hello world");
        assert!(fs.exists("/tmp/hello.txt").await);
        let content = fs.read_to_string("/tmp/hello.txt").await.unwrap();
        assert_eq!(content, "hello world");
        assert!(fs.read_to_string("/tmp/missing.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_mock_fs_ls() {
        let fs = MockFs::new()
            .with_file("/data/a.csv", "a")
            .with_file("/data/b.csv", "b")
            .with_file("/tmp/x.txt", "x");
        let files = fs.ls("/data", "*").await.unwrap();
        assert_eq!(files.len(), 2);
    }

    // ── HttpFetch ──

    #[tokio::test]
    async fn test_mock_http_get() {
        let http = MockHttp::new().with_response("https://example.com", r#"{"ok":true}"#);
        let body = http.get("https://example.com").await.unwrap();
        assert_eq!(body, r#"{"ok":true}"#);
        assert!(http.get("https://missing.com").await.is_err());
    }

    #[tokio::test]
    async fn test_mock_http_post_json() {
        let http = MockHttp::new().with_response("https://api.example.com/submit", "done");
        let body = http.post_json("https://api.example.com/submit", r#"{"x":1}"#).await.unwrap();
        assert_eq!(body, "done");
    }

    // ── KeyStore ──

    #[tokio::test]
    async fn test_mock_keystore() {
        let ks = MockKeyStore::new().with_entry("db_url", "postgres://localhost");
        assert_eq!(ks.get("db_url").await.unwrap(), Some("postgres://localhost".into()));
        assert_eq!(ks.get("missing").await.unwrap(), None);
        ks.set("api_key", "sk-abc123").await.unwrap();
        assert_eq!(ks.get("api_key").await.unwrap(), Some("sk-abc123".into()));
        ks.delete("api_key").await.unwrap();
        assert_eq!(ks.get("api_key").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_mock_keystore_list_prefix() {
        let ks = MockKeyStore::new()
            .with_entry("db.pg", "val1")
            .with_entry("db.mysql", "val2")
            .with_entry("cache.redis", "val3");
        let keys = ks.list_keys("db.").await.unwrap();
        assert_eq!(keys.len(), 2);
    }

    // ── Clock ──

    #[tokio::test]
    async fn test_mock_clock_advance() {
        let clock = MockClock::new();
        assert_eq!(clock.now_unix_secs(), 0.0);
        clock.advance(5.0);
        assert_eq!(clock.now_unix_secs(), 5.0);
    }

    #[tokio::test]
    async fn test_mock_clock_sleep() {
        let clock = MockClock::new();
        clock.sleep(3.0).await;
        assert_eq!(clock.now_unix_secs(), 3.0);
    }

    #[tokio::test]
    async fn test_mock_clock_elapsed() {
        let clock = MockClock::new();
        let start = clock.now_unix_secs();
        clock.advance(10.0);
        assert!((clock.elapsed_secs(start) - 10.0).abs() < 0.001);
    }

    // ── BlobStore ──

    #[tokio::test]
    async fn test_mock_blob_store() {
        let store = MockBlobStore::new();
        store.put("dataset.parquet", vec![0, 1, 2, 3]).await.unwrap();
        assert!(store.exists("dataset.parquet").await);
        let data = store.get("dataset.parquet").await.unwrap().unwrap();
        assert_eq!(data, vec![0, 1, 2, 3]);
        store.delete("dataset.parquet").await.unwrap();
        assert!(!store.exists("dataset.parquet").await);
    }

    // ── ShellExec ──

    #[tokio::test]
    async fn test_mock_shell_exec() {
        let sh = MockShellExec::new().with_result("echo", ShellOutput {
            exit_code: 0,
            stdout: "hello".into(),
            stderr: String::new(),
        });
        let out = sh.run("echo", &[]).await.unwrap();
        assert_eq!(out.stdout, "hello");
        assert_eq!(out.exit_code, 0);
    }

    #[tokio::test]
    async fn test_mock_shell_exec_error() {
        let err = SwanFlowError::io_error("command not found");
        let sh = MockShellExec::new().with_error("bad-cmd", err);
        assert!(sh.run("bad-cmd", &[]).await.is_err());
    }

    #[tokio::test]
    async fn test_mock_shell_exec_default() {
        let sh = MockShellExec::new();
        let out = sh.run("unknown", &[]).await.unwrap();
        assert_eq!(out.exit_code, 0);
        assert_eq!(out.stdout, "");
    }

    // ── CancellationToken ──

    #[tokio::test]
    async fn test_mock_cancellation() {
        let ct = MockCancellationToken::new();
        assert!(!ct.is_cancelled());
        ct.cancel();
        assert!(ct.is_cancelled());
    }

    #[tokio::test]
    async fn test_mock_cancellation_wait_resolves() {
        let ct = std::sync::Arc::new(MockCancellationToken::new());
        let ct_clone = ct.clone();
        let handle = tokio::spawn(async move {
            ct_clone.wait_cancelled().await;
            true
        });
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        ct.cancel();
        assert!(handle.await.unwrap());
    }

    // ── Progress ──

    #[tokio::test]
    async fn test_mock_progress() {
        let p = MockProgress::new();
        p.report(0.3, Some("loading")).await;
        p.report(0.6, Some("processing")).await;
        p.complete(None).await;
        let reports = p.reports();
        assert_eq!(reports.len(), 3);
        assert_eq!(reports[0].1, "loading");
        assert_eq!(reports[2].0, 1.0);
    }

    #[tokio::test]
    async fn test_mock_progress_last() {
        let p = MockProgress::new();
        assert!(p.last_reported().is_none());
        p.report(0.5, Some("mid")).await;
        let last = p.last_reported().unwrap();
        assert_eq!(last.0, 0.5);
    }

    // ── LLMInfer ──

    #[tokio::test]
    async fn test_mock_llm() {
        let llm = MockLLM::new().with_response("Hello, world!");
        let req = LlmRequest {
            model: "test".into(),
            system: None,
            messages: vec![],
            response_schema: None,
            temperature: None,
            max_tokens: None,
            tools: vec![],
        };
        let resp = llm.complete(req).await.unwrap();
        assert_eq!(resp.content, "Hello, world!");
    }

    #[tokio::test]
    async fn test_mock_llm_returns_default_on_empty() {
        let llm = MockLLM::new();
        let req = LlmRequest {
            model: "test".into(),
            system: None,
            messages: vec![],
            response_schema: None,
            temperature: None,
            max_tokens: None,
            tools: vec![],
        };
        let resp = llm.complete(req).await.unwrap();
        assert_eq!(resp.content, "");
    }

    // ── Embedding ──

    #[tokio::test]
    async fn test_mock_embedding_dim() {
        let emb = MockEmbedding::new(128);
        assert_eq!(emb.dimension(), 128);
    }

    #[tokio::test]
    async fn test_mock_embedding_deterministic() {
        let emb = MockEmbedding::new(4);
        let v1 = emb.embed("hello").await.unwrap();
        let v2 = emb.embed("hello").await.unwrap();
        assert_eq!(v1, v2);
    }

    #[tokio::test]
    async fn test_mock_embedding_batch() {
        let emb = MockEmbedding::new(8);
        let results = emb.embed_batch(&["a", "bb", "ccc"]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].len(), 8);
    }

    // ── Timeout ──

    #[tokio::test]
    async fn test_mock_timeout_passes_through() {
        let t = MockTimeout;
        let result = t.with_timeout(10.0, async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_mock_timeout_propagates_error() {
        let t = MockTimeout;
        let err = SwanFlowError::unknown("boom");
        let result: CoreResult<i32> = t.with_timeout(10.0, async { Err(err) }).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_mock_timeout_remaining() {
        let t = MockTimeout;
        assert!((t.remaining_secs() - 60.0).abs() < 0.001);
    }
}
