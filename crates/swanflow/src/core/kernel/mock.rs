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
use crate::core::kernel::plugin::*;
use crate::core::kernel::runtime::*;

/// Lock one of the mocks' interior-mutability `Mutex`es.
///
/// See the twin of this function in `crates/swanflow-core/src/kernel/mock.rs`
/// for the full rationale; this file is a copy of that one and carries the same
/// change so the two trees do not diverge further before §17.2b unifies them.
///
/// Short version: 57 call sites all made the same panic-on-poison choice
/// invisibly. For a mock, recovering is the useful behaviour (a poisoned lock
/// means an earlier caller panicked mid-update; the failing test should report
/// its own assertion, not a second cascading panic). Production code should do
/// the opposite, which is exactly why the decision belongs in one visible place.
fn lk<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

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
        let builder = RecordBatchReceiverStream::builder(schema, 4);
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
        lk(&self.executed).push(sql.to_string());
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

    pub fn with_entry(self, key: &str, value: &str) -> Self {
        lk(&self.store).insert(key.to_string(), value.to_string());
        self
    }
}

#[async_trait]
impl KeyStore for MockKeyStore {
    async fn get(&self, key: &str) -> CoreResult<Option<String>> {
        Ok(lk(&self.store).get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> CoreResult<()> {
        lk(&self.store).insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn delete(&self, key: &str) -> CoreResult<()> {
        lk(&self.store).remove(key);
        Ok(())
    }

    async fn list_keys(&self, prefix: &str) -> CoreResult<Vec<String>> {
        Ok(lk(&self.store)
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
        let mut now = lk(&self.now);
        *now += secs;
    }
}

#[async_trait]
impl Clock for MockClock {
    fn now_unix_secs(&self) -> f64 {
        *lk(&self.now)
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

    pub fn with_blob(self, key: &str, data: Vec<u8>) -> Self {
        lk(&self.blobs).insert(key.to_string(), data);
        self
    }
}

#[async_trait]
impl BlobStore for MockBlobStore {
    async fn put(&self, key: &str, data: Vec<u8>) -> CoreResult<()> {
        lk(&self.blobs).insert(key.to_string(), data);
        Ok(())
    }

    async fn get(&self, key: &str) -> CoreResult<Option<Vec<u8>>> {
        Ok(lk(&self.blobs).get(key).cloned())
    }

    async fn delete(&self, key: &str) -> CoreResult<()> {
        lk(&self.blobs).remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        lk(&self.blobs).contains_key(key)
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
        lk(&self.results).insert(command.to_string(), (Ok(output),));
        self
    }

    pub fn with_error(self, command: &str, error: SwanFlowError) -> Self {
        lk(&self.results).insert(command.to_string(), (Err(error),));
        self
    }
}

#[async_trait]
impl ShellExec for MockShellExec {
    async fn run(&self, command: &str, _args: &[String]) -> CoreResult<ShellOutput> {
        let results = lk(&self.results);
        match results.get(command) {
            Some((Ok(out),)) => Ok(ShellOutput {
                exit_code: out.exit_code,
                stdout: out.stdout.clone(),
                stderr: out.stderr.clone(),
            }),
            Some((Err(e),)) => Err(SwanFlowError::new(e.category, e.code, &e.message)),
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
        *lk(&self.cancelled)
    }

    async fn wait_cancelled(&self) {
        // Spin-wait until cancelled (fine for tests).
        while !self.is_cancelled() {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }

    fn cancel(&self) {
        *lk(&self.cancelled) = true;
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
        lk(&self.reports).clone()
    }
}

#[async_trait]
impl Progress for MockProgress {
    async fn report(&self, fraction: f64, message: Option<&str>) {
        lk(&self.reports).push((fraction, message.unwrap_or_default().to_string()));
    }

    fn last_reported(&self) -> Option<(f64, String)> {
        lk(&self.reports).last().cloned()
    }

    async fn complete(&self, message: Option<&str>) {
        lk(&self.reports).push((1.0, message.unwrap_or("complete").to_string()));
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
        lk(&self.responses).push(content.to_string());
        self
    }
}

#[async_trait]
impl LLMInfer for MockLLM {
    async fn complete(&self, _request: LlmRequest) -> CoreResult<LlmResponse> {
        let mut idx = lk(&self.index);
        let responses = lk(&self.responses);
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
// MockModeration — Moderation
// ---------------------------------------------------------------------------

/// Mock for Moderation — flags specific categories on demand.
pub struct MockModeration {
    flagged: Mutex<Vec<String>>,
    threshold: Mutex<f64>,
}

impl MockModeration {
    pub fn new() -> Self {
        Self {
            flagged: Mutex::new(Vec::new()),
            threshold: Mutex::new(0.5),
        }
    }

    pub fn with_flag(self, category: &str) -> Self {
        lk(&self.flagged).push(category.to_string());
        self
    }

    pub fn with_safe_threshold(self, threshold: f64) -> Self {
        *lk(&self.threshold) = threshold;
        self
    }
}

#[async_trait]
impl Moderation for MockModeration {
    async fn check(&self, _text: &str) -> CoreResult<ModerationResult> {
        let flagged = lk(&self.flagged).clone();
        let is_safe = flagged.is_empty();
        Ok(ModerationResult {
            is_safe,
            flagged_categories: flagged,
            category_scores: std::collections::HashMap::new(),
        })
    }

    async fn check_batch(&self, texts: &[&str]) -> CoreResult<Vec<ModerationResult>> {
        let mut results = Vec::with_capacity(texts.len());
        for t in texts {
            results.push(self.check(t).await?);
        }
        Ok(results)
    }
}

// ---------------------------------------------------------------------------
// MockLogger — Logger
// ---------------------------------------------------------------------------

/// Mock for Logger — records all log entries in memory.
pub struct MockLogger {
    entries: Mutex<Vec<LogEntry>>,
}

/// A recorded log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: LogLevel,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl MockLogger {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }

    pub fn entries(&self) -> Vec<LogEntry> {
        lk(&self.entries).clone()
    }
}

#[async_trait]
impl Logger for MockLogger {
    async fn log(&self, level: LogLevel, message: &str, fields: Vec<(String, String)>) {
        lk(&self.entries).push(LogEntry {
            level,
            message: message.to_string(),
            fields,
        });
    }
}

// ---------------------------------------------------------------------------
// MockNotification — Notification
// ---------------------------------------------------------------------------

/// Mock for Notification — records sent messages.
pub struct MockNotification {
    sent: Mutex<Vec<(String, String, String)>>,
}

impl MockNotification {
    pub fn new() -> Self {
        Self {
            sent: Mutex::new(Vec::new()),
        }
    }

    pub fn sent(&self) -> Vec<(String, String, String)> {
        lk(&self.sent).clone()
    }
}

#[async_trait]
impl Notification for MockNotification {
    async fn send(&self, channel: &str, title: &str, body: &str) -> CoreResult<()> {
        lk(&self.sent).push((channel.to_string(), title.to_string(), body.to_string()));
        Ok(())
    }

    async fn alert(&self, title: &str, body: &str) -> CoreResult<()> {
        self.send("alert", title, body).await
    }
}

// ---------------------------------------------------------------------------
// MockCache — Cache
// ---------------------------------------------------------------------------

/// Mock for Cache — in-memory HashMap with mock TTL.
pub struct MockCache {
    store: Mutex<HashMap<String, (String, u64)>>,
}

impl MockCache {
    pub fn new() -> Self {
        Self {
            store: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Cache for MockCache {
    async fn get(&self, key: &str) -> CoreResult<Option<String>> {
        Ok(lk(&self.store).get(key).map(|(v, _)| v.clone()))
    }

    async fn set(&self, key: &str, value: &str, ttl_secs: u64) -> CoreResult<()> {
        lk(&self.store).insert(key.to_string(), (value.to_string(), ttl_secs));
        Ok(())
    }

    async fn invalidate(&self, key: &str) -> CoreResult<()> {
        lk(&self.store).remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        lk(&self.store).contains_key(key)
    }
}

// ---------------------------------------------------------------------------
// MockTelemetry — Telemetry
// ---------------------------------------------------------------------------

/// Labels attached to one metric sample.
type Labels = Vec<(String, String)>;
/// One recorded counter sample: `(name, value, labels)`.
type CounterSample = (String, u64, Labels);
/// One recorded histogram or gauge sample: `(name, value, labels)`.
type ValueSample = (String, f64, Labels);

/// Mock for Telemetry — records all metric operations.
pub struct MockTelemetry {
    counters: Mutex<Vec<CounterSample>>,
    histograms: Mutex<Vec<ValueSample>>,
    gauges: Mutex<Vec<ValueSample>>,
}

impl MockTelemetry {
    pub fn new() -> Self {
        Self {
            counters: Mutex::new(Vec::new()),
            histograms: Mutex::new(Vec::new()),
            gauges: Mutex::new(Vec::new()),
        }
    }

    pub fn counters(&self) -> Vec<CounterSample> {
        lk(&self.counters).clone()
    }
}

#[async_trait]
impl Telemetry for MockTelemetry {
    async fn counter_inc(&self, name: &str, value: u64, labels: Vec<(String, String)>) {
        lk(&self.counters).push((name.to_string(), value, labels));
    }

    async fn histogram_observe(&self, name: &str, value: f64, labels: Vec<(String, String)>) {
        lk(&self.histograms).push((name.to_string(), value, labels));
    }

    async fn gauge_set(&self, name: &str, value: f64, labels: Vec<(String, String)>) {
        lk(&self.gauges).push((name.to_string(), value, labels));
    }
}

// ---------------------------------------------------------------------------
// MockEventBus — EventBus
// ---------------------------------------------------------------------------

/// A subscriber: `(id, callback)`, where the callback receives `(topic, payload)`.
type Subscriber = (String, Box<dyn Fn(&str, &str) + Send>);

/// Mock for EventBus — in-memory publish/subscribe with direct callback invocation.
pub struct MockEventBus {
    subscribers: Mutex<HashMap<String, Vec<Subscriber>>>,
    next_id: Mutex<u64>,
}

impl MockEventBus {
    pub fn new() -> Self {
        Self {
            subscribers: Mutex::new(HashMap::new()),
            next_id: Mutex::new(1),
        }
    }
}

#[async_trait]
impl EventBus for MockEventBus {
    async fn publish(&self, topic: &str, payload: &str) -> CoreResult<()> {
        let subs = lk(&self.subscribers);
        if let Some(callbacks) = subs.get(topic) {
            for (_, cb) in callbacks {
                cb(topic, payload);
            }
        }
        Ok(())
    }

    async fn subscribe<F>(&self, topic: &str, callback: F) -> String
    where
        F: Fn(&str, &str) + Send + 'static,
    {
        let mut subs = lk(&self.subscribers);
        let mut next = lk(&self.next_id);
        let id = format!("sub_{}", *next);
        *next += 1;
        subs.entry(topic.to_string())
            .or_default()
            .push((id.clone(), Box::new(callback)));
        id
    }

    async fn unsubscribe(&self, subscription_id: &str) -> CoreResult<()> {
        let mut subs = lk(&self.subscribers);
        for callbacks in subs.values_mut() {
            callbacks.retain(|(id, _)| id != subscription_id);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MockSchemaRegistry — SchemaRegistry
// ---------------------------------------------------------------------------

/// Mock for SchemaRegistry — in-memory schema storage.
pub struct MockSchemaRegistry {
    schemas: Mutex<HashMap<String, Vec<SchemaColumn>>>,
}

impl MockSchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl SchemaRegistry for MockSchemaRegistry {
    async fn register_schema(&self, name: &str, columns: Vec<SchemaColumn>) -> CoreResult<()> {
        lk(&self.schemas).insert(name.to_string(), columns);
        Ok(())
    }

    async fn get_schema(&self, name: &str) -> CoreResult<Option<Vec<SchemaColumn>>> {
        Ok(lk(&self.schemas).get(name).cloned())
    }

    async fn find_by_column(&self, column_name: &str) -> CoreResult<Vec<String>> {
        let schemas = lk(&self.schemas);
        let mut result = Vec::new();
        for (name, cols) in schemas.iter() {
            if cols.iter().any(|c| c.name == column_name) {
                result.push(name.clone());
            }
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// MockAuthProvider — AuthProvider
// ---------------------------------------------------------------------------

/// Mock for AuthProvider — pre-configured token, always authorizes.
pub struct MockAuthProvider {
    token_value: Mutex<String>,
}

impl MockAuthProvider {
    pub fn new() -> Self {
        Self {
            token_value: Mutex::new("mock_token".into()),
        }
    }

    pub fn with_token(self, value: &str) -> Self {
        *lk(&self.token_value) = value.to_string();
        self
    }
}

#[async_trait]
impl AuthProvider for MockAuthProvider {
    async fn authenticate(&self, _credentials: AuthCredentials) -> CoreResult<AuthToken> {
        Ok(AuthToken {
            value: lk(&self.token_value).clone(),
            expires_at: None,
            permissions: vec!["*".into()],
        })
    }

    async fn authorize(&self, _token: &AuthToken, _permission: &str) -> CoreResult<bool> {
        Ok(true)
    }

    async fn validate(&self, _token: &AuthToken) -> CoreResult<bool> {
        Ok(true)
    }
}

// ---------------------------------------------------------------------------
// MockPluginHost — PluginHost
// ---------------------------------------------------------------------------

/// Mock for PluginHost — pre-registered plugins with canned responses.
pub struct MockPluginHost {
    plugins: Mutex<HashMap<String, PluginDescriptor>>,
    loaded: Mutex<Vec<String>>,
}

impl MockPluginHost {
    pub fn new() -> Self {
        Self {
            plugins: Mutex::new(HashMap::new()),
            loaded: Mutex::new(Vec::new()),
        }
    }

    pub fn with_plugin(self, name: &str, version: &str) -> Self {
        lk(&self.plugins).insert(
            name.to_string(),
            PluginDescriptor {
                name: name.to_string(),
                version: version.to_string(),
                plugin_type: PluginType::Wasm,
                description: String::new(),
                functions: vec!["parse".into()],
            },
        );
        self
    }
}

#[async_trait]
impl PluginHost for MockPluginHost {
    async fn scan(&self, _dir: &str) -> CoreResult<Vec<PluginDescriptor>> {
        Ok(lk(&self.plugins).values().cloned().collect())
    }

    async fn load(&self, name: &str) -> CoreResult<PluginHandle> {
        if lk(&self.plugins).contains_key(name) {
            lk(&self.loaded).push(name.to_string());
            Ok(PluginHandle {
                name: name.to_string(),
                plugin_type: PluginType::Wasm,
            })
        } else {
            Err(SwanFlowError::unknown(format!("plugin not found: {name}")))
        }
    }

    async fn init(
        &self,
        _handle: &PluginHandle,
        _config: HashMap<String, String>,
    ) -> CoreResult<()> {
        Ok(())
    }

    async fn run(
        &self,
        handle: &PluginHandle,
        _function: &str,
        _args: &[u8],
    ) -> CoreResult<Vec<u8>> {
        Ok(format!("result from {}", handle.name).into_bytes())
    }

    async fn unload(&self, handle: &PluginHandle) -> CoreResult<()> {
        lk(&self.loaded).retain(|n| n != &handle.name);
        Ok(())
    }

    fn loaded(&self) -> Vec<String> {
        lk(&self.loaded).clone()
    }
}

// ---------------------------------------------------------------------------
// MockWasmExec — WasmExec
// ---------------------------------------------------------------------------

/// Mock for WasmExec — pre-configured modules with canned results.
pub struct MockWasmExec {
    modules: Mutex<HashMap<String, WasmModule>>,
}

impl MockWasmExec {
    pub fn new() -> Self {
        Self {
            modules: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_module(self, name: &str, exports: Vec<String>) -> Self {
        lk(&self.modules).insert(
            name.to_string(),
            WasmModule {
                name: name.to_string(),
                exports,
            },
        );
        self
    }
}

#[async_trait]
impl WasmExec for MockWasmExec {
    async fn compile(&self, bytes: &[u8]) -> CoreResult<WasmModule> {
        let name = format!("mod_{}", bytes.len());
        let module = lk(&self.modules).get(&name).cloned().unwrap_or(WasmModule {
            name,
            exports: vec!["add".into()],
        });
        Ok(module)
    }

    async fn execute(
        &self,
        _module: &WasmModule,
        _function: &str,
        args: &[u8],
    ) -> CoreResult<Vec<u8>> {
        // Mock: sum all bytes and return as a single-byte result.
        let sum: u8 = args.iter().sum();
        Ok(vec![0, sum])
    }

    fn has_function(&self, module: &WasmModule, function: &str) -> bool {
        module.exports.contains(&function.to_string())
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
        let body = http
            .post_json("https://api.example.com/submit", r#"{"x":1}"#)
            .await
            .unwrap();
        assert_eq!(body, "done");
    }

    // ── KeyStore ──

    #[tokio::test]
    async fn test_mock_keystore() {
        let ks = MockKeyStore::new().with_entry("db_url", "postgres://localhost");
        assert_eq!(
            ks.get("db_url").await.unwrap(),
            Some("postgres://localhost".into())
        );
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
        store
            .put("dataset.parquet", vec![0, 1, 2, 3])
            .await
            .unwrap();
        assert!(store.exists("dataset.parquet").await);
        let data = store.get("dataset.parquet").await.unwrap().unwrap();
        assert_eq!(data, vec![0, 1, 2, 3]);
        store.delete("dataset.parquet").await.unwrap();
        assert!(!store.exists("dataset.parquet").await);
    }

    // ── ShellExec ──

    #[tokio::test]
    async fn test_mock_shell_exec() {
        let sh = MockShellExec::new().with_result(
            "echo",
            ShellOutput {
                exit_code: 0,
                stdout: "hello".into(),
                stderr: String::new(),
            },
        );
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

    // ── Moderation ──

    #[tokio::test]
    async fn test_mock_moderation_pass() {
        let m = MockModeration::new();
        let result = m.check("safe content").await.unwrap();
        assert!(result.is_safe);
        assert!(result.flagged_categories.is_empty());
    }

    #[tokio::test]
    async fn test_mock_moderation_flag() {
        let m = MockModeration::new().with_flag("violence");
        let result = m.check("kill them").await.unwrap();
        assert!(!result.is_safe);
        assert!(result.flagged_categories.contains(&"violence".to_string()));
    }

    #[tokio::test]
    async fn test_mock_moderation_batch() {
        let m = MockModeration::new()
            .with_flag("hate")
            .with_safe_threshold(0.8);
        let results = m.check_batch(&["hello", "bad stuff"]).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    // ── Logger ──

    #[tokio::test]
    async fn test_mock_logger() {
        let log = MockLogger::new();
        log.info("startup complete").await;
        log.error("connection failed").await;
        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.entries()[0].level, LogLevel::Info);
    }

    // ── Notification ──

    #[tokio::test]
    async fn test_mock_notification() {
        let n = MockNotification::new();
        n.send("slack:#alerts", "Test", "Body").await.unwrap();
        n.alert("URGENT", "Server down").await.unwrap();
        assert_eq!(n.sent().len(), 2);
    }

    // ── Cache ──

    #[tokio::test]
    async fn test_mock_cache() {
        let c = MockCache::new();
        c.set("key1", "val1", 60).await.unwrap();
        assert!(c.exists("key1").await);
        assert_eq!(c.get("key1").await.unwrap(), Some("val1".into()));
        c.invalidate("key1").await.unwrap();
        assert!(!c.exists("key1").await);
    }

    // ── Telemetry ──

    #[tokio::test]
    async fn test_mock_telemetry() {
        let t = MockTelemetry::new();
        t.counter_inc("requests", 1, vec![]).await;
        t.histogram_observe("latency", 0.5, vec![]).await;
        t.gauge_set("memory", 1024.0, vec![]).await;
        assert_eq!(t.counters().len(), 1);
    }

    // ── EventBus ──

    #[tokio::test]
    async fn test_mock_eventbus() {
        use std::sync::Arc;
        let bus = Arc::new(MockEventBus::new());
        let _bus_clone = bus.clone();
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ev_clone = events.clone();
        let sid = bus
            .subscribe("node.complete", move |topic, payload| {
                ev_clone
                    .lock()
                    .unwrap()
                    .push((topic.to_string(), payload.to_string()));
            })
            .await;
        bus.publish("node.complete", r#"{"id":"n1"}"#)
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(events.lock().unwrap().len(), 1);
        bus.unsubscribe(&sid).await.unwrap();
    }

    // ── SchemaRegistry ──

    #[tokio::test]
    async fn test_mock_schema_registry() {
        let sr = MockSchemaRegistry::new();
        let cols = vec![
            SchemaColumn {
                name: "id".into(),
                data_type: "INTEGER".into(),
                nullable: false,
                entity_type: None,
            },
            SchemaColumn {
                name: "name".into(),
                data_type: "VARCHAR".into(),
                nullable: true,
                entity_type: Some("Patient".into()),
            },
        ];
        sr.register_schema("patients", cols).await.unwrap();
        let schema = sr.get_schema("patients").await.unwrap().unwrap();
        assert_eq!(schema.len(), 2);
        let found = sr.find_by_column("name").await.unwrap();
        assert_eq!(found, vec!["patients"]);
    }

    // ── AuthProvider ──

    #[tokio::test]
    async fn test_mock_auth_provider() {
        let auth = MockAuthProvider::new().with_token("tk_abc123");
        let creds = AuthCredentials {
            username: Some("admin".into()),
            password: Some("pass".into()),
            api_key: None,
        };
        let token = auth.authenticate(creds).await.unwrap();
        assert_eq!(token.value, "tk_abc123");
        let authorized = auth.authorize(&token, "read:data").await.unwrap();
        assert!(authorized);
        let valid = auth.validate(&token).await.unwrap();
        assert!(valid);
    }

    // ── PluginHost ──

    #[tokio::test]
    async fn test_mock_plugin_host() {
        let ph = MockPluginHost::new().with_plugin("csv-parser", "1.0.0");
        let descriptors = ph.scan("/plugins").await.unwrap();
        assert_eq!(descriptors.len(), 1);
        let handle = ph.load("csv-parser").await.unwrap();
        assert_eq!(handle.name, "csv-parser");
        let config = std::collections::HashMap::new();
        ph.init(&handle, config).await.unwrap();
        let result = ph.run(&handle, "parse", b"a,b,c").await.unwrap();
        assert!(!result.is_empty());
        ph.unload(&handle).await.unwrap();
    }

    // ── WasmExec ──

    #[tokio::test]
    async fn test_mock_wasm_exec() {
        let w = MockWasmExec::new().with_module("add", vec!["add".into()]);
        let module = w.compile(b"mock wasm bytes").await.unwrap();
        assert!(w.has_function(&module, "add"));
        let result = w.execute(&module, "add", b"\x01\x02").await.unwrap();
        assert_eq!(result, vec![0, 3]);
    }
}

// ── Default for the mocks ────────────────────────────────────────────────
//
// NOTE: this file is a near-duplicate of crates/swanflow-core/src/kernel/mock.rs
// (same 20 mocks, same lints). The fix had to be applied twice, which is the
// cost of keeping two copies alive — see the de-duplication note in
// docs/design.md §17.
//
// Every mock here has a zero-argument `new()`, which is exactly the shape
// `clippy::new_without_default` flags. These impls are `Self::new()` and nothing
// else, so they cannot drift: if a `new()` ever takes arguments this stops
// compiling instead of quietly providing a different default.
macro_rules! default_via_new {
    ($($t:ty),* $(,)?) => {
        $(
            impl Default for $t {
                fn default() -> Self {
                    Self::new()
                }
            }
        )*
    };
}

default_via_new!(
    MockDb,
    MockFs,
    MockHttp,
    MockKeyStore,
    MockClock,
    MockBlobStore,
    MockShellExec,
    MockCancellationToken,
    MockProgress,
    MockLLM,
    MockModeration,
    MockLogger,
    MockNotification,
    MockCache,
    MockTelemetry,
    MockEventBus,
    MockSchemaRegistry,
    MockAuthProvider,
    MockPluginHost,
    MockWasmExec,
);
