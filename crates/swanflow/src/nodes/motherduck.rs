//! MotherDuck Flight node — trigger a MotherDuck Flight run as a DAG node.
//!
//! Type id: `connector.motherduck.flight`
//!
//! ⚠️ SEMANTIC CONTRACT (red-letter, do not soften):
//! 1. **Execution identity is the Flight owner.** MotherDuck docs: "The Flight
//!    runs as its owner, with the owner's token and secrets." This node is a
//!    *trigger*; whatever the Flight writes happens under the owner's
//!    permissions. Deployment requires `GRANT RUN ON FLIGHT` for the service
//!    account on each target Flight.
//! 2. **Version drift.** `MD_RUN_FLIGHT` has no version parameter — it locks
//!    to the Flight's *current* version at trigger time. The run ledger
//!    records `flight_version` per trigger so reruns can be audited, but it
//!    cannot pin the version.
//! 3. **At-least-once.** swanflow checkpointing replays whole levels
//!    (`core/engine/checkpoint.rs` — `completed_levels`), so this node may be
//!    re-executed. The ledger + `RUN_KEY` idempotency key are the mitigation,
//!    NOT a guarantee. Never wire this node to non-idempotent side effects
//!    without an idempotency contract on the Flight side.

use crate::mcp::http::{HttpMcpClient, HttpMcpConfig};
use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Int32Builder, Int64Builder, StringBuilder, UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Future, Stream};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

pub const FLIGHT_NODE_TYPE: &str = "connector.motherduck.flight";

// ---------------------------------------------------------------------------
// Resolved node configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlightConfig {
    pub flight_id: Option<String>,
    pub flight_name: Option<String>,
    pub config: serde_json::Map<String, JsonValue>,
    pub wait: bool,
    pub poll_interval_secs: u64,
    pub timeout_secs: u64,
    pub cancel_on_timeout: bool,
    pub fail_on_nonzero_exit: bool,
    /// Explicit idempotency scope; when absent, derived from flight + config.
    pub run_scope: Option<String>,
    /// Endpoint override (tests / self-hosted relays). Default MotherDuck.
    pub endpoint: Option<String>,
    /// Bearer token; when None, read MOTHERDUCK_TOKEN / MCP_HTTP_TOKEN env.
    pub token: Option<String>,
    /// Ledger directory; when None, no ledger (and no idempotent replay).
    pub ledger_dir: Option<PathBuf>,
}

impl FlightConfig {
    pub fn from_json(config: &JsonValue) -> Result<Self> {
        let flight_id = config["flight_id"].as_str().map(|s| s.to_string());
        let flight_name = config["flight_name"].as_str().map(|s| s.to_string());
        if flight_id.is_none() && flight_name.is_none() {
            return Err(anyhow!(
                "either flight_id or flight_name must be provided"
            ));
        }
        let mut cfg_map = serde_json::Map::new();
        if let Some(obj) = config["config"].as_object() {
            for (k, v) in obj {
                let s = v.as_str().ok_or_else(|| {
                    anyhow!("flight config key '{k}' must be a string value (MotherDuck requirement)")
                })?;
                cfg_map.insert(k.clone(), JsonValue::String(s.to_string()));
            }
        }
        let wait = config["wait"].as_bool().unwrap_or(true);
        let poll_interval_secs = config["poll_interval_secs"].as_u64().unwrap_or(5);
        let timeout_secs = config["timeout_secs"].as_u64().unwrap_or(3600);
        if poll_interval_secs < 5 {
            return Err(anyhow!(
                "poll_interval_secs must be >= 5 (MotherDuck Pulse bills per query; a tighter loop burns credits)"
            ));
        }
        if timeout_secs > 28800 {
            return Err(anyhow!(
                "timeout_secs must be <= 28800 (8h — MotherDuck Business tier single-run cap)"
            ));
        }
        if !wait && timeout_secs != 3600 {
            // Not fatal — fire-and-forget ignores the timeout entirely.
        }
        Ok(Self {
            flight_id,
            flight_name,
            config: cfg_map,
            wait,
            poll_interval_secs,
            timeout_secs,
            cancel_on_timeout: config["cancel_on_timeout"].as_bool().unwrap_or(true),
            fail_on_nonzero_exit: config["fail_on_nonzero_exit"].as_bool().unwrap_or(true),
            run_scope: config["run_scope"].as_str().map(|s| s.to_string()),
            endpoint: config["endpoint"].as_str().map(|s| s.to_string()),
            token: config["token"].as_str().map(|s| s.to_string()),
            ledger_dir: config["ledger_dir"].as_str().map(PathBuf::from),
        })
    }

    /// Stable idempotency key for this trigger: explicit run_scope wins
    /// (template-rendered with {{dag_run_id}} etc. by the engine before it
    /// reaches us); otherwise derived from flight identity + config.
    pub fn run_key(&self) -> String {
        let basis = match (&self.run_scope, &self.flight_id, &self.flight_name) {
            (Some(scope), _, _) => scope.clone(),
            (None, Some(id), _) => format!("flight:{id}"),
            (None, None, Some(name)) => format!("flight_name:{name}"),
            (None, None, None) => "flight:unknown".to_string(),
        };
        let mut hasher = Sha256::new();
        hasher.update(basis.as_bytes());
        hasher.update(serde_json::to_string(&self.config).unwrap_or_default().as_bytes());
        let digest = hasher.finalize();
        digest[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    }

    pub fn http_config(&self) -> Result<HttpMcpConfig> {
        let token = match &self.token {
            Some(t) => t.clone(),
            None => HttpMcpClient::token_from_env()?,
        };
        Ok(HttpMcpConfig {
            endpoint: self.endpoint.clone().unwrap_or_else(|| "https://api.motherduck.com/mcp".into()),
            token,
            ..HttpMcpConfig::default()
        })
    }
}

// ---------------------------------------------------------------------------
// Run ledger — append-only JSONL, one file per node (run_key-scoped).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LedgerRow {
    pub run_key: String,
    pub flight_id: Option<String>,
    pub flight_version: Option<u32>,
    pub run_number: Option<u64>,
    pub run_id: Option<String>,
    pub status: String,
    pub exit_code: Option<i32>,
    pub duration_ms: Option<i64>,
    pub logs_tail: Option<String>,
    pub triggered_at: String,
}

impl LedgerRow {
    pub fn to_output(&self, schema: SchemaRef) -> Result<RecordBatch, DataFusionError> {
        flight_row_to_batch(
            schema,
            self.run_id.as_deref().unwrap_or(""),
            self.run_number.unwrap_or(0),
            "", // flight_name not persisted in ledger (resolves per-run)
            self.flight_version.unwrap_or(0),
            &self.status,
            self.exit_code,
            self.duration_ms,
            self.logs_tail.as_deref().unwrap_or(""),
        )
    }
}

/// Append a ledger row (best-effort: ledger failures never fail the node —
/// they only cost idempotency, and the operator opted in via ledger_dir).
pub fn ledger_append(dir: &std::path::Path, row: &LedgerRow) {
    let _ = std::fs::create_dir_all(dir);
    let line = serde_json::to_string(row).unwrap_or_default();
    let path = dir.join(format!("motherduck_ledger_{}.jsonl", row.run_key));
    use std::io::Write;
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(f, "{line}");
    }
}

/// Find the last terminal (SUCCEEDED) ledger row for a run_key, if any.
pub fn ledger_find_success(dir: &std::path::Path, run_key: &str) -> Option<LedgerRow> {
    let path = dir.join(format!("motherduck_ledger_{run_key}.jsonl"));
    let content = std::fs::read_to_string(path).ok()?;
    content
        .lines()
        .filter_map(|l| serde_json::from_str::<LedgerRow>(l).ok())
        .rev()
        .find(|r| r.status == "SUCCEEDED")
}

// ---------------------------------------------------------------------------
// Output row helpers
// ---------------------------------------------------------------------------

pub fn flight_output_schema() -> Schema {
    Schema::new(vec![
        Field::new("run_id", DataType::Utf8, true),
        Field::new("run_number", DataType::UInt64, true),
        Field::new("flight_name", DataType::Utf8, true),
        Field::new("flight_version", DataType::UInt32, true),
        Field::new("status", DataType::Utf8, true),
        Field::new("exit_code", DataType::Int32, true),
        Field::new("duration_ms", DataType::Int64, true),
        Field::new("logs_tail", DataType::Utf8, true),
    ])
}

#[allow(clippy::too_many_arguments)]
pub fn flight_row_to_batch(
    schema: SchemaRef,
    run_id: &str,
    run_number: u64,
    flight_name: &str,
    flight_version: u32,
    status: &str,
    exit_code: Option<i32>,
    duration_ms: Option<i64>,
    logs_tail: &str,
) -> Result<RecordBatch, DataFusionError> {
    let mut b_run_id = StringBuilder::new();
    let mut b_run_number = UInt64Builder::new();
    let mut b_flight_name = StringBuilder::new();
    let mut b_flight_version = UInt32Builder::new();
    let mut b_status = StringBuilder::new();
    let mut b_exit = Int32Builder::new();
    let mut b_duration = Int64Builder::new();
    let mut b_logs = StringBuilder::new();

    b_run_id.append_value(run_id);
    b_run_number.append_value(run_number);
    b_flight_name.append_value(flight_name);
    b_flight_version.append_value(flight_version);
    b_status.append_value(status);
    match exit_code {
        Some(c) => b_exit.append_value(c),
        None => b_exit.append_null(),
    }
    match duration_ms {
        Some(d) => b_duration.append_value(d),
        None => b_duration.append_null(),
    }
    b_logs.append_value(logs_tail);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(b_run_id.finish()),
            Arc::new(b_run_number.finish()),
            Arc::new(b_flight_name.finish()),
            Arc::new(b_flight_version.finish()),
            Arc::new(b_status.finish()),
            Arc::new(b_exit.finish()),
            Arc::new(b_duration.finish()),
            Arc::new(b_logs.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

// ---------------------------------------------------------------------------
// Logical plan node
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlightLogical {
    pub config: FlightConfig,
    pub schema: Arc<DFSchema>,
}

impl PartialOrd for FlightLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.config.flight_id.partial_cmp(&other.config.flight_id)
    }
}

impl UserDefinedLogicalNodeCore for FlightLogical {
    fn name(&self) -> &str {
        "MotherDuckFlight"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let target = self
            .config
            .flight_id
            .as_deref()
            .or(self.config.flight_name.as_deref())
            .unwrap_or("?");
        write!(f, "MotherDuckFlight: flight={target}")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        Ok(self.clone())
    }
}

// ---------------------------------------------------------------------------
// Physical execution plan
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct FlightExec {
    pub config: FlightConfig,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
}

impl FlightExec {
    pub fn new(config: FlightConfig, schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            config,
            schema,
            cache,
        }
    }
}

impl DisplayAs for FlightExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        let target = self
            .config
            .flight_id
            .as_deref()
            .or(self.config.flight_name.as_deref())
            .unwrap_or("?");
        write!(f, "MotherDuckFlightExec: flight={target}")
    }
}

impl ExecutionPlan for FlightExec {
    fn name(&self) -> &str {
        "MotherDuckFlightExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        Ok(Box::pin(FlightStream {
            config: self.config.clone(),
            schema: self.schema.clone(),
            fut: None,
            done: false,
        }))
    }
}

type FlightFuture = Pin<Box<dyn Future<Output = Result<RecordBatch>> + Send>>;

pub struct FlightStream {
    config: FlightConfig,
    schema: SchemaRef,
    fut: Option<FlightFuture>,
    done: bool,
}

/// The full trigger→poll→emit state machine, as one future.
async fn execute_flight(cfg: FlightConfig, schema: SchemaRef) -> Result<RecordBatch> {
    let run_key = cfg.run_key();

    // 0. Idempotent replay: ledger hit means a previous execution of this
    //    exact trigger already reached SUCCEEDED — re-emit instead of
    //    re-triggering (at-least-once mitigation; see module docs).
    if let Some(dir) = &cfg.ledger_dir {
        if let Some(row) = ledger_find_success(dir, &run_key) {
            return row.to_output(schema).map_err(|e| anyhow!("{e}"));
        }
    }

    let mut client = HttpMcpClient::connect(cfg.http_config()?).await?;
    let _ = client.send_initialized_notification().await;

    // 1. Resolve flight identity: flight_name needs list_flights (only the
    //    owner's flights are listed) — flight_id is used directly.
    let flight_id = match &cfg.flight_id {
        Some(id) => id.clone(),
        None => {
            let result = client
                .call_tool(
                    "list_flights",
                    serde_json::json!({}),
                )
                .await?;
            let text = result["content"][0]["text"]
                .as_str()
                .ok_or_else(|| anyhow!("list_flights returned no text content"))?;
            let flights: JsonValue = serde_json::from_str(text)
                .map_err(|e| anyhow!("parsing list_flights result: {e}"))?;
            flights["flights"]
                .as_array()
                .and_then(|arr| {
                    arr.iter()
                        .find(|f| f["name"].as_str() == cfg.flight_name.as_deref())
                        .cloned()
                })
                .and_then(|f| f["id"].as_str().map(|s| s.to_string()))
                .ok_or_else(|| {
                    anyhow!(
                        "flight '{}' not found in caller's flights (MD_LIST_FLIGHTS only lists flights you own)",
                        cfg.flight_name.as_deref().unwrap_or("?")
                    )
                })?
        }
    };

    // 2. Trigger the run. Config carries RUN_KEY so the Flight side can
    //    deduplicate (reconcile-by-diff pattern).
    let mut run_config = cfg.config.clone();
    run_config.insert("RUN_KEY".to_string(), JsonValue::String(run_key.clone()));
    let mut args = serde_json::Map::new();
    args.insert("flight_id".to_string(), JsonValue::String(flight_id.clone()));
    args.insert("config".to_string(), JsonValue::Object(run_config));
    let run_result = client.call_tool("run_flight", JsonValue::Object(args)).await?;
    let run_text = run_result["content"][0]["text"]
        .as_str()
        .ok_or_else(|| anyhow!("run_flight returned no text content"))?;
    let run: JsonValue =
        serde_json::from_str(run_text).map_err(|e| anyhow!("parsing run_flight result: {e}"))?;
    let run_id = run["run_id"].as_str().unwrap_or_default().to_string();
    let run_number = run["run_number"].as_u64().unwrap_or(0);
    let flight_name = run["flight_name"].as_str().unwrap_or_default().to_string();
    let flight_version = run["flight_version"].as_u64().unwrap_or(0) as u32;

    // Ledger the trigger immediately (crash before terminal state still
    // leaves a trace of what was fired).
    let ledger_row = LedgerRow {
        run_key: run_key.clone(),
        flight_id: Some(flight_id.clone()),
        flight_version: Some(flight_version),
        run_number: Some(run_number),
        run_id: Some(run_id.clone()),
        status: "RUNNING".to_string(),
        exit_code: None,
        duration_ms: None,
        logs_tail: None,
        triggered_at: chrono::Utc::now().to_rfc3339(),
    };
    if let Some(dir) = &cfg.ledger_dir {
        ledger_append(dir, &ledger_row);
    }

    // 3. Fire-and-forget: emit the just-triggered row and return.
    if !cfg.wait {
        return flight_row_to_batch(
            schema,
            &run_id,
            run_number,
            &flight_name,
            flight_version,
            "RUNNING",
            None,
            None,
            "",
        )
        .map_err(|e| anyhow!("{e}"));
    }

    // 4. Poll until terminal, with capped exponential backoff (5s → 15s).
    let mut interval = Duration::from_secs(cfg.poll_interval_secs.max(5));
    let max_interval = Duration::from_secs(15);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(cfg.timeout_secs);
    let started = std::time::Instant::now();

    loop {
        if tokio::time::Instant::now() >= deadline {
            if cfg.cancel_on_timeout {
                let _ = client
                    .call_tool(
                        "cancel_flight_run",
                        serde_json::json!({ "run_id": run_id }),
                    )
                    .await;
            }
            anyhow::bail!(
                "MotherDuck Flight run {run_id} (flight {flight_id}) timed out after {}s{}",
                cfg.timeout_secs,
                if cfg.cancel_on_timeout { " and was cancelled" } else { "" }
            );
        }

        tokio::time::sleep(interval).await;
        interval = (interval * 2).min(max_interval);

        let status_result = client
            .call_tool(
                "get_flight_run",
                serde_json::json!({ "run_id": run_id }),
            )
            .await?;
        let status_text = status_result["content"][0]["text"]
            .as_str()
            .ok_or_else(|| anyhow!("get_flight_run returned no text content"))?;
        let run_state: JsonValue = serde_json::from_str(status_text)
            .map_err(|e| anyhow!("parsing get_flight_run result: {e}"))?;
        let status = run_state["status"].as_str().unwrap_or("UNKNOWN").to_string();

        match status.as_str() {
            "SUCCEEDED" | "FAILED" | "CANCELLED" => {
                let exit_code = run_state["exit_code"].as_i64().map(|c| c as i32);
                let logs_tail = fetch_logs_tail(&mut client, &run_id).await;
                let duration_ms =
                    Some(started.elapsed().as_millis() as i64);

                let terminal_row = LedgerRow {
                    run_key,
                    flight_id: Some(flight_id.clone()),
                    flight_version: Some(flight_version),
                    run_number: Some(run_number),
                    run_id: Some(run_id.clone()),
                    status: status.clone(),
                    exit_code,
                    duration_ms,
                    logs_tail: Some(logs_tail.clone()),
                    triggered_at: ledger_row.triggered_at.clone(),
                };
                if let Some(dir) = &cfg.ledger_dir {
                    ledger_append(dir, &terminal_row);
                }

                if status == "SUCCEEDED" || !cfg.fail_on_nonzero_exit {
                    return flight_row_to_batch(
                        schema,
                        &run_id,
                        run_number,
                        &flight_name,
                        flight_version,
                        &status,
                        exit_code,
                        duration_ms,
                        &logs_tail,
                    )
                    .map_err(|e| anyhow!("{e}"));
                }
                anyhow::bail!(
                    "MotherDuck Flight run {run_id} ended {status} with exit_code={:?}",
                    exit_code
                );
            }
            // PENDING / RUNNING / anything else — keep polling.
            _ => continue,
        }
    }
}

async fn fetch_logs_tail(client: &mut HttpMcpClient, run_id: &str) -> String {
    let logs = client
        .call_tool(
            "get_flight_run_logs",
            serde_json::json!({ "run_id": run_id }),
        )
        .await;
    match logs {
        Ok(v) => {
            let text = v["content"][0]["text"].as_str().unwrap_or("");
            // Keep the last ~2KB of logs — a tail, not the whole log.
            if text.len() > 2048 {
                text[text.len() - 2048..].to_string()
            } else {
                text.to_string()
            }
        }
        Err(_) => String::new(),
    }
}

/// Test seam: run the full state machine outside DataFusion. Production
/// entry is the `FlightStream` poll path; tests call this directly.
#[doc(hidden)]
pub async fn execute_flight_for_tests(
    cfg: FlightConfig,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    execute_flight(cfg, schema).await
}

impl Stream for FlightStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        if self.fut.is_none() {
            let cfg = self.config.clone();
            let schema = self.schema.clone();
            let fut = async move { execute_flight(cfg, schema).await };
            self.fut = Some(Box::pin(fut));
        }

        let res = match Pin::new(self.fut.as_mut().unwrap()).poll(cx) {
            Poll::Ready(res) => res,
            Poll::Pending => return Poll::Pending,
        };

        self.fut = None;
        self.done = true;

        match res {
            Ok(batch) => Poll::Ready(Some(Ok(batch))),
            Err(e) => Poll::Ready(Some(Err(DataFusionError::Execution(format!(
                "MotherDuck Flight node failed: {e}"
            ))))),
        }
    }
}

impl RecordBatchStream for FlightStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ---------------------------------------------------------------------------
// NodeBehavior implementation
// ---------------------------------------------------------------------------

pub struct MotherDuckFlightNode;

#[async_trait]
impl NodeBehavior for MotherDuckFlightNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: FLIGHT_NODE_TYPE.to_string(),
            display_name: "MotherDuck Flight".to_string(),
            description: "Trigger a MotherDuck Flight run (trigger → poll → status row). \
                At-least-once: the Flight runs as its owner — see node docs before wiring \
                to non-idempotent side effects."
                .to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Flight run status row (8 columns)".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "flight_id": { "type": "string", "description": "Flight UUID (preferred)" },
                    "flight_name": { "type": "string", "description": "Flight name (resolved via list_flights; only own flights are listed)" },
                    "config": { "type": "object", "additionalProperties": { "type": "string" }, "description": "Per-run config overrides (string values only)" },
                    "wait": { "type": "boolean", "default": true, "description": "false = fire-and-forget (emit RUNNING row immediately)" },
                    "poll_interval_secs": { "type": "integer", "default": 5, "minimum": 5, "description": "Minimum 5s — Pulse bills per query" },
                    "timeout_secs": { "type": "integer", "default": 3600, "maximum": 28800 },
                    "cancel_on_timeout": { "type": "boolean", "default": true },
                    "fail_on_nonzero_exit": { "type": "boolean", "default": true },
                    "run_scope": { "type": "string", "description": "Idempotency scope; template-render {{dag_run_id}} here. Default: derived from flight+config" },
                    "endpoint": { "type": "string", "description": "MCP endpoint override (tests/self-hosted relays)" },
                    "token": { "type": "string", "description": "Bearer token; default reads MOTHERDUCK_TOKEN env" },
                    "ledger_dir": { "type": "string", "description": "Directory for the idempotency ledger; absent = no replay protection" }
                },
                "required": []
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        FlightConfig::from_json(config)?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let cfg = FlightConfig::from_json(config)?;
        let schema = flight_output_schema();
        let df_schema = Arc::new(DFSchema::try_from(schema)?);
        let logical = FlightLogical {
            config: cfg,
            schema: df_schema,
        };
        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(logical),
            },
        )])
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_requires_flight_identity() {
        let err = FlightConfig::from_json(&serde_json::json!({})).unwrap_err();
        assert!(err.to_string().contains("flight_id"));
    }

    #[test]
    fn config_rejects_short_poll_interval() {
        let cfg = serde_json::json!({ "flight_id": "x", "poll_interval_secs": 1 });
        let err = FlightConfig::from_json(&cfg).unwrap_err();
        assert!(err.to_string().contains("poll_interval_secs"));
    }

    #[test]
    fn config_rejects_huge_timeout() {
        let cfg = serde_json::json!({ "flight_id": "x", "timeout_secs": 99999 });
        assert!(FlightConfig::from_json(&cfg).is_err());
    }

    #[test]
    fn config_rejects_non_string_config_values() {
        let cfg = serde_json::json!({ "flight_id": "x", "config": { "REGION": 7 } });
        let err = FlightConfig::from_json(&cfg).unwrap_err();
        assert!(err.to_string().contains("must be a string"));
    }

    #[test]
    fn run_key_stable_and_scope_sensitive() {
        let base = serde_json::json!({ "flight_id": "abc", "config": { "R": "1" } });
        let a = FlightConfig::from_json(&base).unwrap();
        let b = FlightConfig::from_json(&base).unwrap();
        assert_eq!(a.run_key(), b.run_key());
        // Different scope → different key.
        let scoped = serde_json::json!({ "flight_id": "abc", "run_scope": "dag1/node2" });
        let c = FlightConfig::from_json(&scoped).unwrap();
        assert_ne!(a.run_key(), c.run_key());
        // Different config → different key.
        let other_cfg = serde_json::json!({ "flight_id": "abc", "config": { "R": "2" } });
        let d = FlightConfig::from_json(&other_cfg).unwrap();
        assert_ne!(a.run_key(), d.run_key());
    }

    #[test]
    fn ledger_roundtrip_and_find_success() {
        let dir = tempfile::tempdir().unwrap();
        let row_running = LedgerRow {
            run_key: "k1".into(),
            flight_id: Some("f".into()),
            flight_version: Some(3),
            run_number: Some(17),
            run_id: Some("r".into()),
            status: "RUNNING".into(),
            exit_code: None,
            duration_ms: None,
            logs_tail: None,
            triggered_at: "t0".into(),
        };
        ledger_append(dir.path(), &row_running);
        assert!(ledger_find_success(dir.path(), "k1").is_none());

        let mut row_done = row_running.clone();
        row_done.status = "SUCCEEDED".into();
        row_done.exit_code = Some(0);
        ledger_append(dir.path(), &row_done);
        let found = ledger_find_success(dir.path(), "k1").unwrap();
        assert_eq!(found.status, "SUCCEEDED");
        assert_eq!(found.run_number, Some(17));

        // Ledger row re-emits as a valid output batch.
        let schema = Arc::new(flight_output_schema());
        let batch = found.to_output(schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 8);
    }
}
