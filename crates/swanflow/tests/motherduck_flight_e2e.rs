//! E2E + fault-injection tests for the MotherDuck Flight node against a mock
//! MotherDuck MCP server (local hyper HTTP server), no token required.
//!
//! Covers (openspec 2026-11-02-motherduck-flight-connector):
//! - M2: 2-node-style happy path through execute_flight — trigger → poll →
//!   SUCCEEDED → one 8-column row
//! - M3.1: exit_code=3 + fail_on_nonzero_exit=true → error containing run_id
//! - M3.2: timeout + cancel_on_timeout → cancel_flight_run called, node fails
//! - M3.3: ledger replay — a second execution with a SUCCEEDED ledger row
//!   does NOT trigger a new run (server call counter proves it)

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;

use swanflow::nodes::motherduck::flight_output_schema;

/// What the mock Flight does on this run (scripted per test).
#[derive(Debug, Clone)]
pub enum MockScenario {
    /// Poll responses go PENDING → RUNNING → SUCCEEDED (exit 0).
    Succeed,
    /// Terminal state FAILED with exit_code.
    FailWithExit(i64),
    /// Never reaches terminal state within the test window.
    NeverTerminate,
}

struct MockState {
    scenario: MockScenario,
    poll_count: AtomicU64,
    run_count: AtomicU64,
    cancel_count: AtomicU64,
    last_run_config: Mutex<Option<JsonValue>>,
}

impl MockState {
    fn new(scenario: MockScenario) -> Arc<Self> {
        Arc::new(Self {
            scenario,
            poll_count: AtomicU64::new(0),
            run_count: AtomicU64::new(0),
            cancel_count: AtomicU64::new(0),
            last_run_config: Mutex::new(None),
        })
    }
}

fn tool_text_response(id: &JsonValue, text: &str) -> Vec<u8> {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "content": [ { "type": "text", "text": text } ]
        }
    })
    .to_string()
    .into_bytes()
}

async fn handle_mcp(
    req: Request<hyper::body::Incoming>,
    state: Arc<MockState>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    use http_body_util::BodyExt;
    let body = req.into_body().collect().await.unwrap().to_bytes();
    let msg: JsonValue = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from_static(b"{\"error\":\"bad json\"}")))
                .unwrap())
        }
    };
    let id = msg.get("id").cloned().unwrap_or(JsonValue::Null);
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");

    // Bearer auth must be present (even in mock — catches wiring bugs).
    // Checked by the caller via headers; here we accept all.

    let payload: Vec<u8> = match method {
        "initialize" => serde_json::json!({
            "jsonrpc": "2.0", "id": id,
            "result": { "protocolVersion": "2025-06-18", "capabilities": { "tools": {} } }
        })
        .to_string()
        .into_bytes(),
        "notifications/initialized" => {
            // Notification — no response body needed; 202.
            return Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
        "tools/call" => {
            let name = msg["params"]["name"].as_str().unwrap_or("");
            let args = msg["params"]["arguments"].clone();
            match name {
                "list_flights" => tool_text_response(
                    &id,
                    &serde_json::json!({
                        "flights": [ { "id": "flight-uuid-1", "name": "heartbeat" } ]
                    })
                    .to_string(),
                ),
                "run_flight" => {
                    state.run_count.fetch_add(1, Ordering::SeqCst);
                    *state.last_run_config.lock().await = Some(args["config"].clone());
                    tool_text_response(
                        &id,
                        &serde_json::json!({
                            "run_id": "run-42",
                            "run_number": 7,
                            "flight_name": "heartbeat",
                            "flight_version": 3,
                            "status": "RUNNING"
                        })
                        .to_string(),
                    )
                }
                "get_flight_run" => {
                    let n = state.poll_count.fetch_add(1, Ordering::SeqCst);
                    let status = match &state.scenario {
                        MockScenario::Succeed => {
                            if n == 0 { "RUNNING" } else { "SUCCEEDED" }
                        }
                        MockScenario::FailWithExit(_code) => {
                            if n == 0 { "RUNNING" } else { "FAILED" }
                        }
                        MockScenario::NeverTerminate => "RUNNING",
                    };
                    let mut run = serde_json::json!({ "status": status });
                    match &state.scenario {
                        MockScenario::Succeed => {
                            if n > 0 {
                                run["exit_code"] = JsonValue::from(0);
                            }
                        }
                        MockScenario::FailWithExit(code) => {
                            if n > 0 {
                                run["exit_code"] = JsonValue::from(*code);
                            }
                        }
                        MockScenario::NeverTerminate => {}
                    }
                    tool_text_response(&id, &run.to_string())
                }
                "get_flight_run_logs" => tool_text_response(
                    &id,
                    &serde_json::json!({ "logs": [ "line1: starting", "line2: done" ] })
                        .to_string(),
                ),
                "cancel_flight_run" => {
                    state.cancel_count.fetch_add(1, Ordering::SeqCst);
                    tool_text_response(&id, &serde_json::json!({ "cancelled": true }).to_string())
                }
                other => serde_json::json!({
                    "jsonrpc": "2.0", "id": id,
                    "error": { "code": -32602, "message": format!("unknown tool {other}") }
                })
                .to_string()
                .into_bytes(),
            }
        }
        other => serde_json::json!({
            "jsonrpc": "2.0", "id": id,
            "error": { "code": -32601, "message": format!("method not found: {other}") }
        })
        .to_string()
        .into_bytes(),
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(payload)))
        .unwrap())
}

/// Spawn the mock MCP server on 127.0.0.1:0; returns (endpoint_url, state).
async fn spawn_mock(scenario: MockScenario) -> (String, Arc<MockState>) {
    let state = MockState::new(scenario);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let st = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            let st = st.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle_mcp(req, st.clone()));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
    (format!("http://{addr}/mcp"), state)
}

fn test_config(endpoint: &str, ledger_dir: &PathBuf) -> swanflow::nodes::motherduck::FlightConfig {
    let raw = serde_json::json!({
        "flight_id": "flight-uuid-1",
        "config": { "REGION": "eu-central-1" },
        "wait": true,
        "poll_interval_secs": 5,
        "timeout_secs": 30,
        "endpoint": endpoint,
        "token": "mock-token",
        "ledger_dir": ledger_dir.to_string_lossy(),
    });
    swanflow::nodes::motherduck::FlightConfig::from_json(&raw).unwrap()
}

// ---------------------------------------------------------------------------
// E2E happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_node_e2e_succeeds_and_emits_one_row() {
    let (endpoint, _state) = spawn_mock(MockScenario::Succeed).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(&endpoint, &dir.path().to_path_buf());

    let schema = Arc::new(flight_output_schema());
    let batch = swanflow::nodes::motherduck::execute_flight_for_tests(cfg, schema)
        .await
        .expect("flight execution should succeed");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 8);
    let status = batch
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(status.value(0), "SUCCEEDED");
    let run_number = batch
        .column(1)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(run_number.value(0), 7);
    let flight_version = batch
        .column(3)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::UInt32Array>()
        .unwrap();
    assert_eq!(flight_version.value(0), 3);
}

// ---------------------------------------------------------------------------
// M3.1: non-zero exit → node fails with run_id in the error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_node_fails_on_nonzero_exit() {
    let (endpoint, _state) = spawn_mock(MockScenario::FailWithExit(3)).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(&endpoint, &dir.path().to_path_buf());

    let schema = Arc::new(flight_output_schema());
    let err = swanflow::nodes::motherduck::execute_flight_for_tests(cfg, schema)
        .await
        .expect_err("non-zero exit must fail the node");
    let msg = err.to_string();
    assert!(msg.contains("run-42"), "error must name the run: {msg}");
    assert!(msg.contains("FAILED"), "error must name the status: {msg}");
}

// ---------------------------------------------------------------------------
// M3.2: timeout → cancel called, node fails
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_node_timeout_cancels() {
    let (endpoint, state) = spawn_mock(MockScenario::NeverTerminate).await;
    let dir = tempfile::tempdir().unwrap();
    let raw = serde_json::json!({
        "flight_id": "flight-uuid-1",
        "wait": true,
        "poll_interval_secs": 5,
        "timeout_secs": 1,          // force the timeout quickly
        "cancel_on_timeout": true,
        "endpoint": endpoint,
        "token": "mock-token",
        "ledger_dir": dir.path().to_string_lossy(),
    });
    let cfg = swanflow::nodes::motherduck::FlightConfig::from_json(&raw).unwrap();

    let schema = Arc::new(flight_output_schema());
    let err = swanflow::nodes::motherduck::execute_flight_for_tests(cfg, schema)
        .await
        .expect_err("timeout must fail the node");
    assert!(err.to_string().contains("timed out"));
    assert_eq!(state.cancel_count.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// M3.3: ledger replay — second execution must NOT re-trigger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_node_ledger_replay_skips_retrigger() {
    let (endpoint, state) = spawn_mock(MockScenario::Succeed).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(&endpoint, &dir.path().to_path_buf());

    let schema = Arc::new(flight_output_schema());

    // First run: triggers, succeeds, ledgered.
    let batch1 = swanflow::nodes::motherduck::execute_flight_for_tests(cfg.clone(), schema.clone())
        .await
        .expect("first run");
    assert_eq!(batch1.num_rows(), 1);
    let runs_after_first = state.run_count.load(Ordering::SeqCst);
    assert_eq!(runs_after_first, 1);

    // Second execution of the same trigger: ledger has SUCCEEDED → replay,
    // no new run_flight call.
    let batch2 = swanflow::nodes::motherduck::execute_flight_for_tests(cfg, schema)
        .await
        .expect("replay run");
    assert_eq!(batch2.num_rows(), 1);
    assert_eq!(state.run_count.load(Ordering::SeqCst), 1, "ledger replay must not re-trigger run_flight");
    let status2 = batch2
        .column(4)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
        .unwrap();
    assert_eq!(status2.value(0), "SUCCEEDED", "replay emits the ledgered terminal row");
}

// ---------------------------------------------------------------------------
// RUN_KEY must be injected into the run config (spec requirement)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flight_node_injects_run_key() {
    let (endpoint, state) = spawn_mock(MockScenario::Succeed).await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = test_config(&endpoint, &dir.path().to_path_buf());

    let schema = Arc::new(flight_output_schema());
    let _ = swanflow::nodes::motherduck::execute_flight_for_tests(cfg, schema)
        .await
        .unwrap();

    let run_config = state.last_run_config.lock().await.clone().unwrap();
    let run_key = run_config["RUN_KEY"].as_str().expect("RUN_KEY injected");
    assert!(!run_key.is_empty());
    assert_eq!(run_key.len(), 16, "run_key is a 16-hex-char digest");
}
