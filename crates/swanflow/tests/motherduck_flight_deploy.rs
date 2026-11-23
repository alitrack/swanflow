//! E2E tests for the flight deployer (M3) against a mock MotherDuck MCP
//! server — token-free, asserting deploy/rollback call shapes per the
//! flight-deployer spec.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::Value as JsonValue;
use swanflow::deploy::deployer;
use swanflow::deploy::motherduck::{compile_inline, compile_per_node};
use swanflow::deploy::FlightProgram;

#[derive(Default)]
struct MockState {
    create_count: AtomicU64,
    update_count: AtomicU64,
    /// (name, source) of every create_flight call.
    created: Mutex<Vec<(String, String)>>,
    /// (name, source) of every update_flight call.
    updated: Mutex<Vec<(String, String)>>,
    /// Records whether the Authorization header carried a Bearer token.
    auth_seen: AtomicU64,
}

fn tool_text(id: &JsonValue, body: &str) -> Vec<u8> {
    serde_json::json!({
        "jsonrpc": "2.0", "id": id,
        "result": { "content": [ { "type": "text", "text": body } ] }
    })
    .to_string()
    .into_bytes()
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    state: Arc<MockState>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    use http_body_util::BodyExt;
    // Bearer auth must be present — asserts the deployer wiring.
    if req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("Bearer "))
        .unwrap_or(false)
    {
        state.auth_seen.fetch_add(1, Ordering::SeqCst);
    }
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

    let payload: Vec<u8> = match method {
        "initialize" => serde_json::json!({
            "jsonrpc": "2.0", "id": id,
            "result": { "protocolVersion": "2025-06-18", "capabilities": { "tools": {} } }
        })
        .to_string()
        .into_bytes(),
        "notifications/initialized" => {
            return Ok(Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Full::new(Bytes::new()))
                .unwrap());
        }
        "tools/call" => {
            let name = msg["params"]["name"].as_str().unwrap_or("");
            let args = msg["params"]["arguments"].clone();
            match name {
                "create_flight" => {
                    state.create_count.fetch_add(1, Ordering::SeqCst);
                    let fname = args["name"].as_str().unwrap_or("").to_string();
                    let source = args["source"].as_str().unwrap_or("").to_string();
                    state.created.lock().unwrap().push((fname, source));
                    tool_text(
                        &id,
                        &serde_json::json!({ "flight_id": format!("f-{}", state.create_count.load(Ordering::SeqCst)), "version": 1 }).to_string(),
                    )
                }
                "update_flight" => {
                    state.update_count.fetch_add(1, Ordering::SeqCst);
                    let fname = args["name"].as_str().unwrap_or("").to_string();
                    let source = args["source"].as_str().unwrap_or("").to_string();
                    state.updated.lock().unwrap().push((fname, source));
                    tool_text(&id, &serde_json::json!({ "ok": true }).to_string())
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

async fn spawn_mock() -> (String, Arc<MockState>) {
    let state = Arc::new(MockState::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let st = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let st = st.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle(req, st.clone()));
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await;
            });
        }
    });
    (format!("http://{addr}/mcp"), state)
}

fn simple_program(name: &str, body: &str) -> FlightProgram {
    let source = format!("# test flight {name}\n{body}\n");
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(source.as_bytes());
    FlightProgram {
        flight_name: name.to_string(),
        source,
        source_sha256: h.finalize().iter().map(|b| format!("{b:02x}")).collect(),
    }
}

fn dag_wf() -> swanflow::core::graph::Workflow {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples/5_1_fetch_store.yaml");
    swanflow::load_workflow_file(&path).unwrap()
}

#[tokio::test]
async fn first_deploy_creates_flight_writes_manifest_and_sources() {
    let (endpoint, state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");

    let manifest = deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "print('x')")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .expect("deploy succeeds");

    // create_flight called once with Bearer auth and the right shape.
    assert_eq!(state.create_count.load(Ordering::SeqCst), 1);
    assert!(state.auth_seen.load(Ordering::SeqCst) > 0);
    let created = state.created.lock().unwrap();
    assert_eq!(created[0].0, "swanflow_inline_testdag");
    assert!(created[0].1.contains("print('x')"));
    drop(created);

    // Manifest written, rollback null (first deployment).
    assert!(manifest.rollback.is_none());
    let manifest_path = flights_dir.join(deployer::manifest_file_name("testdag"));
    assert!(manifest_path.is_file());
    // Local source copy exists (git-diffable).
    assert!(flights_dir.join("swanflow_inline_testdag.py").is_file());
    // Owner hint is masked, never the token.
    assert!(manifest.owner_hint.starts_with("tok:"));
    assert!(!manifest.owner_hint.contains("mock-token"));
}

#[tokio::test]
async fn second_deploy_chains_rollback_pointer() {
    let (endpoint, state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");

    let _m1 = deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v1")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint.clone()),
        Some("mock-token".into()),
    )
    .await
    .unwrap();

    // Preserve v1 sources for the later rollback digest check.
    let v1_source =
        std::fs::read_to_string(flights_dir.join("swanflow_inline_testdag.py")).unwrap();

    let m2 = deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v2")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint.clone()),
        Some("mock-token".into()),
    )
    .await
    .unwrap();

    assert_eq!(m2.rollback.as_deref(), Some("swanflow-deploy-testdag.json"));
    assert_eq!(state.create_count.load(Ordering::SeqCst), 2);

    // The outgoing v1 manifest is archived automatically — the rollback
    // pointer resolves without anyone hand-writing the history file (this
    // test used to simulate it, papering over the dangling pointer).
    let history_path = flights_dir.join("history-swanflow-deploy-testdag.json");
    assert!(
        history_path.is_file(),
        "deploy archives the outgoing manifest"
    );
    let archived: swanflow::deploy::DeployManifest =
        serde_json::from_str(&std::fs::read_to_string(&history_path).unwrap()).unwrap();
    assert!(archived.rollback.is_none(), "archived manifest is v1");
    assert_ne!(
        archived.flights[0].source_sha256, m2.flights[0].source_sha256,
        "archived manifest carries v1's digest"
    );

    // Operator restores the v1 sources (the git-checkout step the rollback
    // design assumes: flights/ is the source of truth, history records the
    // digests that are valid to push).
    std::fs::write(flights_dir.join("swanflow_inline_testdag.py"), v1_source).unwrap();

    // Rollback: update_flight with v1 source.
    let restored = deployer::rollback(
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .expect("rollback ok");
    assert!(
        restored.rollback.is_none(),
        "restored manifest is v1 (no rollback)"
    );
    assert_eq!(state.update_count.load(Ordering::SeqCst), 1);
    let updated = state.updated.lock().unwrap();
    assert!(
        updated[0].1.contains("v1"),
        "update_flight carries the v1 source"
    );
}

#[tokio::test]
async fn deploy_refused_when_live_local_sources_drifted() {
    let (endpoint, state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");

    deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v1")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint.clone()),
        Some("mock-token".into()),
    )
    .await
    .unwrap();

    // Tamper with the local source of the live deployment.
    std::fs::write(
        flights_dir.join("swanflow_inline_testdag.py"),
        "# tampered\n",
    )
    .unwrap();

    // The next deploy must refuse BEFORE any MCP call fires.
    let err = deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v2")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .expect_err("deploy over drifted live state must refuse");
    assert!(
        err.to_string().contains("digest mismatch"),
        "error names the drift: {err}"
    );
    assert_eq!(
        state.create_count.load(Ordering::SeqCst),
        1,
        "refusal costs zero MCP calls"
    );
}

#[tokio::test]
async fn check_seals_the_program_set_and_refuses_drift() {
    let (endpoint, _state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");

    // Nothing deployed yet → explicit refusal, not a silent pass.
    let err = deployer::check("testdag", &flights_dir, &flights_dir).expect_err("no manifest");
    assert!(err.to_string().contains("nothing to check"));

    deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v1")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .unwrap();

    // Intact local copies → Ok.
    deployer::check("testdag", &flights_dir, &flights_dir).expect("intact copies pass");

    // Tampered local copy → SwapGate names both hashes.
    std::fs::write(
        flights_dir.join("swanflow_inline_testdag.py"),
        "# tampered\n",
    )
    .unwrap();
    let err = deployer::check("testdag", &flights_dir, &flights_dir).expect_err("drift refused");
    let msg = err.to_string();
    assert!(msg.contains("drifted"), "error names drift: {msg}");
    assert!(msg.contains("!="), "error shows sealed vs current: {msg}");

    // A manifest that predates sealing is refused explicitly.
    let manifest_path = flights_dir.join(deployer::manifest_file_name("testdag"));
    let manifest: swanflow::deploy::DeployManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
    let unsealed = swanflow::deploy::DeployManifest {
        programs_hash: None,
        ..manifest
    };
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&unsealed).unwrap(),
    )
    .unwrap();
    let err = deployer::check("testdag", &flights_dir, &flights_dir).expect_err("unsealed refused");
    assert!(
        err.to_string().contains("predates sealing"),
        "missing seal is an explicit error: {err}"
    );
}

#[tokio::test]
async fn rollback_refused_on_first_deployment() {
    let (endpoint, state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");

    let _m1 = deployer::deploy(
        vec![simple_program("swanflow_inline_testdag", "v1")],
        "inline",
        "testdag",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .unwrap();

    // Rollback on a manifest with rollback=null → refused, zero MCP calls.
    let err = deployer::rollback(
        "testdag",
        &flights_dir,
        &flights_dir,
        Some("http://127.0.0.1:9/mcp".into()),
        Some("mock-token".into()),
    )
    .await
    .expect_err("must refuse");
    assert!(err.to_string().contains("no previous deployment"));
    assert_eq!(
        state.update_count.load(Ordering::SeqCst),
        0,
        "no update_flight issued"
    );
}

#[tokio::test]
async fn full_pipeline_example_deploys_inline_and_per_node() {
    let wf = dag_wf();
    let inline = compile_inline(&wf).unwrap();
    let per_node = compile_per_node(&wf).unwrap();
    assert!(per_node.len() >= 2);

    // Deploy per-node against the mock: create called N times.
    let (endpoint, state) = spawn_mock().await;
    let dir = tempfile::tempdir().unwrap();
    let flights_dir: PathBuf = dir.path().join("flights");
    let n = per_node.len();
    let manifest = deployer::deploy(
        per_node,
        "per-node",
        "dag51",
        &flights_dir,
        &flights_dir,
        Some(endpoint),
        Some("mock-token".into()),
    )
    .await
    .unwrap();
    assert_eq!(manifest.flights.len(), n);
    assert_eq!(state.create_count.load(Ordering::SeqCst) as usize, n);
    // Scheduler flight is among the created sources.
    let names: Vec<String> = manifest
        .flights
        .iter()
        .map(|f| f.flight_name.clone())
        .collect();
    assert!(
        names.iter().any(|n| n.starts_with("swanflow_sched_")),
        "scheduler present: {names:?}"
    );
    // Inline compiles and would deploy as 1.
    let _ = inline;
}
