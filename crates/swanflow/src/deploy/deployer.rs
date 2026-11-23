//! Flight deployer — write-side client: create/update flights over MCP,
//! local `flights/` source copies, chained manifests with rollback.
//!
//! Reuses `crate::mcp::http::HttpMcpClient`; the mock-server E2E asserts the
//! call shapes without touching api.motherduck.com (token-free, per user
//! directive the live path is explicitly unverified).

use super::{DeployManifest, FlightProgram, FlightRecord};
use crate::core::verified_swap::{self, SwapGate};
use crate::mcp::http::{HttpMcpClient, HttpMcpConfig};
use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::path::Path;

pub fn manifest_file_name(dag_id: &str) -> String {
    format!("swanflow-deploy-{dag_id}.json")
}

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// FNV-1a seal over the program set (flight_name, source) — order-independent
/// by construction (`verified_swap::source_hash` sorts by key).
pub fn programs_hash(programs: &[FlightProgram]) -> String {
    verified_swap::source_hash(
        programs
            .iter()
            .map(|p| (p.flight_name.clone(), p.source.as_bytes().to_vec()))
            .collect(),
    )
}

/// Local source copy file name for a flight record.
fn source_path(flights_dir: &Path, flight_name: &str) -> std::path::PathBuf {
    flights_dir.join(format!("{}.py", sanitize_file_stem(flight_name)))
}

/// Mechanical gate before going live over an existing deployment: every
/// flight the live manifest records must still have a local source copy
/// whose digest matches. Refusing here means we never ship over a live
/// state whose source-of-truth has drifted (pattern: fast-anything's
/// "build/enable require the verified Rust sources", #26/#67).
pub fn verify_local_sources(flights_dir: &Path, manifest: &DeployManifest) -> Result<()> {
    for rec in &manifest.flights {
        let src_path = source_path(flights_dir, &rec.flight_name);
        if !src_path.is_file() {
            return Err(anyhow!(
                "local source for {} missing at {} — refusing (sources must be git-tracked)",
                rec.flight_name,
                src_path.display()
            ));
        }
        let source = std::fs::read_to_string(&src_path)?;
        if sha256_hex(&source) != rec.source_sha256 {
            return Err(anyhow!(
                "local source digest mismatch for {} — {} copy drifted from the live manifest; restore it from git (or re-deploy knowingly) before deploying over it",
                rec.flight_name,
                flights_dir.display()
            ));
        }
    }
    Ok(())
}

/// Archive the current manifest to `history-<name>` (byte-exact copy) so the
/// rollback pointer the next manifest sets actually resolves. Before this
/// existed, `deploy` set `rollback` but never wrote the history file — a real
/// deploy→deploy→rollback sequence failed with "previous manifest not found"
/// (the E2E used to paper over it by hand-writing the history file).
pub fn archive_outgoing(manifest_dir: &Path, dag_id: &str) -> Result<()> {
    let manifest_path = manifest_dir.join(manifest_file_name(dag_id));
    if !manifest_path.is_file() {
        return Ok(());
    }
    let bytes = std::fs::read_to_string(&manifest_path)?;
    std::fs::create_dir_all(manifest_dir)?;
    std::fs::write(
        manifest_dir.join(format!("history-{}", manifest_file_name(dag_id))),
        bytes,
    )
    .with_context(|| format!("archiving outgoing manifest for dag {dag_id}"))?;
    Ok(())
}

/// Read-only integrity probe (no token, no network): rebuild the program-set
/// seal from the local `flights/` copies and compare against the sealed hash
/// in the manifest via [`SwapGate`]. Manifests that predate sealing are
/// refused explicitly — a missing seal must never read as "intact".
pub fn check(dag_id: &str, flights_dir: &Path, manifest_dir: &Path) -> Result<()> {
    let manifest_path = manifest_dir.join(manifest_file_name(dag_id));
    if !manifest_path.is_file() {
        return Err(anyhow!(
            "no manifest for dag {dag_id} at {} — nothing to check",
            manifest_path.display()
        ));
    }
    let manifest: DeployManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
    let Some(sealed) = &manifest.programs_hash else {
        return Err(anyhow!(
            "manifest for dag {dag_id} predates sealing (no programs_hash) — re-deploy to seal, then check"
        ));
    };
    let mut current = Vec::new();
    for rec in &manifest.flights {
        let src_path = source_path(flights_dir, &rec.flight_name);
        if !src_path.is_file() {
            return Err(anyhow!(
                "local source for {} missing at {} — cannot rebuild the seal",
                rec.flight_name,
                src_path.display()
            ));
        }
        current.push((rec.flight_name.clone(), std::fs::read(&src_path)?));
    }
    SwapGate::seal(sealed)
        .check(&verified_swap::source_hash(current))
        .map_err(anyhow::Error::msg)
}

/// Masked owner hint — first 4 chars of the token's sha256. Never the token.
fn owner_hint(token: &str) -> String {
    let hex = sha256_hex(token);
    format!("tok:{}", &hex[..8])
}

/// Persist compiled sources under `<flights_dir>/<flight_name>.py`.
fn persist_sources(flights_dir: &Path, programs: &[FlightProgram]) -> Result<()> {
    std::fs::create_dir_all(flights_dir)?;
    for p in programs {
        let path = flights_dir.join(format!("{}.py", sanitize_file_stem(&p.flight_name)));
        std::fs::write(&path, &p.source)
            .with_context(|| format!("writing flight source {}", path.display()))?;
    }
    Ok(())
}

fn sanitize_file_stem(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Deploy programs: create flights on the server, write sources + manifest.
/// `flights_dir` holds source copies; manifest goes to `manifest_dir`.
pub async fn deploy(
    programs: Vec<FlightProgram>,
    mode: &str,
    dag_id: &str,
    flights_dir: &Path,
    manifest_dir: &Path,
    endpoint: Option<String>,
    token: Option<String>,
) -> Result<DeployManifest> {
    // Gate first, network second: if a live manifest exists, its local source
    // copies must still match — we never ship over drifted state, and the
    // refusal costs zero MCP calls.
    let manifest_path = manifest_dir.join(manifest_file_name(dag_id));
    let previous: Option<DeployManifest> = if manifest_path.is_file() {
        let prev: DeployManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
        verify_local_sources(flights_dir, &prev)?;
        Some(prev)
    } else {
        None
    };

    let token = match token {
        Some(t) => t,
        None => HttpMcpClient::token_from_env()?,
    };
    let mut client = HttpMcpClient::connect(HttpMcpConfig {
        endpoint: endpoint.unwrap_or_else(|| "https://api.motherduck.com/mcp".into()),
        token: token.clone(),
        ..HttpMcpConfig::default()
    })
    .await?;

    persist_sources(flights_dir, &programs)?;

    let mut records = Vec::new();
    for p in &programs {
        let result = client
            .call_tool(
                "create_flight",
                serde_json::json!({
                    "name": p.flight_name,
                    "source": p.source,
                }),
            )
            .await
            .with_context(|| format!("create_flight failed for {}", p.flight_name))?;
        let text = result["content"][0]["text"].as_str().unwrap_or("");
        let flight_id = serde_json::from_str::<serde_json::Value>(text)
            .ok()
            .and_then(|v| v["flight_id"].as_str().map(|s| s.to_string()));
        records.push(FlightRecord {
            flight_id,
            flight_name: p.flight_name.clone(),
            version: 1,
            source_sha256: p.source_sha256.clone(),
        });
    }

    let manifest = DeployManifest {
        dag_id: dag_id.to_string(),
        mode: mode.to_string(),
        deployed_at: chrono::Utc::now().to_rfc3339(),
        owner_hint: owner_hint(&token),
        flights: records,
        rollback: previous.map(|_m| manifest_file_name(dag_id)),
        programs_hash: Some(programs_hash(&programs)),
    };
    std::fs::create_dir_all(manifest_dir)?;
    // Archive the outgoing manifest so the rollback pointer above resolves.
    archive_outgoing(manifest_dir, dag_id)?;
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;
    Ok(manifest)
}

/// Rollback: restore the PREVIOUS manifest's sources via update_flight.
/// Refused when the current manifest has no rollback pointer.
pub async fn rollback(
    dag_id: &str,
    flights_dir: &Path,
    manifest_dir: &Path,
    endpoint: Option<String>,
    token: Option<String>,
) -> Result<DeployManifest> {
    let manifest_path = manifest_dir.join(manifest_file_name(dag_id));
    if !manifest_path.is_file() {
        return Err(anyhow!(
            "no manifest for dag {dag_id} at {} — nothing to roll back",
            manifest_path.display()
        ));
    }
    let current: DeployManifest = serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
    let Some(prev_name) = &current.rollback else {
        return Err(anyhow!(
            "dag {dag_id} has no previous deployment (rollback=null) — first deployment cannot be rolled back"
        ));
    };
    let prev_path = manifest_dir.join(format!("history-{prev_name}"));
    if !prev_path.is_file() {
        return Err(anyhow!(
            "previous manifest {} not found at {} — cannot roll back",
            prev_name,
            prev_path.display()
        ));
    }
    let prev: DeployManifest = serde_json::from_str(&std::fs::read_to_string(&prev_path)?)?;
    // Same mechanical gate as deploy: all rollback sources verified before
    // the first update_flight fires (no partial rollback on drift).
    verify_local_sources(flights_dir, &prev)?;

    let token = match token {
        Some(t) => t,
        None => HttpMcpClient::token_from_env()?,
    };
    let mut client = HttpMcpClient::connect(HttpMcpConfig {
        endpoint: endpoint.unwrap_or_else(|| "https://api.motherduck.com/mcp".into()),
        token,
        ..HttpMcpConfig::default()
    })
    .await?;

    // Before overwriting the manifest, archive the current one to history.
    std::fs::create_dir_all(manifest_dir)?;
    std::fs::write(
        manifest_dir.join(format!("history-{}", manifest_file_name(dag_id))),
        serde_json::to_string_pretty(&current)?,
    )?;

    for rec in &prev.flights {
        // Source comes from the local flights/ copy (digest-verified above).
        let src_path = flights_dir.join(format!("{}.py", sanitize_file_stem(&rec.flight_name)));
        let source = std::fs::read_to_string(&src_path)?;
        client
            .call_tool(
                "update_flight",
                serde_json::json!({
                    "flight_id": rec.flight_id,
                    "name": rec.flight_name,
                    "source": source,
                }),
            )
            .await
            .with_context(|| format!("update_flight failed for {}", rec.flight_name))?;
    }

    // Restore the previous manifest as current.
    let mut restored = prev.clone();
    restored.rollback = prev.rollback.clone();
    std::fs::write(&manifest_path, serde_json::to_string_pretty(&restored)?)?;
    Ok(restored)
}
