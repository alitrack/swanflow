//! Flight deployer — write-side client: create/update flights over MCP,
//! local `flights/` source copies, chained manifests with rollback.
//!
//! Reuses `crate::mcp::http::HttpMcpClient`; the mock-server E2E asserts the
//! call shapes without touching api.motherduck.com (token-free, per user
//! directive the live path is explicitly unverified).

use super::{DeployManifest, FlightProgram, FlightRecord};
use crate::mcp::http::{HttpMcpClient, HttpMcpConfig};
use anyhow::{anyhow, Context, Result};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub fn manifest_file_name(dag_id: &str) -> String {
    format!("swanflow-deploy-{dag_id}.json")
}

fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
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
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
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

    // Chain: previous manifest (if any) becomes our rollback pointer.
    let manifest_path = manifest_dir.join(manifest_file_name(dag_id));
    let previous: Option<DeployManifest> = if manifest_path.is_file() {
        Some(serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?)
    } else {
        None
    };

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
        rollback: previous.map(|m| manifest_file_name(dag_id)),
    };
    std::fs::create_dir_all(manifest_dir)?;
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest)?,
    )?;
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
    let current: DeployManifest =
        serde_json::from_str(&std::fs::read_to_string(&manifest_path)?)?;
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
        // Source must come from the local flights/ copy, verified by digest.
        let src_path = flights_dir.join(format!("{}.py", sanitize_file_stem(&rec.flight_name)));
        if !src_path.is_file() {
            return Err(anyhow!(
                "rollback source for {} missing at {} — refusing (sources must be git-tracked)",
                rec.flight_name,
                src_path.display()
            ));
        }
        let source = std::fs::read_to_string(&src_path)?;
        if sha256_hex(&source) != rec.source_sha256 {
            return Err(anyhow!(
                "rollback source digest mismatch for {} — local flights/ copy drifted from manifest",
                rec.flight_name
            ));
        }
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
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&restored)?,
    )?;
    Ok(restored)
}
