//! Materialization ledger (ADR-014 Stage 1).
//!
//! DuckDB-backed ledger tracking what has been materialized and *why it
//! might be stale*: an `upstream_digest` (node config + upstream digests
//! + input table fingerprints) recorded on every successful run. A later
//! run with a matching digest may skip re-materializing; a mismatch (or
//! missing row) means stale.
//!
//! Opt-in governance: the ledger lives in `<cache_dir>/assets.duckdb`
//! (same directory governance as the extfn catalog, ADR won at
//! `623804a`). It is created lazily on first write.

use std::path::Path;

use uuid::Uuid;

/// Digest of everything a node's output depends on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamDigest {
    pub bytes: [u8; 32],
}

impl UpstreamDigest {
    pub fn hex(&self) -> String {
        self.bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// Compute a node's digest from its config JSON and its upstream
/// dependencies' digests (transitive staleness: any upstream change
/// bubbles down).
pub fn compute_digest(
    node_id: Uuid,
    type_id: &str,
    config_json: &str,
    upstream: &[(Uuid, &UpstreamDigest)],
) -> UpstreamDigest {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(node_id.as_bytes());
    h.update(type_id.as_bytes());
    h.update(config_json.as_bytes());
    // Order-independent: sort upstream by id for deterministic digests.
    let mut ups: Vec<(Uuid, &[u8; 32])> = upstream
        .iter()
        .map(|(id, d)| (*id, &d.bytes))
        .collect();
    ups.sort_by_key(|(id, _)| *id);
    for (id, digest) in ups {
        h.update(id.as_bytes());
        h.update(digest);
    }
    UpstreamDigest { bytes: h.finalize().into() }
}

/// One row of asset status (for `swanflow assets list`).
#[derive(Debug, Clone)]
pub struct AssetStatus {
    pub node_id: Uuid,
    pub name: Option<String>,
    pub digest_hex: String,
    pub last_run_at: Option<String>,
    pub status: String, // "materialized" | "stale-config" | "stale-upstream" | "never-run"
}

/// DuckDB-backed materialization ledger.
pub struct AssetLedger {
    conn: duckdb::Connection,
}

impl AssetLedger {
    /// Open (creating if absent) the ledger under `<cache_dir>/assets.duckdb`.
    pub fn open(cache_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(cache_dir)?;
        let path = cache_dir.join("assets.duckdb");
        let conn = duckdb::Connection::open(&path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS asset_runs (
                 node_id TEXT PRIMARY KEY,
                 digest_hex TEXT NOT NULL,
                 last_run_at TEXT NOT NULL,
                 interval_start TEXT,
                 interval_end TEXT
             );",
        )?;
        Ok(Self { conn })
    }

    /// Last recorded digest for a node, if any.
    pub fn recorded_digest(&self, node_id: Uuid) -> anyhow::Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare("SELECT digest_hex FROM asset_runs WHERE node_id = ?1")?;
        let mut rows = stmt.query([node_id.to_string()])?;
        Ok(rows.next()?.map(|r| r.get::<_, String>(0)).transpose()?)
    }

    /// Record (or overwrite) a successful run for a node.
    pub fn record_run(&self, node_id: Uuid, digest: &UpstreamDigest) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT OR REPLACE INTO asset_runs (node_id, digest_hex, last_run_at)
             VALUES (?1, ?2, ?3)",
            [node_id.to_string(), digest.hex(), now],
        )?;
        Ok(())
    }

    /// All recorded rows (for `assets list`).
    pub fn all(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        let mut stmt = self
            .conn
            .prepare("SELECT node_id, digest_hex, last_run_at FROM asset_runs ORDER BY node_id")?;
        let rows = stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn d(bytes: u8) -> UpstreamDigest {
        UpstreamDigest { bytes: [bytes; 32] }
    }

    #[test]
    fn digest_depends_on_config() {
        let id = Uuid::new_v4();
        let a = compute_digest(id, "core.sql.duckdb", "{\"query\":\"SELECT 1\"}", &[]);
        let b = compute_digest(id, "core.sql.duckdb", "{\"query\":\"SELECT 2\"}", &[]);
        assert_ne!(a, b);
    }

    #[test]
    fn digest_transitive_upstream() {
        let id = Uuid::new_v4();
        let up = Uuid::new_v4();
        let d1 = compute_digest(id, "t", "c", &[(up, &d(1))]);
        let d2 = compute_digest(id, "t", "c", &[(up, &d(2))]);
        assert_ne!(d1, d2, "upstream digest change must propagate");
    }

    #[test]
    fn digest_upstream_order_independent() {
        let id = Uuid::new_v4();
        let u1 = Uuid::new_v4();
        let u2 = Uuid::new_v4();
        let a = compute_digest(id, "t", "c", &[(u1, &d(1)), (u2, &d(2))]);
        let b = compute_digest(id, "t", "c", &[(u2, &d(2)), (u1, &d(1))]);
        assert_eq!(a, b);
    }

    #[test]
    fn ledger_roundtrip() {
        let dir = std::env::temp_dir().join(format!("swanflow_ledger_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ledger = AssetLedger::open(&dir).unwrap();
        let id = Uuid::new_v4();
        assert!(ledger.recorded_digest(id).unwrap().is_none());
        ledger.record_run(id, &d(7)).unwrap();
        assert_eq!(ledger.recorded_digest(id).unwrap().as_deref(), Some(d(7).hex().as_str()));
        // overwrite
        ledger.record_run(id, &d(8)).unwrap();
        assert_eq!(ledger.recorded_digest(id).unwrap().as_deref(), Some(d(8).hex().as_str()));
        let all = ledger.all().unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, id.to_string());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
