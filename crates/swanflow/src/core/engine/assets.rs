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

use chrono::Datelike;
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

/// Parse a 64-char lowercase hex digest back to bytes.
pub fn parse_digest_hex(hex: &str) -> Option<UpstreamDigest> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        bytes[i] = (hi * 16 + lo) as u8;
    }
    Some(UpstreamDigest { bytes })
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
        // Cheap existence probe first: if both tables exist, skip DDL —
        // running CREATE TABLE IF NOT EXISTS on a file left with an
        // un-checkpointed WAL (previous process died between write and
        // checkpoint) has produced DuckDB InternalException
        // (MarkBlockAsModified on an already-modified block).
        if conn
            .execute_batch("SELECT 1 FROM asset_runs LIMIT 1; SELECT 1 FROM asset_intervals LIMIT 1")
            .is_err()
        {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS asset_runs (
                     node_id TEXT PRIMARY KEY,
                     digest_hex TEXT NOT NULL,
                     last_run_at TEXT NOT NULL,
                     interval_start TEXT,
                     interval_end TEXT
                 );
                 CREATE TABLE IF NOT EXISTS asset_intervals (
                     node_id TEXT NOT NULL,
                     interval_start TEXT NOT NULL,
                     interval_end TEXT NOT NULL,
                     completed_at TEXT NOT NULL,
                     PRIMARY KEY (node_id, interval_start)
                 );",
            )?;
        }
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

    // ── Stage 2: interval tracking ────────────────────────────────────

    /// Mark one interval as completed for a node.
    pub fn record_interval(
        &self,
        node_id: Uuid,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT OR REPLACE INTO asset_intervals
                 (node_id, interval_start, interval_end, completed_at)
             VALUES (?1, ?2, ?3, ?4)",
            [
                node_id.to_string(),
                start.to_rfc3339(),
                end.to_rfc3339(),
                now,
            ],
        )?;
        Ok(())
    }

    /// Completed interval starts for a node, ascending.
    pub fn completed_intervals(
        &self,
        node_id: Uuid,
    ) -> anyhow::Result<Vec<chrono::DateTime<chrono::Utc>>> {
        let mut stmt = self.conn.prepare(
            "SELECT interval_start FROM asset_intervals WHERE node_id = ?1
             ORDER BY interval_start",
        )?;
        let rows = stmt.query_map([node_id.to_string()], |r| {
            r.get::<_, String>(0)
        })?;
        let mut out = Vec::new();
        for r in rows.flatten() {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&r) {
                out.push(dt.with_timezone(&chrono::Utc));
            }
        }
        Ok(out)
    }
}

/// A half-open daily interval [start, start + 1 day).
#[derive(Debug, Clone, PartialEq)]
pub struct DayInterval {
    pub start: chrono::NaiveDate,
}

impl DayInterval {
    pub fn end(&self) -> chrono::NaiveDate {
        self.start.succ_opt().unwrap_or(self.start)
    }
    pub fn start_dt(&self) -> chrono::DateTime<chrono::Utc> {
        self.start
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Utc)
            .unwrap()
    }
    pub fn end_dt(&self) -> chrono::DateTime<chrono::Utc> {
        self.end()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_local_timezone(chrono::Utc)
            .unwrap()
    }
}

/// Compute missing daily intervals in [from, to] given the completed
/// set — as contiguous runs (batches), SQLMesh-style set difference.
pub fn daily_gaps(
    completed: &[chrono::NaiveDate],
    from: chrono::NaiveDate,
    to: chrono::NaiveDate,
) -> Vec<(chrono::NaiveDate, chrono::NaiveDate)> {
    let done: std::collections::HashSet<chrono::NaiveDate> = completed.iter().copied().collect();
    let mut batches: Vec<(chrono::NaiveDate, chrono::NaiveDate)> = Vec::new();
    let mut cursor = Some(from);
    while let Some(d) = cursor {
        if d > to {
            break;
        }
        if done.contains(&d) {
            cursor = d.succ_opt();
            continue;
        }
        // start of a gap; extend while consecutive days are also missing
        let gap_start = d;
        let mut gap_end = d;
        let mut next = d.succ_opt();
        while let Some(n) = next {
            if n > to || done.contains(&n) {
                break;
            }
            gap_end = n;
            next = n.succ_opt();
        }
        batches.push((gap_start, gap_end));
        cursor = gap_end.succ_opt();
    }
    batches
}

/// Render a calendar view: ✅ done, ❌ missing (gap), ⚪ outside range.
pub fn render_calendar(
    completed: &[chrono::NaiveDate],
    from: chrono::NaiveDate,
    to: chrono::NaiveDate,
) -> String {
    use std::fmt::Write as _;
    let done: std::collections::HashSet<chrono::NaiveDate> = completed.iter().copied().collect();
    let mut out = String::new();
    // weekday header (Mo Tu We ...) starting at `from`'s weekday
    let mut d = from;
    let mut line = String::new();
    // pad to Monday-start
    let weekday = d.weekday().num_days_from_monday();
    for _ in 0..weekday {
        line.push_str("   ");
    }
    loop {
        if d > to {
            break;
        }
        let marker = if done.contains(&d) { "✅ " } else { "❌ " };
        line.push_str(marker);
        if d.weekday() == chrono::Weekday::Sun {
            let _ = writeln!(out, "{}", line);
            line = String::new();
        }
        d = d.succ_opt().unwrap_or(d);
    }
    if !line.trim().is_empty() {
        let _ = writeln!(out, "{}", line);
    }
    out
}

impl AssetLedger {
    /// Record a successful node run: computes the upstream digest (node
    /// id + type + config + upstream digests — transitive via recorded
    /// upstream digests when available) and writes it.
    ///
    /// Best-effort engine hook: failures are logged to stderr, never
    /// propagated.
    pub fn record_asset_run_for(
        &self,
        node_id: Uuid,
        type_id: &str,
        node_config_json: &str,
        upstream: &[(Uuid, String, String)],
    ) -> anyhow::Result<()> {
        // Prefer each upstream's *recorded* digest (captures transitive
        // staleness); fall back to hashing its identity+config.
        let ups: Vec<(Uuid, UpstreamDigest)> = upstream
            .iter()
            .map(|(id, ty, cfg)| {
                let recorded = self
                    .recorded_digest(*id)
                    .ok()
                    .flatten()
                    .and_then(|hex| parse_digest_hex(&hex))
                    .unwrap_or_else(|| compute_digest(*id, ty, cfg, &[]));
                (*id, recorded)
            })
            .collect();
        let refs: Vec<(Uuid, &UpstreamDigest)> =
            ups.iter().map(|(id, d)| (*id, d)).collect();
        let digest = compute_digest(node_id, type_id, node_config_json, &refs);
        self.record_run(node_id, &digest)
    }
}

/// Engine hook: open the ledger (creating lazily) and record a node run.
/// Best-effort — errors are logged, never propagated.
pub fn record_asset_run(
    cache_dir: &str,
    node_id: Uuid,
    type_id: &str,
    node_config_json: &str,
    upstream: &[(Uuid, String, String)],
) {
    let path = std::path::Path::new(cache_dir).join("assets.duckdb");
    let _ = std::fs::create_dir_all(cache_dir);
    let conn = match duckdb::Connection::open(&path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[assets] ledger open failed (ignored): {}", e);
            return;
        }
    };
    let mut needs_init = true;
    // Cheap existence probe: SELECT from the table; on failure run DDL.
    if conn.execute_batch("SELECT 1 FROM asset_runs LIMIT 1").is_ok() {
        needs_init = false;
    }
    if needs_init {
        let _ = conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS asset_runs (
                 node_id TEXT PRIMARY KEY,
                 digest_hex TEXT NOT NULL,
                 last_run_at TEXT NOT NULL,
                 interval_start TEXT,
                 interval_end TEXT
             );
             CREATE TABLE IF NOT EXISTS asset_intervals (
                 node_id TEXT NOT NULL,
                 interval_start TEXT NOT NULL,
                 interval_end TEXT NOT NULL,
                 completed_at TEXT NOT NULL,
                 PRIMARY KEY (node_id, interval_start)
             );",
        );
    }
    let ledger = AssetLedger { conn };
    if let Err(e) = ledger.record_asset_run_for(node_id, type_id, node_config_json, upstream) {
        eprintln!("[assets] ledger record failed (ignored): {}", e);
    }
}

/// Try to skip a fresh node: returns the parquet path when the node's
/// recorded digest matches the freshly recomputed one AND a cached
/// materialization (flow_metadata row + parquet file) exists.
///
/// `None` means "execute normally". Best-effort like the write side:
/// ledger/duckdb errors degrade to executing the node.
pub fn try_skip_fresh(
    cache_dir: &str,
    node_id: Uuid,
    type_id: &str,
    node_config_json: &str,
    upstream: &[(Uuid, String, String)],
    upstream_digests: &[(Uuid, UpstreamDigest)],
) -> Option<String> {
    let path = std::path::Path::new(cache_dir).join("assets.duckdb");
    if !path.exists() {
        return None;
    }
    let conn = match duckdb::Connection::open(&path) {
        Ok(c) => c,
        Err(_) => return None,
    };
    let ledger = AssetLedger { conn };
    let refs: Vec<(Uuid, &UpstreamDigest)> = upstream_digests
        .iter()
        .map(|(id, d)| (*id, d))
        .collect();
    let recomputed = compute_digest(node_id, type_id, node_config_json, &refs);
    let recorded = ledger.recorded_digest(node_id).ok()??;
    if recorded != recomputed.hex() {
        return None;
    }
    // Digest matches — is there a cached materialization to reuse?
    let metadata_path = std::path::Path::new(cache_dir).join("metadata.duckdb");
    let conn = duckdb::Connection::open(&metadata_path).ok()?;
    let parquet_path: Option<String> = conn
        .query_row(
            "SELECT parquet_path FROM flow_metadata WHERE node_id = ? AND parquet_path != ''",
            [&node_id.to_string()],
            |row| row.get(0),
        )
        .ok();
    let parquet_path = parquet_path?;
    if !std::path::Path::new(&parquet_path).exists() {
        return None;
    }
    Some(parquet_path)
}

/// Pure digest computation helper for chain bookkeeping (no ledger).
pub fn recompute_digest(
    node_id: Uuid,
    type_id: &str,
    node_config_json: &str,
    upstream_digests: &[(Uuid, UpstreamDigest)],
) -> Option<UpstreamDigest> {
    let refs: Vec<(Uuid, &UpstreamDigest)> = upstream_digests
        .iter()
        .map(|(id, d)| (*id, d))
        .collect();
    Some(compute_digest(node_id, type_id, node_config_json, &refs))
}

/// Per-node staleness verdict for `assets status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssetVerdict {
    /// Recorded digest matches the recomputed one.
    Fresh,
    /// No ledger row for this node.
    NeverRun,
    /// Node's own config/type changed.
    StaleConfig,
    /// An upstream is stale/never-run, so this node's recorded digest
    /// no longer reflects its true inputs (digest chain mismatch).
    StaleUpstream,
}

impl AssetVerdict {
    pub fn label(&self) -> &'static str {
        match self {
            AssetVerdict::Fresh => "fresh",
            AssetVerdict::NeverRun => "never-run",
            AssetVerdict::StaleConfig => "stale-config",
            AssetVerdict::StaleUpstream => "stale-upstream",
        }
    }
}

/// Recompute the digest chain for a workflow (topological order) and
/// classify every node against the ledger.
///
/// Mirrors the engine hook's semantics exactly: a node's digest hashes
/// its id/type/config plus each upstream's *recomputed* digest (here we
/// have no recorded-digest shortcut — we walk the chain, which is the
/// conservative/authoritative view for "would rerunning change outputs?").
pub fn status_for_workflow(
    ledger: &AssetLedger,
    workflow: &crate::core::graph::Workflow,
) -> anyhow::Result<Vec<(Uuid, String, AssetVerdict)>> {
    use petgraph::algo::toposort;
    use std::collections::HashMap;

    let sorted = toposort(&workflow.graph, None)
        .map_err(|e| anyhow::anyhow!("Cycle detected in workflow: {:?}", e))?;

    let mut digest_map: HashMap<Uuid, UpstreamDigest> = HashMap::new();
    let mut verdicts: HashMap<Uuid, AssetVerdict> = HashMap::new();
    let mut out: Vec<(Uuid, String, AssetVerdict)> = Vec::new();

    for idx in sorted {
        let node = &workflow.graph[idx];
        let upstream: Vec<(Uuid, UpstreamDigest)> = workflow
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .filter_map(|u| {
                let un = &workflow.graph[u];
                digest_map.get(&un.id).map(|d| (un.id, d.clone()))
            })
            .collect();
        let refs: Vec<(Uuid, &UpstreamDigest)> =
            upstream.iter().map(|(id, d)| (*id, d)).collect();
        let digest = compute_digest(
            node.id,
            &node.type_id,
            &node.config.to_string(),
            &refs,
        );
        digest_map.insert(node.id, digest.clone());

        let verdict = match ledger.recorded_digest(node.id)? {
            None => AssetVerdict::NeverRun,
            Some(recorded) => {
                if recorded == digest.hex() {
                    AssetVerdict::Fresh
                } else {
                    // Distinguish: is it my own config, or an upstream?
                    // Recompute with empty upstream set: if that part
                    // alone differs from a same-computed recorded
                    // baseline we cannot know — approximate by checking
                    // whether any upstream is non-fresh.
                    let upstream_stale = workflow
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .any(|u| {
                            let un = &workflow.graph[u];
                            verdicts.get(&un.id) != Some(&AssetVerdict::Fresh)
                        });
                    if upstream_stale {
                        AssetVerdict::StaleUpstream
                    } else {
                        AssetVerdict::StaleConfig
                    }
                }
            }
        };
        verdicts.insert(node.id, verdict.clone());
        out.push((node.id, node.type_id.clone(), verdict));
    }
    Ok(out)
}


#[cfg(test)]
mod tests {
    use super::*;

    fn d(bytes: u8) -> UpstreamDigest {
        UpstreamDigest { bytes: [bytes; 32] }
    }

    #[test]
    fn daily_gaps_contiguous_batching() {
        use chrono::NaiveDate;
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        // done: 1st, 4th, 5th, 8th — range 1..=8
        let done = vec![d("2026-09-01"), d("2026-09-04"), d("2026-09-05"), d("2026-09-08")];
        let gaps = daily_gaps(&done, d("2026-09-01"), d("2026-09-08"));
        // expect: [2..=3] batch, [6..=7] batch
        assert_eq!(gaps.len(), 2);
        assert_eq!(gaps[0], (d("2026-09-02"), d("2026-09-03")));
        assert_eq!(gaps[1], (d("2026-09-06"), d("2026-09-07")));
    }

    #[test]
    fn daily_gaps_empty_and_full() {
        use chrono::NaiveDate;
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        // nothing done -> one contiguous batch covering the whole range
        let gaps = daily_gaps(&[], d("2026-09-01"), d("2026-09-03"));
        assert_eq!(gaps, vec![(d("2026-09-01"), d("2026-09-03"))]);
        // everything done -> no gaps
        let done = vec![d("2026-09-01"), d("2026-09-02"), d("2026-09-03")];
        assert!(daily_gaps(&done, d("2026-09-01"), d("2026-09-03")).is_empty());
    }

    #[test]
    fn daily_gaps_ignores_completed_outside_range() {
        use chrono::NaiveDate;
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        let done = vec![d("2026-08-31"), d("2026-09-02")];
        let gaps = daily_gaps(&done, d("2026-09-01"), d("2026-09-03"));
        assert_eq!(gaps, vec![(d("2026-09-01"), d("2026-09-01")), (d("2026-09-03"), d("2026-09-03"))]);
    }

    #[test]
    fn render_calendar_marks_done_and_missing() {
        use chrono::NaiveDate;
        let d = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
        let done = vec![d("2026-09-01")];
        let cal = render_calendar(&done, d("2026-09-01"), d("2026-09-02"));
        assert!(cal.contains('✅') && cal.contains('❌'), "cal: {cal}");
    }

    #[test]
    fn try_skip_fresh_roundtrip() {
        let dir = std::env::temp_dir().join(format!("swanflow_skip_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let dir_s = dir.to_str().unwrap().to_string();

        // No ledger yet -> no skip
        assert!(try_skip_fresh(&dir_s, Uuid::nil(), "t", "{}", &[], &[]).is_none());

        // Record a run, then skip should find a matching parquet.
        let ledger = AssetLedger::open(std::path::Path::new(&dir_s)).unwrap();
        ledger.record_asset_run_for(Uuid::nil(), "t", "{}", &[]).unwrap();
        let parquet = dir.join("dummy.parquet");
        std::fs::write(&parquet, b"x").unwrap();
        let meta = duckdb::Connection::open(dir.join("metadata.duckdb").to_str().unwrap()).unwrap();
        meta.execute_batch(
            "CREATE TABLE IF NOT EXISTS flow_metadata (node_id TEXT PRIMARY KEY, table_name TEXT, parquet_path TEXT, schema_json TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
             INSERT OR REPLACE INTO flow_metadata (node_id, table_name, parquet_path, schema_json) VALUES ('00000000-0000-0000-0000-000000000000', 't', 'dummy.parquet', '{}');",
        ).unwrap();
        let got = try_skip_fresh(&dir_s, Uuid::nil(), "t", "{}", &[], &[]);
        assert!(got.is_none(), "relative parquet_path must not skip: {:?}", got);

        // Absolute path (as MaterializingStream records) -> skip
        let abs = parquet.to_str().unwrap().to_string();
        meta.execute_batch(&format!(
            "UPDATE flow_metadata SET parquet_path = '{}';", abs
        ))
        .unwrap();
        drop(meta); // release the file lock before re-opening inside try_skip_fresh
        let got = try_skip_fresh(&dir_s, Uuid::nil(), "t", "{}", &[], &[]);
        assert_eq!(got.as_deref(), Some(abs.as_str()));

        // Changed config -> no skip
        assert!(try_skip_fresh(&dir_s, Uuid::nil(), "t", "{\"a\":1}", &[], &[]).is_none());

        let _ = std::fs::remove_dir_all(&dir);
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
