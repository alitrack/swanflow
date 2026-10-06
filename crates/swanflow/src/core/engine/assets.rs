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
use fs2::FileExt;
use uuid::Uuid;

/// Cross-process coordination for the asset ledger (codex-review P0:
/// concurrent writers to assets.duckdb — daemon sensor + manual run +
/// backfill CLI — have produced DuckDB SerializationException).
///
/// Two layers:
/// 1. In-process: a global Mutex serializes ALL ledger connections in
///    this process (the observed conflict was same-process: two
///    independent `Connection`s writing one file).
/// 2. Cross-process: an advisory exclusive flock on `<cache>/assets.lock`
///    (fs2). Held for the guard's lifetime; a second process waits
///    instead of corrupting the file. Dropping the File releases it.
pub(crate) static LEDGER_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Current digest algorithm version (bump when compute_digest's framing
/// or inputs change — mirrors the version tag inside compute_digest).
pub const DIGEST_VERSION: u32 = 2;

thread_local! {
    /// Re-entrancy depth for LedgerGuard::acquire. AssetLedger write
    /// helpers acquire the guard themselves; record_asset_run holds an
    /// outer guard across open+write. A plain Mutex would self-deadlock
    /// on the nested acquire, so the in-process mutex is only taken at
    /// depth 0 (the flock is per-fd and likewise only taken once).
    static GUARD_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
    /// Identity of the cache dir the OUTERMOST guard locked (canonical
    /// path). Nested acquires must target the SAME cache — a nested
    /// guard for a different dir would inherit "exclusive" and write a
    /// ledger its flock does not cover (codex-review P2 r2).
    static GUARD_CACHE_KEY: std::cell::RefCell<Option<std::path::PathBuf>> =
        const { std::cell::RefCell::new(None) };
    /// Whether the OUTERMOST guard on this thread actually holds the
    /// cross-process flock. Nested guards inherit this value; write
    /// paths refuse to proceed when it is false (codex-review P0:
    /// never write the ledger uncoordinated).
    static GUARD_EXCLUSIVE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// RAII guard: in-process mutex + cross-process advisory lock.
/// Re-entrant within a thread (see GUARD_DEPTH).
pub(crate) struct LedgerGuard {
    _pm: Option<std::sync::MutexGuard<'static, ()>>,
    _file: Option<std::fs::File>,
    exclusive: bool,
}

impl LedgerGuard {
    /// True when the cross-process flock is actually held (or this is a
    /// nested guard under an outer one that holds it). Write paths MUST
    /// refuse to proceed when this is false.
    pub(crate) fn exclusive(&self) -> bool {
        self.exclusive
    }

    /// Blocking acquire with a 30s deadline. On timeout or lock-file
    /// open failure the in-process mutex is still held (same-process
    /// writes stay serialized) but `exclusive()` is false and callers
    /// must skip/fail the write — proceeding without the cross-process
    /// lock is exactly the corruption this guard exists to prevent.
    pub(crate) fn acquire(cache_dir: &Path) -> Self {
        Self::acquire_with_deadline(cache_dir, std::time::Duration::from_secs(30))
    }

    pub(crate) fn acquire_with_deadline(cache_dir: &Path, timeout: std::time::Duration) -> Self {
        let depth = GUARD_DEPTH.with(|d| d.get());
        if depth > 0 {
            // Nested acquire on this thread — outer guard owns both
            // layers; inherit its exclusivity so inner write checks see
            // the outer truth. BUT only when it guards the SAME cache:
            // a nested guard for a different cache dir would inherit
            // "exclusive" and write a ledger its flock does not cover.
            // Degrade to non-exclusive (write refused) instead.
            let same_cache = GUARD_CACHE_KEY
                .with(|k| k.borrow().as_ref().map(|c| c == cache_dir).unwrap_or(false));
            let inherited_exclusive = if same_cache {
                GUARD_EXCLUSIVE.with(|e| e.get())
            } else {
                tracing::error!(
                    "nested LedgerGuard targets a different cache dir ({}) than the outer guard — treating as non-exclusive (write refused)",
                    cache_dir.display()
                );
                false
            };
            GUARD_DEPTH.with(|d| d.set(depth + 1));
            return Self {
                _pm: None,
                _file: None,
                exclusive: inherited_exclusive,
            };
        }
        GUARD_DEPTH.with(|d| d.set(1));
        GUARD_EXCLUSIVE.with(|e| e.set(false));
        GUARD_CACHE_KEY.with(|k| *k.borrow_mut() = Some(cache_dir.to_path_buf()));
        let pm = LEDGER_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let _ = std::fs::create_dir_all(cache_dir);
        let lock_path = cache_dir.join("assets.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .ok();
        let Some(file) = file else {
            // Lock file unopenable (read-only cache dir?) — writes must
            // be refused; readers may still proceed via open()'s Err.
            tracing::error!("assets.lock unopenable — ledger writes refused");
            return Self { _pm: Some(pm), _file: None, exclusive: false };
        };
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match file.try_lock_exclusive() {
                Ok(()) => {
                    GUARD_EXCLUSIVE.with(|e| e.set(true));
                    return Self { _pm: Some(pm), _file: Some(file), exclusive: true };
                }
                Err(_) if std::time::Instant::now() >= deadline => {
                    tracing::error!(
                        "assets.lock busy >{:?} — refusing uncoordinated ledger write",
                        timeout
                    );
                    return Self { _pm: Some(pm), _file: Some(file), exclusive: false };
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
    }
}

impl Drop for LedgerGuard {
    fn drop(&mut self) {
        let depth = GUARD_DEPTH.with(|d| d.get());
        if depth > 0 {
            GUARD_DEPTH.with(|d| d.set(depth - 1));
            if depth == 1 {
                GUARD_EXCLUSIVE.with(|e| e.set(false));
                GUARD_CACHE_KEY.with(|k| *k.borrow_mut() = None);
            }
        }
    }
}

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

/// Canonicalize a JSON string for digesting: recursively sort object
/// keys so a user merely re-ordering YAML/JSON keys does not produce a
/// phantom-stale digest (serde_json/preserve_order keeps insertion
/// order, which `config.to_string()` bakes into the hash). Unparseable
/// input is returned unchanged (digest still changes with content).
fn canonicalize_json_str(s: &str) -> String {
    fn sorted(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let mut keys: Vec<&String> = m.keys().collect();
                keys.sort();
                let mut out = serde_json::Map::new();
                for k in keys {
                    out.insert(k.clone(), sorted(&m[k]));
                }
                serde_json::Value::Object(out)
            }
            serde_json::Value::Array(a) => {
                serde_json::Value::Array(a.iter().map(sorted).collect())
            }
            other => other.clone(),
        }
    }
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(v) => sorted(&v).to_string(),
        Err(_) => s.to_string(),
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
    // Version tag + length-prefixed fields (framing v2): raw concatenation
    // of type/config bytes is ambiguous — ("ab","c") and ("a","bc") hash
    // identically. Note: v2 framing + canonicalized config invalidate
    // digests recorded by older builds; affected nodes re-materialize once.
    h.update(b"swanflow-digest-v2");
    let canon = canonicalize_json_str(config_json);
    let mut upd = |h: &mut Sha256, b: &[u8]| {
        h.update(&(b.len() as u64).to_le_bytes());
        h.update(b);
    };
    upd(&mut h, node_id.as_bytes());
    upd(&mut h, type_id.as_bytes());
    upd(&mut h, canon.as_bytes());
    // codex-review P0 (domain C): backfill binds ${interval_start}/
    // ${interval_end} from env at execution time, but the raw config is
    // identical across days — without the interval in the digest every
    // day hashes the same and skip-if-fresh reuses day 1's parquet for
    // all days. Env unset (normal runs) hashes the empty string.
    let iv_start = std::env::var("SWANFLOW_INTERVAL_START").unwrap_or_default();
    let iv_end = std::env::var("SWANFLOW_INTERVAL_END").unwrap_or_default();
    upd(&mut h, iv_start.as_bytes());
    upd(&mut h, iv_end.as_bytes());
    // Order-independent: sort upstream by id for deterministic digests.
    let mut ups: Vec<(Uuid, &[u8; 32])> = upstream
        .iter()
        .map(|(id, d)| (*id, &d.bytes))
        .collect();
    ups.sort_by_key(|(id, _)| *id);
    for (id, digest) in ups {
        upd(&mut h, id.as_bytes());
        upd(&mut h, digest);
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
    /// Kept so write paths can re-acquire the cross-process LedgerGuard.
    cache_dir: std::path::PathBuf,
}

impl AssetLedger {
    /// Open (creating if absent) the ledger under `<cache_dir>/assets.duckdb`.
    ///
    /// Callers keep the returned ledger around while doing reads; the
    /// cross-process `LedgerGuard` is NOT held for the ledger's whole
    /// life (that would serialize readers). Instead, WRITE paths
    /// (`record_run`/`record_interval`/first-DDL) re-acquire the guard
    /// per operation — see `with_write_guard`.
    pub fn open(cache_dir: &Path) -> anyhow::Result<Self> {
        std::fs::create_dir_all(cache_dir)?;
        let path = cache_dir.join("assets.duckdb");
        let guard = LedgerGuard::acquire(cache_dir);
        if !guard.exclusive() {
            // codex-review P0: opening for the write path (DDL possible)
            // without the cross-process flock is how corruption starts.
            // Read callers handle the Err by degrading (tri-state /
            // execute-normally), never by proceeding uncoordinated.
            anyhow::bail!("assets.lock not acquired — refusing ledger open");
        }
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
        // codex-review P1 (r2): DuckDB checkpoints lazily on connection
        // DROP — which happens when the CALLER drops the ledger, outside
        // any guard. Flush now, inside the guard, so the later drop has
        // nothing dirty to write (an uncoordinated checkpoint racing
        // another process's write is the corruption we must prevent).
        let _ = conn.execute_batch("CHECKPOINT");
        drop(guard);
        Ok(Self { conn, cache_dir: cache_dir.to_path_buf() })
    }

    /// Serialize a write against other processes (and our own process's
    /// other connections) for the duration of `f`.
    ///
    /// codex-review P1 (r2): DuckDB connections opened on the same file
    /// in one process are INDEPENDENT instances with private buffer
    /// caches — a connection opened before another's checkpoint holds a
    /// stale view, and checkpointing it later writes that stale view
    /// over the file (observed: 8 guarded writers -> 1 row survived).
    /// Therefore writes run on an OPERATION-SCOPED connection opened
    /// INSIDE the guard (sees every prior guarded checkpoint), flushed
    /// with CHECKPOINT and dropped before the guard releases. The
    /// long-lived read connection never checkpoints anything dirty.
    fn with_write_guard<T>(&self, f: impl FnOnce(&Self) -> anyhow::Result<T>) -> anyhow::Result<T> {
        let _guard = LedgerGuard::acquire(&self.cache_dir);
        if !_guard.exclusive() {
            // P0: never write uncoordinated — fail the operation; callers
            // are best-effort loggers, execution itself is unaffected.
            anyhow::bail!("assets.lock not acquired — refusing ledger write");
        }
        let path = self.cache_dir.join("assets.duckdb");
        let scoped = duckdb::Connection::open(&path)?;
        // First-use DDL (existence probe pattern from open()).
        if scoped
            .execute_batch("SELECT 1 FROM asset_runs LIMIT 1; SELECT 1 FROM asset_intervals LIMIT 1")
            .is_err()
        {
            scoped.execute_batch(
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
        let writer = AssetLedger { conn: scoped, cache_dir: self.cache_dir.clone() };
        let r = f(&writer);
        // Flush inside the guard, then drop the scoped connection still
        // inside it — the lazy drop-checkpoint stays coordinated too.
        let _ = writer.conn.execute_batch("CHECKPOINT");
        r
    }

    /// Last recorded digest for a node, if any.
    ///
    /// Runs on a guard-scoped fresh connection: the long-lived read
    /// connection's private buffer may predate another writer's
    /// checkpoint (codex-review P1 r2 — stale view across connections).
    pub fn recorded_digest(&self, node_id: Uuid) -> anyhow::Result<Option<String>> {
        self.with_write_guard(|this| {
            let mut stmt = this
                .conn
                .prepare("SELECT digest_hex FROM asset_runs WHERE node_id = ?1")?;
            let mut rows = stmt.query([node_id.to_string()])?;
            Ok(rows.next()?.map(|r| r.get::<_, String>(0)).transpose()?)
        })
    }

    /// Record (or overwrite) a successful run for a node.
    pub fn record_run(&self, node_id: Uuid, digest: &UpstreamDigest) -> anyhow::Result<()> {
        let now = chrono::Utc::now().to_rfc3339();
        self.with_write_guard(|this| {
            this.conn.execute(
                "INSERT OR REPLACE INTO asset_runs (node_id, digest_hex, last_run_at)
                 VALUES (?1, ?2, ?3)",
                [node_id.to_string(), digest.hex(), now],
            )?;
            Ok(())
        })
    }

    /// Last successful materialization time for a node (None = never).
    /// Guard-scoped fresh connection (see recorded_digest).
    pub fn last_materialized_at(&self, node_id: Uuid) -> anyhow::Result<Option<chrono::DateTime<chrono::Utc>>> {
        self.with_write_guard(|this| {
            let mut stmt = this
                .conn
                .prepare("SELECT last_run_at FROM asset_runs WHERE node_id = ?1")?;
            let mut rows = stmt.query([node_id.to_string()])?;
            Ok(rows
                .next()?
                .map(|r| r.get::<_, String>(0))
                .transpose()?
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                .map(|t| t.with_timezone(&chrono::Utc)))
        })
    }

    /// All recorded rows (for `assets list`). Guard-scoped fresh
    /// connection (see recorded_digest).
    pub fn all(&self) -> anyhow::Result<Vec<(String, String, String)>> {
        self.with_write_guard(|this| {
            let mut stmt = this
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
        })
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
        self.with_write_guard(|this| {
            this.conn.execute(
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
        })
    }

    /// Completed interval starts for a node, ascending. Guard-scoped
    /// fresh connection (see recorded_digest).
    pub fn completed_intervals(
        &self,
        node_id: Uuid,
    ) -> anyhow::Result<Vec<chrono::DateTime<chrono::Utc>>> {
        self.with_write_guard(|this| {
            let mut stmt = this.conn.prepare(
                "SELECT interval_start FROM asset_intervals WHERE node_id = ?1
                 ORDER BY interval_start",
            )?;
            let rows = stmt.query_map([node_id.to_string()], |r| r.get::<_, String>(0))?;
            let mut out = Vec::new();
            for r in rows.flatten() {
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&r) {
                    out.push(dt.with_timezone(&chrono::Utc));
                }
            }
            Ok(out)
        })
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
    // Serialize against other ledger writers (same process and other
    // processes) for the whole open→DDL→write window.
    let _guard = LedgerGuard::acquire(std::path::Path::new(cache_dir));
    if !_guard.exclusive() {
        // P0: never write uncoordinated. Best-effort hook: log and skip.
        eprintln!("[assets] ledger lock unavailable — run not recorded");
        return;
    }
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
    // Flush this connection's DDL NOW: with_write_guard writes via a
    // separate operation-scoped instance, and THIS connection's lazy
    // drop-checkpoint would otherwise write its stale view (no rows)
    // over the guarded writer's checkpoint (observed cross-process row
    // loss). After an explicit CHECKPOINT it is clean and its drop is a
    // no-op write-wise.
    let _ = conn.execute_batch("CHECKPOINT");
    let ledger = AssetLedger { conn, cache_dir: std::path::PathBuf::from(cache_dir) };
    if let Err(e) = ledger.record_asset_run_for(node_id, type_id, node_config_json, upstream) {
        eprintln!("[assets] ledger record failed (ignored): {}", e);
    }
    // record_asset_run_for flushed via with_write_guard; drop the
    // connection now while the outer guard is still held so any residual
    // lazy checkpoint also stays coordinated.
    drop(ledger);
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
    // Read-mostly, but the open itself can run DDL on first use — hold
    // the guard for open + digest comparison only.
    let _guard = LedgerGuard::acquire(std::path::Path::new(cache_dir));
    if !_guard.exclusive() {
        // P0: can't coordinate — fail safe to executing the node
        // (no skip decision based on an uncoordinated read).
        return None;
    }
    let conn = match duckdb::Connection::open(&path) {
        Ok(c) => c,
        Err(_) => return None,
    };
    let ledger = AssetLedger { conn, cache_dir: std::path::PathBuf::from(cache_dir) };
    // codex-review P1: skip only on a COMPLETE upstream digest set. A
    // missing upstream (filter_map upstream of us silently dropped it)
    // must not let a partial chain accidentally match a stale recorded
    // digest — fail safe to executing the node.
    if upstream.len() != upstream_digests.len() {
        return None;
    }
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

    #[test]
    fn digest_ignores_json_key_order() {
        // P2: preserve_order bakes insertion order into config.to_string();
        // merely re-ordering keys must NOT change the digest (phantom stale).
        let id = Uuid::nil();
        let a = compute_digest(id, "t", r#"{"x":1,"y":{"b":2,"a":3}}"#, &[]);
        let b = compute_digest(id, "t", r#"{"y":{"a":3,"b":2},"x":1}"#, &[]);
        assert_eq!(a.hex(), b.hex(), "key order must not matter");
        // But a real content change must still be detected.
        let c = compute_digest(id, "t", r#"{"x":1,"y":{"b":2,"a":4}}"#, &[]);
        assert_ne!(a.hex(), c.hex());
        // Non-JSON config degrades gracefully (hashed as-is).
        let d = compute_digest(id, "t", "plain-sql: SELECT 1", &[]);
        let e = compute_digest(id, "t", "plain-sql: SELECT 2", &[]);
        assert_ne!(d.hex(), e.hex());
    }

    #[test]
    fn digest_framing_disambiguates_concatenations() {
        // P2: length-prefix prevents ("ab","c") vs ("a","bc") collisions.
        let id = Uuid::nil();
        // type_id "ab" + config "c" vs type_id "a" + config "bc"
        let a = compute_digest(id, "ab", "c", &[]);
        let b = compute_digest(id, "a", "bc", &[]);
        assert_ne!(a.hex(), b.hex(), "framing must disambiguate field splits");
    }

    #[test]
    fn ledger_write_refused_when_lock_held_externally() {
        // P0: a foreign flock holder must make OUR write path refuse to
        // proceed, never write uncoordinated. Hold ONLY the flock (raw
        // fs2 lock, not LedgerGuard — that would also take the in-process
        // mutex and deadlock the spawned thread's acquire).
        use fs2::FileExt as _;
        let dir = std::env::temp_dir().join(format!("swanflow_lockref_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join("assets.lock"))
            .unwrap();
        lock_file.try_lock_exclusive().unwrap();

        let dir_s = dir.to_str().unwrap().to_string();
        std::thread::spawn(move || {
            // Flock is per-fd: a different fd on the same file blocks.
            let g = LedgerGuard::acquire_with_deadline(
                std::path::Path::new(&dir_s),
                std::time::Duration::from_millis(50),
            );
            assert!(!g.exclusive(), "deadline must expire non-exclusive");
            // The guarded write path must refuse: open() bails.
            let r = AssetLedger::open(std::path::Path::new(&dir_s));
            assert!(r.is_err(), "ledger open must be refused without flock");
        })
        .join()
        .unwrap();

        drop(lock_file);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn digest_changes_with_interval_env() {
        // codex-review P0 (domain C): backfill binds ${interval_start} at
        // execution time; the digest must see the interval or every day
        // reuses day 1's parquet under SWANFLOW_SKIP_FRESH=1.
        let id = Uuid::nil();
        let d1 = UpstreamDigest { bytes: [1; 32] };

        // Baseline with env unset — must equal the historical format
        // (empty SHA-256 updates are no-ops, so old ledger rows still hit).
        std::env::remove_var("SWANFLOW_INTERVAL_START");
        std::env::remove_var("SWANFLOW_INTERVAL_END");
        let base = compute_digest(id, "t", "{}", &[(id, &d1)]);

        std::env::set_var("SWANFLOW_INTERVAL_START", "2026-10-01T00:00:00+00:00");
        std::env::set_var("SWANFLOW_INTERVAL_END", "2026-10-02T00:00:00+00:00");
        let day1 = compute_digest(id, "t", "{}", &[(id, &d1)]);

        std::env::set_var("SWANFLOW_INTERVAL_START", "2026-10-02T00:00:00+00:00");
        std::env::set_var("SWANFLOW_INTERVAL_END", "2026-10-03T00:00:00+00:00");
        let day2 = compute_digest(id, "t", "{}", &[(id, &d1)]);

        std::env::remove_var("SWANFLOW_INTERVAL_START");
        std::env::remove_var("SWANFLOW_INTERVAL_END");

        assert_ne!(day1.hex(), day2.hex(), "different days must differ");
        assert_ne!(base.hex(), day1.hex(), "interval run must differ from no-interval run");
        assert_eq!(base.hex(), compute_digest(id, "t", "{}", &[(id, &d1)]).hex());
    }

    #[test]
    fn ledger_guard_mutual_exclusion_across_guards() {
        // Two guards in the same process serialize via LEDGER_MUTEX
        // (would deadlock if acquire were not re-entrant-safe via drop
        // ordering — it isn't re-entrant, so verify sequential acquire
        // works and that dropping releases).
        let dir = std::env::temp_dir().join(format!("swanflow_guard_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_str().unwrap().to_string();
        let g1 = LedgerGuard::acquire(std::path::Path::new(&dir_s));
        drop(g1); // must release; otherwise the next line deadlocks the test
        let _g2 = LedgerGuard::acquire(std::path::Path::new(&dir_s));

        // Cross-process semantics: a separate open of the same lock file
        // with try_lock_exclusive must fail while _g2 is held (flock is
        // per-fd; a second File on the same path is a different lock
        // holder even in-process).
        use fs2::FileExt;
        let f2 = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(std::path::Path::new(&dir_s).join("assets.lock"))
            .unwrap();
        assert!(
            f2.try_lock_exclusive().is_err(),
            "second fd must not acquire while guard holds the flock"
        );
    }

    #[test]
    fn concurrent_ledger_writers_same_process() {
        // codex-review P1 (r2): exercise the PUBLIC API exactly as
        // production uses it — no manually held outer guard. Each thread
        // opens its own AssetLedger (mirrors engine hook + daemon status
        // racing), writes a distinct node id, drops it normally. Correct-
        // ness: all 8 rows survive, file not corrupted. Barrier forces
        // genuine simultaneous contention.
        let dir = std::env::temp_dir().join(format!(
            "swanflow_conc_test_{:?}",
            (std::process::id(), std::thread::current().id())
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let dir_s = dir.to_str().unwrap().to_string();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for i in 0..8u32 {
            let d = dir_s.clone();
            let b = barrier.clone();
            handles.push(std::thread::spawn(move || {
                b.wait();
                let ledger = AssetLedger::open(std::path::Path::new(&d)).unwrap();
                let id = Uuid::new_v4();
                ledger
                    .record_run(id, &UpstreamDigest { bytes: [i as u8; 32] })
                    .unwrap();
                drop(ledger); // normal lifecycle — drop outside any guard
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let ledger = AssetLedger::open(std::path::Path::new(&dir_s)).unwrap();
        assert_eq!(ledger.all().unwrap().len(), 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn nested_guard_cache_identity() {
        // codex-review P2 (r2): a nested LedgerGuard for a DIFFERENT cache
        // dir used to blindly inherit the outer guard's exclusivity —
        // writing a ledger the outer flock does not cover. It must be
        // degraded to non-exclusive (write refused) instead, while a
        // nested guard on the SAME cache still inherits.
        let base = std::env::temp_dir().join(format!(
            "swanflow_nest_{}",
            std::process::id()
        ));
        let dir_a = base.join("a");
        let dir_b = base.join("b");
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&dir_a).unwrap();
        std::fs::create_dir_all(&dir_b).unwrap();

        // Outer guard on A, properly acquired.
        let outer = LedgerGuard::acquire_with_deadline(
            &dir_a,
            std::time::Duration::from_secs(5),
        );
        assert!(outer.exclusive());

        // Nested guard, same cache -> inherits exclusivity.
        let same = LedgerGuard::acquire_with_deadline(
            &dir_a,
            std::time::Duration::from_millis(50),
        );
        assert!(same.exclusive(), "same-cache nested guard must inherit");
        drop(same);

        // Nested guard, DIFFERENT cache -> must NOT inherit.
        let cross = LedgerGuard::acquire_with_deadline(
            &dir_b,
            std::time::Duration::from_millis(50),
        );
        assert!(
            !cross.exclusive(),
            "cross-cache nested guard must be non-exclusive (write refused)"
        );
        drop(cross);
        drop(outer);

        // After the outer guard drops, a fresh guard on B acquires fine.
        let fresh = LedgerGuard::acquire_with_deadline(
            &dir_b,
            std::time::Duration::from_millis(500),
        );
        assert!(fresh.exclusive(), "guard on B must acquire after outer dropped");
        drop(fresh);

        let _ = std::fs::remove_dir_all(&base);
    }
}
