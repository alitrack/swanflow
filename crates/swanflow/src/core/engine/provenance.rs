//! swanbench artifact header (M1: emit the four-layer header for a materialized table).
//!
//! Contract: `alitrack/swanbench` → `specs/artifact.schema.json`.
//! Four layers: description / code / conversation reference / environment snapshot,
//! plus the inputs, outputs and approvals actually observed at execution time.
//!
//! Design notes that matter for correctness:
//!
//! * `artifact_id` must be *stable* for the same (description, code, env, inputs).
//!   We therefore frame the hash with a version tag and length-prefixed fields
//!   (same technique as `assets::compute_digest`) instead of hashing JSON text —
//!   JSON key order must never be able to change the id.
//! * `outputs[].content_sha256` is a **sha256 over an order-independent aggregate
//!   fingerprint** (`fingerprint_kind = "aggregate"`), not a hash of the row stream:
//!   it detects "the table's content changed" cheaply and exactly for small/medium
//!   tables, but it is not row-collision-resistant. The schema carries
//!   `fingerprint_kind` so a `--rerun` comparison can refuse to compare
//!   different kinds instead of silently reporting equality.
//! * Never guess: a table that does not exist, or a query that fails, returns an
//!   error — the header is never emitted with placeholder zeros.

use anyhow::{anyhow, Context, Result};
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bumped when the emitted header's semantics change (not the schema version).
pub const HEADER_IMPL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EngineInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Env {
    pub engine: EngineInfo,
    pub schema_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locale: Option<String>,
    /// `skill:name@version` of the skill package that produced this artifact.
    ///
    /// Part of the identity on purpose: the same SQL run under a different
    /// template version is a different analysis, and "这个结果是用哪版模板算的"
    /// must be answerable from the header alone (specs/skill-package.md §5).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CodeBlock {
    pub language: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nondeterministic: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConversationRef {
    pub session_id: String,
    pub message_range: [i64; 2],
    pub message_range_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableRef {
    pub path: String,
    pub rows: i64,
    pub content_sha256: String,
    /// Per-input schema fingerprint.
    ///
    /// Separate from `env.schema_hash` on purpose: the env-level hash is a
    /// *set* hash that includes the output table, so it cannot be recomputed in
    /// a header-only review (the output table is gone by design). This one can —
    /// the input table is always present — which is what lets `--check` catch
    /// "same rows, same content, different shape".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_sha256: Option<String>,
    /// "aggregate" (this implementation) or "row_stream" (reserved).
    #[serde(default = "default_fingerprint_kind")]
    pub fingerprint_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collected_at: Option<String>,
}

fn default_fingerprint_kind() -> String {
    "aggregate".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Approval {
    pub action_class: String,
    pub tier: String,
    pub granted_by: String,
    pub granted_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
}

/// A second-layer audit record: who re-ran this artifact, at which level, verdict.
///
/// Deliberately **not** part of `compute_artifact_id`: appending a replication
/// record must never change the artifact's identity, otherwise the id could not
/// be used to prove "same result" across reviews.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Replication {
    pub level: String,
    pub verdict: String,
    pub at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ArtifactHeader {
    pub artifact_id: String,
    pub version: u32,
    pub created_at: String,
    pub description: String,
    pub code: CodeBlock,
    pub conversation_ref: ConversationRef,
    pub env: Env,
    pub inputs: Vec<TableRef>,
    pub outputs: Vec<TableRef>,
    pub approvals: Vec<Approval>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub replications: Vec<Replication>,
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Schema fingerprint: sha256 over canonical `name|type` lines, in DESCRIBE order.
pub fn schema_hash(conn: &duckdb::Connection, table: &str) -> Result<String> {
    let sql = format!("DESCRIBE SELECT * FROM {}", quote_ident(table));
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("describe {table}"))?;
    let mut rows = stmt.query([]).context("run describe")?;
    let mut lines: Vec<String> = Vec::new();
    while let Some(row) = rows.next().context("read describe row")? {
        let name: String = row.get(0).context("describe column_name")?;
        let ty: String = row.get(1).context("describe column_type")?;
        lines.push(format!("{}|{}", name.trim(), ty.trim()));
    }
    if lines.is_empty() {
        return Err(anyhow!("table {table} has no columns (does it exist?)"));
    }
    let mut h = Sha256::new();
    h.update(b"swanbench-schema-v1");
    for line in &lines {
        h.update((line.len() as u64).to_le_bytes());
        h.update(line.as_bytes());
    }
    Ok(hex(&h.finalize()))
}

/// Schema *set* fingerprint: the output table plus every input table, each
/// labelled, hashed in declaration order.
///
/// Why this exists: hashing only the output table let two different input
/// schemas share one `artifact_id`. Reproduced during M1 verification — input
/// column `z INTEGER` vs `z BIGINT`, same values, same output table
/// (`out_n: n BIGINT`), identical id `sha256:a958f80f…`. That is exactly the
/// failure §4 promises cannot happen ("输入变了要能发现"), and it fails in the
/// direction that matters: a reviewer cannot tell the artifact was produced
/// from a different shape of data.
///
/// Input schemas are included by *identity*, so a change to any input's
/// columns or types moves the id even when the values and the output are
/// untouched.
pub fn schema_set_hash(
    conn: &duckdb::Connection,
    output_table: &str,
    input_tables: &[&str],
) -> Result<String> {
    let mut parts: Vec<String> = Vec::with_capacity(input_tables.len() + 1);
    // Output first, then inputs in the caller's order — the same order
    // `compute_artifact_id` walks `inputs[]`, so the two stay reconcilable.
    parts.push(format!("out|{}|{}", output_table, schema_hash(conn, output_table)?));
    for t in input_tables {
        parts.push(format!("in|{}|{}", t, schema_hash(conn, t)?));
    }
    let mut h = Sha256::new();
    h.update(b"swanbench-schema-set-v1");
    for line in &parts {
        h.update((line.len() as u64).to_le_bytes());
        h.update(line.as_bytes());
    }
    Ok(hex(&h.finalize()))
}

/// Engine version as reported by DuckDB itself (`SELECT version()`).
pub fn engine_version(conn: &duckdb::Connection) -> Result<String> {
    let v: String = conn
        .query_row("SELECT version()", [], |r| r.get(0))
        .context("duckdb version()")?;
    Ok(v)
}

/// Order-independent content fingerprint: (row count, sha256 of the aggregate).
///
/// `sum(hash(row))` is commutative, so row order cannot change the result, and
/// `sum` over `UBIGINT` widens to `HUGEINT` in DuckDB so realistic tables cannot
/// overflow it. NULLs are rendered as an empty string (documented weakness: a
/// NULL and an empty string collide in the fingerprint).
pub fn table_fingerprint(conn: &duckdb::Connection, table: &str) -> Result<(i64, String)> {
    let sql_desc = format!("DESCRIBE SELECT * FROM {}", quote_ident(table));
    let mut stmt = conn.prepare(&sql_desc)?;
    let cols: Vec<String> = {
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(row.get::<_, String>(0)?);
        }
        out
    };
    if cols.is_empty() {
        return Err(anyhow!("table {table} has no columns (does it exist?)"));
    }
    let row_expr = cols
        .iter()
        .map(|c| format!("COALESCE(CAST({} AS VARCHAR), '')", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT count(*), CAST(sum(hash(concat_ws('|', {row_expr}))) AS VARCHAR) FROM {}",
        quote_ident(table)
    );
    let (rows, agg): (i64, Option<String>) = conn
        .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
        .with_context(|| format!("fingerprint {table}"))?;
    // Empty table: sum() is NULL (verified against DuckDB: count=0, agg=NULL).
    let agg = agg.unwrap_or_else(|| "0".to_string());
    let agg = if agg.is_empty() { "0".to_string() } else { agg };
    let mut h = Sha256::new();
    h.update(b"swanbench-fingerprint-v1");
    let payload = format!("{table}|{rows}|{agg}");
    h.update((payload.len() as u64).to_le_bytes());
    h.update(payload.as_bytes());
    Ok((rows, hex(&h.finalize())))
}

/// Content-addressed id: sha256 over length-prefixed (description, code, env, inputs).
/// Same code + same env + same inputs MUST yield the same id.
pub fn compute_artifact_id(h: &ArtifactHeader) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"swanbench-artifact-v1");
    let mut feed = |s: &str| {
        hasher.update((s.len() as u64).to_le_bytes());
        hasher.update(s.as_bytes());
    };
    feed(&h.description);
    feed(&h.code.language);
    feed(&h.code.text);
    for nd in &h.code.nondeterministic {
        feed(nd);
    }
    feed(&h.env.engine.name);
    feed(&h.env.engine.version);
    feed(&h.env.schema_hash);
    if let Some(p) = &h.env.params {
        feed(&p.to_string());
    }
    // The skill package version is part of the identity: a different template
    // version is a different analysis, even with identical code text.
    if let Some(skill) = &h.env.skill {
        feed(skill);
    }
    for input in &h.inputs {
        feed(&input.path);
        feed(&input.rows.to_string());
        feed(&input.content_sha256);
        // Feed the schema fingerprint too, so a header edited to point at a
        // differently-shaped table cannot keep the original id.
        if let Some(schema) = &input.schema_sha256 {
            feed(schema);
        }
    }
    format!("sha256:{}", hex(&hasher.finalize()))
}

/// Attach the producing skill package and **re-derive the artifact id**.
///
/// Separate from `append_replication` on purpose, and the difference is the
/// whole point: an audit record must not move the id, but the identity of the
/// analysis must. A different skill version is a different analysis.
pub fn with_skill(mut header: ArtifactHeader, skill: Option<String>) -> ArtifactHeader {
    if header.env.skill != skill {
        header.env.skill = skill;
        header.artifact_id = compute_artifact_id(&header);
    }
    header
}

/// Build a header for one materialized output table.
///
/// `inputs` are the tables/files the caller knows the result depended on; pass
/// the same list on every run so the id stays comparable.
pub fn build_header(
    conn: &duckdb::Connection,
    output_table: &str,
    description: &str,
    code_text: &str,
    code_language: &str,
    input_tables: &[&str],
    conversation: ConversationRef,
    approvals: Vec<Approval>,
) -> Result<ArtifactHeader> {
    let engine = EngineInfo {
        name: "duckdb".to_string(),
        version: engine_version(conn)?,
    };
    // §4: `env.schema_hash` exists so that "输入变了要能发现". Hashing the
    // output table alone did not deliver that — two different input schemas
    // could share one artifact_id. Now every input table's structure is in it.
    let schema_hash_of_output = schema_hash(conn, output_table)?;
    let env_schema_hash = schema_set_hash(conn, output_table, input_tables)?;
    let (out_rows, out_hash) = table_fingerprint(conn, output_table)?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let mut inputs = Vec::new();
    for t in input_tables {
        let (rows, content) = table_fingerprint(conn, t)?;
        inputs.push(TableRef {
            path: (*t).to_string(),
            rows,
            content_sha256: content,
            schema_sha256: Some(schema_hash(conn, t)?),
            fingerprint_kind: default_fingerprint_kind(),
            collected_at: Some(now.clone()),
        });
    }

    let mut header = ArtifactHeader {
        artifact_id: String::new(),
        version: 1,
        created_at: now,
        description: description.to_string(),
        code: CodeBlock {
            language: code_language.to_string(),
            text: code_text.to_string(),
            nondeterministic: detect_nondeterminism(code_text),
        },
        conversation_ref: conversation,
        env: Env {
            engine,
            schema_hash: env_schema_hash,
            params: None,
            locale: std::env::var("LANG").ok(),
            // Set afterwards by `with_skill` (which re-derives the id). None
            // here means "no skill package was involved", not "unknown".
            skill: None,
        },
        inputs,
        outputs: vec![TableRef {
            path: output_table.to_string(),
            rows: out_rows,
            content_sha256: out_hash,
            schema_sha256: Some(schema_hash_of_output.clone()),
            fingerprint_kind: default_fingerprint_kind(),
            collected_at: None,
        }],
        approvals,
        replications: Vec::new(),
    };
    header.artifact_id = compute_artifact_id(&header);
    Ok(header)
}

/// Cheap, honest detector: flag constructs that make a rerun differ by design.
pub fn detect_nondeterminism(code: &str) -> Vec<String> {
    let lower = code.to_ascii_lowercase();
    let mut found = Vec::new();
    for (needle, label) in [
        ("now()", "now()"),
        ("current_timestamp", "current_timestamp"),
        ("current_date", "current_date"),
        ("random()", "random()"),
        ("uuid()", "uuid()"),
        ("gen_random_uuid", "gen_random_uuid"),
    ] {
        if lower.contains(needle) {
            found.push(label.to_string());
        }
    }
    found
}

/// Serialize next to the artifact (pretty JSON; humans read these in support calls).
pub fn write_header(path: &std::path::Path, header: &ArtifactHeader) -> Result<()> {
    let json = serde_json::to_string_pretty(header).context("serialize artifact header")?;
    std::fs::write(path, json).with_context(|| format!("write header {}", path.display()))?;
    Ok(())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

// ─────────────────────────── M1.c: replicate ───────────────────────────
//
// Three levels from `docs/design.md` §5. `check` is seconds, `rerun` is minutes.
// The acceptance rule for `rerun` is strict: it must work from the header alone —
// no original session, no agent, no template. Everything it needs is in the file.

/// Verdicts. Same vocabulary as `specs/artifact.schema.json#/properties/replications`.
pub mod verdict {
    pub const PASS: &str = "pass";
    pub const DATA_CHANGED: &str = "data_changed";
    pub const INCOMPLETE_HEADER: &str = "incomplete_header";
    pub const DIFF: &str = "diff";
    pub const VERSION_MISMATCH: &str = "version_mismatch";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InputStatus {
    pub path: String,
    /// `match` / `rows_changed` / `content_changed` / `missing`
    pub status: String,
    pub rows_expected: i64,
    pub rows_observed: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckReport {
    pub verdict: String,
    /// Header completeness problems (empty when the header is sound).
    pub problems: Vec<String>,
    pub inputs: Vec<InputStatus>,
    /// True when the stored `artifact_id` re-derives from the header's own fields.
    pub id_verified: bool,
    /// Set when the engine version differs from the one recorded in the header.
    pub engine_note: Option<String>,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDiff {
    pub column: String,
    pub extra_in_original: i64,
    pub extra_in_rerun: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RerunReport {
    pub verdict: String,
    pub problems: Vec<String>,
    pub rows_expected: i64,
    pub rows_observed: i64,
    pub hash_expected: String,
    pub hash_observed: String,
    /// Column-level localisation — never just "inconsistent".
    pub column_diffs: Vec<ColumnDiff>,
    pub columns_only_in_original: Vec<String>,
    pub columns_only_in_rerun: Vec<String>,
    pub engine_note: Option<String>,
    /// True when the code contains constructs that make a rerun differ by design;
    /// the comparison is then reported as partial instead of pass/fail.
    pub nondeterministic: Vec<String>,
    /// Nothing below certifies the result when this is true (non-deterministic code).
    pub partial: bool,
    /// False when there was no stored copy of the output table to diff against
    /// (the honest "third party received only the header" case).
    pub stored_table_compared: bool,
    /// If we failed to compute the column diff for a reason other than the
    /// table being genuinely absent, this contains the error text.
    pub diff_error: Option<String>,
    /// Meaningful only when `stored_table_compared`.
    pub stored_table_matches: bool,
    pub detail: String,
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Read a header back from disk. Fails loudly on bad JSON — a header we cannot
/// parse is an incomplete header, not an empty one.
pub fn load_header(path: &std::path::Path) -> Result<ArtifactHeader> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read header {}", path.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parse header {}", path.display()))
}

/// Structural completeness check — the same rules the M0 validator applies, so
/// the CLI and the spec validator cannot drift apart.
pub fn validate_header(h: &ArtifactHeader) -> Vec<String> {
    let mut problems = Vec::new();
    if h.artifact_id.len() != 71 || !h.artifact_id.starts_with("sha256:") {
        problems.push("artifact_id must be sha256:<64 hex>".to_string());
    }
    if h.description.trim().is_empty() {
        problems.push("description is empty (result reproducible but not understandable)".into());
    }
    if h.code.text.trim().is_empty() {
        problems.push("code.text is empty (nothing can be re-run from this header)".into());
    }
    if !matches!(h.code.language.as_str(), "sql" | "dag" | "python") {
        problems.push(format!("code.language '{}' is not one of sql/dag/python", h.code.language));
    }
    if h.env.engine.version.trim().is_empty() {
        problems.push("env.engine.version is empty (version drift is the #1 cause of failed reruns)".into());
    }
    if h.env.schema_hash.len() != 64 {
        problems.push("env.schema_hash must be 64 hex chars".into());
    }
    if h.inputs.is_empty() {
        problems.push("inputs is empty (cannot tell 'wrong answer' from 'data changed')".into());
    }
    if h.outputs.is_empty() {
        problems.push("outputs is empty (nothing to compare against)".into());
    }
    for (i, input) in h.inputs.iter().enumerate() {
        if input.content_sha256.len() != 64 {
            problems.push(format!("inputs[{i}].content_sha256 must be 64 hex chars"));
        }
        match &input.schema_sha256 {
            Some(s) if s.len() == 64 => {}
            Some(_) => problems.push(format!("inputs[{i}].schema_sha256 must be 64 hex chars")),
            // A header without it cannot be checked for "same rows, same
            // content, different shape" — that is a real gap, not a style
            // issue, so it is refused by field name rather than passed over.
            None => problems.push(format!(
                "inputs[{i}].schema_sha256 is missing — re-emit the header with a current \
                 swanflow: without it a reviewer cannot verify the input's shape"
            )),
        }
    }
    for (i, out) in h.outputs.iter().enumerate() {
        if out.content_sha256.len() != 64 {
            problems.push(format!("outputs[{i}].content_sha256 must be 64 hex chars"));
        }
    }
    if h.conversation_ref.message_range_sha256.len() != 64 {
        problems.push("conversation_ref.message_range_sha256 must be 64 hex chars".into());
    }
    problems
}

/// Structural check with no database: completeness + id integrity only.
///
/// Used by `provenance check` when no `--db` is given, and by the M0 validator
/// path. It never claims anything about the data.
pub fn check_structure(header: &ArtifactHeader) -> CheckReport {
    let problems = validate_header(header);
    let id_verified = compute_artifact_id(header) == header.artifact_id;
    let (v, detail) = if !id_verified {
        (
            verdict::INCOMPLETE_HEADER,
            "artifact_id does not re-derive from the header's own fields — the header was edited"
                .to_string(),
        )
    } else if !problems.is_empty() {
        (
            verdict::INCOMPLETE_HEADER,
            "header is incomplete — fix the header before trusting any re-check".to_string(),
        )
    } else {
        (
            verdict::PASS,
            "structural check only (no database given): inputs were not re-fingerprinted".to_string(),
        )
    };
    CheckReport {
        verdict: v.into(),
        problems,
        inputs: Vec::new(),
        id_verified,
        engine_note: None,
        detail,
    }
}

/// Open the database to be **reviewed**, read-only.
///
/// `check` only reads, but `Connection::open` gets write access, and a write-capable
/// handle can checkpoint/shorten a WAL on exit — i.e. touch a customer's medical
/// file during a "read-only" review. If another process holds the file, failing
/// loudly is the right outcome; silently writing is not.
pub fn open_read_only(path: &std::path::Path) -> Result<duckdb::Connection> {
    let config = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .context("build read-only duckdb config")?;
    duckdb::Connection::open_with_flags(path, config)
        .with_context(|| format!("open {} read-only", path.display()))
}

/// Level 1: is the premise still intact? Seconds, no execution.
///
/// Also re-derives `artifact_id` from the header's own fields — if the stored id
/// does not match, the header was edited after the fact and nothing below it can
/// be trusted.
pub fn check(conn: &duckdb::Connection, header: &ArtifactHeader) -> Result<CheckReport> {
    let problems = validate_header(header);
    let id_verified = compute_artifact_id(header) == header.artifact_id;
    let engine_observed = engine_version(conn).ok();
    let engine_note = match (&engine_observed, header.env.engine.version.as_str()) {
        (Some(now), want) if now != want => Some(format!(
            "engine version differs: header {want}, this machine {now} — a rerun here is not comparable"
        )),
        _ => None,
    };

    if !problems.is_empty() {
        return Ok(CheckReport {
            verdict: verdict::INCOMPLETE_HEADER.into(),
            problems,
            inputs: Vec::new(),
            id_verified,
            engine_note,
            detail: "header is incomplete — fix the header before trusting any re-check".into(),
        });
    }

    let mut inputs = Vec::new();
    let mut changed: Vec<String> = Vec::new();
    for input in &header.inputs {
        let (rows, content) = match table_fingerprint(conn, &input.path) {
            Ok(v) => v,
            Err(_) => {
                changed.push(format!("{} (missing)", input.path));
                inputs.push(InputStatus {
                    path: input.path.clone(),
                    status: "missing".into(),
                    rows_expected: input.rows,
                    rows_observed: -1,
                });
                continue;
            }
        };
        // Schema verification. Per-input, because the input table is always
        // present even in a header-only review (env.schema_hash is a set hash
        // that also covers the absent output table, so it cannot be recomputed
        // here). Without this, "same rows, same content, different shape"
        // passed silently — the exact hole inputs[].schema_sha256 exists for.
        let schema_changed = match &input.schema_sha256 {
            Some(recorded) => match schema_hash(conn, &input.path) {
                Ok(live) => &live != recorded,
                // Table missing is already reported above; do not double-count.
                Err(_) => false,
            },
            None => false,
        };
        let (status, note) = if rows != input.rows {
            (
                "rows_changed",
                format!("{}: {} rows now, {} recorded", input.path, rows, input.rows),
            )
        } else if content != input.content_sha256 {
            (
                "content_changed",
                format!("{}: same row count, different content", input.path),
            )
        } else if schema_changed {
            (
                "schema_changed",
                format!(
                    "{}: same row count and same content, but the table's SCHEMA changed \
                     (columns or types) — this is not the same input",
                    input.path
                ),
            )
        } else {
            ("match", String::new())
        };
        if !note.is_empty() {
            changed.push(note);
        }
        inputs.push(InputStatus {
            path: input.path.clone(),
            status: status.into(),
            rows_expected: input.rows,
            rows_observed: rows,
        });
    }

    let (v, detail) = if !changed.is_empty() {
        (
            verdict::DATA_CHANGED,
            format!("data changed since this artifact was made: {}", changed.join("; ")),
        )
    } else if !id_verified {
        (
            verdict::INCOMPLETE_HEADER,
            "artifact_id does not re-derive from the header's own fields — the header was edited"
                .into(),
        )
    } else {
        (
            verdict::PASS,
            "header complete, artifact_id re-derives, every input fingerprint still matches".into(),
        )
    };

    Ok(CheckReport {
        verdict: v.into(),
        problems: Vec::new(),
        inputs,
        id_verified,
        engine_note,
        detail,
    })
}

fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn describe_columns(conn: &duckdb::Connection, qualified: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(&format!("DESCRIBE SELECT * FROM {qualified}"))?;
    let mut rows = stmt.query([])?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(row.get::<_, String>(0)?);
    }
    Ok(out)
}

/// Where the rerun differs from the shipped table:
/// (column-level multiset diffs, columns only in the original, columns only in the rerun).
type ColumnDiffSet = (Vec<ColumnDiff>, Vec<String>, Vec<String>);

/// Where exactly does the rerun differ from the shipped result?
///
/// Multiset semantics (`EXCEPT ALL`) per column, so duplicates count and no
/// ordering assumption is needed. Column set differences are reported as names.
///
/// Three outcomes, because "no difference" has three meanings here:
/// `Ok(Some(..))` compared (an empty diff set means they match), `Ok(None)` the
/// original has no such table (the honest header-only review), and `Err(..)` the
/// comparison itself failed — which must reach the report, never be swallowed.
fn column_diff(
    conn: &duckdb::Connection,
    original_db: &std::path::Path,
    out_table: &str,
) -> Result<Option<ColumnDiffSet>> {
    let new_cols = describe_columns(conn, &quote_ident(out_table))?;
    conn.execute_batch(&format!(
        "ATTACH '{}' AS sb_orig (READ_ONLY);",
        escape_sql_string(&original_db.display().to_string())
    ))
    .context("attach the original database read-only for column diff")?;
    let orig_qualified = format!("sb_orig.{}", quote_ident(out_table));
    let new_qualified = format!("main.{}", quote_ident(out_table));
    // "Nothing to compare against" and "the comparison broke" must not look alike.
    // Only the genuinely absent case may be silent; an attach/catalog failure has
    // to reach the report, otherwise a broken diff would read as a clean pass.
    let present: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'sb_orig' AND table_name = ?",
            [out_table],
            |r| r.get(0),
        )
        .context("list tables in the attached original database")?;
    if present == 0 {
        return Ok(None);
    }
    let orig_cols = describe_columns(conn, &orig_qualified)?;

    let columns_only_in_original: Vec<String> = orig_cols
        .iter()
        .filter(|c| !new_cols.contains(c))
        .cloned()
        .collect();
    let columns_only_in_rerun: Vec<String> = new_cols
        .iter()
        .filter(|c| !orig_cols.contains(c))
        .cloned()
        .collect();

    let mut column_diffs = Vec::new();
    for col in orig_cols.iter().filter(|c| new_cols.contains(c)) {
        let q = quote_ident(col);
        let extra_orig: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT {q} FROM {orig_qualified} EXCEPT ALL SELECT {q} FROM {new_qualified})"
                ),
                [],
                |r| r.get(0),
            )
            .context("column diff (original side)")?;
        let extra_new: i64 = conn
            .query_row(
                &format!(
                    "SELECT count(*) FROM (SELECT {q} FROM {new_qualified} EXCEPT ALL SELECT {q} FROM {orig_qualified})"
                ),
                [],
                |r| r.get(0),
            )
            .context("column diff (rerun side)")?;
        if extra_orig != 0 || extra_new != 0 {
            column_diffs.push(ColumnDiff {
                column: col.clone(),
                extra_in_original: extra_orig,
                extra_in_rerun: extra_new,
            });
        }
    }
    Ok(Some((
        column_diffs,
        columns_only_in_original,
        columns_only_in_rerun,
    )))
}

/// Level 2: re-run the recorded code and compare outputs bit-for-bit, with
/// column-level localisation on mismatch.
///
/// Non-destructive: the rerun happens in a throwaway copy of the database, so a
/// customer's working file is never modified by a review.
pub fn rerun(db: &std::path::Path, header: &ArtifactHeader) -> Result<RerunReport> {
    let problems = validate_header(header);
    if !problems.is_empty() {
        return Ok(RerunReport {
            verdict: verdict::INCOMPLETE_HEADER.into(),
            problems,
            rows_expected: 0,
            rows_observed: 0,
            hash_expected: String::new(),
            hash_observed: String::new(),
            column_diffs: Vec::new(),
            columns_only_in_original: Vec::new(),
            columns_only_in_rerun: Vec::new(),
            engine_note: None,
            nondeterministic: Vec::new(),
            partial: false,
            stored_table_compared: false,
            stored_table_matches: false,
            diff_error: None,
            detail: "header is incomplete — a rerun would prove nothing".into(),
        });
    }
    if header.code.language != "sql" {
        return Err(anyhow!(
            "code.language '{}' cannot be re-run yet (only sql is implemented)",
            header.code.language
        ));
    }
    if header.outputs.len() != 1 {
        return Err(anyhow!(
            "rerun supports exactly one output table for now (header declares {})",
            header.outputs.len()
        ));
    }

    let out_table = header.outputs[0].path.clone();
    let dir = tempfile::tempdir().context("create rerun workdir")?;
    let work = dir.path().join("rerun.duckdb");
    std::fs::copy(db, &work)
        .with_context(|| format!("copy {} for a non-destructive rerun", db.display()))?;
    // A committed-but-not-checkpointed write may live in the WAL file; copy it too
    // or the rerun would silently run against older data than the header recorded.
    let wal_src = std::path::PathBuf::from(format!("{}.wal", db.display()));
    if wal_src.exists() {
        let _ = std::fs::copy(&wal_src, format!("{}.wal", work.display()));
    }

    let conn = duckdb::Connection::open(&work)
        .with_context(|| format!("open rerun copy {}", work.display()))?;
    let engine_observed = engine_version(&conn)?;
    let engine_note = if engine_observed != header.env.engine.version {
        Some(format!(
            "engine version differs: header {}, this machine {} — the code was re-run, but a \
             difference here is a version difference, not a wrong answer",
            header.env.engine.version, engine_observed
        ))
    } else {
        None
    };

    // Drop the recorded output so the recorded code can recreate it verbatim.
    conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", quote_ident(&out_table)))
        .with_context(|| format!("drop {out_table} in the rerun copy"))?;
    conn.execute_batch(&header.code.text).with_context(|| {
        format!(
            "re-run the code recorded in the header — note that {out_table} was dropped first, \
             so code that appends to (instead of creating) the output table cannot be re-run \
             this way; that is a limitation, not a wrong answer"
        )
    })?;

    let (rows_observed, hash_observed) = table_fingerprint(&conn, &out_table)
        .with_context(|| format!("fingerprint re-run output {out_table}"))?;
    let expected = &header.outputs[0];

    // A kind mismatch means the two hashes are not answering the same question.
    if expected.fingerprint_kind != default_fingerprint_kind() {
        return Err(anyhow!(
            "refusing to compare: header output fingerprint_kind is '{}' but reruns here produce '{}' — \
             different kinds are not comparable (docs/design.md §8)",
            expected.fingerprint_kind,
            default_fingerprint_kind()
        ));
    }

    // Column-level localisation, best effort: the rerun itself is already proven
    // (it ran from the header alone); the original table is only needed to say
    // *where* the output differs, so a missing original must not fail the rerun.
    // `None` means "there was no stored table to compare against" — distinct from
    // "there was one and it matched".
    let mut diff_error: Option<String> = None;
    let stored = match column_diff(&conn, db, &out_table) {
        // Some(Some(..)) = compared; Some(None) = nothing on disk to compare;
        // None = the comparison itself failed (surfaced, never swallowed).
        Ok(inner) => inner,
        Err(e) => {
            diff_error = Some(format!("{e:#}"));
            None
        }
    };
    let (mut column_diffs, columns_only_in_original, columns_only_in_rerun) = stored
        .clone()
        .unwrap_or_else(|| (Vec::new(), Vec::new(), Vec::new()));
    column_diffs.sort_by(|a, b| a.column.cmp(&b.column));
    // The table currently on disk disagrees with the header. That is a finding in
    // its own right: it means the shipped result was altered after it was made.
    let stored_table_differs = stored
        .as_ref()
        .map(|(d, gone, new)| !d.is_empty() || !gone.is_empty() || !new.is_empty())
        .unwrap_or(false);

    let stored_table_matches = stored
        .as_ref()
        .map(|(d, gone, new)| d.is_empty() && gone.is_empty() && new.is_empty())
        .unwrap_or(false);
    let identical = rows_observed == expected.rows && hash_observed == expected.content_sha256;
    let partial = !header.code.nondeterministic.is_empty();
    let (v, detail) = if engine_note.is_some() {
        (
            verdict::VERSION_MISMATCH,
            "engine version differs — reported as a version difference, not as a wrong answer".into(),
        )
    } else if stored_table_differs && identical {
        // The dangerous case, and the reason this is not just a hash compare: the
        // code still reproduces the header, but the table sitting in the database
        // is not what the header says. Someone changed the result by hand.
        let cols: Vec<String> = column_diffs
            .iter()
            .map(|d| {
                format!(
                    "{} (-{} / +{})",
                    d.column, d.extra_in_original, d.extra_in_rerun
                )
            })
            .collect();
        (
            verdict::DIFF,
            format!(
                "the re-run reproduces the header, but the table stored in the database does not \
                 match it — the shipped result was altered after the fact; columns {}",
                if cols.is_empty() {
                    "differ in column set".to_string()
                } else {
                    cols.join(", ")
                }
            ),
        )
    } else if identical {
        (
            verdict::PASS,
            format!("re-ran the recorded code; {rows_observed} rows, identical aggregate fingerprint"),
        )
    } else if !column_diffs.is_empty() || !columns_only_in_original.is_empty() {
        let cols: Vec<String> = column_diffs
            .iter()
            .map(|d| {
                format!(
                    "{} (-{} / +{})",
                    d.column, d.extra_in_original, d.extra_in_rerun
                )
            })
            .collect();
        (
            verdict::DIFF,
            format!(
                "output differs: {} rows now vs {} recorded; columns {}",
                rows_observed, expected.rows, cols.join(", ")
            ),
        )
    } else {
        (
            verdict::DIFF,
            format!(
                "output differs: {} rows now vs {} recorded; aggregate fingerprint {} vs {}",
                rows_observed,
                expected.rows,
                &hash_observed[..12.min(hash_observed.len())],
                &expected.content_sha256[..12.min(expected.content_sha256.len())]
            ),
        )
    };

    Ok(RerunReport {
        verdict: v.into(),
        problems: Vec::new(),
        rows_expected: expected.rows,
        rows_observed,
        hash_expected: expected.content_sha256.clone(),
        hash_observed,
        column_diffs,
        columns_only_in_original,
        columns_only_in_rerun,
        engine_note,
        nondeterministic: header.code.nondeterministic.clone(),
        partial,
        stored_table_compared: stored.is_some(),
        stored_table_matches,
        diff_error,
        detail,
    })
}

/// Append the second-layer audit record. Identity-preserving by construction.
pub fn append_replication(
    header: &mut ArtifactHeader,
    level: &str,
    v: &str,
    by: Option<String>,
    detail: Option<String>,
) {
    header.replications.push(Replication {
        level: level.to_string(),
        verdict: v.to_string(),
        at: now_rfc3339(),
        by,
        detail,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conv() -> ConversationRef {
        ConversationRef {
            session_id: "sess_test".into(),
            message_range: [1, 9],
            message_range_sha256: "0".repeat(64),
        }
    }

    fn setup() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE sales(id INT, region VARCHAR);")
            .unwrap();
        conn.execute_batch("INSERT INTO sales VALUES (1,'east'),(2,'west'),(3,'north');")
            .unwrap();
        conn
    }

    #[test]
    fn fingerprint_is_order_independent_and_change_sensitive() {
        let conn = setup();
        let (rows1, h1) = table_fingerprint(&conn, "sales").unwrap();
        conn.execute_batch("CREATE TABLE tmp AS SELECT * FROM sales;")
            .unwrap();
        conn.execute_batch("DELETE FROM sales;").unwrap();
        conn.execute_batch("INSERT INTO sales SELECT * FROM tmp ORDER BY id DESC;")
            .unwrap();
        let (rows2, h2) = table_fingerprint(&conn, "sales").unwrap();
        assert_eq!(rows1, rows2);
        assert_eq!(h1, h2, "row order must not change the fingerprint");

        conn.execute_batch("UPDATE sales SET region='south' WHERE id=2;")
            .unwrap();
        let (_, h3) = table_fingerprint(&conn, "sales").unwrap();
        assert_ne!(h1, h3, "content change must change the fingerprint");
    }

    #[test]
    fn schema_hash_tracks_column_types() {
        let conn = setup();
        let a = schema_hash(&conn, "sales").unwrap();
        conn.execute_batch("CREATE TABLE other(id INT, region VARCHAR);")
            .unwrap();
        assert_eq!(
            a,
            schema_hash(&conn, "other").unwrap(),
            "same shape, same hash"
        );
        conn.execute_batch("CREATE TABLE wider(id BIGINT, region VARCHAR);")
            .unwrap();
        assert_ne!(
            a,
            schema_hash(&conn, "wider").unwrap(),
            "type change must change hash"
        );
    }

    #[test]
    fn a_different_input_schema_must_move_the_artifact_id() {
        // Reproduces the hole found during M1 verification: `env.schema_hash`
        // used to fingerprint only the OUTPUT table, so an input whose column
        // type changed (INTEGER -> BIGINT — same values, same output table)
        // kept the same artifact_id. That breaks the one invariant §4 promises
        // ("输入变了要能发现"), and it breaks it in the direction that matters:
        // a reviewer cannot tell the artifact came from a different shape of
        // data.
        let mk = |input_type: &str| {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch(&format!("CREATE TABLE jiesuan(a INTEGER, z {input_type});"))
                .unwrap();
            conn.execute_batch("INSERT INTO jiesuan VALUES (1,100),(2,200),(3,300);")
                .unwrap();
            conn.execute_batch("CREATE TABLE out_n AS SELECT count(*) AS n FROM jiesuan;")
                .unwrap();
            conn
        };
        let code = "CREATE TABLE out_n AS SELECT count(*) AS n FROM jiesuan;";
        let narrow = mk("INTEGER");
        let wide = mk("BIGINT");

        let h_narrow = build_header(
            &narrow, "out_n", "row count", code, "sql", &["jiesuan"], conv(), vec![],
        )
        .unwrap();
        let h_wide = build_header(
            &wide, "out_n", "row count", code, "sql", &["jiesuan"], conv(), vec![],
        )
        .unwrap();

        // Preconditions that made this hole invisible: output shape, output
        // content and input content are all identical. Only the input *schema*
        // differs — so only a schema-sensitive id can catch it.
        assert_eq!(
            schema_hash(&narrow, "out_n").unwrap(),
            schema_hash(&wide, "out_n").unwrap(),
            "the output table is identical, so an output-only schema hash sees nothing"
        );
        assert_eq!(
            h_narrow.outputs[0].content_sha256, h_wide.outputs[0].content_sha256,
            "the output content is identical too"
        );
        assert_eq!(
            h_narrow.inputs[0].content_sha256, h_wide.inputs[0].content_sha256,
            "and the input content hashes are identical — values were unchanged"
        );
        assert_ne!(
            h_narrow.artifact_id, h_wide.artifact_id,
            "an input schema change MUST move the id even when values and output match"
        );
    }

    #[test]
    fn empty_table_fingerprints_without_erroring() {
        // DuckDB returns count=0 and agg=NULL for sum() over no rows; the header
        // must not swallow that as an error (verified against the real engine).
        let conn = setup();
        conn.execute_batch("CREATE TABLE nothing(id INT);").unwrap();
        let (rows, h) = table_fingerprint(&conn, "nothing").unwrap();
        assert_eq!(rows, 0);
        assert_eq!(h.len(), 64);
    }

    #[test]
    fn missing_table_is_an_error_not_a_zero_header() {
        let conn = setup();
        assert!(table_fingerprint(&conn, "nope").is_err());
        assert!(schema_hash(&conn, "nope").is_err());
    }

    #[test]
    fn artifact_id_is_stable_and_input_sensitive() {
        let conn = setup();
        let h1 = build_header(
            &conn,
            "sales",
            "region totals",
            "SELECT 1",
            "sql",
            &["sales"],
            conv(),
            vec![],
        )
        .unwrap();
        let h2 = build_header(
            &conn,
            "sales",
            "region totals",
            "SELECT 1",
            "sql",
            &["sales"],
            conv(),
            vec![],
        )
        .unwrap();
        assert_eq!(h1.artifact_id, h2.artifact_id, "same inputs => same id");
        assert!(h1.artifact_id.starts_with("sha256:"));

        conn.execute_batch("INSERT INTO sales VALUES (4,'east');")
            .unwrap();
        let h3 = build_header(
            &conn,
            "sales",
            "region totals",
            "SELECT 1",
            "sql",
            &["sales"],
            conv(),
            vec![],
        )
        .unwrap();
        assert_ne!(h1.artifact_id, h3.artifact_id, "data change must change id");
    }

    #[test]
    fn header_serializes_with_contract_fields() {
        let conn = setup();
        let h = build_header(
            &conn,
            "sales",
            "region totals",
            "SELECT now(), 1",
            "sql",
            &["sales"],
            conv(),
            vec![],
        )
        .unwrap();
        let v = serde_json::to_value(&h).unwrap();
        for key in [
            "artifact_id",
            "version",
            "created_at",
            "description",
            "code",
            "conversation_ref",
            "env",
            "inputs",
            "outputs",
            "approvals",
        ] {
            assert!(v.get(key).is_some(), "missing contract field {key}");
        }
        assert_eq!(v["env"]["engine"]["name"], "duckdb");
        assert_eq!(h.code.nondeterministic, vec!["now()".to_string()]);
        assert_eq!(v["inputs"][0]["fingerprint_kind"], "aggregate");
    }

    fn header_for(conn: &duckdb::Connection) -> ArtifactHeader {
        build_header(
            conn,
            "out_region",
            "region totals",
            "CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1;",
            "sql",
            &["sales"],
            conv(),
            vec![],
        )
        .unwrap()
    }

    fn setup_with_output() -> duckdb::Connection {
        let conn = setup();
        conn.execute_batch(
            "CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1;",
        )
        .unwrap();
        conn
    }

    #[test]
    fn check_passes_when_the_premise_still_holds() {
        let conn = setup_with_output();
        let h = header_for(&conn);
        let r = check(&conn, &h).unwrap();
        assert_eq!(r.verdict, verdict::PASS, "detail: {}", r.detail);
        assert!(r.id_verified);
        assert_eq!(r.inputs.len(), 1);
        assert_eq!(r.inputs[0].status, "match");
    }

    #[test]
    fn check_reports_data_changed_after_one_row_moves() {
        let conn = setup_with_output();
        let h = header_for(&conn);
        conn.execute_batch("UPDATE sales SET region='south' WHERE id=2;")
            .unwrap();
        let r = check(&conn, &h).unwrap();
        assert_eq!(r.verdict, verdict::DATA_CHANGED, "detail: {}", r.detail);
        assert_eq!(r.inputs[0].status, "content_changed");
        assert!(r.detail.contains("sales"), "detail must name the input");
    }

    #[test]
    fn check_detects_a_hand_edited_header() {
        let conn = setup_with_output();
        let mut h = header_for(&conn);
        h.description = "a different, nicer sounding claim".into();
        let r = check(&conn, &h).unwrap();
        assert!(!r.id_verified, "id must stop re-deriving after the edit");
        assert_eq!(r.verdict, verdict::INCOMPLETE_HEADER);
        assert!(r.detail.contains("artifact_id"), "detail: {}", r.detail);
    }

    #[test]
    fn check_refuses_an_incomplete_header_by_name() {
        let conn = setup_with_output();
        let mut h = header_for(&conn);
        h.code.text = String::new();
        h.inputs.clear();
        let r = check(&conn, &h).unwrap();
        assert_eq!(r.verdict, verdict::INCOMPLETE_HEADER);
        assert!(r.problems.iter().any(|p| p.contains("code.text")));
        assert!(r.problems.iter().any(|p| p.contains("inputs")));
    }

    fn seed_db(path: &std::path::Path) {
        // Same engine version links into this test binary and into the rerun, so
        // the file format matches by construction (see the smoke script for the
        // Python-2.0.0/storage-v69 interop case).
        let conn = duckdb::Connection::open(path).unwrap();
        conn.execute_batch(
            "CREATE TABLE sales(id INT, region VARCHAR);\
             INSERT INTO sales VALUES (1,'east'),(2,'west'),(3,'north');\
             CREATE TABLE out_region AS SELECT region, count(*) AS n FROM sales GROUP BY 1;",
        )
        .unwrap();
    }

    #[test]
    fn rerun_reproduces_the_same_number_from_the_header_alone() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("work.duckdb");
        seed_db(&db);
        let h = {
            let conn = duckdb::Connection::open(&db).unwrap();
            header_for(&conn)
        };

        let r = rerun(&db, &h).unwrap();
        assert_eq!(r.verdict, verdict::PASS, "detail: {}", r.detail);
        assert_eq!(r.rows_observed, h.outputs[0].rows);
        assert_eq!(r.hash_observed, h.outputs[0].content_sha256);
        assert!(r.column_diffs.is_empty());
        assert!(
            r.diff_error.is_none(),
            "the column diff must have actually run, not been silently skipped: {:?}",
            r.diff_error
        );
        // The review must not have touched the original file.
        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM out_region", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, h.outputs[0].rows);
    }

    #[test]
    fn rerun_localises_a_difference_to_a_column() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("work.duckdb");
        seed_db(&db);
        let h = {
            let conn = duckdb::Connection::open(&db).unwrap();
            let h = header_for(&conn);
            // Tamper with the shipped result: the code is unchanged, so the rerun
            // re-creates the correct table while the original is now wrong.
            conn.execute_batch("UPDATE out_region SET n = n + 100 WHERE region='east';")
                .unwrap();
            h
        };

        let r = rerun(&db, &h).unwrap();
        assert_eq!(r.verdict, verdict::DIFF, "detail: {}", r.detail);
        assert!(
            r.column_diffs.iter().any(|d| d.column == "n"),
            "diff must name the column: {:?}",
            r.column_diffs
        );
        assert!(r.stored_table_compared && !r.stored_table_matches);
        assert!(
            r.detail.contains("altered after the fact"),
            "the verdict must say the shipped result was altered, not that the code is wrong: {}",
            r.detail
        );
    }

    #[test]
    fn rerun_passes_when_only_the_header_reaches_the_reviewer() {
        // The real third-party case: the reviewer gets the raw inputs and the
        // header, and no copy of the shipped output table at all.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("work.duckdb");
        seed_db(&db);
        let h = {
            let conn = duckdb::Connection::open(&db).unwrap();
            let h = header_for(&conn);
            conn.execute_batch("DROP TABLE out_region").unwrap();
            h
        };

        let r = rerun(&db, &h).unwrap();
        assert_eq!(r.verdict, verdict::PASS, "detail: {}", r.detail);
        assert_eq!(r.hash_observed, h.outputs[0].content_sha256);
        assert!(!r.stored_table_compared, "there was nothing on disk to compare");
        assert!(r.column_diffs.is_empty());
    }

    #[test]
    fn rerun_refuses_to_compare_a_different_fingerprint_kind() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("work.duckdb");
        seed_db(&db);
        let mut h = {
            let conn = duckdb::Connection::open(&db).unwrap();
            header_for(&conn)
        };
        h.outputs[0].fingerprint_kind = "row_stream".into();
        let err = rerun(&db, &h).unwrap_err().to_string();
        assert!(
            err.contains("refusing to compare"),
            "must refuse rather than silently report equality: {err}"
        );
    }

    #[test]
    fn read_only_open_cannot_write() {
        // The point of open_read_only: a review must not be *able* to change the
        // file it is reviewing, not merely refrain from doing so.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("work.duckdb");
        seed_db(&db);
        let conn = open_read_only(&db).unwrap();
        assert!(
            conn.execute_batch("DELETE FROM sales").is_err(),
            "a read-only handle must refuse a write"
        );
        // ... and the data is still there, observed through the same handle.
        let n: i64 = conn
            .query_row("SELECT count(*) FROM sales", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3);
    }

    #[test]
    fn check_catches_a_shape_change_that_content_hashing_cannot() {
        // The gap this closes: same rows, same values, different column type.
        // Content fingerprints are identical, so only the per-input schema
        // fingerprint can tell — and `--check` must say so instead of PASS.
        let mk = |input_type: &str| {
            let conn = duckdb::Connection::open_in_memory().unwrap();
            conn.execute_batch(&format!("CREATE TABLE jiesuan(a INTEGER, z {input_type});"))
                .unwrap();
            conn.execute_batch("INSERT INTO jiesuan VALUES (1,100),(2,200),(3,300);")
                .unwrap();
            conn.execute_batch("CREATE TABLE out_n AS SELECT count(*) AS n FROM jiesuan;")
                .unwrap();
            conn
        };
        let code = "CREATE TABLE out_n AS SELECT count(*) AS n FROM jiesuan;";
        let narrow = mk("INTEGER");
        let wide = mk("BIGINT");
        let h = build_header(
            &narrow, "out_n", "row count", code, "sql", &["jiesuan"], conv(), vec![],
        )
        .unwrap();

        // The content really is unchanged — this is what made the hole silent.
        assert_eq!(
            h.inputs[0].content_sha256,
            table_fingerprint(&wide, "jiesuan").unwrap().1,
            "content fingerprint is identical, so it cannot be what catches this"
        );

        let r = check(&wide, &h).unwrap();
        assert_eq!(r.verdict, verdict::DATA_CHANGED, "a shape change must not pass");
        assert_eq!(r.inputs[0].status, "schema_changed");
        assert!(r.detail.contains("SCHEMA"), "detail must name the reason: {}", r.detail);
    }

    #[test]
    fn a_header_without_per_input_schema_is_refused_by_field_name() {
        let conn = setup_with_output();
        let mut h = header_for(&conn);
        h.inputs[0].schema_sha256 = None;
        let problems = validate_header(&h);
        assert!(
            problems.iter().any(|p| p.contains("inputs[0].schema_sha256")),
            "must name the field, not fail vaguely: {problems:?}"
        );
        assert_eq!(check(&conn, &h).unwrap().verdict, verdict::INCOMPLETE_HEADER);
    }

    #[test]
    fn attaching_a_skill_moves_the_id_but_a_replication_does_not() {
        let conn = setup_with_output();
        let h = header_for(&conn);
        let base = h.artifact_id.clone();
        assert!(h.env.skill.is_none());

        let tagged = with_skill(h.clone(), Some("医保自查-DRG高倍率病例筛查@0.1.0".into()));
        assert_eq!(tagged.env.skill.as_deref(), Some("医保自查-DRG高倍率病例筛查@0.1.0"));
        assert_ne!(tagged.artifact_id, base, "a different template version is a different analysis");
        // ...and the new id must still re-derive from its own fields.
        assert_eq!(compute_artifact_id(&tagged), tagged.artifact_id);

        // Contrast: an audit record must NOT move the id (that is what makes
        // repeated reviews possible at all).
        let mut audited = tagged.clone();
        append_replication(&mut audited, "check", verdict::PASS, Some("lhy".into()), None);
        assert_eq!(compute_artifact_id(&audited), tagged.artifact_id);
    }

    #[test]
    fn a_locale_difference_does_not_change_the_id() {
        // DECISION (swanbench §14.5): locale is recorded but deliberately NOT in
        // the identity, because DuckDB does not let it change results. Verified
        // over 5 locales (C / zh_CN / en_US / de_DE / tr_TR) x 6 query shapes
        // (CJK ordering, number and date formatting, aggregation, string
        // grouping, upper/lower incl. the Turkish dotted-i trap): every result
        // was identical, row for row.
        //
        // This test exists so the decision is not silently reversed. If someone
        // starts feeding locale into the hash, this goes red — and they should
        // read §14.5 before overriding it, because the cost is real: the same
        // analysis on a differently-configured machine would then stop matching
        // its own artifact, which is the failure mode this tool exists to
        // prevent.
        let conn = setup_with_output();
        let h = header_for(&conn);
        let base = compute_artifact_id(&h);

        let mut moved = h.clone();
        moved.env.locale = Some("de_DE.UTF-8".into());
        assert_eq!(compute_artifact_id(&moved), base, "locale must not move the id");

        let mut unset = h.clone();
        unset.env.locale = None;
        assert_eq!(compute_artifact_id(&unset), base, "locale absent must not move the id");
    }

    #[test]
    fn appending_a_replication_does_not_change_the_artifact_id() {
        let conn = setup_with_output();
        let mut h = header_for(&conn);
        let id = h.artifact_id.clone();
        append_replication(&mut h, "check", verdict::PASS, Some("lhy".into()), None);
        assert_eq!(compute_artifact_id(&h), id, "audit records must not move the id");
        assert_eq!(h.replications.len(), 1);
        let json = serde_json::to_string(&h).unwrap();
        assert!(json.contains("\"replications\""));
    }
}
