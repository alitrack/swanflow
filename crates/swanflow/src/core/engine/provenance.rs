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
    for input in &h.inputs {
        feed(&input.path);
        feed(&input.rows.to_string());
        feed(&input.content_sha256);
    }
    format!("sha256:{}", hex(&hasher.finalize()))
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
    let schema_hash = schema_hash(conn, output_table)?;
    let (out_rows, out_hash) = table_fingerprint(conn, output_table)?;
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);

    let mut inputs = Vec::new();
    for t in input_tables {
        let (rows, content) = table_fingerprint(conn, t)?;
        inputs.push(TableRef {
            path: (*t).to_string(),
            rows,
            content_sha256: content,
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
            schema_hash,
            params: None,
            locale: std::env::var("LANG").ok(),
        },
        inputs,
        outputs: vec![TableRef {
            path: output_table.to_string(),
            rows: out_rows,
            content_sha256: out_hash,
            fingerprint_kind: default_fingerprint_kind(),
            collected_at: None,
        }],
        approvals,
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
}
