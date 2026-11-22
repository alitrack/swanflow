//! SQL lineage extraction (planning-time, zero-execution).
//!
//! Parses a DuckDB-dialect SQL payload with sqlparser-rs (already a
//! transitive dependency via DataFusion) and extracts the tables the
//! statement reads from and writes to. Used by `NodeBehavior::analyze`
//! implementations (currently `core.sql.duckdb`) to surface lineage as
//! node diagnostics — visible in `swanflow inspect` output and on the
//! canvas diagnostics channel, with no execution required.
//!
//! Design notes:
//! - Engine input aliases (`input_0`, `input_1`, ...) are excluded from
//!   reads: they are the node's declared ports, not external lineage.
//! - CTE names (WITH clauses) are excluded: they are statement-local.
//! - DuckDB table functions that take a *path argument* (read_parquet,
//!   read_csv, read_json, glob, etc.) are reported as file reads so a
//!   query that bypasses `input_0` still shows what it touches.
//! - Unparseable SQL yields an empty analysis — lineage is best-effort
//!   diagnostics, never a hard error.

use std::collections::BTreeSet;
use std::fmt;

use sqlparser::ast::{Statement, TableFactor, Visit};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

/// Result of a lineage pass over one SQL payload.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SqlLineage {
    /// Tables or file paths the statement reads from (sorted, deduped).
    pub reads: Vec<String>,
    /// Tables the statement writes to (INSERT/CTAS targets, sorted, deduped).
    pub writes: Vec<String>,
}

impl fmt::Display for SqlLineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "reads [")?;
        write!(f, "{}", self.reads.join(", "))?;
        write!(f, "] writes [")?;
        write!(f, "{}", self.writes.join(", "))?;
        write!(f, "]")
    }
}

/// DuckDB table functions whose first literal argument is a file path/glob
/// rather than a table name. Reporting them as reads keeps lineage honest
/// for queries that source data directly from files.
const PATH_FUNCTION_NAMES: &[&str] = &[
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "read_json",
    "read_json_auto",
    "read_ndjson",
    "read_ndjson_auto",
    "read_xlsx",
    "read_iceberg",
    "read_text",
    "read_blob",
    "glob",
    "parquet_scan",
    "csv_scan",
];

#[derive(Default)]
struct Collector {
    reads: BTreeSet<String>,
    writes: BTreeSet<String>,
    cte_names: BTreeSet<String>,
}

impl Collector {
    fn is_engine_alias(name: &str) -> bool {
        name == "input_0"
            || (name.len() > 6
                && name.starts_with("input_")
                && name[6..].chars().all(|c| c.is_ascii_digit()))
    }

    fn record_read(&mut self, name: &str) {
        let n = name.trim_matches(|c| c == '"' || c == '`' || c == '\'');
        if n.is_empty() || Self::is_engine_alias(n) || self.cte_names.contains(n) {
            return;
        }
        self.reads.insert(n.to_string());
    }

    fn record_write(&mut self, name: &str) {
        let n = name.trim_matches(|c| c == '"' || c == '`' || c == '\'');
        if n.is_empty() || Self::is_engine_alias(n) {
            return;
        }
        self.writes.insert(n.to_string());
    }
}

pub(crate) fn object_name_to_string(name: &sqlparser::ast::ObjectName) -> String {
    name.0
        .iter()
        .map(|p| match p {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
            sqlparser::ast::ObjectNamePart::Function(f) => f.name.to_string(),
        })
        .collect::<Vec<_>>()
        .join(".")
}

/// Extract lineage from a SQL payload. Best-effort: returns an empty
/// [`SqlLineage`] when the payload cannot be parsed.
pub fn extract_lineage(sql: &str) -> SqlLineage {
    let dialect = DuckDbDialect {};

    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return SqlLineage::default(),
    };

    let mut c = Collector::default();

    for stmt in &statements {
        // Collect CTE names first so body references to them are skipped.
        collect_cte_names(stmt, &mut c);
        visit_statement(stmt, &mut c);
    }

    SqlLineage {
        reads: c.reads.into_iter().collect(),
        writes: c.writes.into_iter().collect(),
    }
}

fn collect_cte_names(stmt: &Statement, c: &mut Collector) {
    // WITH clauses hang off Query.bodies / TableFactor derived tables; the
    // robust generic way is to walk every Cte node in the AST.
    let mut cte_visitor = CteNameVisitor {
        names: &mut c.cte_names,
    };
    let _ = stmt.visit(&mut cte_visitor);
}

struct CteNameVisitor<'a> {
    names: &'a mut BTreeSet<String>,
}

impl<'a> sqlparser::ast::Visitor for CteNameVisitor<'a> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<()> {
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.names.insert(cte.alias.name.value.clone());
            }
        }
        std::ops::ControlFlow::Continue(())
    }
}

use std::ops::ControlFlow;

/// Walk a statement collecting reads/writes via the generic Visitor API.
fn visit_statement(stmt: &Statement, c: &mut Collector) {
    // Writes: statement-level targets.
    match stmt {
        Statement::Insert(insert) => {
            if let sqlparser::ast::TableObject::TableName(name) = &insert.table {
                c.record_write(&object_name_to_string(name));
            }
        }
        Statement::CreateTable(ct) => {
            c.record_write(&object_name_to_string(&ct.name));
        }
        Statement::Delete(d) => {
            for name in &d.tables {
                let n = object_name_to_string(name);
                c.record_write(&n);
                c.record_read(&n);
            }
        }
        Statement::Update { table, .. } => {
            if let TableFactor::Table { name, .. } = &table.relation {
                let n = object_name_to_string(name);
                c.record_write(&n);
                c.record_read(&n);
            }
        }
        _ => {}
    }

    let mut v = ReadWriteVisitor { collector: c };
    let _ = stmt.visit(&mut v);
}

struct ReadWriteVisitor<'a> {
    collector: &'a mut Collector,
}

impl<'a> sqlparser::ast::Visitor for ReadWriteVisitor<'a> {
    type Break = ();

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, args, .. } = table {
            let fname = object_name_to_string(name).to_lowercase();
            // Table function with path argument -> record the literal.
            if let Some(tfa) = args {
                if PATH_FUNCTION_NAMES.contains(&fname.as_str()) {
                    if let Some(first) = tfa.args.first() {
                        let rendered = format!("{}", first);
                        let trimmed = rendered.trim();
                        if trimmed.starts_with('\'') || trimmed.starts_with('"') {
                            let path = trimmed.trim_matches(|ch| ch == '\'' || ch == '"');
                            if !path.is_empty() {
                                self.collector.reads.insert(path.to_string());
                                return ControlFlow::Continue(());
                            }
                        }
                    }
                }
                // Other table functions: skip (unknown semantics).
                return ControlFlow::Continue(());
            }
            self.collector.record_read(&object_name_to_string(name));
        }
        ControlFlow::Continue(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn reads(sql: &str) -> Vec<String> {
        extract_lineage(sql).reads
    }

    fn writes(sql: &str) -> Vec<String> {
        extract_lineage(sql).writes
    }

    #[test]
    fn plain_select_single_table() {
        let l = extract_lineage("SELECT a FROM orders");
        assert_eq!(l.reads, vec!["orders".to_string()]);
        assert!(l.writes.is_empty());
    }

    #[test]
    fn input_alias_excluded() {
        let l = extract_lineage("SELECT id * 10 AS scaled FROM input_0");
        assert!(l.reads.is_empty());
        assert!(l.writes.is_empty());
    }

    #[test]
    fn multi_input_aliases_excluded() {
        let l = extract_lineage("SELECT * FROM input_0 JOIN input_1 USING (id)");
        assert!(l.reads.is_empty());
    }

    #[test]
    fn join_and_schema_qualified() {
        let l =
            extract_lineage("SELECT * FROM main.orders o JOIN public.customers c ON o.cid = c.id");
        assert_eq!(
            l.reads,
            vec!["main.orders".to_string(), "public.customers".to_string()]
        );
    }

    #[test]
    fn cte_references_excluded() {
        let l = extract_lineage(
            "WITH recent AS (SELECT * FROM events WHERE ts > '2026-01-01') \
             SELECT * FROM recent JOIN users USING (uid)",
        );
        assert_eq!(l.reads, vec!["events".to_string(), "users".to_string()]);
    }

    #[test]
    fn nested_cte_shadowing() {
        let l = extract_lineage("WITH t AS (SELECT * FROM raw_a) SELECT * FROM t");
        assert_eq!(l.reads, vec!["raw_a".to_string()]);
    }

    #[test]
    fn insert_records_read_and_write() {
        let l = extract_lineage(
            "INSERT INTO monthly_summary SELECT month, sum(v) FROM input_0 GROUP BY month",
        );
        assert!(l.reads.is_empty()); // input_0 is the port alias
        assert_eq!(l.writes, vec!["monthly_summary".to_string()]);
    }

    #[test]
    fn ctas_records_write_and_read() {
        let l = extract_lineage("CREATE TABLE summary AS SELECT * FROM staging_orders");
        assert_eq!(l.reads, vec!["staging_orders".to_string()]);
        assert_eq!(l.writes, vec!["summary".to_string()]);
    }

    #[test]
    fn subquery_reads_collected() {
        let l = extract_lineage(
            "SELECT * FROM (SELECT region FROM geo_map) g JOIN sales s ON g.region = s.region",
        );
        assert_eq!(l.reads, vec!["geo_map".to_string(), "sales".to_string()]);
    }

    #[test]
    fn table_function_path_recorded() {
        let l = extract_lineage("SELECT count(*) FROM read_parquet('data/lake/orders/*.parquet')");
        assert_eq!(l.reads, vec!["data/lake/orders/*.parquet".to_string()]);
    }

    #[test]
    fn quoted_identifiers_unwrapped() {
        let l = extract_lineage("SELECT * FROM \"my table\"");
        assert_eq!(l.reads, vec!["my table".to_string()]);
    }

    #[test]
    fn unparseable_sql_is_empty_not_error() {
        let l = extract_lineage("SELCT broken FROM {{{");
        assert!(l.reads.is_empty() && l.writes.is_empty());
    }

    #[test]
    fn union_reads_deduped() {
        let l = extract_lineage(
            "SELECT * FROM alpha UNION ALL SELECT * FROM alpha UNION ALL SELECT * FROM beta",
        );
        assert_eq!(l.reads, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn multiple_statements() {
        let l = extract_lineage("SELECT * FROM a; INSERT INTO b SELECT * FROM c;");
        assert_eq!(l.reads, vec!["a".to_string(), "c".to_string()]);
        assert_eq!(l.writes, vec!["b".to_string()]);
    }

    #[test]
    fn update_reads_and_writes() {
        let l = extract_lineage("UPDATE targets SET v = v + 1 WHERE k IN (SELECT k FROM src)");
        assert_eq!(l.writes, vec!["targets".to_string()]);
        assert_eq!(l.reads, vec!["src".to_string(), "targets".to_string()]);
    }
}
