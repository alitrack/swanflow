//! Column-level lineage extraction (planning-time, zero-execution).
//!
//! Extends the table-level pass in [`super::sql_lineage`] with per-output-
//! column provenance for plain SELECT statements. For each projection item
//! we walk its expression tree and record the (table_or_alias, column)
//! pairs it reads from, then group those pairs by the output column name.
//!
//! Column-level lineage is intentionally *coarser* than a full dataflow
//! solver: it answers "which source columns feed this output column" —
//! enough for canvas column mapping, impact analysis and documentation.
//! It does not track expressions through nested subqueries or CTEs.
//!
//! Best-effort like the table pass: unparseable or unsupported shapes
//! yield an empty result, never an error.

use std::collections::BTreeMap;
use std::fmt;

use sqlparser::ast::{Expr, Query, SetExpr, Statement, TableFactor, Visit};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use super::sql_lineage::object_name_to_string;

/// One source column reference: (table_or_alias, column).
pub type ColumnSource = (String, String);

/// Column-level provenance for one SQL payload.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ColumnLineage {
    /// output column name -> source (table_or_alias, column) pairs.
    /// Sorted by output name; pairs sorted within each entry.
    pub columns: BTreeMap<String, Vec<ColumnSource>>,
}

impl fmt::Display for ColumnLineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.columns.is_empty() {
            return write!(f, "(none)");
        }
        let parts: Vec<String> = self
            .columns
            .iter()
            .map(|(out, srcs)| {
                let src = srcs
                    .iter()
                    .map(|(t, c)| format!("{}.{}", t, c))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{} <= {}", out, src)
            })
            .collect();
        write!(f, "{}", parts.join("; "))
    }
}

/// Extract column-level lineage from a SQL payload. Best-effort.
pub fn extract_column_lineage(sql: &str) -> ColumnLineage {
    let dialect = DuckDbDialect {};
    let statements = match Parser::parse_sql(&dialect, sql) {
        Ok(stmts) => stmts,
        Err(_) => return ColumnLineage::default(),
    };

    // Use the first plain SELECT statement; others (CTAS bodies, set ops,
    // subqueries) are future work.
    for stmt in &statements {
        if let Statement::Query(q) = stmt {
            return column_lineage_from_query(q);
        }
    }
    ColumnLineage::default()
}

fn column_lineage_from_query(q: &Query) -> ColumnLineage {
    let body = match q.body.as_ref() {
        SetExpr::Select(sel) => sel,
        _ => return ColumnLineage::default(),
    };

    // Resolve FROM into alias -> real table name (unqualified columns
    // attribute to the single source if there is exactly one).
    let mut sources: Vec<(String, String)> = Vec::new(); // (alias_or_name, table)
    if !body.from.is_empty() {
        for twj in &body.from {
            collect_from(&twj.relation, &mut sources);
            // JOINs under each FROM item contribute columns too.
            for j in &twj.joins {
                collect_from(&j.relation, &mut sources);
            }
        }
    }

    let mut out = ColumnLineage::default();
    for item in &body.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(e) => {
                let name = expr_output_name(e);
                let srcs = expr_column_sources(e);
                if !srcs.is_empty() {
                    out.columns.insert(name, resolve_aliases(srcs, &sources));
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, alias } => {
                let srcs = expr_column_sources(expr);
                if !srcs.is_empty() {
                    out.columns
                        .insert(alias.value.clone(), resolve_aliases(srcs, &sources));
                }
            }
            sqlparser::ast::SelectItem::Wildcard(_)
            | sqlparser::ast::SelectItem::QualifiedWildcard(..) => {
                // Wildcards need schema knowledge; skipped (documented limit).
            }
        }
    }
    out
}

/// Map (qualifier, column) pairs through the FROM alias map. Unqualified
/// references pass through with the source's alias when unambiguous.
fn resolve_aliases(
    srcs: Vec<(Option<String>, String)>,
    sources: &[(String, String)],
) -> Vec<ColumnSource> {
    let mut resolved: Vec<ColumnSource> = Vec::new();
    for (qual, col) in srcs {
        match qual {
            Some(q) => {
                // qualified: match alias or table name
                if let Some((_, table)) = sources.iter().find(|(a, _)| a == &q) {
                    resolved.push((table.clone(), col));
                } else if let Some((a, _)) = sources.iter().find(|(_, t)| t == &q) {
                    resolved.push((a.clone(), col));
                } else {
                    resolved.push((q, col));
                }
            }
            None => {
                // unqualified: attribute to the single source if unique
                if sources.len() == 1 {
                    resolved.push((sources[0].1.clone(), col));
                } else {
                    resolved.push(("?".into(), col));
                }
            }
        }
    }
    resolved.sort();
    resolved.dedup();
    resolved
}

fn collect_from(tf: &TableFactor, out: &mut Vec<(String, String)>) {
    match tf {
        TableFactor::Table { name, alias, .. } => {
            let table = object_name_to_string(name);
            let alias_or_name = alias
                .as_ref()
                .map(|a| a.name.value.clone())
                .unwrap_or_else(|| table.clone());
            out.push((alias_or_name, table));
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            ..
        } => {
            let _ = lateral;
            let _ = subquery;
            // Derived tables: record alias with itself as table (column
            // provenance through subqueries is future work).
            if let Some(a) = alias {
                out.push((a.name.value.clone(), a.name.value.clone()));
            }
        }
        _ => {}
    }
}

/// Natural output name for an expression (matches DuckDB's rules closely
/// enough for diagnostics: identifier as-is, else lowercased display).
fn expr_output_name(e: &Expr) -> String {
    match e {
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(parts) => {
            // DuckDB names qualified refs by their last part (s.id -> id).
            parts.last().map(|i| i.value.clone()).unwrap_or_default()
        }
        _ => e.to_string().to_lowercase(),
    }
}

/// Collect (qualifier, column) references inside one projection expression.
fn expr_column_sources(e: &Expr) -> Vec<(Option<String>, String)> {
    let mut v = ColumnRefVisitor { refs: Vec::new() };
    let _ = e.visit(&mut v);
    v.refs
}

struct ColumnRefVisitor {
    refs: Vec<(Option<String>, String)>,
}

impl sqlparser::ast::Visitor for ColumnRefVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        match expr {
            Expr::Identifier(ident) => {
                self.refs.push((None, ident.value.clone()));
            }
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qual: Vec<&str> = parts[..parts.len() - 1]
                    .iter()
                    .map(|i| i.value.as_str())
                    .collect();
                self.refs
                    .push((Some(qual.join(".")), parts.last().unwrap().value.clone()));
            }
            _ => {}
        }
        std::ops::ControlFlow::Continue(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_columns() {
        let l = extract_column_lineage("SELECT id, name FROM input_0");
        assert_eq!(
            l.columns.get("id"),
            Some(&vec![("input_0".into(), "id".into())])
        );
        assert_eq!(
            l.columns.get("name"),
            Some(&vec![("input_0".into(), "name".into())])
        );
    }

    #[test]
    fn aliased_expression() {
        let l = extract_column_lineage("SELECT id * 10 AS scaled FROM input_0");
        assert_eq!(
            l.columns.get("scaled"),
            Some(&vec![("input_0".into(), "id".into())])
        );
    }

    #[test]
    fn qualified_join_columns() {
        let l = extract_column_lineage(
            "SELECT s.id, c.name FROM input_0 s JOIN customers c ON s.id = c.id",
        );
        assert_eq!(
            l.columns.get("id"),
            Some(&vec![("input_0".into(), "id".into())])
        );
        assert_eq!(
            l.columns.get("name"),
            Some(&vec![("customers".into(), "name".into())])
        );
    }

    #[test]
    fn multi_ref_expression() {
        let l = extract_column_lineage("SELECT price * qty AS total FROM input_0");
        assert_eq!(
            l.columns.get("total"),
            Some(&vec![
                ("input_0".into(), "price".into()),
                ("input_0".into(), "qty".into()),
            ])
        );
    }

    #[test]
    fn function_expression() {
        let l = extract_column_lineage("SELECT upper(name) AS uname FROM input_0");
        assert_eq!(
            l.columns.get("uname"),
            Some(&vec![("input_0".into(), "name".into())])
        );
    }

    #[test]
    fn wildcard_skipped() {
        let l = extract_column_lineage("SELECT * FROM input_0");
        assert!(l.columns.is_empty());
    }

    #[test]
    fn unparseable_is_empty() {
        let l = extract_column_lineage("SELCT broken {{{");
        assert!(l.columns.is_empty());
    }

    #[test]
    fn cross_join_unqualified_defaults_to_unknown() {
        let l = extract_column_lineage("SELECT a FROM input_0, other_table");
        // Two sources, unqualified ref: ambiguous -> "?" marker
        assert_eq!(l.columns.get("a"), Some(&vec![("?".into(), "a".into())]));
    }
}
