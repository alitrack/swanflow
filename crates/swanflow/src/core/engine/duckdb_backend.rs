use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{Expr, LogicalPlan};
use duckdb::{Config, Connection};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::backend::{
    EngineCapabilities, QueryEngine, QueryEngineRef, RegisteredTable, TableSource,
};

/// DuckDB-backed query engine.
///
/// Provides a pure-DuckDB execution path. Ideal for local ETL workloads where
/// DuckDB's SQL optimizer and vectorized execution outperform DataFusion's
/// LogicalPlan-based approach for simple pipelines.
///
/// Also supports SQL compilation: turning DataFusion LogicalPlans into DuckDB SQL.
pub struct DuckDBEngine {
    conn: Arc<Mutex<Connection>>,
    db_path: String,
}

impl fmt::Debug for DuckDBEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckDBEngine")
            .field("db_path", &self.db_path)
            .finish()
    }
}

/// Depth-first walk of a LogicalPlan collecting table names referenced by
/// TableScan nodes (used to recover node_<uuid> upstream tables for SqlProxy
/// input aliasing on the DuckDB backend).
fn walk_plan_tables(
    plan: &datafusion::logical_expr::LogicalPlan,
    f: &mut dyn FnMut(&str),
) {
    use datafusion::logical_expr::LogicalPlan;
    if let LogicalPlan::TableScan(scan) = plan {
        f(scan.table_name.table());
    }
    for input in plan.inputs() {
        walk_plan_tables(input, f);
    }
}

impl DuckDBEngine {
    /// Open or create a DuckDB database at the given path.
    /// Use `":memory:"` for an in-memory database.
    pub fn open(db_path: &str) -> Result<Self> {
        // allow_unsigned_extensions is required to LOAD locally-built (unsigned)
        // extensions such as the luajit UDF extension. Signed extensions from
        // the official/community registries are unaffected.
        let config = Config::default().allow_unsigned_extensions().map_err(|e| {
            datafusion::error::DataFusionError::External(Box::new(e))
        })?;
        let conn = Connection::open_with_flags(db_path, config)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Optional: load extra DuckDB extensions listed in SWANFLOW_DUCKDB_EXTENSIONS
        // (comma-separated paths). Missing files are warned, not fatal — this must
        // never break workflows that don't use extensions.
        if let Ok(exts) = std::env::var("SWANFLOW_DUCKDB_EXTENSIONS") {
            for ext in exts.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                match conn.execute_batch(&format!("LOAD '{ext}'")) {
                    Ok(()) => eprintln!("[duckdb] loaded extension: {ext}"),
                    Err(e) => eprintln!(
                        "[duckdb] WARNING: failed to load extension '{ext}': {e}"
                    ),
                }
            }
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            db_path: db_path.to_string(),
        })
    }

    /// Create an in-memory DuckDB engine (fast, ephemeral).
    pub fn new_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    /// Access the inner DuckDB connection (for direct operations).
    pub fn connection(&self) -> &Arc<Mutex<Connection>> {
        &self.conn
    }

    fn capabilities_static() -> EngineCapabilities {
        EngineCapabilities {
            in_memory_tables: true,
            logical_plan_execution: false, // DuckDB uses SQL, not LogicalPlans
            sql_compilation: true,
            external_attach: true,
            concurrent_reads: true,
        }
    }

    /// Execute a SQL statement (no return) — used for DDL like CREATE TABLE.
    async fn execute_ddl(&self, sql: &str) -> Result<()> {
        let guard = self.conn.lock().await;
        guard
            .execute(sql, [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}

/// Quote a SQL identifier (table/view/column name) with double quotes and
/// escape embedded `"` as `""`. P2-1a: closes identifier-injection via names.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Return true if `sql` contains a statement separator (`;`) outside of
/// single-quoted string literals and `--` line comments. P2-1b: used to
/// reject multi-statement input before it reaches DuckDB.
fn contains_multi_statement(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut in_string = false;
    let mut in_line_comment = false;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if in_line_comment {
            if c == b'\n' {
                in_line_comment = false;
            }
        } else if in_string {
            if c == b'\'' {
                // '' is an escaped quote inside a string literal.
                if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                    i += 1;
                } else {
                    in_string = false;
                }
            }
        } else if c == b'\'' {
            in_string = true;
        } else if c == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            in_line_comment = true;
            i += 1;
        } else if c == b';' {
            return true;
        }
        i += 1;
    }
    false
}

#[async_trait]
impl QueryEngine for DuckDBEngine {
    fn name(&self) -> &str {
        "DuckDB"
    }

    fn capabilities(&self) -> &EngineCapabilities {
        static CAPS: std::sync::LazyLock<EngineCapabilities> =
            std::sync::LazyLock::new(DuckDBEngine::capabilities_static);
        &CAPS
    }

    async fn register_table(&self, table: RegisteredTable) -> Result<()> {
        let name = table.name.clone();
        let conn = self.conn.clone();

        match &table.source {
            TableSource::InMemory { batches } => {
                if batches.is_empty() {
                    // Create an empty table from schema
                    let schema = table.schema;
                    let col_defs: Vec<String> = schema
                        .fields()
                        .iter()
                        .map(|f| {
                            format!(
                                "{} {}",
                                quote_ident(f.name()),
                                arrow_type_to_duckdb_type(f.data_type())
                            )
                        })
                        .collect();
                    let create_sql = format!(
                        "CREATE TABLE IF NOT EXISTS {} ({})",
                        quote_ident(&name),
                        col_defs.join(", ")
                    );
                    self.execute_ddl(&create_sql).await?;
                } else {
                    // Insert each batch
                    let schema = batches[0].schema();
                    let col_defs: Vec<String> = schema
                        .fields()
                        .iter()
                        .map(|f| {
                            format!(
                                "{} {}",
                                quote_ident(f.name()),
                                arrow_type_to_duckdb_type(f.data_type())
                            )
                        })
                        .collect();
                    let create_sql = format!(
                        "CREATE TABLE IF NOT EXISTS {} ({})",
                        quote_ident(&name),
                        col_defs.join(", ")
                    );
                    self.execute_ddl(&create_sql).await?;

                    for batch in batches {
                        let insert_sql = build_insert_sql(&name, batch)?;
                        self.execute_ddl(&insert_sql).await?;
                    }
                }
            }
            TableSource::Parquet { path } => {
                let sql = format!(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_parquet('{}')",
                    quote_ident(&name), path
                );
                self.execute_ddl(&sql).await?;
            }
            TableSource::Csv { path, has_header } => {
                let header_str = if *has_header { "true" } else { "false" };
                let sql = format!(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM read_csv('{}', header={})",
                    quote_ident(&name), path, header_str
                );
                self.execute_ddl(&sql).await?;
            }
            TableSource::Native { identifier } => {
                // Table already exists in DuckDB — register a view alias
                let sql = format!(
                    "CREATE OR REPLACE VIEW {} AS SELECT * FROM {}",
                    quote_ident(&name), quote_ident(identifier)
                );
                self.execute_ddl(&sql).await?;
            }
        }

        Ok(())
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        let guard = self.conn.lock().await;
        let mut stmt = guard
            .prepare("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?")
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let count: i64 = stmt
            .query_row([&name], |row| row.get(0))
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(count > 0)
    }

    async fn deregister_table(&self, name: &str) -> Result<()> {
        let sql = format!("DROP VIEW IF EXISTS {}", quote_ident(name));
        self.execute_ddl(&sql).await
    }

    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // P2-1b: reject multi-statement input (only a single statement allowed).
        if contains_multi_statement(sql) {
            return Err(datafusion::error::DataFusionError::External(
                anyhow::anyhow!("execute_sql rejects multiple SQL statements").into(),
            ));
        }
        let guard = self.conn.lock().await;
        let mut stmt = guard
            .prepare(sql)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Execute first, THEN read column metadata. The duckdb crate's
        // column_count()/schema() panic if the query hasn't been executed
        // ("The statement was not executed yet") — the C API has no way to
        // get column counts without executing. Streaming table functions
        // (e.g. the luajit extension's luajit_table) hit this even for
        // ordinary SELECTs.
        let mut rows = stmt
            .query([])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        // schema() is available after query() — yields the duckdb crate's
        // Arrow schema (arrow-58). We map its fields to our own DataType
        // via the string bridge below (arrow-57 / datafusion 52).
        let duck_schema = rows
            .as_ref()
            .expect("statement reference after query")
            .schema();

        // Build schema from column info
        let col_count = duck_schema.fields().len();
        // P2-3: unknown type strings still fall back to Utf8, but no longer silently.
        let duckdb_to_arrow = |col_type: &str, col_name: &str, col_index: usize| -> DataType {
            match col_type.to_uppercase().as_str() {
                "BOOLEAN" => DataType::Boolean,
                "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" | "INT8" | "INT16"
                | "INT32" | "INT64" | "UINT8" | "UINT16" | "UINT32" | "UINT64" => {
                    DataType::Int64
                }
                "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" | "FLOAT32" | "FLOAT64" => {
                    DataType::Float64
                }
                "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "DATETIME" => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                "DATE" => DataType::Date32,
                "VARCHAR" | "TEXT" | "STRING" | "BLOB" | "UUID" | "ENUM" | "UTF8"
                | "LARGEUTF8" | "BINARY" | "LARGEBINARY" => DataType::Utf8,
                _ => {
                    // Unrecognized type bridge: keep the Utf8 fallback (no panic),
                    // but surface it on stderr so mis-typed data is not silent.
                    eprintln!(
                        "[duckdb] WARNING: unrecognizable column type \"{}\" at col {} ({}), falling back to Utf8",
                        col_type, col_index, col_name
                    );
                    DataType::Utf8
                }
            }
        };

        let mut fields = Vec::with_capacity(col_count);
        for field in duck_schema.fields().iter() {
            let col_type = duckdb_type_to_str(field.data_type());
            let col_name = field.name().clone();
            fields.push(Field::new(
                col_name.clone(),
                duckdb_to_arrow(&col_type, &col_name, fields.len()),
                true,
            ));
        }
        let schema = Arc::new(Schema::new(fields));

        // Read all rows into column builders
        let mut builders: Vec<Box<dyn ColumnBuilder>> = schema
            .fields()
            .iter()
            .map(|f| make_builder(f.data_type()))
            .collect();

        while let Some(row) = rows
            .next()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        {
            for (i, builder) in builders.iter_mut().enumerate() {
                builder.append_from_duckdb(&row, i)?;
            }
        }

        let columns: Vec<ArrayRef> =
            builders.into_iter().map(|mut b| b.finish()).collect();

        if columns.is_empty() || columns[0].len() == 0 {
            Ok(vec![])
        } else {
            let batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            Ok(vec![batch])
        }
    }

    async fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        // DuckDB doesn't execute LogicalPlans — compile to SQL first, then execute.
        let sql = self.compile_to_sql(plan)?;
        self.execute_sql(&sql).await
    }

    fn compile_to_sql(&self, plan: &LogicalPlan) -> Result<String> {
        use datafusion::logical_expr::LogicalPlan as LP;
        match plan {
            LP::Projection(proj) => {
                let input_sql = self.compile_to_sql(proj.input.as_ref())?;
                let exprs: Vec<String> = proj
                    .expr
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect();
                Ok(format!("SELECT {} FROM ({}) AS _sub", exprs.join(", "), input_sql))
            }
            LP::Filter(filter) => {
                let input_sql = self.compile_to_sql(filter.input.as_ref())?;
                Ok(format!(
                    "SELECT * FROM ({}) AS _sub WHERE {}",
                    input_sql, filter.predicate
                ))
            }
            LP::Aggregate(agg) => {
                let input_sql = self.compile_to_sql(agg.input.as_ref())?;
                let group_keys: Vec<String> =
                    agg.group_expr.iter().map(|e| format!("{}", e)).collect();
                let aggs: Vec<String> = agg.aggr_expr.iter().map(|e| format!("{}", e)).collect();

                let select = if group_keys.is_empty() {
                    aggs.join(", ")
                } else {
                    format!("{}, {}", group_keys.join(", "), aggs.join(", "))
                };

                let group_by = if group_keys.is_empty() {
                    String::new()
                } else {
                    format!(" GROUP BY {}", group_keys.join(", "))
                };

                Ok(format!(
                    "SELECT {} FROM ({}) AS _sub{}",
                    select, input_sql, group_by
                ))
            }
            LP::Sort(sort) => {
                let input_sql = self.compile_to_sql(sort.input.as_ref())?;
                let order: Vec<String> = sort
                    .expr
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect();
                Ok(format!(
                    "SELECT * FROM ({}) AS _sub ORDER BY {}",
                    input_sql,
                    order.join(", ")
                ))
            }
            LP::Limit(limit) => {
                let input_sql = self.compile_to_sql(limit.input.as_ref())?;
                // P1-4: render LIMIT/OFFSET so row_limit (and any Limit plan)
                // is honored on the DuckDB path. Previously the clause was
                // silently dropped, returning the full table.
                let mut sql = format!("SELECT * FROM ({}) AS _sub", input_sql);
                // DuckDB canonical clause order: LIMIT {fetch} [OFFSET {skip}].
                // OFFSET is emitted only for a positive skip (OFFSET 0 is a no-op,
                // so suppress it to keep the SQL clean).
                if let Some(fetch) = &limit.fetch {
                    if let Some(n) = literal_int_value(fetch) {
                        sql.push_str(&format!(" LIMIT {n}"));
                    }
                }
                if let Some(skip) = &limit.skip {
                    if let Some(n) = literal_int_value(skip) {
                        if n != 0 {
                            sql.push_str(&format!(" OFFSET {n}"));
                        }
                    }
                }
                Ok(sql)
            }
            LP::TableScan(scan) => {
                Ok(format!("SELECT * FROM {}", quote_ident(&scan.table_name.to_string())))
            }
            LP::Subquery(_) => {
                let inputs = plan.inputs();
                if inputs.is_empty() {
                    Err(datafusion::error::DataFusionError::Plan(
                        "Subquery has no input".to_string(),
                    ))
                } else {
                    self.compile_to_sql(inputs[0])
                }
            }
            LP::SubqueryAlias(_) => {
                let inputs = plan.inputs();
                let inner = self.compile_to_sql(inputs[0])?;
                // Extract alias name from the plan's display
                let alias_name = format!("{}", plan.schema().as_ref());
                Ok(format!("({}) AS _subquery_alias", inner))
            }
            LP::EmptyRelation(empty) => {
                if empty.produce_one_row {
                    Ok("SELECT 1".to_string())
                } else {
                    Ok("SELECT * FROM (VALUES (NULL)) AS _empty WHERE FALSE".to_string())
                }
            }
            LP::Extension(ext) => {
                // P1-4 fix: extension nodes are NOT all no-ops. Treating every
                // single-input extension as a pass-through silently dropped
                // whole SQL transforms (e.g. SqlProxy carries a real query —
                // compiling it as its input made the node a no-op and user
                // SQL/writes vanished from the DuckDB pipeline).
                // Pass-through is only correct for pure wrapper nodes; a
                // node that carries its own computation must be compiled,
                // and unknown carriers must fail loudly instead of
                // disappearing.
                if ext.node.name() == "SqlProxy" {
                    // Deferred import: nodes module is part of the same crate.
                    if let Some(proxy) =
                        crate::nodes::sql::SqlProxyLogical::from_extension(ext)
                    {
                        // r4 C4: the DuckDB staged executor registers upstream
                        // outputs as node_<uuid> tables, but SqlProxy SQL
                        // references input_0..N aliases. Without aliasing, the
                        // SQL would either fail to resolve or (worse) silently
                        // target an unrelated table. Emit TEMP VIEW aliases for
                        // every input whose upstream node id we can recover
                        // from the input LogicalPlan (TableScan on node_<uuid>).
                        let mut prologue = String::new();
                        let mut resolved = 0usize;
                        for (i, input_plan) in proxy.inputs.iter().enumerate() {
                            let mut upstream_table: Option<String> = None;
                            walk_plan_tables(input_plan, &mut |name| {
                                if upstream_table.is_none()
                                    && name.starts_with("node_")
                                {
                                    upstream_table = Some(name.to_string());
                                }
                            });
                            if let Some(src) = upstream_table {
                                prologue.push_str(&format!(
                                    "CREATE OR REPLACE TEMP VIEW input_{i} AS SELECT * FROM {src}; "
                                ));
                                resolved += 1;
                            }
                        }
                        if resolved == proxy.inputs.len() && !proxy.inputs.is_empty() {
                            let inlined =
                                crate::core::engine::planner::inline_placeholders(
                                    &proxy.query,
                                    &proxy.params,
                                );
                            return Ok(format!("{}{}", prologue, inlined));
                        }
                        // Cannot recover every upstream table (e.g. complex
                        // input plans): fail loudly instead of running SQL
                        // against unresolved input_0 (silent wrong data).
                        return Err(datafusion::error::DataFusionError::Plan(
                            "SqlProxy on the DuckDB backend requires resolvable                              node_<uuid> inputs for input_0..N aliasing"
                                .to_string(),
                        ));
                    }
                }
                if ext.node.name() == "MissingNodePlaceholder" {
                    // r4 C7a: placeholders must fail loudly on this backend too.
                    // A single-input pass-through silently forwarded the input,
                    // defeating the fail-closed mapper contract for KNIME
                    // writer nodes (output would appear to succeed).
                    return Err(datafusion::error::DataFusionError::Plan(
                        "missing-node placeholder cannot execute on the DuckDB backend"
                            .to_string(),
                    ));
                }
                if ext.node.inputs().len() == 1 {
                    // Wrapper nodes (MaterializerLogical, StateProbeLogical,
                    // etc.) transparently forward to their single input.
                    self.compile_to_sql(&ext.node.inputs()[0])
                } else {
                    Err(datafusion::error::DataFusionError::NotImplemented(format!(
                        "SQL compilation for extension node '{}' with {} inputs",
                        ext.node.name(),
                        ext.node.inputs().len()
                    )))
                }
            }
            LP::Join(join) => {
                let left_sql = self.compile_to_sql(join.left.as_ref())?;
                let right_sql = self.compile_to_sql(join.right.as_ref())?;
                // Inner join with no filter = CROSS JOIN
                let is_cross = join.join_type == datafusion::logical_expr::JoinType::Inner
                    && join.filter.is_none();
                let join_keyword = if is_cross {
                    "CROSS JOIN"
                } else {
                    match join.join_type {
                        datafusion::logical_expr::JoinType::Inner => "INNER JOIN",
                        datafusion::logical_expr::JoinType::Left => "LEFT JOIN",
                        datafusion::logical_expr::JoinType::Right => "RIGHT JOIN",
                        datafusion::logical_expr::JoinType::Full => "FULL OUTER JOIN",
                        datafusion::logical_expr::JoinType::LeftSemi => "LEFT SEMI JOIN",
                        datafusion::logical_expr::JoinType::LeftAnti => "LEFT ANTI JOIN",
                        datafusion::logical_expr::JoinType::RightSemi => "RIGHT SEMI JOIN",
                        datafusion::logical_expr::JoinType::RightAnti => "RIGHT ANTI JOIN",
                        datafusion::logical_expr::JoinType::LeftMark => "LEFT MARK JOIN",
                        datafusion::logical_expr::JoinType::RightMark => "RIGHT MARK JOIN",
                    }
                };
                let condition = if is_cross {
                    String::new()
                } else {
                    join.filter
                        .as_ref()
                        .map(|e| format!(" ON {}", e))
                        .unwrap_or_default()
                };
                Ok(format!(
                    "SELECT * FROM ({}) AS _l {} ({}) AS _r{}",
                    left_sql, join_keyword, right_sql, condition
                ))
            }
            LP::Union(union) => {
                let parts: Result<Vec<String>> = union
                    .inputs
                    .iter()
                    .map(|p| self.compile_to_sql(p))
                    .collect();
                Ok(format!("({})", parts?.join(" UNION ALL ")))
            }
            LP::Distinct(_) => {
                let inputs = plan.inputs();
                let input_sql = self.compile_to_sql(inputs[0])?;
                Ok(format!("SELECT DISTINCT * FROM ({}) AS _sub", input_sql))
            }
            _ => Err(datafusion::error::DataFusionError::NotImplemented(format!(
                "SQL compilation not implemented for LogicalPlan variant: {:?}",
                std::mem::discriminant(plan)
            ))),
        }
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let guard = self.conn.lock().await;
        let mut stmt = guard
            .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let tables: Vec<String> = rows.filter_map(|r| r.ok()).collect();
        Ok(tables)
    }

    async fn get_table_schema(&self, name: &str) -> Result<SchemaRef> {
        let guard = self.conn.lock().await;
        let mut stmt = guard
            .prepare(&format!("DESCRIBE {}", quote_ident(name)))
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let mut fields = Vec::new();
        let mut rows = stmt
            .query([])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        while let Some(row) = rows
            .next()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        {
            let col_name: String = row
                .get(0)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let col_type: String = row
                .get(1)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let arrow_type = match col_type.to_uppercase().as_str() {
                "BOOLEAN" => DataType::Boolean,
                "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "HUGEINT" => DataType::Int64,
                "FLOAT" | "DOUBLE" | "REAL" | "DECIMAL" => DataType::Float64,
                "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "DATETIME" => {
                    DataType::Timestamp(TimeUnit::Nanosecond, None)
                }
                "DATE" => DataType::Date32,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(col_name, arrow_type, true));
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    fn arc_clone(&self) -> QueryEngineRef {
        Arc::new(Self {
            conn: self.conn.clone(),
            db_path: self.db_path.clone(),
        })
    }
}

// --- Helper types for DuckDB → Arrow conversion ---

trait ColumnBuilder: Send {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>;
    fn finish(&mut self) -> ArrayRef;
}

struct Int64ColumnBuilder(Int64Builder);
struct Float64ColumnBuilder(Float64Builder);
struct BooleanColumnBuilder(BooleanBuilder);
struct TimestampColumnBuilder(TimestampNanosecondBuilder);
struct StringColumnBuilder(StringBuilder);

impl ColumnBuilder for Int64ColumnBuilder {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let val: Option<i64> = row.get(index)?;
        self.0.append_option(val);
        Ok(())
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl ColumnBuilder for Float64ColumnBuilder {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let val: Option<f64> = row.get(index)?;
        self.0.append_option(val);
        Ok(())
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl ColumnBuilder for BooleanColumnBuilder {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let val: Option<bool> = row.get(index)?;
        self.0.append_option(val);
        Ok(())
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl ColumnBuilder for TimestampColumnBuilder {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Read timestamp as string to avoid chrono::NaiveDateTime FromSql issues
        let val: Option<String> = row.get(index)?;
        match val {
            Some(s) => {
                // Parse common timestamp formats
                let nanos = if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                    dt.and_utc().timestamp_nanos_opt().unwrap_or(0)
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
                    dt.and_utc().timestamp_nanos_opt().unwrap_or(0)
                } else {
                    0
                };
                self.0.append_value(nanos);
            }
            None => self.0.append_null(),
        }
        Ok(())
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl ColumnBuilder for StringColumnBuilder {
    fn append_from_duckdb(
        &mut self,
        row: &duckdb::Row,
        index: usize,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let val: Option<String> = row.get(index)?;
        self.0.append_option(val);
        Ok(())
    }
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

fn make_builder(data_type: &DataType) -> Box<dyn ColumnBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanColumnBuilder(BooleanBuilder::new())),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Box::new(Int64ColumnBuilder(Int64Builder::new()))
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            Box::new(Float64ColumnBuilder(Float64Builder::new()))
        }
        DataType::Timestamp(_, _) => {
            Box::new(TimestampColumnBuilder(TimestampNanosecondBuilder::new()))
        }
        _ => Box::new(StringColumnBuilder(StringBuilder::new())),
    }
}

/// Convert duckdb::arrow::datatypes::DataType to a string representation.
fn duckdb_type_to_str(dt: &duckdb::arrow::datatypes::DataType) -> String {
    format!("{:?}", dt)
}

fn arrow_type_to_duckdb_type(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "BIGINT",
        DataType::Float16 | DataType::Float32 | DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// Build an INSERT statement from a RecordBatch.
fn build_insert_sql(table_name: &str, batch: &RecordBatch) -> Result<String> {
    let schema = batch.schema();
    let col_names: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();

    let mut value_rows = Vec::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut values = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..batch.num_columns() {
            let col = batch.column(col_idx);
            let val = scalar_to_sql_literal(col, row_idx)?;
            values.push(val);
        }
        value_rows.push(format!("({})", values.join(", ")));
    }

    Ok(format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_ident(table_name),
        col_names.join(", "),
        value_rows.join(", ")
    ))
}

fn scalar_to_sql_literal(col: &ArrayRef, row_idx: usize) -> Result<String> {
    if col.is_null(row_idx) {
        return Ok("NULL".to_string());
    }
    Ok(match col.data_type() {
        DataType::Boolean => {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::BooleanArray>()
                .unwrap();
            if arr.value(row_idx) {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        DataType::Int64 => {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
                .unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Float64Array>()
                .unwrap();
            arr.value(row_idx).to_string()
        }
        DataType::Utf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StringArray>()
                .unwrap();
            format!("'{}'", arr.value(row_idx).replace('\'', "''"))
        }
        _ => {
            // Fallback: use Display
            let scalar = ScalarValue::try_from_array(col, row_idx)?;
            format!("'{}'", scalar.to_string().replace('\'', "''"))
        }
    })
}

/// Extract an integer value from a literal `Expr` (DataFusion 52 represents
/// `Limit::skip/fetch` as `Option<Box<Expr>>`, built from integer literals by
/// `LogicalPlanBuilder::limit`). Returns `None` for non-literal expressions
/// (defensive: such a plan would then skip the clause rather than emit junk).
fn literal_int_value(expr: &Expr) -> Option<i64> {
    if let Expr::Literal(sv, _schema) = expr {
        match sv {
            ScalarValue::Int8(v) => v.map(|n| n as i64),
            ScalarValue::Int16(v) => v.map(|n| n as i64),
            ScalarValue::Int32(v) => v.map(|n| n as i64),
            ScalarValue::Int64(v) => *v,
            ScalarValue::UInt8(v) => v.map(|n| n as i64),
            ScalarValue::UInt16(v) => v.map(|n| n as i64),
            ScalarValue::UInt32(v) => v.map(|n| n as i64),
            ScalarValue::UInt64(v) => v.map(|n| i64::try_from(n).unwrap_or(i64::MAX)),
            _ => None,
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::logical_expr::{LogicalPlanBuilder, LogicalTableSource};
    use tokio::runtime::Runtime;

    #[test]
    fn test_duckdb_engine_in_memory() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();
            assert_eq!(engine.name(), "DuckDB");
            assert!(engine.capabilities().sql_compilation);
            assert!(engine.capabilities().external_attach);
        });
    }

    #[test]
    fn test_register_and_query_in_memory_table() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));

            let id_arr = Int64Array::from(vec![1, 2, 3]);
            let name_arr = StringArray::from(vec!["alice", "bob", "carol"]);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_arr), Arc::new(name_arr)],
            )
            .unwrap();

            let table = RegisteredTable {
                name: "users".to_string(),
                schema,
                source: TableSource::InMemory {
                    batches: vec![batch],
                },
            };

            engine.register_table(table).await.unwrap();
            assert!(engine.table_exists("users").await.unwrap());

            let result = engine
                .execute_sql("SELECT * FROM users ORDER BY id")
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 3);
        });
    }

    #[test]
    fn test_limit_clause_honored_on_duckdb_path() {
        // P1-4 regression: compile_to_sql used to DROP the Limit clause
        // (skip/fetch), so row_limit middleware silently returned the full
        // table on the DuckDB path. This test asserts the *executed* row
        // count, not just the plan string (the plan-level assertion in
        // middleware.rs only checks the Debug repr and would pass either way).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ]));

            let id_arr = Int64Array::from(vec![1, 2, 3, 4, 5]);
            let name_arr = StringArray::from(vec![
                "a", "b", "c", "d", "e",
            ]);
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_arr), Arc::new(name_arr)],
            )
            .unwrap();

            let table = RegisteredTable {
                name: "users5".to_string(),
                schema: schema.clone(),
                source: TableSource::InMemory {
                    batches: vec![batch],
                },
            };

            engine.register_table(table).await.unwrap();

            // Build the plan exactly the way row_limit middleware produces it
            // at runtime: Limit(skip/fetch) directly over a TableScan — no
            // projection layer on top (that is what the middleware appends,
            // and what compile_to_sql's LP::Limit branch used to drop).
            // DataFusion 52's LogicalTableSource is the TableSource variant
            // that works with LogicalPlanBuilder::scan; compile_to_sql only
            // needs the table name, which resolves against the engine's own
            // in-memory table above.
            let lts = Arc::new(LogicalTableSource::new(schema.clone()));
            let plan = LogicalPlanBuilder::scan("users5", lts.clone(), None)
                .unwrap()
                .limit(0, Some(2))
                .unwrap()
                .build()
                .unwrap();

            let sql = engine.compile_to_sql(&plan);
            assert!(sql.is_ok(), "compile_to_sql failed: {sql:?}");
            let sql = sql.unwrap();
            assert!(
                sql.contains("LIMIT 2"),
                "Limit clause must be rendered by compile_to_sql: {sql}"
            );
            assert!(
                !sql.to_uppercase().contains("OFFSET"),
                "skip=0 must not emit OFFSET: {sql}"
            );

            // Execution-level assertion: the compiled SQL must actually
            // return 2 rows (not the full 5-row table).
            let result = engine.execute_sql(&sql).await.unwrap();
            let rows: usize = result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "DuckDB execution must honor LIMIT: {sql}");

            // OFFSET must be honored too (skip=3, fetch=2 → rows 4..5).
            let plan = LogicalPlanBuilder::scan("users5", lts.clone(), None)
                .unwrap()
                .limit(3, Some(2))
                .unwrap()
                .build()
                .unwrap();
            let sql = engine.compile_to_sql(&plan).unwrap();
            assert!(
                sql.contains("LIMIT 2") && sql.contains("OFFSET 3"),
                "OFFSET+LIMIT must both be rendered: {sql}"
            );
            let result = engine.execute_sql(&sql).await.unwrap();
            let rows: usize = result.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2, "DuckDB execution must honor OFFSET+LIMIT: {sql}");
        });
    }

    #[test]
    fn test_list_tables() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();
            let tables = engine.list_tables().await.unwrap();
            // New in-memory DB has no tables
            assert!(tables.is_empty());

            // Register a table
            engine
                .execute_ddl("CREATE TABLE test (x INTEGER)")
                .await
                .unwrap();
            let tables = engine.list_tables().await.unwrap();
            assert!(tables.contains(&"test".to_string()));
        });
    }

    /// Integration: luajit extension loads and executes via SWANFLOW_DUCKDB_EXTENSIONS.
    /// Requires a locally-built unsigned luajit.duckdb_extension (see duckdb-luajit repo).
    /// Gated on the env var — never a hard dependency in CI.
    #[test]
    fn test_luajit_extension_via_env() {
        let ext = match std::env::var("SWANFLOW_LUAJIT_EXT") {
            Ok(p) if !p.is_empty() => p,
            _ => return, // not configured — skip
        };
        // Duplicate the env plumbing: the engine reads SWANFLOW_DUCKDB_EXTENSIONS at open.
        std::env::set_var("SWANFLOW_DUCKDB_EXTENSIONS", &ext);
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();
            // Probe row.get with raw Value first to isolate builder vs FromSql.
            {
                let guard = engine.connection().lock().await;
                let mut stmt = guard
                    .prepare("SELECT * FROM luajit_table('return {\"a\", \"b\"}')")
                    .unwrap();
                let mut rows = stmt.query([]).unwrap();
                let mut got: Vec<(duckdb::types::Value, duckdb::types::Value)> = vec![];
                while let Some(row) = rows.next().unwrap() {
                    let v0 = row.get::<usize, duckdb::types::Value>(0).unwrap();
                    let v1 = row.get::<usize, duckdb::types::Value>(1).unwrap();
                    got.push((v0, v1));
                }
                assert_eq!(got.len(), 2);
                assert_eq!(got[0].1, duckdb::types::Value::Text("a".into()));
            }
            // Full path through execute_sql (builder conversion).
            let result = engine
                .execute_sql("SELECT * FROM luajit_table('return {\"a\", \"b\"}')")
                .await
                .unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 2);
        });
    }
    #[test]
    fn test_execute_sql_rejects_multiple_statements() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let engine = DuckDBEngine::new_in_memory().unwrap();
            // P2-1b: a statement separator must be rejected.
            assert!(engine.execute_sql("SELECT 1; SELECT 2").await.is_err());
            // A semicolon inside a string literal is NOT a separator -> allowed.
            assert!(engine.execute_sql("SELECT 'a;b' AS x").await.is_ok());
        });
    }

}
