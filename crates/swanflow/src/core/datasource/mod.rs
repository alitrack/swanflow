use datafusion::prelude::*;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, DisplayAs, DisplayFormatType,
    ExecutionPlan, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;

#[non_exhaustive]
pub enum DynamicArrayBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    Timestamp(TimestampNanosecondBuilder),
    Utf8(StringBuilder),
}

impl DynamicArrayBuilder {
    pub fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Timestamp(_, _) => {
                Self::Timestamp(TimestampNanosecondBuilder::with_capacity(capacity))
            }
            _ => Self::Utf8(StringBuilder::with_capacity(capacity, capacity * 32)),
        }
    }

    /// Append a null value to the builder.
    pub fn append_null(&mut self) {
        match self {
            Self::Int64(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::Timestamp(b) => b.append_null(),
            Self::Utf8(b) => b.append_null(),
        }
    }

    /// Append an Int64 value.
    pub fn append_int64(&mut self, val: i64) {
        if let Self::Int64(b) = self {
            b.append_value(val);
        }
    }

    /// Append a Float64 value.
    pub fn append_float64(&mut self, val: f64) {
        if let Self::Float64(b) = self {
            b.append_value(val);
        }
    }

    /// Append a boolean value.
    pub fn append_bool(&mut self, val: bool) {
        if let Self::Boolean(b) = self {
            b.append_value(val);
        }
    }

    /// Append a string value.
    pub fn append_string(&mut self, val: &str) {
        if let Self::Utf8(b) = self {
            b.append_value(val);
        }
    }

    /// Append a timestamp in nanoseconds.
    pub fn append_timestamp_nanos(&mut self, nanos: i64) {
        if let Self::Timestamp(b) = self {
            b.append_value(nanos);
        }
    }

    /// Append an optional Int64 value.
    pub fn append_option_i64(&mut self, val: Option<i64>) {
        if let Self::Int64(b) = self {
            b.append_option(val);
        }
    }

    /// Append an optional Float64 value.
    pub fn append_option_f64(&mut self, val: Option<f64>) {
        if let Self::Float64(b) = self {
            b.append_option(val);
        }
    }

    /// Append an optional boolean value.
    pub fn append_option_bool(&mut self, val: Option<bool>) {
        if let Self::Boolean(b) = self {
            b.append_option(val);
        }
    }

    /// Append an optional string value.
    pub fn append_option_string(&mut self, val: Option<String>) {
        if let Self::Utf8(b) = self {
            b.append_option(val);
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Timestamp(b) => Arc::new(b.finish()) as ArrayRef,
            Self::Utf8(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

/// Logical node for ClickHouse scanning over HTTP (Arrow stream format).
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ClickHouseScanLogical {
    pub url: String,
    pub query: String,
    pub schema: DFSchemaRef,
}

impl PartialOrd for ClickHouseScanLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ClickHouseScanLogical {
    fn name(&self) -> &str {
        "ClickHouseScan"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClickHouseScan(query={})", self.query)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

/// Physical execution plan for ClickHouse HTTP scanning (Arrow stream).
#[derive(Debug)]
pub struct ClickHouseScanExec {
    pub url: String,
    pub query: String,
    pub project_schema: SchemaRef,
    cache: PlanProperties,
}

impl ClickHouseScanExec {
    pub fn new(url: String, query: String, project_schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(project_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            url,
            query,
            project_schema,
            cache,
        }
    }
}

impl DisplayAs for ClickHouseScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ClickHouseScanExec(query={})", self.query)
    }
}

impl ExecutionPlan for ClickHouseScanExec {
    fn name(&self) -> &'static str {
        "ClickHouseScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.project_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.url.clone(),
            self.query.clone(),
            self.project_schema.clone(),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let url = self.url.clone();
        let query = self.query.clone();
        let schema = self.project_schema.clone();

        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
            use datafusion::arrow::ipc::reader::StreamReader;
            let client = reqwest::Client::new();
            let arrow_url = format!(
                "{}/?query={}&format=Arrow",
                url.trim_end_matches('/'),
                urlencode_for_ch(&query)
            );
            let resp = client
                .get(&arrow_url)
                .send()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "ClickHouse HTTP {} : {}",
                    status, body
                )));
            }
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            if !bytes.is_empty() {
                let reader =
                    StreamReader::try_new(bytes.as_ref(), None).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                for batch in reader {
                    let batch = batch.map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                    let _ = tx.send(Ok(batch)).await;
                }
            }
            Ok(())
        });

        Ok(builder.build())
    }
}

/// Minimal percent-encoding for ClickHouse query strings (unreserved set).
fn urlencode_for_ch(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

/// Logical node for DuckDB-based external scanning (Postgres, SQLite).
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DuckDBScanLogical {
    pub connection_path: String,
    pub extensions: Vec<String>,
    pub attach_sql: Option<String>,
    pub query: String,
    pub schema: DFSchemaRef,
    /// Upstream DataFusion plans materialized as DuckDB tables input_0..N
    /// before the query runs (used by e.g. luajit scalar mode). Empty for
    /// pure external scans (postgres/sqlite/http).
    pub input_tables: Vec<LogicalPlan>,
}

impl PartialOrd for DuckDBScanLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for DuckDBScanLogical {
    fn name(&self) -> &str {
        "DuckDBScan"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.input_tables.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDBScan(query={})", self.query)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            input_tables: inputs,
            ..self.clone()
        })
    }
}

/// Physical execution plan for DuckDB external scanning.
#[derive(Debug)]
pub struct DuckDBScanExec {
    pub connection_path: String,
    pub extensions: Vec<String>,
    pub attach_sql: Option<String>,
    pub query: String,
    pub project_schema: SchemaRef,
    /// Physical plans for the input_tables (planner converts logical->physical).
    input_plans: Vec<Arc<dyn ExecutionPlan>>,
    cache: PlanProperties,
}

impl DuckDBScanExec {
    pub fn new(
        connection_path: String,
        extensions: Vec<String>,
        attach_sql: Option<String>,
        query: String,
        project_schema: SchemaRef,
    ) -> Self {
        Self::with_inputs(
            connection_path,
            extensions,
            attach_sql,
            query,
            project_schema,
            Vec::new(),
        )
    }

    pub fn with_inputs(
        connection_path: String,
        extensions: Vec<String>,
        attach_sql: Option<String>,
        query: String,
        project_schema: SchemaRef,
        input_plans: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(project_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            connection_path,
            extensions,
            attach_sql,
            query,
            project_schema,
            input_plans,
            cache,
        }
    }
}

impl DisplayAs for DuckDBScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDBScanExec(query={})", self.query)
    }
}

impl ExecutionPlan for DuckDBScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "DuckDBScanExec"
    }
    fn schema(&self) -> SchemaRef {
        self.project_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.input_plans.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DuckDBScanExec::with_inputs(
            self.connection_path.clone(),
            self.extensions.clone(),
            self.attach_sql.clone(),
            self.query.clone(),
            self.project_schema.clone(),
            children,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Everything (DuckDB conn, input materialization, COPY, read-back) runs
        // inside a spawned task — execute() must not block_on upstream streams:
        // those streams are polled by the SAME runtime workers, blocking here
        // deadlocks (wf78 hang root cause).
        let mut builder = RecordBatchReceiverStream::builder(self.project_schema.clone(), 1);
        let tx = builder.tx();

        let connection_path = self.connection_path.clone();
        let extensions = self.extensions.clone();
        let attach_sql = self.attach_sql.clone();
        let query = self.query.clone();
        let project_schema = self.project_schema.clone();
        // inputs are executed in the spawned task via their own plans
        let input_plans = self.input_plans.clone();
        let task_context = _context.clone();

        builder.spawn(async move {
            let config = duckdb::Config::default()
                .allow_unsigned_extensions()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let conn = duckdb::Connection::open_with_flags(&connection_path, config)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            for ext in &extensions {
                // r4 C5: by-name INSTALLs must pass the shared allowlist.
                if !(ext.contains('/') || ext.contains('\\') || ext.ends_with(".duckdb_extension")) {
                    let name = ext.trim();
                    let safe = !name.is_empty()
                        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                        && crate::nodes::sql_duckdb::ALLOWED_INSTALLABLE_EXTENSIONS
                            .contains(&name);
                    if !safe {
                        return Err(datafusion::error::DataFusionError::Plan(format!(
                            "extension '{}' is not in the installable allowlist",
                            ext
                        )));
                    }
                }
                if ext.contains('/') || ext.contains('\\') || ext.ends_with(".duckdb_extension") {
                    conn.execute(&format!("LOAD '{}';", ext), [])
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                } else {
                    conn.execute(&format!("INSTALL {};", ext), []).ok();
                    conn.execute(&format!("LOAD {};", ext), [])
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                }
            }

            if let Some(ref attach) = attach_sql {
                conn.execute(attach, [])
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            }

            // Materialize upstream inputs as DuckDB tables input_0..N via temp
            // parquet (same bridge technique as sql_duckdb::run_duckdb_query).
            for (i, plan) in input_plans.iter().enumerate() {
                let mut stream = plan.execute(0, task_context.clone())?;
                let f = std::env::temp_dir()
                    .join(format!("rustflow_dbscan_in_{}_{}.parquet", i, uuid::Uuid::new_v4()));
                use parquet::arrow::ArrowWriter;
                let mut writer: Option<ArrowWriter<std::fs::File>> = None;
                while let Some(batch) = stream.next().await {
                    let batch = batch?;
                    if writer.is_none() {
                        let file = std::fs::File::create(&f)
                            .map_err(datafusion::error::DataFusionError::IoError)?;
                        writer = Some(
                            ArrowWriter::try_new(file, batch.schema(), None)
                                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?,
                        );
                    }
                    if let Some(w) = writer.as_mut() {
                        w.write(&batch)
                            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    }
                }
                if writer.is_none() {
                    return Err(datafusion::error::DataFusionError::Internal(format!(
                        "DuckDBScan input_{i} produced no batches"
                    )));
                }
                writer
                    .unwrap()
                    .close()
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let in_path = f.to_string_lossy().replace('\'', "''");
                conn.execute(
                    &format!(
                        "CREATE TEMP TABLE input_{i} AS SELECT * FROM read_parquet('{in_path}')"
                    ),
                    [],
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let _ = std::fs::remove_file(&f);
            }

            // Use a temporary parquet file to bridge DuckDB and DataFusion Arrow versions
            let tmp_dir = std::env::temp_dir();
            let tmp_file = tmp_dir.join(format!("rustflow_duckdb_{}.parquet", uuid::Uuid::new_v4()));
            let tmp_path = tmp_file.to_str().unwrap();

            let copy_sql = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query, tmp_path);
            conn.execute(&copy_sql, [])
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // Read back with DataFusion
            let ctx = SessionContext::new();
            let df = ctx
                .read_parquet(tmp_path, datafusion::prelude::ParquetReadOptions::default())
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let batches = df
                .collect()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            // The parquet reader (arrow 57) forces BYTE_ARRAY columns to view
            // types (Utf8View/BinaryView), while `project_schema` came from the
            // DuckDB C stream (Utf8/Binary). Downstream coalescers assert on the
            // mismatch (debug panic / release soundness hazard). Cast every batch
            // column to the declared schema — Utf8View→Utf8 etc. — before streaming.
            // (Found by pi worker tier-1 dataflow verification, 2026-09-21.)
            let target_schema = project_schema.as_ref();
            for b in batches {
                let columns = target_schema
                    .fields()
                    .iter()
                    .zip(b.columns())
                    .map(|(field, col)| arrow::compute::cast(col, field.data_type()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                let batch = arrow::record_batch::RecordBatch::try_new(
                    Arc::new(target_schema.clone()),
                    columns,
                )
                .map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                if tx.send(Ok(batch)).await.is_err() {
                    break; // receiver dropped
                }
            }

            // Clean up
            std::fs::remove_file(tmp_file).ok();

            Ok(())
        });

        Ok(builder.build())
    }
}

// ---------------------------------------------------------------------------
// Connector Trait — formal interface for external database connectors
// ---------------------------------------------------------------------------

/// Configuration for connecting to an external database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnConfig {
    pub url: String,
    pub driver: String,       // "postgres", "mysql", "clickhouse", "mssql"
    pub max_connections: u32, // default 10
    pub timeout_secs: u64,    // default 30
}

impl Default for ConnConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            driver: "postgres".to_string(),
            max_connections: 10,
            timeout_secs: 30,
        }
    }
}

use crate::core::error::CoreResult;

/// Formal Connector trait — implement for each database backend.
#[async_trait::async_trait]
pub trait Connector: Send + Sync {
    /// Connect to the database using the given config.
    async fn connect(config: ConnConfig) -> CoreResult<Self>
    where
        Self: Sized;

    /// List all available table names.
    async fn list_tables(&self) -> CoreResult<Vec<String>>;

    /// Get the Arrow schema for a given table.
    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef>;

    /// Scan a table with an optional column projection and return an Arrow record batch stream.
    async fn scan(
        &self,
        table: &str,
        projection: &[usize],
    ) -> CoreResult<SendableRecordBatchStream>;

    /// Return the human-readable driver name (e.g. "postgres", "mysql", "clickhouse").
    fn name(&self) -> &'static str;
}


pub mod excel;
pub use excel::*;

// ---------------------------------------------------------------------------
// DuckDB Write Sink — persists Arrow batches into a durable DuckDB table.
//
// Bridge strategy (mirrors DuckDBScanExec's read direction): batches are
// spilled to a temp parquet file, then `CREATE OR REPLACE TABLE ... AS SELECT
// * FROM read_parquet(...)` lands them in the target database. This keeps
// zero-copy Arrow handling in the DataFusion direction and lets DuckDB do the
// durable write with native type inference.
// ---------------------------------------------------------------------------

/// Write mode for `DuckDBWriteSink`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WriteMode {
    /// `CREATE OR REPLACE TABLE` — each run replaces the whole table (default,
    /// historical behavior).
    Replace,
    /// Append rows; when `partition_by` is non-empty, rows in the same
    /// partition (per the partition key spec) are deleted first, making
    /// per-interval backfills idempotent.
    Append { partition_by: Vec<PartitionCol> },
    /// Upsert by primary key(s): incoming rows whose key exists in the
    /// target are updated (non-key columns), others inserted. Idempotent —
    /// re-running the same batch yields the same table state.
    /// (Ported from Starlake's writeStrategy APPEND_WITH_MERGE family,
    ///  #2026111603 spike.)
    Merge { keys: Vec<String> },
    /// Type-2 slowly changing dimension: keyed rows are closed (valid_to =
    /// run marker, is_current = false) when any tracked attribute changed,
    /// and the new version inserted as current. First run creates the table
    /// with the SCD2 control columns.
    /// (Ported from Starlake's writeStrategy SCD2, #2026111603 spike.)
    Scd2 {
        keys: Vec<String>,
        /// Attribute columns tracked for change detection. Empty = all
        /// non-key columns.
        tracked: Vec<String>,
        /// Start-of-validity column added to the table (default
        /// `_valid_from`).
        valid_from: String,
        /// End-of-validity column (default `_valid_to`, NULL = current).
        valid_to: String,
        /// Current-row flag column (default `_is_current`).
        is_current: String,
    },
}

/// One partition key column. `trunc` (e.g. "day"/"hour") applies
/// `date_trunc('<trunc>', col)` to both sides of the delete predicate —
/// a backfill for one hour of a timestamp column replaces exactly that
/// hour's rows, not the whole column value's rows.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionCol {
    pub column: String,
    /// DuckDB date_trunc unit: year|quarter|month|week|day|hour|minute|second
    pub trunc: Option<String>,
}

impl PartitionCol {
    /// SQL expression referencing this key over a table alias/qualifier
    /// (empty qualifier = unqualified). Escaped identifier included.
    fn sql_expr(&self, qualifier: &str) -> String {
        let col = self.column.replace('"', "\"\"");
        let ident = if qualifier.is_empty() {
            format!("\"{}\"", col)
        } else {
            format!("{}.\"{}\"", qualifier, col)
        };
        match &self.trunc {
            Some(u) => format!("date_trunc('{}', {})", u.replace('\'', "''"), ident),
            None => ident,
        }
    }
}

/// Logical node for writing upstream data into a DuckDB table.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DuckDBWriteSinkLogical {
    pub db_path: String,
    pub table: String,
    pub write_mode: WriteMode,
    pub inputs: Vec<LogicalPlan>,
}

impl PartialOrd for DuckDBWriteSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for DuckDBWriteSinkLogical {
    fn name(&self) -> &str {
        "DuckDBWriteSink"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.inputs.first().map(|p| p.schema()).unwrap_or_else(|| {
            static EMPTY: std::sync::OnceLock<DFSchemaRef> = std::sync::OnceLock::new();
            EMPTY.get_or_init(|| Arc::new(DFSchema::empty()))
        })
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDBWriteSink(db={}, table={})", self.db_path, self.table)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            db_path: self.db_path.clone(),
            table: self.table.clone(),
            write_mode: self.write_mode.clone(),
            inputs,
        })
    }
}

/// Physical execution plan: drain all input batches, spill to parquet, and
/// load them into `table` inside `db_path`. (Bridge = temp parquet, same as
/// `DuckDBScanExec` reads.)
#[derive(Debug)]
pub struct DuckDBWriteSinkExec {
    pub db_path: String,
    pub table: String,
    pub write_mode: WriteMode,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    pub out_schema: SchemaRef,
    cache: PlanProperties,
}

impl DuckDBWriteSinkExec {
    pub fn new(
        db_path: String,
        table: String,
        write_mode: WriteMode,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        out_schema: SchemaRef,
    ) -> Self {
        // CoalescePartitionsExec discipline (Bug-7 family, mirrors
        // DuckSqlExec::new): we execute every input at partition 0 only,
        // so multi-partition inputs (UnionExec etc.) must be coalesced
        // first or partitions 1..N are silently dropped.
        let inputs: Vec<Arc<dyn ExecutionPlan>> = inputs
            .into_iter()
            .map(|i| Arc::new(CoalescePartitionsExec::new(i)) as Arc<dyn ExecutionPlan>)
            .collect();
        let cache = PlanProperties::new(
            EquivalenceProperties::new(out_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            db_path,
            table,
            write_mode,
            inputs,
            out_schema,
            cache,
        }
    }

    /// Guard shared by `merge` / `scd2`: a batch that carries two rows under
    /// the same key has no order-independent result. `merge` would leave both
    /// rows behind (DELETE+INSERT without a PK constraint), `scd2` would open
    /// two `is_current` versions for the key. Either outcome depends on batch
    /// layout, so fail loudly instead — the caller dedupes upstream
    /// (adversarial review F4/F5, 2026-11-20).
    fn reject_duplicate_keys(
        conn: &duckdb::Connection,
        parquet: &str,
        keys: &[String],
        mode: &str,
    ) -> std::result::Result<(), String> {
        let cols = keys
            .iter()
            .map(|k| format!("\"{}\"", k.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT count(*) FROM (SELECT 1 FROM read_parquet('{parquet}') \
             GROUP BY {cols} HAVING count(*) > 1)"
        );
        let dup: i64 = conn.query_row(&sql, [], |r| r.get(0)).map_err(|e| {
            format!("duckdb_write mode '{mode}': cannot check primary key uniqueness: {e}")
        })?;
        if dup == 0 {
            return Ok(());
        }
        Err(format!(
            "duckdb_write mode '{mode}': this batch contains {dup} duplicate primary key \
             combination(s) over key(s) [{}]. Each key may appear at most once per batch — \
             dedupe upstream (a batch with repeated keys has no order-independent result).",
            keys.join(", ")
        ))
    }

    /// Core write routine: record batches -> temp parquet file -> DuckDB
    /// table via `read_parquet(...)` (the same temp-parquet bridge
    /// `DuckDBScanExec` uses for reads; `read_arrow` is an extension function
    /// and not guaranteed present). Exposed `pub` so unit tests verify the
    /// durable write without a full execution plan.
    ///
    /// Concurrency: DuckDB connections opened within one process to the same
    /// file share one database instance, and concurrent `CREATE TABLE`
    /// transactions hit catalog write-write conflicts — observed as silently
    /// lost rows (8 writers, 1 row durable). A per-db-path process-local
    /// mutex serializes writers; a second process is kept out by DuckDB's own
    /// file lock, retried for a few seconds before failing.
    pub fn write_batches(
        db_path: &str,
        table: &str,
        batches: &[RecordBatch],
        write_mode: &WriteMode,
    ) -> Result<usize> {
        use parquet::arrow::ArrowWriter;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex, OnceLock, Weak};

        if batches.is_empty() {
            return Ok(0);
        }
        let schema = batches[0].schema();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // 1. Spill batches to a temp parquet file (independent `parquet`
        //    crate, same as the materialization cache path). Done BEFORE
        //    taking the per-db mutex — pure CPU/IO with no shared state,
        //    serializing it needlessly blocked other writers to the same
        //    db (codex-review r3 P2).
        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join(format!(
            "swanflow_duckdb_write_{}.parquet",
            uuid::Uuid::new_v4()
        ));
        {
            let file = std::fs::File::create(&tmp_file)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            for b in batches {
                writer
                    .write(b)
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            }
            writer
                .close()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        }
        let tmp_path = tmp_file.to_str().unwrap_or("").to_string();

        // 1b. Normalize the lock key (codex-review r3 P1): `/tmp/x.db`,
        //     `/tmp/./x.db`, relative spellings and symlinks can all name
        //     the SAME DuckDB file while hashing to different mutexes —
        //     re-opening the catalog-conflict / silent-row-loss race this
        //     lock exists to close. Canonicalize the file when present;
        //     otherwise canonicalize the parent and append the file name
        //     (path may not exist yet on first write).
        let lock_key = {
            let p = std::path::Path::new(db_path);
            if let Ok(canon) = p.canonicalize() {
                canon.to_string_lossy().into_owned()
            } else {
                let parent = p.parent().unwrap_or(std::path::Path::new("."));
                let file_name = p
                    .file_name()
                    .map(|f| f.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let canon_parent = parent
                    .canonicalize()
                    .unwrap_or_else(|_| parent.to_path_buf());
                canon_parent.join(file_name).to_string_lossy().into_owned()
            }
        };

        // Weak registry (codex-review r3 P2): a long-running daemon writing
        // templated/temporary db paths would grow a strong-ref entry per
        // path forever. Weak lets unreferenced entries be replaced, and a
        // dead entry is upgraded-or-recreated on lookup.
        static SINK_LOCKS: OnceLock<Mutex<HashMap<String, Weak<Mutex<()>>>>> = OnceLock::new();
        let locks = SINK_LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
        let per_db = {
            let mut map = locks.lock().unwrap_or_else(|p| p.into_inner());
            // Upgrade-or-recreate: a live entry is shared with any
            // concurrent writer to the same file; a dead one (all writers
            // done) is replaced fresh.
            if let Some(arc) = map.get(&lock_key).and_then(Weak::upgrade) {
                arc
            } else {
                let arc = Arc::new(Mutex::new(()));
                map.insert(lock_key.clone(), Arc::downgrade(&arc));
                arc
            }
        };
        // Take the writer mutex only around the DuckDB interaction.
        let _serialize = per_db.lock().unwrap_or_else(|p| p.into_inner());

        // 2. Durable write via DuckDB. `CREATE OR REPLACE` makes re-runs
        //    idempotent; a subsequent run replaces the previous table.
        //    Append mode INSERTs; with `partition_by`, rows sharing the
        //    incoming partition values are deleted first (idempotent
        //    per-interval backfills).
        //
        // Cross-process: DuckDB holds an exclusive file lock on the db
        // while another process has it open. A brief conflicting-lock
        // window is normal under concurrent runs — retry for a few
        // seconds before surfacing the error to the node.
        let mut conn = None;
        for attempt in 0..40 {
            match duckdb::Connection::open_with_flags(db_path, duckdb::Config::default()) {
                Ok(c) => {
                    conn = Some(c);
                    break;
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("Conflicting lock") && attempt < 39 {
                        std::thread::sleep(std::time::Duration::from_millis(250));
                        continue;
                    }
                    return Err(datafusion::error::DataFusionError::External(Box::new(e)));
                }
            }
        }
        let conn = match conn {
            Some(c) => c,
            // Unreachable: the loop either breaks with Some(c) or returns
            // Err. Keep a defensive arm mapping the last retry error.
            None => {
                return Err(datafusion::error::DataFusionError::External(format!(
                    "duckdb open retry exhausted for {db_path}"
                )
                .into()))
            }
        };

        let safe_table = table.replace('"', "\"\"");
        let tmp = tmp_path.replace('\'', "''");
        match write_mode {
            WriteMode::Replace => {
                let sql = format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_parquet('{}')",
                    safe_table, tmp
                );
                let res = conn
                    .execute(&sql, [])
                    .map_err(|e| {
                        let _ = std::fs::remove_file(&tmp_file);
                        datafusion::error::DataFusionError::External(Box::new(e))
                    })
                    .map(|_| total_rows);
                let _ = std::fs::remove_file(&tmp_file);
                res
            }
            WriteMode::Append { partition_by } => {
                // Partitioned idempotent append: within one transaction,
                // create-if-missing, DELETE exactly the incoming partitions
                // (tuple-exact via EXISTS over the staging rows — handles
                // multi-column keys, NULL keys, and date_trunc'ed keys),
                // then INSERT. Re-running an interval replaces exactly its
                // own rows.
                let has_keys = !partition_by.is_empty();
                // First pass (no keys or keys not yet resolvable): plain
                // create-if-missing + insert.
                let first_pass = if has_keys {
                    // With keys we cannot use the eager INSERT (it would
                    // duplicate rows before the targeted DELETE); staging
                    // table approach below handles it in one pass.
                    conn.execute_batch(&format!(
                        "BEGIN TRANSACTION;\n\
                         CREATE TABLE IF NOT EXISTS \"{}\" AS \
                         SELECT * FROM read_parquet('{}') LIMIT 0;",
                        safe_table, tmp
                    ))
                } else {
                    conn.execute_batch(&format!(
                        "BEGIN TRANSACTION;\n\
                         CREATE TABLE IF NOT EXISTS \"{}\" AS \
                         SELECT * FROM read_parquet('{}') LIMIT 0;\n\
                         INSERT INTO \"{}\" BY NAME SELECT * FROM read_parquet('{}');",
                        safe_table, tmp, safe_table, tmp
                    ))
                };
                first_pass
                    .and_then(|_| {
                        if !has_keys {
                            return conn.execute_batch("COMMIT;");
                        }
                        // DELETE rows whose partition tuple matches any row
                        // in the incoming batch (staging = the temp parquet),
                        // then INSERT. NULL keys: `t.k IS NOT DISTINCT FROM
                        // s.k` treats NULL equal to NULL.
                        let cond = partition_by
                            .iter()
                            .map(|k| {
                                format!(
                                    "{} IS NOT DISTINCT FROM {}",
                                    k.sql_expr("t"),
                                    k.sql_expr("s")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(" AND ");
                        conn.execute_batch(&format!(
                            "DELETE FROM \"{t}\" t WHERE EXISTS (SELECT 1 FROM read_parquet('{p}') s WHERE {cond});\n\
                             INSERT INTO \"{t}\" BY NAME SELECT * FROM read_parquet('{p}');\n\
                             COMMIT;",
                            t = safe_table,
                            p = tmp,
                            cond = cond
                        ))
                    })
                    .map_err(|e| {
                        let _ = std::fs::remove_file(&tmp_file);
                        let _ = conn.execute_batch("ROLLBACK;");
                        datafusion::error::DataFusionError::External(Box::new(e))
                    })
                    .map(|_| {
                        let _ = std::fs::remove_file(&tmp_file);
                        total_rows
                    })
            }
            // ---- #2026111603 spike: Starlake writeStrategy ports ----
            WriteMode::Merge { keys } => {
                // Upsert by primary key in one transaction: staging rows
                // drive a single-pass DELETE-matching-target + INSERT
                // (equivalent to MERGE WHEN MATCHED THEN UPDATE /
                // WHEN NOT MATCHED THEN INSERT for full-row replacement,
                // which DuckDB also spells DELETE+INSERT; DuckDB 1.4+
                // lacks a native MERGE statement on plain tables).
                //
                // `BY NAME` (not positional) so an upstream column reorder
                // cannot silently write values into the wrong columns — the
                // `append` branch already does it this way (review F2).
                if let Err(msg) = Self::reject_duplicate_keys(&conn, &tmp, keys, "merge") {
                    let _ = std::fs::remove_file(&tmp_file);
                    return Err(datafusion::error::DataFusionError::Execution(msg));
                }
                let key_cond = keys
                    .iter()
                    .map(|k| {
                        let k = k.replace('"', "\"\"");
                        format!("t.\"{k}\" IS NOT DISTINCT FROM s.\"{k}\"")
                    })
                    .collect::<Vec<_>>()
                    .join(" AND ");
                let sql = format!(
                    "BEGIN TRANSACTION;\n\
                     CREATE TABLE IF NOT EXISTS \"{t}\" AS SELECT * FROM read_parquet('{p}') LIMIT 0;\n\
                     DELETE FROM \"{t}\" t WHERE EXISTS (SELECT 1 FROM read_parquet('{p}') s WHERE {kc});\n\
                     INSERT INTO \"{t}\" BY NAME SELECT * FROM read_parquet('{p}');\n\
                     COMMIT;",
                    t = safe_table,
                    p = tmp,
                    kc = key_cond
                );
                conn.execute_batch(&sql)
                    .map_err(|e| {
                        let _ = std::fs::remove_file(&tmp_file);
                        let _ = conn.execute_batch("ROLLBACK;");
                        datafusion::error::DataFusionError::External(Box::new(e))
                    })
                    .map(|_| {
                        let _ = std::fs::remove_file(&tmp_file);
                        total_rows
                    })
            }
            WriteMode::Scd2 { keys, tracked, valid_from, valid_to, is_current } => {
                // Type-2 SCD, Starlake-style, in one transaction:
                //  1. create-if-missing with control columns bolted on;
                //  2. CLOSE changed current rows in place (valid_to = this
                //     run's epoch marker, is_current = false) — history is
                //     kept, never deleted;
                //  3. insert the incoming rows as new current versions,
                //     but only keys that have no open current row (changed
                //     or brand-new). Unchanged keys keep their existing
                //     validity window untouched — re-running the same batch
                //     is a no-op.
                if let Err(msg) = Self::reject_duplicate_keys(&conn, &tmp, keys, "scd2") {
                    let _ = std::fs::remove_file(&tmp_file);
                    return Err(datafusion::error::DataFusionError::Execution(msg));
                }
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                let q = |s: &String| s.replace('"', "\"\"");
                let key_cond = keys
                    .iter()
                    .map(|k| format!("t.\"{}\" IS NOT DISTINCT FROM s.\"{}\"", q(k), q(k)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                // `tracked` empty = every non-key column (the documented
                // default, same as Starlake's SCD2). Resolved against the
                // staging schema — NOT "close unconditionally": an empty
                // predicate used to collapse the WHERE to key-only, so every
                // run closed the current row even when nothing had changed
                // (adversarial review F1; reproduced by
                // `write_batches_scd2_default_tracked_means_all_non_key_columns`).
                let key_set: std::collections::HashSet<&str> =
                    keys.iter().map(|k| k.as_str()).collect();
                let control = [valid_from.as_str(), valid_to.as_str(), is_current.as_str()];
                let tracked_effective: Vec<String> = if tracked.is_empty() {
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().to_string())
                        .filter(|n| {
                            !key_set.contains(n.as_str()) && !control.contains(&n.as_str())
                        })
                        .collect()
                } else {
                    tracked.clone()
                };
                // Close only rows whose tracked attributes actually differ. If
                // no trackable column is left (key-only table), nothing can
                // change, so there is nothing to close.
                let close_stmt = if tracked_effective.is_empty() {
                    String::new()
                } else {
                    let change_cond = tracked_effective
                        .iter()
                        .map(|a| {
                            let a = q(a);
                            format!("t.\"{a}\" IS DISTINCT FROM s.\"{a}\"")
                        })
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    format!(
                        "UPDATE \"{t}\" AS t\n\
                           SET \"{vt}\" = make_timestamp({now}), \"{ic}\" = FALSE\n\
                           WHERE t.\"{ic}\" IS NOT DISTINCT FROM TRUE\n\
                             AND EXISTS (SELECT 1 FROM read_parquet('{p}') s\n\
                                          WHERE {kc} AND ({chg}));\n",
                        t = safe_table,
                        vt = q(valid_to),
                        ic = q(is_current),
                        now = now,
                        p = tmp,
                        kc = key_cond,
                        chg = change_cond
                    )
                };
                let vf = q(valid_from);
                let vt = q(valid_to);
                let ic = q(is_current);
                // Diagnose a table that predates scd2 (built by replace/append
                // or by an external tool): `CREATE TABLE IF NOT EXISTS` would
                // silently no-op and the close/insert would then die with a
                // raw binder error (review F9).
                let table_exists: bool = conn
                    .query_row(
                        &format!(
                            "SELECT count(*) FROM duckdb_tables() WHERE table_name = '{}'",
                            table.replace('\'', "''")
                        ),
                        [],
                        |r| r.get::<_, i64>(0),
                    )
                    .map(|n| n > 0)
                    .unwrap_or(false);
                if table_exists
                    && conn
                        .prepare(&format!(
                            "SELECT \"{vf}\", \"{vt}\", \"{ic}\" FROM \"{t}\" LIMIT 0",
                            vf = vf,
                            vt = vt,
                            ic = ic,
                            t = safe_table
                        ))
                        .is_err()
                {
                    let _ = std::fs::remove_file(&tmp_file);
                    return Err(datafusion::error::DataFusionError::Execution(format!(
                        "duckdb_write mode 'scd2': target table \"{}\" already exists but has no \
                         scd2 control columns ({} / {} / {}). Write it with mode: replace or use a \
                         new table name before switching it to scd2.",
                        table, valid_from, valid_to, is_current
                    )));
                }
                let sql = format!(
                    "BEGIN TRANSACTION;\n\
                     CREATE TABLE IF NOT EXISTS \"{t}\" AS\n\
                       SELECT *, CAST(NULL AS TIMESTAMP) AS \"{vf}\", CAST(NULL AS TIMESTAMP) AS \"{vt}\", CAST(NULL AS BOOLEAN) AS \"{ic}\"\n\
                       FROM read_parquet('{p}') LIMIT 0;\n\
                     {close}\
                     INSERT INTO \"{t}\" BY NAME\n\
                       SELECT s.*, make_timestamp({now}) AS \"{vf}\", CAST(NULL AS TIMESTAMP) AS \"{vt}\", TRUE AS \"{ic}\"\n\
                       FROM read_parquet('{p}') s\n\
                       WHERE NOT EXISTS (\n\
                         SELECT 1 FROM \"{t}\" t\n\
                         WHERE {kc} AND t.\"{ic}\" IS NOT DISTINCT FROM TRUE\n\
                       );\n\
                     COMMIT;",
                    t = safe_table,
                    p = tmp,
                    kc = key_cond,
                    close = close_stmt,
                    vf = vf,
                    vt = vt,
                    ic = ic,
                    now = now
                );
                conn.execute_batch(&sql)
                    .map_err(|e| {
                        let _ = std::fs::remove_file(&tmp_file);
                        let _ = conn.execute_batch("ROLLBACK;");
                        datafusion::error::DataFusionError::External(Box::new(e))
                    })
                    .map(|_| {
                        let _ = std::fs::remove_file(&tmp_file);
                        total_rows
                    })
            }
        }
    }
}

impl DisplayAs for DuckDBWriteSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DuckDBWriteSinkExec(db={}, table={})", self.db_path, self.table)
    }
}

impl ExecutionPlan for DuckDBWriteSinkExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "DuckDBWriteSinkExec"
    }
    fn schema(&self) -> SchemaRef {
        self.out_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.db_path.clone(),
            self.table.clone(),
            self.write_mode.clone(),
            children,
            self.out_schema.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut streams = Vec::new();
        for input in &self.inputs {
            streams.push(input.execute(partition, context.clone())?);
        }
        let input_count = streams.len();
        Ok(Box::pin(DuckDBWriteSinkStream {
            db_path: self.db_path.clone(),
            table: self.table.clone(),
            write_mode: self.write_mode.clone(),
            inputs: streams,
            schema: self.out_schema.clone(),
            pending: Vec::new(),
            finished_mask: vec![false; input_count],
            written: false,
        }))
    }
}

struct DuckDBWriteSinkStream {
    db_path: String,
    table: String,
    write_mode: WriteMode,
    inputs: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    pending: Vec<RecordBatch>,
    finished_mask: Vec<bool>,
    written: bool,
}

impl RecordBatchStream for DuckDBWriteSinkStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for DuckDBWriteSinkStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let n = self.inputs.len();
        let mut any_pending = false;

        for i in 0..n {
            if self.finished_mask[i] {
                continue;
            }
            match self.inputs[i].poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.pending.push(batch.clone());
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => self.finished_mask[i] = true,
                Poll::Pending => any_pending = true,
            }
        }

        if any_pending {
            Poll::Pending
        } else if self.finished_mask.iter().all(|&f| f) {
            if !self.written {
                self.written = true;
                // Clone the immutable fields first, then take the mutable
                // buffer — avoids borrowing `self` immutably and mutably in
                // one expression.
                let db_path = self.db_path.clone();
                let table = self.table.clone();
                let write_mode = self.write_mode.clone();
                let batches = std::mem::take(&mut self.pending);
                Self::flush(&db_path, &table, batches, &write_mode)?;
            }
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl DuckDBWriteSinkStream {
    fn flush(
        db_path: &str,
        table: &str,
        batches: Vec<RecordBatch>,
        write_mode: &WriteMode,
    ) -> Result<()> {
        let rows = DuckDBWriteSinkExec::write_batches(db_path, table, &batches, write_mode)?;
        println!(
            "DuckDBWriteSink: persisted {} rows into {}::{}",
            rows, db_path, table
        );
        Ok(())
    }
}

#[cfg(test)]
mod duckdb_write_sink_tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn sample_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int64Array::from(vec![31, 25])),
                Arc::new(Float64Array::from(vec![91.5, 77.0])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["carol"])),
                Arc::new(Int64Array::from(vec![42])),
                Arc::new(Float64Array::from(vec![88.25])),
            ],
        )
        .unwrap();
        vec![b1, b2]
    }

    /// One-row batch with the same schema as `sample_batches`.
    fn sample_batches_named(name: &str, score: f64) -> Vec<RecordBatch> {
        sample_batches_with_age(name, 30, score)
    }

    /// One-row batch with explicit age (for composite-key tests).
    fn sample_batches_with_age(name: &str, age: i64, score: f64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![name.to_string()])),
                Arc::new(Int64Array::from(vec![age])),
                Arc::new(Float64Array::from(vec![score])),
            ],
        )
        .unwrap()]
    }

    fn temp_db_path() -> (String, std::path::PathBuf) {
        let db = std::env::temp_dir()
            .join(format!("swan_sink_union_{}.db", uuid::Uuid::new_v4()));
        let s = db.to_str().unwrap().to_string();
        (s, db)
    }

    #[tokio::test]
    async fn multi_partition_union_input_rows_all_persisted() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::*;
        use std::sync::Arc;

        // Multi-partition input: 3-branch UNION ALL physical plan is a
        // UnionExec with 3 output partitions (Bug-7 family regression).
        let ctx = SessionContext::new();
        let df = ctx
            .sql("SELECT 'a' AS v UNION ALL SELECT 'b' UNION ALL SELECT 'c'")
            .await
            .unwrap();
        let logical = df.into_optimized_plan().unwrap();
        let phys = ctx.state().create_physical_plan(&logical).await.unwrap();
        assert_eq!(
            phys.properties().output_partitioning().partition_count(),
            3,
            "precondition: UNION ALL must be multi-partition here"
        );

        let (db_str, db_path) = temp_db_path();
        let schema = phys.schema();
        let sink = Arc::new(DuckDBWriteSinkExec::new(
            db_str.clone(),
            "union_out".to_string(),
            WriteMode::Replace,
            vec![phys],
            schema,
        ));
        let out = collect(sink, ctx.task_ctx()).await.unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "sink must forward every input row");

        // Durable readback on a fresh connection: all 3 rows in DuckDB.
        let conn = duckdb::Connection::open(&db_path).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM union_out", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3, "DuckDBWriteSinkExec must not drop partitions 1..N");
    }

    #[test]
    fn write_batches_persists_to_duckdb_and_reads_back() {
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let batches = sample_batches();

        let rows = DuckDBWriteSinkExec::write_batches(&db_str, "people", &batches, &WriteMode::Replace).unwrap();
        assert_eq!(rows, 3);

        // Durable: a FRESH connection must see the data.
        let conn = duckdb::Connection::open(&db).unwrap();
        let (n, total_age, min_name): (i64, i64, String) = conn
            .query_row(
                "SELECT count(*), sum(age), min(name) FROM people",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(total_age, 98); // 31+25+42
        assert_eq!(min_name, "alice");

        // Floats survived the round-trip.
        let score: f64 = conn
            .query_row("SELECT score FROM people WHERE name='carol'", [], |r| r.get(0))
            .unwrap();
        assert!((score - 88.25).abs() < 1e-9);
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn concurrent_writers_to_same_table_all_land() {
        // Test gap flagged in codex-review r1: multiple nodes (or retries)
        // can target the SAME db+table concurrently. DuckDB takes a file
        // lock on open, so naive concurrent write_batches must either
        // serialize or retry — never silently lose a writer. 8 writers,
        // one distinct name each; all 8 rows must be durable at the end.
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Append { partition_by: vec![] };
        let names: Vec<String> = (0..8).map(|i| format!("p{i}")).collect();

        let (tx, rx) = std::sync::mpsc::channel::<String>();
        std::thread::scope(|s| {
            for name in &names {
                let db_str = db_str.clone();
                let mode = mode.clone();
                let tx = tx.clone();
                s.spawn(move || {
                    let batches = sample_batches_named(name, 1.0);
                    if let Err(e) =
                        DuckDBWriteSinkExec::write_batches(&db_str, "people", &batches, &mode)
                    {
                        let _ = tx.send(format!("{name}: {e}"));
                    }
                });
            }
        });
        drop(tx);
        let mut errs: Vec<String> = rx.iter().collect();
        errs.sort();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap_or(-1);
        println!("concurrent writers: rows={n} errs={errs:?}");
        assert!(errs.is_empty(), "writers failed: {errs:?}");
        assert_eq!(n, 8, "all 8 writers must be durable");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn concurrent_writers_alias_paths_share_one_lock() {
        // codex-review r3 P1: the per-db mutex is keyed by a NORMALIZED
        // path. Raw-string keys let `/tmp/x.db`, `/tmp/./x.db`, a
        // relative spelling, or a symlink alias of the same file get
        // DIFFERENT mutexes — reopening the silent-row-loss race. Half
        // the writers here address the file through aliases; all 8 rows
        // must still be durable.
        let tmp = std::env::temp_dir();
        let db = tmp.join(format!("swan_duckdb_alias_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        // Pre-create so canonicalize() and the symlink resolve to it.
        duckdb::Connection::open(&db).unwrap();
        let dotted = db
            .parent()
            .unwrap()
            .join(format!("./{}", db.file_name().unwrap().to_str().unwrap()));
        let link = tmp.join(format!("swan_duckdb_alias_{}.lnk", uuid::Uuid::new_v4()));
        std::os::unix::fs::symlink(&db, &link).expect("symlink");
        let mode = WriteMode::Append { partition_by: vec![] };

        let (tx, rx) = std::sync::mpsc::channel::<String>();
        std::thread::scope(|s| {
            for i in 0..8 {
                let which = match i % 4 {
                    0 => db_str.clone(),
                    1 => dotted.to_str().unwrap().to_string(),
                    2 => link.to_str().unwrap().to_string(),
                    _ => db_str.clone(),
                };
                let mode = mode.clone();
                let tx = tx.clone();
                let name = format!("a{i}");
                s.spawn(move || {
                    let batches = sample_batches_named(&name, 1.0);
                    if let Err(e) =
                        DuckDBWriteSinkExec::write_batches(&which, "people", &batches, &mode)
                    {
                        let _ = tx.send(format!("{name}: {e}"));
                    }
                });
            }
        });
        drop(tx);
        let mut errs: Vec<String> = rx.iter().collect();
        errs.sort();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap_or(-1);
        println!("alias writers: rows={n} errs={errs:?}");
        assert!(errs.is_empty(), "writers failed: {errs:?}");
        assert_eq!(n, 8, "all 8 alias-path writers must be durable");
        std::fs::remove_file(&db).ok();
        std::fs::remove_file(&link).ok();
    }

    #[test]
    fn write_batches_is_idempotent_replace() {
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let batches = sample_batches();

        DuckDBWriteSinkExec::write_batches(&db_str, "people", &batches, &WriteMode::Replace).unwrap();
        // Second run with fewer rows must REPLACE, not append.
        let one = vec![batches[0].slice(0, 1)];
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &one, &WriteMode::Replace).unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_append_accumulates() {
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let batches = sample_batches();

        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &batches,
            &WriteMode::Append { partition_by: vec![] },
        )
        .unwrap();
        // Second run with a different single row must APPEND.
        let other = sample_batches_named("dave", 77.5);
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &other,
            &WriteMode::Append { partition_by: vec![] },
        )
        .unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 4);
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_append_partition_is_idempotent() {
        // The core backfill promise: re-writing the same partition replaces
        // exactly its own rows, never duplicates, never touches siblings.
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Append { partition_by: vec![PartitionCol { column: "name".into(), trunc: None }] };

        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches(),
            &mode,
        )
        .unwrap();
        // Re-write alice's partition with a different score.
        let alice_v2 = sample_batches_named("alice", 99.0);
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &alice_v2, &mode).unwrap();
        // Re-write alice AGAIN — still exactly one alice row (double-run safe).
        let alice_v3 = sample_batches_named("alice", 100.0);
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &alice_v3, &mode).unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3, "3 original people, alice replaced in place");
        let alice_score: f64 = conn
            .query_row("SELECT score FROM people WHERE name='alice'", [], |r| r.get(0))
            .unwrap();
        assert!((alice_score - 100.0).abs() < 1e-9, "alice is the latest write");
        let bob_score: f64 = conn
            .query_row("SELECT score FROM people WHERE name='bob'", [], |r| r.get(0))
            .unwrap();
        assert!((bob_score - 77.0).abs() < 1e-9, "sibling partition untouched");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_append_multi_column_key() {
        // Composite partition key: (name, age). Re-writing alice@31 must
        // replace exactly that row even if alice@32 also exists.
        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Append {
            partition_by: vec![
                PartitionCol { column: "name".into(), trunc: None },
                PartitionCol { column: "age".into(), trunc: None },
            ],
        };
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &sample_batches(), &mode).unwrap();
        // alice aged 32 arrives — a DIFFERENT composite partition.
        let alice32 = sample_batches_with_age("alice", 32, 55.0);
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &alice32, &mode).unwrap();
        // Re-write alice@31 with a new score — replaces in place.
        let alice31v2 = sample_batches_with_age("alice", 31, 99.0);
        DuckDBWriteSinkExec::write_batches(&db_str, "people", &alice31v2, &mode).unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 4, "alice has two age-partitions, each exactly once");
        let s31: f64 = conn
            .query_row("SELECT score FROM people WHERE name='alice' AND age=31", [], |r| r.get(0))
            .unwrap();
        assert!((s31 - 99.0).abs() < 1e-9, "alice@31 replaced with latest");
        let s32: f64 = conn
            .query_row("SELECT score FROM people WHERE name='alice' AND age=32", [], |r| r.get(0))
            .unwrap();
        assert!((s32 - 55.0).abs() < 1e-9, "sibling composite partition untouched");
        std::fs::remove_file(&db).ok();
    }

    // ---- #2026111603 spike tests: Merge / Scd2 write modes ----

    #[test]
    fn write_batches_merge_upserts_by_key() {
        let db = std::env::temp_dir().join(format!("swan_duckdb_merge_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        // Seed: alice(30) + bob via one batch each, keyed by name.
        let mode = WriteMode::Merge { keys: vec!["name".to_string()] };
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("alice", 30, 1.0), &mode,
        )
        .unwrap();
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("bob", 40, 2.0), &mode,
        )
        .unwrap();
        // Upsert: same key (alice) new attributes + a fresh key (carol).
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("alice", 31, 9.9), &mode,
        )
        .unwrap();
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("carol", 25, 3.0), &mode,
        )
        .unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 3, "merge must not duplicate keyed rows");
        let (age, score): (i64, f64) = conn
            .query_row(
                "SELECT age, score FROM people WHERE name = 'alice'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(age, 31, "merge must update attributes of existing key");
        assert!((score - 9.9).abs() < 1e-9);
        // Idempotency: re-running the same batch changes nothing.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("alice", 31, 9.9), &mode,
        )
        .unwrap();
        let n2: i64 = conn
            .query_row("SELECT count(*) FROM people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n2, 3, "merge re-run must be idempotent");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_scd2_tracks_changes() {
        let db = std::env::temp_dir().join(format!("swan_duckdb_scd2_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Scd2 {
            keys: vec!["name".to_string()],
            tracked: vec!["age".to_string()],
            valid_from: "_valid_from".to_string(),
            valid_to: "_valid_to".to_string(),
            is_current: "_is_current".to_string(),
        };
        // Run 1: alice age 30 — becomes the initial current version.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_people", &sample_batches_with_age("alice", 30, 1.0), &mode,
        )
        .unwrap();
        // Run 2: alice age 31 — tracked attr changed -> new current
        // version, old version closed.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_people", &sample_batches_with_age("alice", 31, 1.0), &mode,
        )
        .unwrap();
        // Run 3: alice age 31 again — no tracked change -> still 2 rows.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_people", &sample_batches_with_age("alice", 31, 1.5), &mode,
        )
        .unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM dim_people", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "scd2: one closed version + one current version");
        let cur_age: i64 = conn
            .query_row(
                "SELECT age FROM dim_people WHERE _is_current",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cur_age, 31, "scd2 current version must carry new attribute");
        let closed: i64 = conn
            .query_row(
                "SELECT count(*) FROM dim_people WHERE NOT _is_current AND _valid_to IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(closed, 1, "scd2 old version must be closed with valid_to");
        std::fs::remove_file(&db).ok();
    }

    // ---- review round (sigil@Mac 12345) regression tests ----
    // Each of these was written against the FIRST implementation of the
    // merge/scd2 spike and was RED before the follow-up fixes. They cover the
    // gaps the original two tests left open (adversarial review findings
    // F1/F2/F3/F4/F5/F12 from `.campaign/out/sigil-review-writemodes.md`).

    /// Two rows for the SAME key in one batch, distinct (age, score).
    fn sample_batches_dup_key(
        name: &str,
        ages: [i64; 2],
        scores: [f64; 2],
    ) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![name.to_string(), name.to_string()])),
                Arc::new(Int64Array::from(ages.to_vec())),
                Arc::new(Float64Array::from(scores.to_vec())),
            ],
        )
        .unwrap()]
    }

    /// Same data as `sample_batches_with_age` but with columns REORDERED
    /// (age, score, name) — a batch whose physical column order differs from
    /// the target table's. `INSERT ... SELECT *` silently mis-maps this.
    fn sample_batches_reordered(name: &str, age: i64, score: f64) -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![age])),
                Arc::new(Float64Array::from(vec![score])),
                Arc::new(StringArray::from(vec![name.to_string()])),
            ],
        )
        .unwrap()]
    }

    #[test]
    fn write_batches_scd2_default_tracked_means_all_non_key_columns() {
        // Documented default (config_schema `tracked_columns` + the WriteMode
        // doc): "Empty = all non-key columns". So with `tracked: []`, a change
        // in ANY non-key column must open a new version, and an UNCHANGED row
        // must not. Pre-fix, an empty `tracked` produced an empty change
        // predicate → every run closed the current row unconditionally:
        // re-running the identical batch appended a bogus history version.
        let db = std::env::temp_dir().join(format!("swan_duckdb_scd2def_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Scd2 {
            keys: vec!["name".to_string()],
            tracked: vec![],
            valid_from: "_valid_from".to_string(),
            valid_to: "_valid_to".to_string(),
            is_current: "_is_current".to_string(),
        };
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_default", &sample_batches_with_age("alice", 30, 1.0), &mode,
        )
        .unwrap();
        // Identical batch re-run: default tracked means "compare every non-key
        // column" — nothing changed, so nothing may be closed or inserted.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_default", &sample_batches_with_age("alice", 30, 1.0), &mode,
        )
        .unwrap();
        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM dim_default", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "identical re-run must be a no-op (idempotent), got {n} versions");

        // A non-key column that is NOT `age` changed → new version.
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_default", &sample_batches_with_age("alice", 30, 2.0), &mode,
        )
        .unwrap();
        // Fresh connection: a connection opened before the write keeps its own
        // view of the table (DuckDB pins the catalog snapshot per connection),
        // so re-reading on the old handle would report stale counts.
        drop(conn);
        let conn = duckdb::Connection::open(&db).unwrap();
        let n2: i64 = conn
            .query_row("SELECT count(*) FROM dim_default", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n2, 2, "score is a non-key column: its change must open a version");
        let cur: (i64, f64) = conn
            .query_row(
                "SELECT age, score FROM dim_default WHERE _is_current",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(cur.0, 30);
        assert!((cur.1 - 2.0).abs() < 1e-9, "current version carries the new score");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_merge_rejects_duplicate_keys_within_one_batch() {
        // A MERGE whose source carries two rows under the same key has an
        // order-dependent result (last-wins, first-wins, or duplicated rows).
        // Silent either-way is worse than a loud failure: the target state
        // would depend on batch/file layout. Must be rejected with a
        // diagnostic that names the mode and the key count.
        let db = std::env::temp_dir().join(format!("swan_duckdb_mergedup_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Merge { keys: vec!["name".to_string()] };
        let err = DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_dup_key("alice", [30, 31], [1.0, 2.0]), &mode,
        )
        .expect_err("duplicate keys in one merge batch must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("duplicate") && msg.contains("name"),
            "error must name the duplicate-key condition and the key columns: {msg}"
        );
        // The table must not have been left behind in a half-written state.
        let conn = duckdb::Connection::open(&db).unwrap();
        let exists: i64 = conn
            .query_row(
                "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'people'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exists, 0, "rejected batch must not create the target table");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_merge_is_column_order_independent() {
        // C7: `INSERT ... SELECT *` assumes the staging parquet column order
        // matches the target table. Any upstream that reorders columns (very
        // common: a SELECT list reordered, a union of differently-ordered
        // batches, a parquet read from another writer) silently writes values
        // into the WRONG columns. Merge must map BY NAME, like Append does.
        let db = std::env::temp_dir().join(format!("swan_duckdb_mergeorder_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        // Target table established with (name, age, score).
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_with_age("alice", 30, 1.0), &WriteMode::Replace,
        )
        .unwrap();
        // Merge a batch whose physical order is (age, score, name).
        let mode = WriteMode::Merge { keys: vec!["name".to_string()] };
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &sample_batches_reordered("alice", 31, 9.5), &mode,
        )
        .unwrap();
        let conn = duckdb::Connection::open(&db).unwrap();
        let row: (String, i64, f64) = conn
            .query_row("SELECT name, age, score FROM people", [], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?))
            })
            .unwrap();
        assert_eq!(row.0, "alice", "name column must not receive a numeric value");
        assert_eq!(row.1, 31, "age must land in age");
        assert!((row.2 - 9.5).abs() < 1e-9, "score must land in score");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_scd2_closed_version_carries_a_real_timestamp() {
        // `_valid_to` must be a wall-clock timestamp at (or just before) the
        // closing run — not a mangled epoch (the spike divided microseconds by
        // 1000 and fed the result to a millisecond formatter, so a unit slip
        // would land the column decades off without failing any assertion).
        let db = std::env::temp_dir().join(format!("swan_duckdb_scd2ts_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Scd2 {
            keys: vec!["name".to_string()],
            tracked: vec!["age".to_string()],
            valid_from: "_valid_from".to_string(),
            valid_to: "_valid_to".to_string(),
            is_current: "_is_current".to_string(),
        };
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_ts", &sample_batches_with_age("alice", 30, 1.0), &mode,
        )
        .unwrap();
        DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_ts", &sample_batches_with_age("alice", 31, 1.0), &mode,
        )
        .unwrap();
        let conn = duckdb::Connection::open(&db).unwrap();
        // Both control timestamps must be within an hour of now(). The control
        // columns are written as `make_timestamp(micros)` — a UTC-based
        // TIMESTAMP — so compare against now() AT TIME ZONE 'UTC' rather than
        // the session's local-time TIMESTAMPTZ. `_valid_to` is NULL on the
        // open (current) version by definition, so only closed rows are
        // checked for it.
        let vf_ok: bool = conn
            .query_row(
                "SELECT count(*) FILTER (
                          WHERE abs(date_diff('second', _valid_from, (now() AT TIME ZONE 'UTC'))) < 3600
                        ) = count(*)
                 FROM dim_ts",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(vf_ok, "_valid_from must be a current wall-clock timestamp");

        let (closed_n, closed_ok): (i64, bool) = conn
            .query_row(
                "SELECT count(*),
                        count(*) FILTER (
                          WHERE abs(date_diff('second', _valid_to, (now() AT TIME ZONE 'UTC'))) < 3600
                        ) = count(*)
                 FROM dim_ts WHERE NOT _is_current",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(closed_n > 0, "expected one closed version");
        assert!(closed_ok, "_valid_to must be a current wall-clock timestamp");
        // And the window must be coherent: closed version's valid_to equals the
        // current version's valid_from (no gap, no overlap).
        let coherent: i64 = conn
            .query_row(
                "SELECT count(*) FROM dim_ts c
                   JOIN dim_ts o ON o.name = c.name AND NOT o._is_current
                  WHERE c._is_current AND c._valid_from = o._valid_to",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(coherent, 1, "closed.valid_to must equal current.valid_from");
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_scd2_rejects_duplicate_keys_within_one_batch() {
        // Same ambiguity as merge: one key with two versions in one batch has
        // no defensible ordering. Reject loudly.
        let db = std::env::temp_dir().join(format!("swan_duckdb_scd2dup_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Scd2 {
            keys: vec!["name".to_string()],
            tracked: vec!["age".to_string()],
            valid_from: "_valid_from".to_string(),
            valid_to: "_valid_to".to_string(),
            is_current: "_is_current".to_string(),
        };
        let err = DuckDBWriteSinkExec::write_batches(
            &db_str, "dim_dup", &sample_batches_dup_key("alice", [30, 31], [1.0, 2.0]), &mode,
        )
        .expect_err("duplicate keys in one scd2 batch must be rejected");
        assert!(
            format!("{err}").contains("duplicate"),
            "error must name the duplicate-key condition: {err}"
        );
        std::fs::remove_file(&db).ok();
    }

    #[test]
    fn write_batches_append_hour_truncated_timestamp_key() {
        // 'ts:hour' — backfilling one hour replaces exactly that hour's
        // rows even when the timestamp seconds differ (the pre-trunc value
        // would never match under plain equality).
        use datafusion::arrow::array::TimestampMicrosecondArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None), true),
            Field::new("v", DataType::Int64, true),
        ]));
        // 2026-10-06 10:15:00 and 10:45:00 UTC — same HOUR partition.
        let mk = |usec: i64, v: i64| {
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(TimestampMicrosecondArray::from(vec![usec])),
                    Arc::new(Int64Array::from(vec![v])),
                ],
            )
            .unwrap()]
        };
        let hour = 3600i64 * 1_000_000;
        let t1015 = 1_799_284_100i64 * 1_000_000; // arbitrary epoch base
        let t1045 = t1015 + 30 * 60 * 1_000_000;
        let t1215 = t1015 + 2 * hour; // different hour

        let db = std::env::temp_dir().join(format!("swan_duckdb_write_{}.db", uuid::Uuid::new_v4()));
        let db_str = db.to_str().unwrap().to_string();
        let mode = WriteMode::Append {
            partition_by: vec![PartitionCol { column: "ts".into(), trunc: Some("hour".into()) }],
        };
        DuckDBWriteSinkExec::write_batches(&db_str, "ticks", &mk(t1015, 1), &mode).unwrap();
        DuckDBWriteSinkExec::write_batches(&db_str, "ticks", &mk(t1215, 7), &mode).unwrap();
        // Backfill the 10:00 hour with the 10:45 value — must REPLACE the
        // 10:15 row (same truncated hour), keep the 12:00 row untouched.
        DuckDBWriteSinkExec::write_batches(&db_str, "ticks", &mk(t1045, 2), &mode).unwrap();

        let conn = duckdb::Connection::open(&db).unwrap();
        let n: i64 = conn
            .query_row("SELECT count(*) FROM ticks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 2, "hour-partition replaced in place; other hour kept");
        // The surviving 10:00-hour row carries ts = t1045, v = 2; the 12:00
        // row carries ts = t1215. Split at t1015 + 1h (covers the 10-hour).
        let cutoff = t1015 + hour;
        let v10: i64 = conn
            .query_row(
                "SELECT v FROM ticks WHERE ts < make_timestamp(?)",
                [cutoff],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v10, 2, "10:00-hour row is the backfilled value");
        std::fs::remove_file(&db).ok();
    }

    /// httpfs is a STATIC core extension in bundled DuckDB — no INSTALL/LOAD.
    /// Diagnostic: probe the real bundled version against the live JSON API
    /// (jsonplaceholder's Content-Length header is wrong, which trips
    /// httpfs's length check), with and without force_download.
    #[test]
    fn httpfs_static_fetch_jsonplaceholder() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let ver: String = conn.query_row("SELECT version()", [], |r| r.get(0)).unwrap();
        eprintln!("[diag] bundled duckdb = {ver}");

        // Baseline: no force_download.
        let base = conn.query_row(
            "SELECT count(*) FROM read_json_auto('https://jsonplaceholder.typicode.com/users')",
            [],
            |r| r.get::<_, i64>(0),
        );
        eprintln!("[diag] baseline read_json_auto: {base:?}");

        // With force_download=true.
        let c2 = duckdb::Connection::open_in_memory().unwrap();
        c2.execute_batch("SET force_download=true;").expect("force_download is a valid GUC in this build");
        let forced = c2.query_row(
            "SELECT count(*) FROM read_json_auto('https://jsonplaceholder.typicode.com/users')",
            [],
            |r| r.get::<_, i64>(0),
        );
        eprintln!("[diag] force_download read_json_auto: {forced:?}");

        // The decisive assertion: one of the two must yield 10 users.
        let base_n = base.ok();
        let forced_n = forced.ok();
        assert!(
            base_n == Some(10) || forced_n == Some(10),
            "httpfs could not fetch jsonplaceholder users (baseline={base_n:?}, forced={forced_n:?})"
        );
    }
}
