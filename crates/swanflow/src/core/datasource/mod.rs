use datafusion::prelude::*;
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringArray, StringBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use rust_xlsxwriter::{Chart, ChartType, Color, Format, Workbook};
use serde_json::Value as JsonValue;
use std::collections::HashMap;

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
                use futures::TryStreamExt;
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

use crate::core::error::{CoreResult, SwanFlowError};

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
    /// Append rows; when `partition_by` is set, rows of the same partition
    /// value are deleted first, making per-interval backfills idempotent.
    Append { partition_by: Option<String> },
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

    /// Core write routine: record batches -> temp parquet file -> DuckDB
    /// table via `read_parquet(...)`. Mirrors `DuckDBScanExec`'s read
    /// direction (also a temp-parquet bridge), which is proven to work with
    /// the bundled libduckdb; `read_arrow` is an extension function and not
    /// guaranteed present. Exposed `pub` so unit tests verify the durable
    /// write without a full execution plan.
    pub fn write_batches(
        db_path: &str,
        table: &str,
        batches: &[RecordBatch],
        write_mode: &WriteMode,
    ) -> Result<usize> {
        use parquet::arrow::ArrowWriter;

        if batches.is_empty() {
            return Ok(0);
        }
        let schema = batches[0].schema();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // 1. Spill batches to a temp parquet file (independent `parquet`
        //    crate, same as the materialization cache path).
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

        // 2. Durable write via DuckDB. `CREATE OR REPLACE` makes re-runs
        //    idempotent; a subsequent run replaces the previous table.
        //    Append mode INSERTs; with `partition_by`, rows sharing the
        //    incoming partition values are deleted first (idempotent
        //    per-interval backfills).
        let conn = duckdb::Connection::open_with_flags(db_path, duckdb::Config::default())
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

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
                // Partition values present in this batch (NULLs excluded —
                // SQL `IN` cannot express them; such rows always append).
                let part_vals: Option<Vec<String>> = partition_by.as_ref().map(|col| {
                    let col = col.replace('"', "\"\"");
                    batches
                        .iter()
                        .flat_map(|b| {
                            b.schema()
                                .index_of(col.as_str())
                                .ok()
                                .and_then(|i| b.column(i).as_any().downcast_ref::<datafusion::arrow::array::StringArray>())
                                .map(|a| (0..a.len()).filter(move |&i| !a.is_null(i)).map(move |i| a.value(i).replace('\'', "''")).collect::<Vec<_>>())
                                .unwrap_or_default()
                        })
                        .collect::<std::collections::BTreeSet<_>>()
                        .into_iter()
                        .collect()
                });
                conn.execute_batch(&format!(
                    "BEGIN TRANSACTION;\n\
                     CREATE TABLE IF NOT EXISTS \"{}\" AS \
                     SELECT * FROM read_parquet('{}') LIMIT 0;\n\
                     INSERT INTO \"{}\" SELECT * FROM read_parquet('{}');",
                    safe_table, tmp, safe_table, tmp
                ))
                .and_then(|_| {
                    // Idempotency for partitioned backfills: within the same
                    // transaction, delete-then-insert for the partitions we
                    // are about to write. The CREATE..AS..LIMIT 0 above only
                    // fires on first creation; existing partitions are
                    // pruned here so a re-run of the same interval replaces
                    // exactly its own rows.
                    if let (Some(col), Some(vals)) = (partition_by, part_vals) {
                        if !vals.is_empty() {
                            let col = col.replace('"', "\"\"");
                            let list = vals
                                .iter()
                                .map(|v| format!("'{}'", v))
                                .collect::<Vec<_>>()
                                .join(", ");
                            // Re-order: DELETE must come BEFORE INSERT, so
                            // undo the eager INSERT above and redo both in
                            // the right order.
                            return conn.execute_batch(&format!(
                                "ROLLBACK;\n\
                                 BEGIN TRANSACTION;\n\
                                 CREATE TABLE IF NOT EXISTS \"{}\" AS \
                                 SELECT * FROM read_parquet('{}') LIMIT 0;\n\
                                 DELETE FROM \"{}\" WHERE \"{}\" IN ({});\n\
                                 INSERT INTO \"{}\" SELECT * FROM read_parquet('{}');\n\
                                 COMMIT;",
                                safe_table, tmp, safe_table, col, list, safe_table, tmp
                            ));
                        }
                    }
                    conn.execute_batch("COMMIT;")
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        vec![RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![name.to_string()])),
                Arc::new(Int64Array::from(vec![30])),
                Arc::new(Float64Array::from(vec![score])),
            ],
        )
        .unwrap()]
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
            &WriteMode::Append { partition_by: None },
        )
        .unwrap();
        // Second run with a different single row must APPEND.
        let other = sample_batches_named("dave", 77.5);
        DuckDBWriteSinkExec::write_batches(
            &db_str, "people", &other,
            &WriteMode::Append { partition_by: None },
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
        let mode = WriteMode::Append { partition_by: Some("name".into()) };

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
