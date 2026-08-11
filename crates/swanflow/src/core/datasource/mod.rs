use datafusion::prelude::*;
use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;

use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanBuilder, Float64Array, Float64Builder, Int64Array, Int64Builder,
    StringArray, StringBuilder, TimestampNanosecondBuilder,
};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchemaRef, ScalarValue};
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
use sqlx::any::AnyTypeInfo;
use std::collections::HashMap;

pub fn sqlx_type_to_arrow(ti: &AnyTypeInfo) -> DataType {
    use sqlx::TypeInfo;
    let name = ti.name().to_uppercase();
    match name.as_str() {
        "BOOLEAN" | "BOOL" | "TINYINT(1)" => DataType::Boolean,
        "SMALLINT" | "INT2" | "INT" | "INTEGER" | "INT4" | "BIGINT" | "INT8" | "INT64" => {
            DataType::Int64
        }
        "FLOAT" | "REAL" | "FLOAT4" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" | "NUMERIC"
        | "DECIMAL" => DataType::Float64,
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "DATE" => DataType::Date32,
        _ => DataType::Utf8,
    }
}

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

    pub fn append_from_row(&mut self, row: &sqlx::any::AnyRow, index: usize) {
        use sqlx::Row;
        match self {
            Self::Int64(b) => {
                let val: Option<i64> = row.try_get(index).ok();
                b.append_option(val);
            }
            Self::Float64(b) => {
                let val: Option<f64> = row.try_get(index).ok();
                b.append_option(val);
            }
            Self::Boolean(b) => {
                let val: Option<bool> = row.try_get(index).ok();
                b.append_option(val);
            }
            Self::Timestamp(b) => {
                if let Ok(s) = row.try_get::<String, _>(index) {
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
                        b.append_value(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
                    } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
                        b.append_value(dt.timestamp_nanos_opt().unwrap_or(0));
                    } else {
                        b.append_null();
                    }
                } else {
                    b.append_null();
                }
            }
            Self::Utf8(b) => {
                let val: Option<String> = row.try_get(index).ok();
                b.append_option(val);
            }
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

/// Logical node for SQLx-based generic database scanning.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SqlxScanLogical {
    pub url: String,
    pub query: String,
    pub schema: DFSchemaRef,
    pub params: Vec<ScalarValue>,
}

impl PartialOrd for SqlxScanLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for SqlxScanLogical {
    fn name(&self) -> &str {
        "SqlxScan"
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
        write!(f, "SqlxScan(query={})", self.query)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

/// Physical execution plan for SQLx generic database scanning.
#[derive(Debug)]
pub struct SqlxScanExec {
    pub url: String,
    pub query: String,
    pub project_schema: SchemaRef,
    pub params: Vec<ScalarValue>,
    cache: PlanProperties,
}

impl SqlxScanExec {
    pub fn new(
        url: String,
        query: String,
        project_schema: SchemaRef,
        params: Vec<ScalarValue>,
    ) -> Self {
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
            params,
            cache,
        }
    }
}

impl DisplayAs for SqlxScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SqlxScanExec(query={})", self.query)
    }
}

impl ExecutionPlan for SqlxScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "SqlxScanExec"
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
            self.params.clone(),
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
        let params = self.params.clone();

        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
            sqlx::any::install_default_drivers();
            let pool = sqlx::AnyPool::connect(&url)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut query_obj = sqlx::query(&query);
            for param in &params {
                match param {
                    ScalarValue::Int64(Some(i)) => query_obj = query_obj.bind(*i),
                    ScalarValue::Float64(Some(f)) => query_obj = query_obj.bind(*f),
                    ScalarValue::Boolean(Some(b)) => query_obj = query_obj.bind(*b),
                    ScalarValue::Utf8(Some(s)) => query_obj = query_obj.bind(s.clone()),
                    _ => {
                        let s = format!("{}", param);
                        query_obj = query_obj.bind(s);
                    }
                }
            }
            let mut rows = query_obj.fetch(&pool);

            let mut builders: Vec<DynamicArrayBuilder> = schema
                .fields()
                .iter()
                .map(|f| DynamicArrayBuilder::new(f.data_type(), 1000))
                .collect();

            let mut count = 0;
            while let Some(row_res) = rows.next().await {
                let row = row_res
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                for (i, builder) in builders.iter_mut().enumerate() {
                    builder.append_from_row(&row, i);
                }
                count += 1;
                if count >= 1000 {
                    let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
                    let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                    })?;
                    let _ = tx.send(Ok(batch)).await;
                    builders = schema
                        .fields()
                        .iter()
                        .map(|f| DynamicArrayBuilder::new(f.data_type(), 1000))
                        .collect();
                    count = 0;
                }
            }

            if count > 0 {
                let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
                let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                let _ = tx.send(Ok(batch)).await;
            }
            Ok(())
        });

        Ok(builder.build())
    }
}

/// Logical node for DuckDB-based external scanning (Postgres, SQLite).
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DuckDBScanLogical {
    pub connection_path: String,
    pub extensions: Vec<String>,
    pub attach_sql: Option<String>,
    pub query: String,
    pub schema: DFSchemaRef,
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
        vec![]
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
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
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
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // allow_unsigned_extensions so locally-built extensions (e.g. luajit)
        // can be LOADed by absolute path; registry extensions are unaffected.
        let config = duckdb::Config::default()
            .allow_unsigned_extensions()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let conn = duckdb::Connection::open_with_flags(&self.connection_path, config)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        for ext in &self.extensions {
            // Absolute paths (or bare .duckdb_extension paths) are LOADed
            // directly — they're local unsigned builds, INSTALL would fail.
            // Bare names go through INSTALL + LOAD (registry extensions).
            if ext.contains('/') || ext.contains('\\') || ext.ends_with(".duckdb_extension") {
                conn.execute(&format!("LOAD '{}';", ext), [])
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            } else {
                conn.execute(&format!("INSTALL {};", ext), []).ok();
                conn.execute(&format!("LOAD {};", ext), [])
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            }
        }

        if let Some(ref attach) = self.attach_sql {
            conn.execute(attach, [])
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        }

        // Use a temporary parquet file to bridge DuckDB and DataFusion Arrow versions
        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join(format!("rustflow_duckdb_{}.parquet", uuid::Uuid::new_v4()));
        let tmp_path = tmp_file.to_str().unwrap();

        let copy_sql = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", self.query, tmp_path);
        conn.execute(&copy_sql, [])
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Read back with DataFusion
        let ctx = SessionContext::new();
        let df = futures::executor::block_on(
            ctx.read_parquet(tmp_path, datafusion::prelude::ParquetReadOptions::default()),
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let batches = futures::executor::block_on(df.collect())
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Clean up
        std::fs::remove_file(tmp_file).ok();

        Ok(Box::pin(
            datafusion::physical_plan::memory::MemoryStream::try_new(
                batches,
                self.project_schema.clone(),
                None,
            )?,
        ))
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

pub struct S3Manager {
    client: S3Client,
    bucket: String,
}

impl S3Manager {
    pub async fn new(bucket: String) -> anyhow::Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = S3Client::new(&config);
        Ok(Self { client, bucket })
    }

    pub async fn upload_file(&self, key: &str, path: &str) -> anyhow::Result<()> {
        let body = ByteStream::from_path(std::path::Path::new(path)).await?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_presigned_url(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> anyhow::Result<String> {
        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(expires_in)?)
            .await?;
        Ok(presigned.uri().to_string())
    }

    pub async fn put_presigned_url(
        &self,
        key: &str,
        expires_in: Duration,
    ) -> anyhow::Result<String> {
        let presigned = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(PresigningConfig::expires_in(expires_in)?)
            .await?;
        Ok(presigned.uri().to_string())
    }
}

pub mod excel;
pub use excel::*;
