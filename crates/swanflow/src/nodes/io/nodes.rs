use anyhow::{anyhow, Result};
use async_trait::async_trait;
use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableConfigExt, ListingTableUrl,
};
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::prelude::*;
use duckdb::Connection;
use futures::future::BoxFuture;
use futures::Stream;
use futures::StreamExt;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::core::datasource::{ExcelScanLogical, ExcelScanMode, ExcelSinkLogical};
use crate::nodes::config as cfg;
use crate::nodes::connectors::clickhouse::ClickHouseConnector;
use crate::nodes::connectors::{ConnConfig, Connector};
use crate::nodes::{InferredMetadata, NodeBehavior, NodeSpec, PortSpec, TypedNodeBehavior};

pub struct DuckDBHelper;

impl DuckDBHelper {
    pub fn open_connection(path: &str) -> Result<Connection> {
        // Read-only semantics for file-backed databases: this helper only
        // serves query_arrow (SELECT via COPY bridge). A read-write open
        // would silently CREATE an empty database file when the path
        // doesn't exist — masking the config mistake and littering the
        // filesystem. In-memory databases can't be read-only, so they
        // keep the default (read-write) config.
        let conn = if path == ":memory:" {
            // allow_unsigned_extensions: locally-built extension binaries
            // (turbovec/luajit/etc.) cannot be LOADed without it. Signed
            // registry extensions are unaffected.
            let config = duckdb::Config::default()
                .allow_unsigned_extensions()
                .map_err(|e| anyhow!("Failed to build DuckDB config: {}", e))?;
            Connection::open_with_flags(path, config)
                .map_err(|e| anyhow!("Failed to open DuckDB: {}", e))?
        } else {
            if !std::path::Path::new(path).is_file() {
                return Err(anyhow!("DuckDB database file does not exist: {}", path));
            }
            let config = duckdb::Config::default()
                .allow_unsigned_extensions()
                .and_then(|c| c.access_mode(duckdb::AccessMode::ReadOnly))
                .map_err(|e| anyhow!("Failed to build DuckDB config: {}", e))?;
            Connection::open_with_flags(path, config)
                .map_err(|e| anyhow!("Failed to open DuckDB read-only: {}", e))?
        };
        Ok(conn)
    }

    pub fn prepare_connection(conn: &Connection, extensions: &[&str]) -> Result<()> {
        for ext in extensions {
            conn.execute(&format!("INSTALL {};", ext), []).ok();
            conn.execute(&format!("LOAD {};", ext), [])
                .map_err(|e| anyhow!("Failed to load DuckDB extension {}: {}", ext, e))?;
        }
        Ok(())
    }

    pub async fn query_arrow(
        path: &str,
        query: &str,
        extensions: &[String],
        attach_sql: &Option<String>,
    ) -> Result<Vec<RecordBatch>> {
        let conn = Self::open_connection(path)?;
        Self::prepare_connection(
            &conn,
            &extensions.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        )?;

        if let Some(sql) = attach_sql {
            conn.execute_batch(sql)
                .map_err(|e| anyhow!("Attach SQL failed: {}", e))?;
        }

        // Use a temporary parquet file to bridge DuckDB and DataFusion
        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join(format!("duckdb_bridge_{}.parquet", uuid::Uuid::new_v4()));
        let tmp_path = tmp_file.to_str().unwrap();

        let copy_sql = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query, tmp_path);
        conn.execute(&copy_sql, [])
            .map_err(|e| anyhow!("DuckDB COPY failed: {}", e))?;

        let ctx = SessionContext::new();
        let batches = {
            let df = ctx
                .read_parquet(tmp_path, ParquetReadOptions::default())
                .await?;
            df.collect().await
        }
        .map_err(|e| anyhow!("DataFusion read back failed: {}", e))?;

        std::fs::remove_file(tmp_path).ok();

        Ok(batches)
    }
}

pub struct DuckDBReadNode;

#[async_trait]
impl NodeBehavior for DuckDBReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.duckdb_read".to_string(),
            display_name: "DuckDB Reader".to_string(),
            description: "Read data from a DuckDB database or Parquet via DuckDB".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Query Result".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": ":memory:" },
                    "query": { "type": "string" }
                },
                "required": ["query"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "query")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = cfg::optional_string(config, "path").unwrap_or_else(|| ":memory:".to_string());
        let query = cfg::required_string(config, "query")?;
        // extensions the user declared in config must flow into BOTH the
        // schema-inference probe and the runtime DuckDBScanLogical — the old
        // code hardcoded `&[]`/`vec![]` here, silently dropping the config so
        // LOADed table functions vanished at COPY time.
        let extensions = cfg::optional_string_array(config, "extensions")?;

        // Infer the REAL schema by running the query once (temp-parquet bridge).
        // Previously this returned an EmptyRelation (0 rows) — the data was
        // never actually emitted, only the schema. Now we return a real
        // DuckDBScan that re-runs the query at execution time and streams the
        // rows downstream.
        let batches = DuckDBHelper::query_arrow(&path, &query, &extensions, &None).await?;
        if batches.is_empty() {
            return Err(anyhow!("Query returned no data for schema inference"));
        }
        let schema = batches[0].schema();
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;
        eprintln!("[duckdb_read] {} -> {} rows x {} cols", query, batches.iter().map(|b| b.num_rows()).sum::<usize>(), schema.fields().len());

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                connection_path: path,
                extensions,
                attach_sql: None,
                query,
                schema: Arc::new(df_schema),
                input_tables: Vec::new(),
            }),
        })])
    }
}
impl TypedNodeBehavior for DuckDBReadNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}


pub struct DuckDBWriteNode;

#[async_trait]
impl NodeBehavior for DuckDBWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.duckdb_write".to_string(),
            display_name: "DuckDB Writer".to_string(),
            description: "Persist upstream rows into a table in a DuckDB database file (CREATE OR REPLACE, idempotent)."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "data.db",
                               "description": "DuckDB database file (created if missing). Use ':memory:' to discard after the run." },
                    "table": { "type": "string",
                               "description": "Target table name; replaced on each run." },
                    "mode": { "type": "string", "enum": ["replace", "append"], "default": "replace",
                               "description": "replace = CREATE OR REPLACE whole table each run; append = INSERT, accumulating across runs (for partitioned backfills)." },
                    "partition_by": { "type": "string",
                               "description": "Partition key(s) for append-mode idempotency: comma-separated columns, each optionally 'col:unit' (unit = date_trunc granularity: day/hour/...). Incoming rows' partitions are deleted before insert — re-running an interval replaces exactly its own rows." }
                },
                "required": ["table"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "table")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let table = cfg::required_string(config, "table")?;
        let path =
            cfg::optional_string(config, "path").unwrap_or_else(|| "data.db".to_string());
        // mode: "replace" (default, historical) | "append" — with optional
        // partition_by for idempotent per-interval backfills.
        // partition_by: single column string, or array of columns; each
        // entry may carry a date_trunc unit as "col:unit" (e.g. "ts:hour").
        let parse_partition = |v: Option<&str>| -> Result<Vec<crate::core::datasource::PartitionCol>> {
            let Some(spec) = v else { return Ok(Vec::new()) };
            let mut cols = Vec::new();
            for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                match part.split_once(':') {
                    Some((col, trunc)) => cols.push(crate::core::datasource::PartitionCol {
                        column: col.trim().to_string(),
                        trunc: Some(trunc.trim().to_string()),
                    }),
                    None => cols.push(crate::core::datasource::PartitionCol {
                        column: part.to_string(),
                        trunc: None,
                    }),
                }
            }
            if cols.iter().any(|c| c.column.is_empty()) {
                return Err(anyhow::anyhow!("invalid partition_by '{spec}': empty column name"));
            }
            Ok(cols)
        };
        let write_mode = match cfg::optional_string(config, "mode").as_deref() {
            None | Some("replace") => crate::core::datasource::WriteMode::Replace,
            Some("append") => crate::core::datasource::WriteMode::Append {
                partition_by: parse_partition(cfg::optional_string(config, "partition_by").as_deref())?,
            },
            Some(other) => {
                return Err(anyhow::anyhow!(
                    "invalid mode '{other}': expected \"replace\" or \"append\""
                ))
            }
        };
        let sink = LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::DuckDBWriteSinkLogical {
                db_path: path,
                table,
                write_mode,
                inputs,
            }),
        });
        Ok(vec![sink])
    }
}
impl TypedNodeBehavior for DuckDBWriteNode {
    const INPUT_COUNT: usize = 1;
    const OUTPUT_COUNT: usize = 0;
}


pub struct HttpFetchNode;

#[async_trait]
impl NodeBehavior for HttpFetchNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.http_fetch".to_string(),
            display_name: "HTTP Fetch".to_string(),
            description: "Fetch JSON data from an API and convert to Arrow batches".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "JSON Data as Arrow".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "method": { "type": "string", "default": "GET" },
                    "timeout_secs": { "type": "number", "default": 30 },
                    "query": { "type": "string",
                        "description": "Override the DuckDB query that extracts rows from the fetched JSON. When omitted, defaults to SELECT * FROM read_json_auto('<url>'). Use it for nested payloads, e.g. SELECT p.* FROM read_json_auto('<url>') CROSS JOIN UNNEST(products) AS p." }
                },
                "required": ["url"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "url")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let url = cfg::required_string(config, "url")?;
        let _method = cfg::optional_string(config, "method").unwrap_or_else(|| "GET".to_string());
        let _timeout_secs = cfg::optional_number(config, "timeout_secs").unwrap_or(30.0);

        // httpfs is a core extension in DuckDB — already built into the binary,
        // no INSTALL/LOAD needed. `SET force_download=true` handles APIs that
        // report incorrect Content-Length (gzip streaming). We run the fetch
        // once to infer the REAL schema — without this the scan plan carries
        // an empty schema and downstream sinks see zero columns.
        // `query` overrides the extraction (needed for nested payloads,
        // e.g. `SELECT p.* FROM read_json_auto('<url>') CROSS JOIN UNNEST(products) AS p`);
        // the default is the bare auto-parse.
        let url_esc = url.replace('\'', "''");
        let query = match cfg::optional_string(config, "query") {
            Some(q) if !q.trim().is_empty() => q,
            _ => format!("SELECT * FROM read_json_auto('{}')", url_esc),
        };
        let force = Some("SET force_download=true;".to_string());
        let batches = match DuckDBHelper::query_arrow(":memory:", &query, &[], &force).await {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[http_fetch] schema-infer FAILED for {url}: {e:#}");
                return Err(e);
            }
        };
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let cols = batches.first().map(|b| b.num_columns()).unwrap_or(0);
        eprintln!("[http_fetch] {url} -> {rows} rows x {cols} cols (schema inferred)");
        if batches.is_empty() || cols == 0 {
            return Err(anyhow!("http_fetch: query returned no rows/columns for {url}"));
        }
        let arrow_schema = batches[0].schema();
        let df_schema = DFSchema::try_from(arrow_schema)?;

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                connection_path: ":memory:".to_string(),
                extensions: vec![],
                attach_sql: Some("SET force_download=true;".to_string()),
                query,
                schema: Arc::new(df_schema),
                input_tables: Vec::new(),
            }),
        })])
    }
}
impl TypedNodeBehavior for HttpFetchNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}


pub struct ParquetReadNode;

#[async_trait]
impl NodeBehavior for ParquetReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.parquet_read".to_string(),
            display_name: "Parquet Reader".to_string(),
            description: "Read data from a Parquet file or directory.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let path = cfg::required_string(config, "path")?;
        // Same silent-success hazard as CsvReadNode: DataFusion's
        // listing-table scan yields empty schema + zero batches for a
        // nonexistent path. Fail fast at validation.
        cfg::validate_local_file(&path, "Parquet")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = cfg::required_string(config, "path")?;
        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(&path, ParquetReadOptions::default())
            .await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for ParquetReadNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}


pub struct CsvReadNode;

#[async_trait]
impl NodeBehavior for CsvReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.csv_read".to_string(),
            display_name: "CSV Reader".to_string(),
            description: "Read data from a CSV file.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let path = cfg::required_string(config, "path")?;
        // DataFusion's listing-table scan does NOT error on a nonexistent
        // path — it yields an empty schema and zero batches, so a typo'd CSV
        // path silently "succeeds" with no data. Fail fast at validation.
        cfg::validate_local_file(&path, "CSV")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = cfg::required_string(config, "path")?;
        let ctx = SessionContext::new();
        let df = ctx.read_csv(&path, CsvReadOptions::new()).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}
impl TypedNodeBehavior for CsvReadNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}


pub struct UniversalReadNode;

impl UniversalReadNode {
    fn resolve_source(config: &JsonValue) -> Result<String> {
        if let Some(source) = cfg::optional_string(config, "source") {
            if source.trim().is_empty() {
                return Err(anyhow!("source must not be empty"));
            }
            Ok(source)
        } else {
            cfg::required_string(config, "path")
        }
    }

    pub fn resolve_route_metadata(config: &JsonValue) -> Result<JsonValue> {
        let source = Self::resolve_source(config)?;
        let source_type =
            cfg::optional_string(config, "source_type").unwrap_or_else(|| "auto".to_string());
        let lowered = source.to_lowercase();

        let resolved_source_type = match source_type.as_str() {
            "auto" => {
                if lowered.starts_with("http://") || lowered.starts_with("https://") {
                    "url"
                } else if lowered.starts_with("select ")
                    || lowered.starts_with("with ")
                    || lowered.starts_with("table:")
                {
                    "table"
                } else {
                    "file"
                }
            }
            "url" | "file" | "table" => source_type.as_str(),
            _ => {
                return Err(anyhow!(
                    "source_type must be one of auto|url|file|table, got {}",
                    source_type
                ))
            }
        };

        let normalized_source = if resolved_source_type == "table" && lowered.starts_with("table:")
        {
            source[6..].trim().to_string()
        } else {
            source
        };

        Ok(json!({
            "source": normalized_source,
            "requested_source_type": source_type,
            "resolved_source_type": resolved_source_type
        }))
    }

    fn detect_format(path: &str) -> Option<FileFormat> {
        let path = path.to_lowercase();
        if path.ends_with(".parquet") {
            Some(FileFormat::Parquet)
        } else if path.ends_with(".csv") {
            Some(FileFormat::Csv)
        } else if path.ends_with(".json") || path.ends_with(".jsonl") {
            Some(FileFormat::Json)
        } else {
            None
        }
    }

    fn discover_partitions(dir_path: &str) -> Vec<String> {
        let mut partitions = Vec::new();
        if let Ok(entries) = std::fs::read_dir(dir_path) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    let name = entry.file_name().into_string().unwrap_or_default();
                    if name.contains('=') {
                        let parts: Vec<&str> = name.split('=').collect();
                        if !parts.is_empty() {
                            partitions.push(parts[0].to_string());
                        }
                    }
                }
            }
        }
        partitions
    }

    fn delimiter_byte(config: &JsonValue) -> u8 {
        cfg::optional_string(config, "delimiter")
            .and_then(|d| d.as_bytes().first().copied())
            .unwrap_or(b',')
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileFormat {
    Parquet,
    Csv,
    Json,
}

#[async_trait]
impl NodeBehavior for UniversalReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.universal_read".to_string(),
            display_name: "Universal Reader".to_string(),
            description: "Intelligent reader that automatically detects format and supports directory scanning.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "source": { "type": "string" },
                    "source_type": { "type": "string", "default": "auto", "enum": ["auto", "url", "file", "table"] },
                    "format": { "type": "string", "default": "auto", "enum": ["auto", "parquet", "csv", "json", "avro", "arrow"] },
                    "recursive": { "type": "boolean", "default": false },
                    "has_header": { "type": "boolean", "default": true },
                    "delimiter": { "type": "string", "default": "," },
                    "multi_line": { "type": "boolean", "default": false }
                },
                "anyOf": [
                    { "required": ["path"] },
                    { "required": ["source"] }
                ]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let route = Self::resolve_route_metadata(config)?;
        // Same silent-success hazard as CsvReadNode for local file routes:
        // DataFusion's listing-table scan yields empty schema + zero batches
        // for a nonexistent path (the JSON branch already canonicalizes, but
        // the CSV/Parquet branches don't). Only check the "file" route —
        // url/table routes have their own error paths. Directories and globs
        // are legitimate inputs here, so accept any existing path.
        if route["resolved_source_type"] == "file" {
            let source = route["source"].as_str().unwrap_or_default();
            let p = std::path::Path::new(source);
            let contains_glob = source
                .bytes()
                .any(|b| matches!(b, b'*' | b'?' | b'[' | b']'));
            let ok = if contains_glob {
                p.parent()
                    .filter(|d| !d.as_os_str().is_empty())
                    .map(|d| d.is_dir())
                    .unwrap_or(true)
            } else {
                p.exists()
            };
            if !ok {
                return Err(anyhow!("Source path does not exist: {}", source));
            }
        }
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let route = Self::resolve_route_metadata(config)?;
        let source = route["source"]
            .as_str()
            .ok_or_else(|| anyhow!("route source missing"))?
            .to_string();
        let resolved_source_type = route["resolved_source_type"]
            .as_str()
            .ok_or_else(|| anyhow!("resolved_source_type missing"))?;

        if resolved_source_type == "table" {
            let sess_config = SessionConfig::new().with_target_partitions(1);
            let ctx = SessionContext::new_with_config(sess_config);
            let df = ctx.sql(&source).await?;
            return Ok(InferredMetadata {
                schemas: vec![Arc::new(df.schema().clone())],
                sample_data: None,
            });
        }
        if resolved_source_type == "url" {
            return Err(anyhow!(
                "URL route is not directly executable by universal_read; use core.io.http_fetch first ({})",
                source
            ));
        }

        let path = source;
        let format_str =
            cfg::optional_string(config, "format").unwrap_or_else(|| "auto".to_string());

        let format = if format_str == "auto" {
            Self::detect_format(&path)
                .ok_or_else(|| anyhow!("Could not detect format for {}", path))?
        } else {
            match format_str.as_str() {
                "parquet" => FileFormat::Parquet,
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                _ => return Err(anyhow!("Unsupported format override: {}", format_str)),
            }
        };

        let sess_config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(sess_config);
        let is_dir = std::path::Path::new(&path).is_dir();

        match format {
            FileFormat::Parquet => {
                let mut opts = ParquetReadOptions::default();
                if is_dir {
                    let abs_path = std::fs::canonicalize(&path)?;
                    let partitions = Self::discover_partitions(abs_path.to_str().unwrap());
                    if !partitions.is_empty() {
                        opts.table_partition_cols = partitions
                            .into_iter()
                            .map(|p| (p, DataType::Utf8))
                            .collect();
                    }
                }
                let df = ctx.read_parquet(&path, opts).await?;
                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df.schema().clone())],
                    sample_data: None,
                })
            }
            FileFormat::Csv => {
                let has_header = cfg::optional_bool(config, "has_header").unwrap_or(true);
                let delimiter = Self::delimiter_byte(config);
                let opts = CsvReadOptions::new()
                    .has_header(has_header)
                    .delimiter(delimiter);
                let df = ctx.read_csv(&path, opts).await?;
                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df.schema().clone())],
                    sample_data: None,
                })
            }
            FileFormat::Json => {
                let abs_path = std::fs::canonicalize(&path)?;
                let query = format!(
                    "SELECT * FROM read_json_auto('{}')",
                    abs_path.to_str().unwrap()
                );

                let batches = DuckDBHelper::query_arrow(":memory:", &query, &[], &None).await?;
                if batches.is_empty() {
                    return Err(anyhow!("JSON file is empty or invalid: {}", path));
                }

                let schema = batches[0].schema();
                let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df_schema)],
                    sample_data: None,
                })
            }
        }
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let route = Self::resolve_route_metadata(config)?;
        let source = route["source"]
            .as_str()
            .ok_or_else(|| anyhow!("route source missing"))?
            .to_string();
        let resolved_source_type = route["resolved_source_type"]
            .as_str()
            .ok_or_else(|| anyhow!("resolved_source_type missing"))?;

        if resolved_source_type == "table" {
            let sess_config = SessionConfig::new().with_target_partitions(1);
            let ctx = SessionContext::new_with_config(sess_config);
            let df = ctx.sql(&source).await?;
            return Ok(vec![df.into_optimized_plan()?]);
        }
        if resolved_source_type == "url" {
            return Err(anyhow!(
                "URL route is not directly executable by universal_read; use core.io.http_fetch first ({})",
                source
            ));
        }

        let path = source;
        let format_str =
            cfg::optional_string(config, "format").unwrap_or_else(|| "auto".to_string());
        let recursive = cfg::optional_bool(config, "recursive").unwrap_or(false);

        let format = if format_str == "auto" {
            Self::detect_format(&path)
                .ok_or_else(|| anyhow!("Could not detect format for {}", path))?
        } else {
            match format_str.as_str() {
                "parquet" => FileFormat::Parquet,
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                _ => return Err(anyhow!("Unsupported format override: {}", format_str)),
            }
        };

        let sess_config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(sess_config);

        // Handle directory vs file
        let is_dir = std::path::Path::new(&path).is_dir();

        if is_dir {
            match format {
                FileFormat::Parquet => {
                    let path = std::path::Path::new(&path);
                    if !path.exists() {
                        return Err(anyhow!("Path does not exist: {}", path.display()));
                    }
                    let abs_path = std::fs::canonicalize(&path)?;
                    let table_url = ListingTableUrl::parse(abs_path.to_str().unwrap())?;

                    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                        .with_collect_stat(true)
                        .with_file_extension(".parquet")
                        .with_target_partitions(1);

                    let config =
                        ListingTableConfig::new(table_url).with_listing_options(listing_options);

                    let config = config.infer(&ctx.state()).await?;
                    let table = Arc::new(ListingTable::try_new(config)?);

                    let df = ctx.read_table(table)?;
                    let df =
                        df.repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Csv => {
                    let has_header = cfg::optional_bool(config, "has_header").unwrap_or(true);
                    let delimiter = Self::delimiter_byte(config);
                    let opts = CsvReadOptions::new()
                        .has_header(has_header)
                        .delimiter(delimiter);
                    let df = ctx.read_csv(&path, opts).await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Json => {
                    let path_ref = std::path::Path::new(&path);
                    if !path_ref.exists() {
                        return Err(anyhow!("Path does not exist: {}", path));
                    }
                    let abs_path = std::fs::canonicalize(&path)?;
                    let query = if recursive {
                        format!(
                            "SELECT * FROM read_json_auto('{}/**/*.json')",
                            abs_path.to_str().unwrap()
                        )
                    } else {
                        format!(
                            "SELECT * FROM read_json_auto('{}/*.json')",
                            abs_path.to_str().unwrap()
                        )
                    };

                    let batches = DuckDBHelper::query_arrow(":memory:", &query, &[], &None).await?;
                    if batches.is_empty() {
                        return Err(anyhow!("No JSON data found in {}", path));
                    }

                    let schema = batches[0].schema();
                    let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

                    Ok(vec![LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                                connection_path: ":memory:".to_string(),
                                extensions: vec![],
                                attach_sql: None,
                                query,
                                schema: Arc::new(df_schema),
                                input_tables: Vec::new(),
                            }),
                        },
                    )])
                }
            }
        } else {
            // Single file path
            match format {
                FileFormat::Parquet => {
                    let df = ctx
                        .read_parquet(&path, ParquetReadOptions::default())
                        .await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Csv => {
                    let has_header = cfg::optional_bool(config, "has_header").unwrap_or(true);
                    let delimiter = Self::delimiter_byte(config);
                    let opts = CsvReadOptions::new()
                        .has_header(has_header)
                        .delimiter(delimiter);
                    let df = ctx.read_csv(&path, opts).await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Json => {
                    let path_ref = std::path::Path::new(&path);
                    if !path_ref.exists() {
                        return Err(anyhow!("Path does not exist: {}", path));
                    }
                    let abs_path = std::fs::canonicalize(&path)?;
                    let query = format!(
                        "SELECT * FROM read_json_auto('{}')",
                        abs_path.to_str().unwrap()
                    );

                    let batches = DuckDBHelper::query_arrow(":memory:", &query, &[], &None).await?;
                    if batches.is_empty() {
                        return Err(anyhow!("JSON file is empty or invalid: {}", path));
                    }

                    let schema = batches[0].schema();
                    let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

                    Ok(vec![LogicalPlan::Extension(
                        datafusion::logical_expr::Extension {
                            node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                                connection_path: ":memory:".to_string(),
                                extensions: vec![],
                                attach_sql: None,
                                query,
                                schema: Arc::new(df_schema),
                                input_tables: Vec::new(),
                            }),
                        },
                    )])
                }
            }
        }
    }
}
impl TypedNodeBehavior for UniversalReadNode {
    const INPUT_COUNT: usize = 0;
    const OUTPUT_COUNT: usize = 1;
}


pub struct DbConnectorNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbDriver {
    DuckDBPostgres,
    DuckDBSqlite,
    DuckDBMysql,
    ClickHouseConnector,
}

impl DbConnectorNode {
    fn select_driver(url: &str) -> Option<DbDriver> {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Some(DbDriver::DuckDBPostgres)
        } else if url.starts_with("mysql://") || url.starts_with("mariadb://") {
            Some(DbDriver::DuckDBMysql)
        } else if url.starts_with("sqlite://") || url.starts_with("sqlite3://") {
            Some(DbDriver::DuckDBSqlite)
        } else if url.starts_with("http://") || url.starts_with("https://") {
            Some(DbDriver::ClickHouseConnector)
        } else {
            None
        }
    }

    async fn build_duckdb_plan(
        expanded_url: &str,
        query: &str,
        driver: DbDriver,
    ) -> Result<Vec<LogicalPlan>> {
        let ext = match driver {
            DbDriver::DuckDBPostgres => "postgres",
            DbDriver::DuckDBMysql => "mysql",
            _ => "sqlite",
        };

        let attach_sql = match driver {
            DbDriver::DuckDBPostgres => Some(format!(
                "ATTACH '{}' AS ext_db (TYPE postgres);",
                expanded_url
            )),
            DbDriver::DuckDBMysql => Some(format!(
                "ATTACH '{}' AS ext_db (TYPE mysql);",
                expanded_url
            )),
            _ => {
                let db_path = expanded_url.trim_start_matches("sqlite://");
                Some(format!("ATTACH '{}' AS ext_db (TYPE sqlite);", db_path))
            }
        };

        let batches =
            DuckDBHelper::query_arrow(":memory:", query, &[ext.to_string()], &attach_sql).await?;
        if batches.is_empty() {
            return Err(anyhow!("Query returned no data for schema inference"));
        }
        let schema = batches[0].schema();
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                    connection_path: ":memory:".to_string(),
                    extensions: vec![ext.to_string()],
                    attach_sql,
                    query: query.to_string(),
                    schema: Arc::new(df_schema),
                    input_tables: Vec::new(),
                }),
            },
        )])
    }
}

#[async_trait]
impl NodeBehavior for DbConnectorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.db_connector".to_string(),
            display_name: "Database Connector".to_string(),
            description: "Connect to external databases (PostgreSQL, MySQL, ClickHouse, SQLite, etc.) using native connectors or DuckDB scanners.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Table Data".into(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "url": { "type": "string" },
                    "query": { "type": "string" },
                    "table": { "type": "string", "description": "Table name (alternative to query)" },
                    "driver": {
                        "type": "string",
                        "enum": ["auto", "postgres", "mysql", "clickhouse", "duckdb_postgres", "duckdb_sqlite", "duckdb_mysql"],
                        "default": "auto"
                    }
                },
                "oneOf": [
                    { "required": ["url", "query"] },
                    { "required": ["url", "table"] }
                ]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "url")?;
        if config
            .get("query")
            .and_then(|v| v.as_str())
            .map_or(true, |s| s.is_empty())
            && config
                .get("table")
                .and_then(|v| v.as_str())
                .map_or(true, |s| s.is_empty())
        {
            return Err(anyhow!("Either 'query' or 'table' must be provided"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let url = cfg::required_string(config, "url")?;
        let query = cfg::optional_string(config, "query");
        let table = cfg::optional_string(config, "table");
        let driver = cfg::optional_string(config, "driver").unwrap_or_else(|| "auto".to_string());

        // ENV expansion for security
        let expanded_url = if url.contains("${") {
            let mut result = url.clone();
            let re = regex::Regex::new(r"\$\{([A-Z0-9_]+)\}").unwrap();
            for cap in re.captures_iter(&url) {
                let var_name = &cap[1];
                if let Ok(val) = std::env::var(var_name) {
                    result = result.replace(&cap[0], &val);
                }
            }
            result
        } else {
            url.clone()
        };

        // Determine the driver
        let resolved_driver = if driver == "auto" {
            Self::select_driver(&expanded_url)
        } else {
            match driver.as_str() {
                "postgres" | "duckdb_postgres" => Some(DbDriver::DuckDBPostgres),
                "mysql" | "duckdb_mysql" => Some(DbDriver::DuckDBMysql),
                "duckdb_sqlite" => Some(DbDriver::DuckDBSqlite),
                "clickhouse" => Some(DbDriver::ClickHouseConnector),
                _ => None,
            }
        }
        .ok_or_else(|| {
            anyhow!(
                "Unsupported database URL: {}. Supported schemes: postgres://, postgresql://, mysql://, mariadb://, sqlite://, http(s):// (ClickHouse)",
                expanded_url
            )
        })?;

        // Build the effective query
        let effective_query = match (query, table) {
            (Some(q), _) if !q.is_empty() => q,
            (_, Some(t)) if !t.is_empty() => format!("SELECT * FROM {}", t),
            _ => return Err(anyhow!("Either 'query' or 'table' must be provided")),
        };

        match resolved_driver {
            DbDriver::DuckDBPostgres | DbDriver::DuckDBSqlite | DbDriver::DuckDBMysql => {
                // DuckDB scanner path (postgres / sqlite / mysql ATTACH)
                Self::build_duckdb_plan(&expanded_url, &effective_query, resolved_driver).await
            }
            DbDriver::ClickHouseConnector => {
                // Schema discovery via ClickHouseConnector (HTTP), execution
                // via dedicated ClickHouseScanExec (Arrow stream over HTTP).
                let conn_config = ConnConfig {
                    url: expanded_url.clone(),
                    driver: "clickhouse".to_string(),
                    max_connections: 10,
                    timeout_secs: 30,
                };
                let connector = ClickHouseConnector::connect(conn_config).await?;
                let schema = connector
                    .get_schema(&effective_query.trim_start_matches("SELECT * FROM "))
                    .await?;
                let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);

                Ok(vec![LogicalPlan::Extension(
                    datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::ClickHouseScanLogical {
                            url: expanded_url,
                            query: effective_query,
                            schema: df_schema,
                        }),
                    },
                )])
            }
        }
    }
}

pub struct GraphIndexWriterNode;

#[async_trait]
impl NodeBehavior for GraphIndexWriterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "io.graph_index_writer".to_string(),
            display_name: "Graph Index Writer".to_string(),
            description: "Persist nodes and edges to DuckDB graph index".to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "nodes".into(),
                    description: "Entities table".into(),
                },
                PortSpec {
                    id: 1,
                    name: "edges".into(),
                    description: "Relationships table".into(),
                },
            ],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {}
            }),
        }
    }

    fn validate(&self, _config: &JsonValue) -> Result<()> {
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.len() < 2 {
            return Err(anyhow!("GraphIndexWriter requires Nodes and Edges inputs"));
        }

        let node = Arc::new(GraphIndexWriterLogical {
            nodes_input: inputs[0].clone(),
            edges_input: inputs[1].clone(),
        });

        Ok(vec![LogicalPlan::Extension(Extension { node })])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub struct GraphIndexWriterLogical {
    pub nodes_input: LogicalPlan,
    pub edges_input: LogicalPlan,
}

impl UserDefinedLogicalNodeCore for GraphIndexWriterLogical {
    fn name(&self) -> &str {
        "GraphIndexWriter"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.nodes_input, &self.edges_input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref EMPTY_SCHEMA: DFSchemaRef = Arc::new(DFSchema::empty());
        }
        &EMPTY_SCHEMA
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphIndexWriter")
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            nodes_input: inputs[0].clone(),
            edges_input: inputs[1].clone(),
        })
    }
}

pub struct GraphIndexWriterExec {
    pub nodes_input: Arc<dyn ExecutionPlan>,
    pub edges_input: Arc<dyn ExecutionPlan>,
    pub cache_dir: String,
    cache: PlanProperties,
}

impl fmt::Debug for GraphIndexWriterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GraphIndexWriterExec").finish()
    }
}

impl GraphIndexWriterExec {
    pub fn new(
        nodes_input: Arc<dyn ExecutionPlan>,
        edges_input: Arc<dyn ExecutionPlan>,
        cache_dir: String,
    ) -> Self {
        // CRITICAL: coalesce both inputs to 1 partition — same partition-drop hazard
        // as the other graph execs; multi-partition inputs would lose node/edge rows.
        let nodes_input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                nodes_input,
            ),
        );
        let edges_input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(
                edges_input,
            ),
        );
        let schema = Arc::new(Schema::empty());
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            nodes_input,
            edges_input,
            cache_dir,
            cache,
        }
    }
}

impl DisplayAs for GraphIndexWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GraphIndexWriterExec")
    }
}

impl ExecutionPlan for GraphIndexWriterExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.nodes_input, &self.edges_input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.cache_dir.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let nodes_stream = self.nodes_input.execute(partition, context.clone())?;
        let edges_stream = self.edges_input.execute(partition, context)?;
        Ok(Box::pin(GraphIndexingStream {
            nodes_stream,
            edges_stream,
            cache_dir: self.cache_dir.clone(),
            state: IndexerState::Polling,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "GraphIndexWriterExec"
    }
}

enum IndexerState {
    Polling,
    Writing(BoxFuture<'static, std::result::Result<(), DataFusionError>>),
    Done,
}

struct GraphIndexingStream {
    nodes_stream: SendableRecordBatchStream,
    edges_stream: SendableRecordBatchStream,
    cache_dir: String,
    state: IndexerState,
}

impl Stream for GraphIndexingStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                IndexerState::Polling => {
                    let mut nodes_input = std::mem::replace(
                        &mut self.nodes_stream,
                        Box::pin(
                            datafusion::physical_plan::stream::EmptyRecordBatchStream::new(
                                Arc::new(Schema::empty()),
                            ),
                        ),
                    );
                    let mut edges_input = std::mem::replace(
                        &mut self.edges_stream,
                        Box::pin(
                            datafusion::physical_plan::stream::EmptyRecordBatchStream::new(
                                Arc::new(Schema::empty()),
                            ),
                        ),
                    );
                    let cache_dir = self.cache_dir.clone();

                    let fut = async move {
                        let metadata_path = format!("{}/metadata.duckdb", cache_dir);

                        let mut nodes_batches = Vec::new();
                        while let Some(Ok(batch)) = nodes_input.next().await {
                            nodes_batches.push(batch);
                        }

                        let mut edges_batches = Vec::new();
                        while let Some(Ok(batch)) = edges_input.next().await {
                            edges_batches.push(batch);
                        }

                        tokio::task::spawn_blocking(move || {
                            let conn = Connection::open(&metadata_path).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "GraphIndexWriter failed to open metadata db {metadata_path}: {e}"
                                ))
                            })?;
                            // BUGFIX: materialized parquet round-trips BYTE_ARRAY as
                            // Utf8View (see planner.rs parquet reader note), so the
                            // historical StringArray downcasts could never succeed.
                            // Cast the whole batch to the canonical layout first.
                            let nodes_target = arrow_schema_of(&[
                                ("id", DataType::Int64),
                                ("name", DataType::Utf8),
                                ("label", DataType::Utf8),
                                ("properties", DataType::Utf8),
                            ]);
                            let edges_target = arrow_schema_of(&[
                                ("source_id", DataType::Int64),
                                ("target_id", DataType::Int64),
                                ("type", DataType::Utf8),
                                ("weight", DataType::Float64),
                            ]);
                            for batch in nodes_batches {
                                let batch = cast_record_batch(&batch, nodes_target.clone())?;
                                let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().ok_or_else(|| DataFusionError::Execution("graph nodes column 0 must be Int64".into()))?;
                                let names = batch.column(1).as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("graph nodes column 1 (name) must be Utf8".into()))?;
                                let labels = batch.column(2).as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("graph nodes column 2 (label) must be Utf8".into()))?;
                                let properties = batch.column(3).as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("graph nodes column 3 (properties) must be Utf8".into()))?;
                                for i in 0..batch.num_rows() {
                                    conn.execute(
                                        "INSERT OR REPLACE INTO __graph_nodes (id, name, label, properties) VALUES (?, ?, ?, ?)",
                                        duckdb::params![ids.value(i), names.value(i), labels.value(i), properties.value(i)],
                                    ).map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "GraphIndexWriter failed to insert node {}: {e}",
                                            ids.value(i)
                                        ))
                                    })?;
                                }
                            }
                            for batch in edges_batches {
                                let batch = cast_record_batch(&batch, edges_target.clone())?;
                                let src_ids = batch.column(0).as_any().downcast_ref::<Int64Array>().ok_or_else(|| DataFusionError::Execution("graph edges column 0 must be Int64".into()))?;
                                let dst_ids = batch.column(1).as_any().downcast_ref::<Int64Array>().ok_or_else(|| DataFusionError::Execution("graph edges column 1 must be Int64".into()))?;
                                let types = batch.column(2).as_any().downcast_ref::<StringArray>().ok_or_else(|| DataFusionError::Execution("graph edges column 2 must be Utf8".into()))?;
                                let weights = batch.column(3).as_any().downcast_ref::<Float64Array>().ok_or_else(|| DataFusionError::Execution("graph edges column 3 must be Float64".into()))?;
                                for i in 0..batch.num_rows() {
                                    conn.execute(
                                        "INSERT OR REPLACE INTO __graph_edges (source_id, target_id, type, weight) VALUES (?, ?, ?, ?)",
                                        duckdb::params![src_ids.value(i), dst_ids.value(i), types.value(i), weights.value(i)],
                                    ).map_err(|e| {
                                        DataFusionError::Execution(format!(
                                            "GraphIndexWriter failed to insert edge {}->{}: {e}",
                                            src_ids.value(i), dst_ids.value(i)
                                        ))
                                    })?;
                                }
                            }
                            // Explicit checkpoint: read-only consumers (e.g.
                            // GraphRetrieverExec ATTACHes this file read-only)
                            // cannot replay the WAL. Without a checkpoint the
                            // just-inserted rows are invisible to them (observed:
                            // same-process write-then-retrieve returned 0 rows).
                            conn.execute("CHECKPOINT", []).map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "GraphIndexWriter failed to checkpoint: {e}"
                                ))
                            })?;
                            Ok::<(), DataFusionError>(())
                        }).await.map_err(|e| DataFusionError::Execution(format!("Spawn blocking error: {}", e)))??;
                        Ok(())
                    };
                    self.state = IndexerState::Writing(Box::pin(fut));
                }
                IndexerState::Writing(fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(Ok(_)) => {
                        self.state = IndexerState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(e)) => {
                        self.state = IndexerState::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                IndexerState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for GraphIndexingStream {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::empty())
    }
}

// ---- GraphIndexWriter helpers: canonical-schema cast --------------------
fn arrow_schema_of(cols: &[(&str, DataType)]) -> SchemaRef {
    let fields: Vec<_> = cols
        .iter()
        .map(|(n, t)| Field::new(*n, t.clone(), true))
        .collect();
    std::sync::Arc::new(Schema::new(fields))
}

fn cast_record_batch(
    batch: &RecordBatch,
    target: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    if batch.schema() == target {
        return Ok(batch.clone());
    }
    let cols: Result<Vec<_>, DataFusionError> = target
        .fields()
        .iter()
        .zip(batch.columns().iter())
        .map(|(f, c)| {
            arrow::compute::cast(c, f.data_type()).map_err(|e| {
                DataFusionError::Execution(format!(
                    "GraphIndexWriter cast to {} failed: {e}",
                    f.data_type()
                ))
            })
        })
        .collect();
    RecordBatch::try_new(target, cols?)
        .map_err(|e| DataFusionError::Execution(format!("GraphIndexWriter batch rebuild: {e}")))
}
