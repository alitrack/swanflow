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
use sqlx::{Column, Row};
use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};


use crate::core::datasource::{ExcelScanLogical, ExcelScanMode, ExcelSinkLogical};
use crate::nodes::config as cfg;
use crate::nodes::connectors::clickhouse::ClickHouseConnector;
use crate::nodes::connectors::mysql::MySqlConnector;
use crate::nodes::connectors::postgres::PostgresConnector;
use crate::nodes::connectors::{ConnConfig, Connector};
use crate::nodes::{InferredMetadata, NodeBehavior, NodeSpec, PortSpec};

pub struct ExcelReadNode;

impl ExcelReadNode {
    /// Promotes Excel cell data to appropriate Arrow data type.
    /// This handles type inference from raw Excel data.
    #[allow(dead_code)]
    fn promote_type(current: &DataType, data: &Data) -> DataType {
        use calamine::Data::*;

        match data {
            // Empty cells don't change the type
            Empty => current.clone(),
            // String type stays as string
            String(_) => DataType::Utf8,
            // Float type
            Float(_) => DataType::Float64,
            // Integer type - promote to Int64
            Int(_) => DataType::Int64,
            // Boolean type
            Bool(_) => DataType::Boolean,
            // DateTime - store as string for now (could be improved with proper datetime type)
            DateTime(_) | DateTimeIso(_) | DurationIso(_) => DataType::Utf8,
            // Error types - convert to null
            Error(_) => DataType::Null,
        }
    }
}

#[async_trait]
impl NodeBehavior for ExcelReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_read".to_string(),
            display_name: "Excel Reader".to_string(),
            description:
                "Read data from an Excel file with automatic type promotion and a rejects port"
                    .to_string(),
            input_ports: vec![],
            output_ports: vec![
                PortSpec {
                    id: 0,
                    name: "output".into(),
                    description: "Validated Data".into(),
                },
                PortSpec {
                    id: 1,
                    name: "rejects".into(),
                    description: "Rows with type errors".into(),
                },
            ],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "sheet": { "type": "string" },
                    "has_header": { "type": "boolean", "default": true },
                    "sample_rows": { "type": "integer", "default": 100 }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "path")?;
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let path = cfg::required_string(config, "path")?;
        let sheet_name = cfg::optional_string(config, "sheet");
        let has_header = cfg::optional_bool(config, "has_header").unwrap_or(true);
        let sample_limit = cfg::optional_number(config, "sample_rows")
            .unwrap_or(100.0)
            .max(0.0) as usize;

        let mut workbook: Xlsx<_> = open_workbook(&path)?;
        let sheet = if let Some(name) = sheet_name.as_deref() {
            workbook
                .worksheet_range(name)
                .map_err(|e| anyhow!("Error reading sheet {}: {}", name, e))?
        } else {
            workbook
                .worksheet_range_at(0)
                .ok_or_else(|| anyhow!("No sheets found"))?
                .map_err(|e| anyhow!("Error reading first sheet: {}", e))?
        };

        let mut rows = sheet.rows();
        let headers: Vec<String> = if has_header {
            if let Some(header_row) = rows.next() {
                header_row.iter().map(|c: &Data| c.to_string()).collect()
            } else {
                return Err(anyhow!("Empty sheet"));
            }
        } else {
            let first_row = sheet.rows().next().ok_or_else(|| anyhow!("Empty sheet"))?;
            (0..first_row.len())
                .map(|i| format!("column_{}", i))
                .collect()
        };

        let main_fields: Vec<Field> = headers
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let main_schema = Arc::new(Schema::new(main_fields));

        let rejects_fields = vec![
            Field::new("row_index", DataType::Int64, false),
            Field::new("raw_data", DataType::Utf8, false),
            Field::new("error_message", DataType::Utf8, false),
        ];
        let rejects_schema = Arc::new(Schema::new(rejects_fields));

        let mut sample_batches = Vec::new();
        if sample_limit > 0 {
            let mut builders: Vec<StringBuilder> = (0..main_schema.fields().len())
                .map(|_| StringBuilder::new())
                .collect();

            let mut data_rows = sheet.rows();
            if has_header {
                data_rows.next();
            }

            for _ in 0..sample_limit {
                if let Some(row) = data_rows.next() {
                    for (i, cell) in row.iter().enumerate() {
                        builders[i].append_value(cell.to_string());
                    }
                } else {
                    break;
                }
            }

            let columns: Vec<ArrayRef> = builders
                .into_iter()
                .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                .collect();
            let batch = RecordBatch::try_new(main_schema.clone(), columns)?;
            sample_batches.push(batch);
        }

        Ok(InferredMetadata {
            schemas: vec![
                Arc::new(DFSchema::try_from(main_schema)?),
                Arc::new(DFSchema::try_from(rejects_schema)?),
            ],
            sample_data: Some(sample_batches),
        })
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(cfg::required_string(config, "path")?);
        let sheet = cfg::optional_string(config, "sheet");
        let mode = ExcelScanMode::Validated;

        let scan_node = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelScanLogical {
                path,
                sheet,
                mode,
                schema: Arc::new(DFSchema::empty()), // Placeholder, will be inferred
            }),
        });

        Ok(vec![scan_node])
    }
}

pub struct ExcelWriteNode;

#[async_trait]
impl NodeBehavior for ExcelWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_write".to_string(),
            display_name: "Excel Writer".to_string(),
            description: "Write data to an Excel file.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data to write".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "sheet": { "type": "string", "default": "Sheet1" },
                    "convert_to_table": { "type": "boolean", "default": true }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "path")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(cfg::required_string(config, "path")?);
        let sheet = cfg::optional_string(config, "sheet").unwrap_or_else(|| "Sheet1".to_string());
        let convert_to_table = cfg::optional_bool(config, "convert_to_table").unwrap_or(true);

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelSinkLogical {
                path,
                sheets: vec![sheet],
                inputs,
                styles: json!({}),
                convert_to_table,
                conditional_formatting: json!([]),
            }),
        });

        Ok(vec![sink_plan])
    }
}

pub struct ExcelTemplateNode;

#[async_trait]
impl NodeBehavior for ExcelTemplateNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_template".to_string(),
            display_name: "Excel Template Filler".to_string(),
            description: "Fill a pre-styled Excel template with data based on markers like {{name}} or table injections.".to_string(),
            input_ports: vec![
                PortSpec { id: 0, name: "input".into(), description: "Data to inject".into() },
            ],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "template_path": { "type": "string" },
                    "output_path": { "type": "string" },
                    "sheet": { "type": "string", "default": "Sheet1" },
                    "markers": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "Map of field names to markers in the template"
                    },
                    "table_start": {
                        "type": "string",
                        "description": "Starting cell for table injection (e.g., 'A10')"
                    }
                },
                "required": ["template_path", "output_path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "template_path")?;
        cfg::required_string(config, "output_path")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let template_path = PathBuf::from(cfg::required_string(config, "template_path")?);
        let output_path = PathBuf::from(cfg::required_string(config, "output_path")?);
        let sheet = cfg::optional_string(config, "sheet").unwrap_or_else(|| "Sheet1".to_string());
        let table_start = cfg::optional_string(config, "table_start");

        let mut markers = std::collections::HashMap::new();
        if let Some(obj) = config["markers"].as_object() {
            for (k, v) in obj {
                if let Some(marker) = v.as_str() {
                    markers.insert(k.clone(), marker.to_string());
                }
            }
        }

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::ExcelTemplateSinkLogical {
                template_path,
                output_path,
                sheet,
                markers,
                table_start,
                inputs,
            }),
        });

        Ok(vec![sink_plan])
    }
}

pub struct ExcelSheetNamesNode;

#[async_trait]
impl NodeBehavior for ExcelSheetNamesNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_sheet_names".to_string(),
            display_name: "Excel Sheet Names".to_string(),
            description: "List all worksheet names in an Excel file.".to_string(),
            input_ports: vec![],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".into(),
                description: "Sheet Names".into(),
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
        cfg::required_string(config, "path")?;
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        _config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sheet_name",
            DataType::Utf8,
            false,
        )]));
        Ok(InferredMetadata {
            schemas: vec![Arc::new(DFSchema::try_from(schema)?)],
            sample_data: None,
        })
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = cfg::required_string(config, "path")?;
        let workbook: Xlsx<_> = open_workbook(&path)?;
        let names = workbook.sheet_names();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "sheet_name",
            DataType::Utf8,
            false,
        )]));
        let mut values = Vec::new();

        for name in names {
            values.push(vec![datafusion::logical_expr::Expr::Literal(
                datafusion::scalar::ScalarValue::Utf8(Some(name)),
                None,
            )]);
        }

        let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);

        Ok(vec![LogicalPlan::Values(
            datafusion::logical_expr::Values {
                schema: df_schema,
                values,
            },
        )])
    }
}

pub struct ExcelReportNode;

#[async_trait]
impl NodeBehavior for ExcelReportNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_report".to_string(),
            display_name: "Excel Report Builder".to_string(),
            description: "Build a multi-sheet Excel report with automated charts.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "data".into(),
                description: "Data for the report".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "title": { "type": "string", "default": "Automated Report" },
                    "charts": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "type": { "type": "string", "enum": ["column", "line", "pie"] },
                                "title": { "type": "string" },
                                "data_range": { "type": "string" },
                                "label_range": { "type": "string" },
                                "x_axis": { "type": "string" },
                                "y_axis": { "type": "string" },
                                "offset": {
                                    "type": "object",
                                    "properties": {
                                        "row": { "type": "integer" },
                                        "col": { "type": "integer" }
                                    }
                                }
                            }
                        }
                    }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        cfg::required_string(config, "path")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(cfg::required_string(config, "path")?);
        let charts = config["charts"].clone();

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::ExcelReportSinkLogical {
                path,
                charts,
                inputs,
            }),
        });

        Ok(vec![sink_plan])
    }
}

pub struct DuckDBHelper;

impl DuckDBHelper {
    pub fn open_connection(path: &str) -> Result<Connection> {
        Connection::open(path).map_err(|e| anyhow!("Failed to open DuckDB: {}", e))
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

        // Use query_arrow to get the schema reliably without holding Connection across await
        let batches = DuckDBHelper::query_arrow(&path, &query, &[], &None).await?;
        if batches.is_empty() {
            return Err(anyhow!("Query returned no data for schema inference"));
        }

        let schema = batches[0].schema();
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

        Ok(vec![LogicalPlan::EmptyRelation(
            datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(df_schema),
            },
        )])
    }
}

pub struct DuckDBWriteNode;

#[async_trait]
impl NodeBehavior for DuckDBWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.duckdb_write".to_string(),
            display_name: "DuckDB Writer".to_string(),
            description: "Write data to a DuckDB table".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".into(),
                description: "Data".into(),
            }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string", "default": "data.db" },
                    "table": { "type": "string" }
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
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        Ok(inputs)
    }
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
                    "timeout_secs": { "type": "number", "default": 30 }
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

        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                connection_path: ":memory:".to_string(),
                extensions: vec!["httpfs".to_string()],
                attach_sql: None,
                query: format!("SELECT * FROM read_json_auto('{}')", url),
                schema: Arc::new(DFSchema::empty()),
            }),
        })])
    }
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
        cfg::required_string(config, "path")?;
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
        cfg::required_string(config, "path")?;
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
        let _route = Self::resolve_route_metadata(config)?;
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
                            }),
                        },
                    )])
                }
            }
        }
    }
}

pub struct DbConnectorNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbDriver {
    DuckDBPostgres,
    DuckDBSqlite,
    SqlxAny,
    PostgresConnector,
    MySqlConnector,
    ClickHouseConnector,
}

impl DbConnectorNode {
    fn select_driver(url: &str) -> DbDriver {
        if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            DbDriver::DuckDBPostgres
        } else if url.starts_with("sqlite://") {
            DbDriver::DuckDBSqlite
        } else {
            DbDriver::SqlxAny
        }
    }

    async fn build_duckdb_plan(
        expanded_url: &str,
        query: &str,
        driver: DbDriver,
    ) -> Result<Vec<LogicalPlan>> {
        let ext = if driver == DbDriver::DuckDBPostgres {
            "postgres"
        } else {
            "sqlite"
        };

        let (path, attach_sql) = if driver == DbDriver::DuckDBPostgres {
            (
                ":memory:".to_string(),
                Some(format!("ATTACH '{}' AS ext_db (TYPE postgres);", expanded_url)),
            )
        } else {
            let db_path = expanded_url.trim_start_matches("sqlite://");
            (
                ":memory:".to_string(),
                Some(format!("ATTACH '{}' AS ext_db (TYPE sqlite);", db_path)),
            )
        };

        let batches = DuckDBHelper::query_arrow(&path, query, &[ext.to_string()], &attach_sql)
            .await?;
        if batches.is_empty() {
            return Err(anyhow!("Query returned no data for schema inference"));
        }
        let schema = batches[0].schema();
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                    connection_path: path,
                    extensions: vec![ext.to_string()],
                    attach_sql,
                    query: query.to_string(),
                    schema: Arc::new(df_schema),
                }),
            },
        )])
    }

    async fn build_sqlx_any_plan(
        expanded_url: &str,
        query: &str,
    ) -> Result<Vec<LogicalPlan>> {
        sqlx::any::install_default_drivers();
        let normalized_url = if expanded_url.starts_with("sqlite3://") {
            expanded_url.replace("sqlite3://", "sqlite://")
        } else {
            expanded_url.to_string()
        };
        let pool = sqlx::AnyPool::connect(&normalized_url)
            .await
            .map_err(|e| anyhow!("SQLx connection failed: {}", e))?;

        let query_obj = sqlx::query(query);
        let first_row = query_obj
            .fetch_one(&pool)
            .await
            .map_err(|e| anyhow!("SQLx query failed for schema inference: {}", e))?;

        let mut fields = Vec::new();
        for col in first_row.columns() {
            let arrow_type = crate::core::datasource::sqlx_type_to_arrow(col.type_info());
            fields.push(Field::new(col.name().to_string(), arrow_type, true));
        }

        let schema = Arc::new(Schema::new(fields));
        let df_schema = datafusion::common::DFSchema::try_from(schema)?;

        Ok(vec![LogicalPlan::Extension(
            datafusion::logical_expr::Extension {
                node: Arc::new(crate::core::datasource::SqlxScanLogical {
                    url: normalized_url,
                    query: query.to_string(),
                    schema: Arc::new(df_schema),
                    params: vec![],
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
                        "enum": ["auto", "postgres", "mysql", "clickhouse", "duckdb_postgres", "duckdb_sqlite", "sqlx_any"],
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
        if config.get("query").and_then(|v| v.as_str()).map_or(true, |s| s.is_empty())
            && config.get("table").and_then(|v| v.as_str()).map_or(true, |s| s.is_empty())
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
                "postgres" => DbDriver::PostgresConnector,
                "mysql" => DbDriver::MySqlConnector,
                "clickhouse" => DbDriver::ClickHouseConnector,
                "duckdb_postgres" => DbDriver::DuckDBPostgres,
                "duckdb_sqlite" => DbDriver::DuckDBSqlite,
                "sqlx_any" => DbDriver::SqlxAny,
                _ => DbDriver::SqlxAny,
            }
        };

        // Build the effective query
        let effective_query = match (query, table) {
            (Some(q), _) if !q.is_empty() => q,
            (_, Some(t)) if !t.is_empty() => format!("SELECT * FROM {}", t),
            _ => return Err(anyhow!("Either 'query' or 'table' must be provided")),
        };

        match resolved_driver {
            DbDriver::DuckDBPostgres | DbDriver::DuckDBSqlite => {
                // Keep existing DuckDB path
                Self::build_duckdb_plan(&expanded_url, &effective_query, resolved_driver).await
            }
            DbDriver::SqlxAny => {
                // Keep existing SQLx Any path
                Self::build_sqlx_any_plan(&expanded_url, &effective_query).await
            }
            DbDriver::PostgresConnector => {
                // Use new PostgresConnector for schema discovery
                let conn_config = ConnConfig {
                    url: expanded_url.clone(),
                    driver: "postgres".to_string(),
                    max_connections: 10,
                    timeout_secs: 30,
                };
                let connector = PostgresConnector::connect(conn_config).await?;
                let schema = connector.get_schema(&effective_query.trim_start_matches("SELECT * FROM ")).await?;
                let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);

                Ok(vec![LogicalPlan::Extension(
                    datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::SqlxScanLogical {
                            url: expanded_url,
                            query: effective_query,
                            schema: df_schema,
                            params: vec![],
                        }),
                    },
                )])
            }
            DbDriver::MySqlConnector => {
                // Use new MySqlConnector for schema discovery
                let conn_config = ConnConfig {
                    url: expanded_url.clone(),
                    driver: "mysql".to_string(),
                    max_connections: 10,
                    timeout_secs: 30,
                };
                let connector = MySqlConnector::connect(conn_config).await?;
                let schema = connector.get_schema(&effective_query.trim_start_matches("SELECT * FROM ")).await?;
                let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);

                Ok(vec![LogicalPlan::Extension(
                    datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::SqlxScanLogical {
                            url: expanded_url,
                            query: effective_query,
                            schema: df_schema,
                            params: vec![],
                        }),
                    },
                )])
            }
            DbDriver::ClickHouseConnector => {
                // Use new ClickHouseConnector for schema discovery
                let conn_config = ConnConfig {
                    url: expanded_url.clone(),
                    driver: "clickhouse".to_string(),
                    max_connections: 10,
                    timeout_secs: 30,
                };
                let connector = ClickHouseConnector::connect(conn_config).await?;
                let schema = connector.get_schema(&effective_query.trim_start_matches("SELECT * FROM ")).await?;
                let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);

                Ok(vec![LogicalPlan::Extension(
                    datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::SqlxScanLogical {
                            url: expanded_url,
                            query: effective_query,
                            schema: df_schema,
                            params: vec![],
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
                            if let Ok(conn) = Connection::open(&metadata_path) {
                                for batch in nodes_batches {
                                    let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                                    let labels = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                                    let properties = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
                                    for i in 0..batch.num_rows() {
                                        conn.execute(
                                            "INSERT OR REPLACE INTO __graph_nodes (id, label, properties) VALUES (?, ?, ?)",
                                            duckdb::params![ids.value(i), labels.value(i), properties.value(i)],
                                        ).ok();
                                    }
                                }
                                for batch in edges_batches {
                                    let src_ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                                    let dst_ids = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                                    let types = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
                                    let weights = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
                                    for i in 0..batch.num_rows() {
                                        conn.execute(
                                            "INSERT OR REPLACE INTO __graph_edges (source_id, target_id, type, weight) VALUES (?, ?, ?, ?)",
                                            duckdb::params![src_ids.value(i), dst_ids.value(i), types.value(i), weights.value(i)],
                                        ).ok();
                                    }
                                }
                            }
                        }).await.map_err(|e| DataFusionError::Execution(format!("Spawn blocking error: {}", e)))?;
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
