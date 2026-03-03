use anyhow::{anyhow, Result};
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, ListingTableConfigExt};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::prelude::*;
use duckdb::Connection;
use serde_json::Value as JsonValue;
use serde_json::json;
use sqlx::{Column, Row};
use std::path::PathBuf;
use std::sync::Arc;

use crate::core::datasource::{ExcelScanLogical, ExcelScanMode, ExcelSinkLogical};
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
        config["path"]
            .as_str()
            .ok_or_else(|| anyhow!("Excel Path is required"))?;
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let path = config["path"].as_str().unwrap();
        let sheet_name = config["sheet"].as_str();
        let has_header = config["has_header"].as_bool().unwrap_or(true);
        let sample_limit = config["sample_rows"].as_u64().unwrap_or(100) as usize;

        let mut workbook: Xlsx<_> = open_workbook(path)?;
        let sheet = if let Some(name) = sheet_name {
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
        let path = PathBuf::from(config["path"].as_str().unwrap());
        let sheet = config["sheet"].as_str().map(|s| s.to_string());
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
        config["path"]
            .as_str()
            .ok_or_else(|| anyhow!("Excel Path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(config["path"].as_str().unwrap());
        let sheet = config["sheet"].as_str().unwrap_or("Sheet1").to_string();
        let convert_to_table = config["convert_to_table"].as_bool().unwrap_or(true);

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelSinkLogical {
                path,
                sheets: vec![sheet],
                inputs,
                styles: json!({}),
                convert_to_table,
                conditional_formatting: json!({}),
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
        config["template_path"].as_str().ok_or_else(|| anyhow!("template_path is required"))?;
        config["output_path"].as_str().ok_or_else(|| anyhow!("output_path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let template_path = PathBuf::from(config["template_path"].as_str().unwrap());
        let output_path = PathBuf::from(config["output_path"].as_str().unwrap());
        let sheet = config["sheet"].as_str().unwrap_or("Sheet1").to_string();
        let table_start = config["table_start"].as_str().map(|s| s.to_string());
        
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
            output_ports: vec![PortSpec { id: 0, name: "output".into(), description: "Sheet Names".into() }],
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
        config["path"].as_str().ok_or_else(|| anyhow!("path is required"))?;
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        _config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let schema = Arc::new(Schema::new(vec![Field::new("sheet_name", DataType::Utf8, false)]));
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
        let path = config["path"].as_str().unwrap().to_string();
        let workbook: Xlsx<_> = open_workbook(&path)?;
        let names = workbook.sheet_names();
        
        let schema = Arc::new(Schema::new(vec![Field::new("sheet_name", DataType::Utf8, false)]));
        let mut values = Vec::new();
        
        for name in names {
            values.push(vec![datafusion::logical_expr::Expr::Literal(datafusion::scalar::ScalarValue::Utf8(Some(name)), None)]);
        }
        
        let df_schema = Arc::new(datafusion::common::DFSchema::try_from(schema.clone())?);
        
        Ok(vec![LogicalPlan::Values(datafusion::logical_expr::Values {
            schema: df_schema,
            values,
        })])
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
            input_ports: vec![
                PortSpec { id: 0, name: "data".into(), description: "Data for the report".into() },
            ],
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
        config["path"].as_str().ok_or_else(|| anyhow!("path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(config["path"].as_str().unwrap());
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
        Self::prepare_connection(&conn, &extensions.iter().map(|s| s.as_str()).collect::<Vec<_>>())?;
        
        if let Some(sql) = attach_sql {
            conn.execute_batch(sql).map_err(|e| anyhow!("Attach SQL failed: {}", e))?;
        }

        // Use a temporary parquet file to bridge DuckDB and DataFusion
        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join(format!("duckdb_bridge_{}.parquet", uuid::Uuid::new_v4()));
        let tmp_path = tmp_file.to_str().unwrap();

        let copy_sql = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query, tmp_path);
        conn.execute(&copy_sql, []).map_err(|e| anyhow!("DuckDB COPY failed: {}", e))?;
        
        let ctx = SessionContext::new();
        let batches = {
            let df = ctx.read_parquet(tmp_path, ParquetReadOptions::default()).await?;
            df.collect().await
        }.map_err(|e| anyhow!("DataFusion read back failed: {}", e))?;
        
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
        config["query"]
            .as_str()
            .ok_or_else(|| anyhow!("Query is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap_or(":memory:");
        let query = config["query"].as_str().unwrap_or("SELECT 1");
        
        // Use query_arrow to get the schema reliably without holding Connection across await
        let batches = DuckDBHelper::query_arrow(path, query, &vec![], &None).await?;
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
        config["table"]
            .as_str()
            .ok_or_else(|| anyhow!("Table name is required"))?;
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
                    "method": { "type": "string", "default": "GET" }
                },
                "required": ["url"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["url"]
            .as_str()
            .ok_or_else(|| anyhow!("URL is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let url = config["url"].as_str().unwrap().to_string();
        let _method = config["method"].as_str().unwrap_or("GET").to_string();

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
        config["path"]
            .as_str()
            .ok_or_else(|| anyhow!("Parquet Path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(path, ParquetReadOptions::default())
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
        config["path"]
            .as_str()
            .ok_or_else(|| anyhow!("CSV Path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_csv(path, CsvReadOptions::new()).await?;
        Ok(vec![df.into_optimized_plan()?])
    }
}

pub struct UniversalReadNode;

impl UniversalReadNode {
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
                    "format": { "type": "string", "default": "auto", "enum": ["auto", "parquet", "csv", "json", "avro", "arrow"] },
                    "recursive": { "type": "boolean", "default": false },
                    "has_header": { "type": "boolean", "default": true },
                    "delimiter": { "type": "string", "default": "," },
                    "multi_line": { "type": "boolean", "default": false }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["path"].as_str().ok_or_else(|| anyhow!("Path is required"))?;
        Ok(())
    }

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let path = config["path"].as_str().unwrap();
        let format_str = config["format"].as_str().unwrap_or("auto");
        
        let format = if format_str == "auto" {
            Self::detect_format(path).ok_or_else(|| anyhow!("Could not detect format for {}", path))?
        } else {
            match format_str {
                "parquet" => FileFormat::Parquet,
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                _ => return Err(anyhow!("Unsupported format override: {}", format_str)),
            }
        };

        let sess_config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(sess_config);
        let is_dir = std::path::Path::new(path).is_dir();

        match format {
            FileFormat::Parquet => {
                let mut opts = ParquetReadOptions::default();
                if is_dir {
                    let abs_path = std::fs::canonicalize(path)?;
                    let partitions = Self::discover_partitions(abs_path.to_str().unwrap());
                    if !partitions.is_empty() {
                        opts.table_partition_cols = partitions.into_iter()
                            .map(|p| (p, DataType::Utf8))
                            .collect();
                    }
                }
                let df = ctx.read_parquet(path, opts).await?;
                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df.schema().clone())],
                    sample_data: None,
                })
            }
            FileFormat::Csv => {
                let has_header = config["has_header"].as_bool().unwrap_or(true);
                let delimiter = config["delimiter"].as_str().unwrap_or(",").as_bytes()[0];
                let opts = CsvReadOptions::new().has_header(has_header).delimiter(delimiter);
                let df = ctx.read_csv(path, opts).await?;
                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df.schema().clone())],
                    sample_data: None,
                })
            }
            FileFormat::Json => {
                let abs_path = std::fs::canonicalize(path)?;
                let query = format!("SELECT * FROM read_json_auto('{}')", abs_path.to_str().unwrap());
                
                let batches = DuckDBHelper::query_arrow(":memory:", &query, &vec![], &None).await?;
                if batches.is_empty() { return Err(anyhow!("JSON file is empty or invalid: {}", path)); }
                
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
        let path = config["path"].as_str().unwrap();
        let format_str = config["format"].as_str().unwrap_or("auto");
        let recursive = config["recursive"].as_bool().unwrap_or(false);
        
        let format = if format_str == "auto" {
            Self::detect_format(path).ok_or_else(|| anyhow!("Could not detect format for {}", path))?
        } else {
            match format_str {
                "parquet" => FileFormat::Parquet,
                "csv" => FileFormat::Csv,
                "json" => FileFormat::Json,
                _ => return Err(anyhow!("Unsupported format override: {}", format_str)),
            }
        };

        let sess_config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(sess_config);

        // Handle directory vs file
        let is_dir = std::path::Path::new(path).is_dir();
        
        if is_dir {
            match format {
                FileFormat::Parquet => {
                    let abs_path = std::fs::canonicalize(path)?;
                    let table_url = ListingTableUrl::parse(abs_path.to_str().unwrap())?;
                    
                    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                        .with_collect_stat(true)
                        .with_file_extension(".parquet")
                        .with_target_partitions(1);
                    
                    let config = ListingTableConfig::new(table_url)
                        .with_listing_options(listing_options);
                    
                    let config = config.infer(&ctx.state()).await?;
                    let table = Arc::new(ListingTable::try_new(config)?);
                    
                    let df = ctx.read_table(table)?;
                    let df = df.repartition(datafusion::logical_expr::Partitioning::RoundRobinBatch(1))?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Csv => {
                    let has_header = config["has_header"].as_bool().unwrap_or(true);
                    let delimiter = config["delimiter"].as_str().unwrap_or(",").as_bytes()[0];
                    let opts = CsvReadOptions::new().has_header(has_header).delimiter(delimiter);
                    let df = ctx.read_csv(path, opts).await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Json => {
                    let abs_path = std::fs::canonicalize(path)?;
                    let query = if recursive {
                        format!("SELECT * FROM read_json_auto('{}/**/*.json')", abs_path.to_str().unwrap())
                    } else {
                        format!("SELECT * FROM read_json_auto('{}/*.json')", abs_path.to_str().unwrap())
                    };
                    
                    let batches = DuckDBHelper::query_arrow(":memory:", &query, &vec![], &None).await?;
                    if batches.is_empty() { return Err(anyhow!("No JSON data found in {}", path)); }
                    
                    let schema = batches[0].schema();
                    let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

                    Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                            connection_path: ":memory:".to_string(),
                            extensions: vec![],
                            attach_sql: None,
                            query,
                            schema: Arc::new(df_schema),
                        }),
                    })])
                }
            }
        } else {
            // Single file path
            match format {
                FileFormat::Parquet => {
                    let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Csv => {
                    let has_header = config["has_header"].as_bool().unwrap_or(true);
                    let delimiter = config["delimiter"].as_str().unwrap_or(",").as_bytes()[0];
                    let opts = CsvReadOptions::new().has_header(has_header).delimiter(delimiter);
                    let df = ctx.read_csv(path, opts).await?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Json => {
                    let abs_path = std::fs::canonicalize(path)?;
                    let query = format!("SELECT * FROM read_json_auto('{}')", abs_path.to_str().unwrap());
                    
                    let batches = DuckDBHelper::query_arrow(":memory:", &query, &vec![], &None).await?;
                    if batches.is_empty() { return Err(anyhow!("JSON file is empty or invalid: {}", path)); }
                    
                    let schema = batches[0].schema();
                    let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

                    Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                        node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                            connection_path: ":memory:".to_string(),
                            extensions: vec![],
                            attach_sql: None,
                            query,
                            schema: Arc::new(df_schema),
                        }),
                    })])
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
}

#[async_trait]
impl NodeBehavior for DbConnectorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.db_connector".to_string(),
            display_name: "Database Connector".to_string(),
            description: "Connect to external databases using DuckDB scanners or SQLx.".to_string(),
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
                    "query": { "type": "string" }
                },
                "required": ["url", "query"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["url"].as_str().ok_or_else(|| anyhow!("Database URL is required"))?;
        config["query"].as_str().ok_or_else(|| anyhow!("SQL query is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let url = config["url"].as_str().unwrap();
        let query = config["query"].as_str().unwrap();
        
        // ENV expansion for security
        let expanded_url = if url.contains("${") {
            let mut result = url.to_string();
            let re = regex::Regex::new(r"\$\{([A-Z0-9_]+)\}").unwrap();
            for cap in re.captures_iter(url) {
                let var_name = &cap[1];
                if let Ok(val) = std::env::var(var_name) {
                    result = result.replace(&cap[0], &val);
                }
            }
            result
        } else {
            url.to_string()
        };

        let driver = Self::select_driver(&expanded_url);
        
        match driver {
            DbDriver::DuckDBPostgres | DbDriver::DuckDBSqlite => {
                let ext = if driver == DbDriver::DuckDBPostgres { "postgres" } else { "sqlite" };
                
                let (path, attach_sql) = if driver == DbDriver::DuckDBPostgres {
                    (":memory:", Some(format!("ATTACH '{}' AS ext_db (TYPE postgres);", expanded_url)))
                } else {
                    let db_path = expanded_url.trim_start_matches("sqlite://");
                    (":memory:", Some(format!("ATTACH '{}' AS ext_db (TYPE sqlite);", db_path)))
                };
                
                // Use query_arrow to get the schema reliably without holding Connection
                let batches = DuckDBHelper::query_arrow(path, query, &vec![ext.to_string()], &attach_sql).await?;
                if batches.is_empty() {
                    return Err(anyhow!("Query returned no data for schema inference"));
                }
                let schema = batches[0].schema();
                let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;
                
                Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                        connection_path: path.to_string(),
                        extensions: vec![ext.to_string()],
                        attach_sql,
                        query: query.to_string(),
                        schema: Arc::new(df_schema),
                    }),
                })])
            }
            DbDriver::SqlxAny => {
                // SQLx path: dynamic schema discovery
                sqlx::any::install_default_drivers();
                let normalized_url = if expanded_url.starts_with("sqlite3://") {
                    expanded_url.replace("sqlite3://", "sqlite://")
                } else {
                    expanded_url.to_string()
                };
                let pool = sqlx::AnyPool::connect(&normalized_url).await
                    .map_err(|e| anyhow!("SQLx connection failed: {}", e))?;
                
                let query_obj = sqlx::query(query);
                let first_row = query_obj.fetch_one(&pool).await
                    .map_err(|e| anyhow!("SQLx query failed for schema inference: {}", e))?;
                
                let mut fields = Vec::new();
                for col in first_row.columns() {
                    let arrow_type = crate::core::datasource::sqlx_type_to_arrow(col.type_info());
                    fields.push(Field::new(col.name().to_string(), arrow_type, true));
                }
                
                let schema = Arc::new(Schema::new(fields));
                let df_schema = datafusion::common::DFSchema::try_from(schema)?;

                Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(crate::core::datasource::SqlxScanLogical {
                        url: normalized_url,
                        query: query.to_string(),
                        schema: Arc::new(df_schema),
                        params: vec![], // TODO: Support parameters in DbConnectorNode if needed
                    }),
                })])
            }
        }
    }
}
