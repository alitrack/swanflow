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
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::prelude::*;
use duckdb::Connection;
use serde_json::Value as JsonValue;
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

            let mut count = 0;
            for row in data_rows.take(sample_limit) {
                count += 1;
                for (i, cell) in row.iter().enumerate() {
                    if i < builders.len() {
                        builders[i].append_value(cell.to_string());
                    }
                }
            }

            if count > 0 {
                let columns: Vec<ArrayRef> = builders
                    .into_iter()
                    .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                    .collect();
                let batch = RecordBatch::try_new(main_schema.clone(), columns)?;
                sample_batches.push(batch);
            }
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
        let metadata = self.infer_schema(vec![], config).await?;
        let path = PathBuf::from(config["path"].as_str().unwrap());
        let sheet = config["sheet"].as_str().map(|s| s.to_string());

        let main_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelScanLogical {
                path: path.clone(),
                sheet: sheet.clone(),
                mode: ExcelScanMode::Validated,
                schema: metadata.schemas[0].clone(),
            }),
        });

        let rejects_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelScanLogical {
                path,
                sheet,
                mode: ExcelScanMode::Rejects,
                schema: metadata.schemas[1].clone(),
            }),
        });

        Ok(vec![main_plan, rejects_plan])
    }
}

pub struct ExcelWriteNode;

#[async_trait]
impl NodeBehavior for ExcelWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_write".to_string(),
            display_name: "Excel Writer".to_string(),
            description: "Write data to an Excel file with multi-sheet, chart and pivot support".to_string(),
            input_ports: vec![
                PortSpec { id: 0, name: "sheet_0".into(), description: "Data for sheet 1".into() },
                PortSpec { id: 1, name: "sheet_1".into(), description: "Data for sheet 2 (Optional)".into() },
                PortSpec { id: 2, name: "sheet_2".into(), description: "Data for sheet 3 (Optional)".into() },
            ],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "sheets": { 
                        "type": "array", 
                        "items": { "type": "string" },
                        "default": ["Sheet1"]
                    },
                    "styles": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": { "type": "string" },
                                "format": {
                                    "type": "object",
                                    "properties": {
                                        "bold": { "type": "boolean" },
                                        "font_color": { "type": "string" },
                                        "bg_color": { "type": "string" },
                                        "num_format": { "type": "string" }
                                    }
                                }
                            },
                            "required": ["column", "format"]
                        }
                    },
                    "convert_to_table": { "type": "boolean", "default": false },
                    "conditional_formatting": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": { "type": "string" },
                                "type": { "type": "string", "enum": ["cell", "average", "duplicate", "unique"] },
                                "criteria": { "type": "string" },
                                "value": { "type": "number" },
                                "format": { "type": "object" }
                            }
                        }
                    }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["path"]
            .as_str()
            .ok_or_else(|| anyhow!("Output Path is required"))?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = PathBuf::from(config["path"].as_str().unwrap());
        let sheets: Vec<String> = if let Some(arr) = config["sheets"].as_array() {
            arr.iter().map(|v| v.as_str().unwrap_or("Sheet").to_string()).collect()
        } else {
            vec!["Sheet1".to_string()]
        };

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelSinkLogical {
                path,
                sheets,
                styles: config["styles"].clone(),
                convert_to_table: config["convert_to_table"].as_bool().unwrap_or(false),
                conditional_formatting: config["conditional_formatting"].clone(),
                inputs,
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
        let mut workbook: Xlsx<_> = open_workbook(&path)?;
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

    pub fn query_arrow(conn: &Connection, query: &str) -> Result<Vec<RecordBatch>> {
        // Use temporary parquet to bridge crate versions
        let tmp_file = std::env::temp_dir().join(format!("rustflow_query_{}.parquet", uuid::Uuid::new_v4()));
        let tmp_path = tmp_file.to_str().unwrap();

        let copy_sql = format!("COPY ({}) TO '{}' (FORMAT PARQUET)", query, tmp_path);
        conn.execute(&copy_sql, []).map_err(|e| anyhow!("DuckDB COPY failed: {}", e))?;
        
        let ctx = SessionContext::new();
        let batches = futures::executor::block_on(async {
            let df = ctx.read_parquet(tmp_path, ParquetReadOptions::default()).await?;
            df.collect().await
        }).map_err(|e| anyhow!("DataFusion read back failed: {}", e))?;
        
        std::fs::remove_file(tmp_file).ok();
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
        
        // Open DuckDB connection to validate query and infer schema
        let conn = Connection::open(path)?;
        
        // Execute query to get result schema (for validation)
        let mut stmt = conn.prepare(query)?;
        let column_count = stmt.column_count();
        
        let mut fields = Vec::new();
        for i in 0..column_count {
            let duckdb_type = stmt.column_type(i);
            // duckdb-rs might return its own re-exported arrow DataType
            // Convert it to our arrow crate's DataType by string representation if needed, 
            // or better, if it's the same major version, they might be compatible.
            // For now, let's use a more robust way to get the type if the enum match is failing.
            let arrow_type = match format!("{:?}", duckdb_type).as_str() {
                "Int64" | "BigInt" => DataType::Int64,
                "Float32" | "Float" => DataType::Float32,
                "Float64" | "Double" => DataType::Float64,
                "Utf8" | "Varchar" | "VarChar" => DataType::Utf8,
                "Boolean" | "Bool" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            fields.push(Field::new(stmt.column_name(i)?, arrow_type, true));
        }
        
        let schema = Arc::new(Schema::new(fields));
        let df_schema = datafusion::common::DFSchema::try_from(schema)?;

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

    async fn infer_schema(
        &self,
        _input_schemas: Vec<DFSchemaRef>,
        config: &JsonValue,
    ) -> Result<InferredMetadata> {
        let url = config["url"].as_str().unwrap();
        let client = reqwest::Client::new();
        let response = client.get(url).send().await?;
        let json: JsonValue = response.json().await?;
        let items = if json.is_array() {
            json.as_array().unwrap().clone()
        } else {
            vec![json]
        };
        if items.is_empty() {
            return Err(anyhow!("Empty JSON response"));
        }

        // We'll use a placeholder schema for now or try to infer it.
        // DataFusion SessionContext can sometimes infer from JSON.
        let schema = Arc::new(Schema::new(vec![Field::new("data", DataType::Utf8, true)]));
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

        Ok(InferredMetadata {
            schemas: vec![Arc::new(df_schema)],
            sample_data: None,
        })
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let metadata = self.infer_schema(vec![], config).await?;
        Ok(vec![LogicalPlan::EmptyRelation(
            datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: metadata.schemas[0].clone(),
            },
        )])
    }
}

pub struct ParquetReadNode;

#[async_trait]
impl NodeBehavior for ParquetReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.parquet".to_string(),
            display_name: "Parquet Reader".to_string(),
            description: "Read data from a Parquet file".to_string(),
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
            type_id: "core.io.csv".to_string(),
            display_name: "CSV Reader".to_string(),
            description: "Read data from a CSV file".to_string(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileFormat {
    Parquet,
    Csv,
    Json,
    Avro,
    Arrow,
}

impl UniversalReadNode {
    fn detect_format(path: &str) -> Option<FileFormat> {
        let path_lower = path.to_lowercase();
        if path_lower.ends_with(".parquet") {
            return Some(FileFormat::Parquet);
        }
        if path_lower.ends_with(".csv") {
            return Some(FileFormat::Csv);
        }
        if path_lower.ends_with(".json") || path_lower.ends_with(".ndjson") {
            return Some(FileFormat::Json);
        }
        if path_lower.ends_with(".avro") {
            return Some(FileFormat::Avro);
        }
        if path_lower.ends_with(".arrow") {
            return Some(FileFormat::Arrow);
        }
        
        // Basic magic number detection could be added here if needed
        None
    }

    fn discover_partitions(path: &str) -> Vec<(String, DataType)> {
        let mut partitions = std::collections::BTreeMap::new();
        Self::discover_partitions_recursive(std::path::Path::new(path), &mut partitions);
        partitions.into_iter().map(|(k, v)| (k, v)).collect()
    }

    fn discover_partitions_recursive(path: &std::path::Path, partitions: &mut std::collections::BTreeMap<String, DataType>) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if entry.path().is_dir() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.contains('=') {
                        let parts: Vec<&str> = name.splitn(2, '=').collect();
                        if parts.len() == 2 {
                            let key = parts[0].to_string();
                            let val = parts[1];
                            let dt = if val.parse::<i64>().is_ok() {
                                DataType::Int64
                            } else if val.parse::<f64>().is_ok() {
                                DataType::Float64
                            } else {
                                DataType::Utf8
                            };
                            partitions.entry(key).or_insert(dt);
                        }
                    }
                    // Recurse to find nested partitions
                    Self::discover_partitions_recursive(&entry.path(), partitions);
                }
            }
        }
    }
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

        let mut sess_config = SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(sess_config);
        let is_dir = std::path::Path::new(path).is_dir();

        match format {
            FileFormat::Parquet => {
                let mut opts = ParquetReadOptions::default();
                if is_dir {
                    let abs_path = std::fs::canonicalize(path)?;
                    let partitions = Self::discover_partitions(abs_path.to_str().unwrap());
                    if !partitions.is_empty() {
                        opts.table_partition_cols = partitions;
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
                let df = ctx.read_json(path, NdJsonReadOptions::default()).await?;
                Ok(InferredMetadata {
                    schemas: vec![Arc::new(df.schema().clone())],
                    sample_data: None,
                })
            }
            _ => Err(anyhow!("Schema inference not yet fully implemented for {:?}", format)),
        }
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let format_str = config["format"].as_str().unwrap_or("auto");
        let _recursive = config["recursive"].as_bool().unwrap_or(false);
        
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

        let mut sess_config = SessionConfig::new().with_target_partitions(1);
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
                    let abs_path = std::fs::canonicalize(path)?;
                    let glob_path = format!("file://{}/**/*.csv", abs_path.to_str().unwrap());
                    let table_url = ListingTableUrl::parse(glob_path)?;
                    
                    let has_header = config["has_header"].as_bool().unwrap_or(true);
                    let delimiter = config["delimiter"].as_str().unwrap_or(",").as_bytes()[0];
                    
                    let csv_format = datafusion::datasource::file_format::csv::CsvFormat::default()
                        .with_has_header(has_header)
                        .with_delimiter(delimiter);
                    
                    let listing_options = ListingOptions::new(Arc::new(csv_format))
                        .with_collect_stat(true)
                        .with_target_partitions(1);
                    
                    let config = ListingTableConfig::new(table_url)
                        .with_listing_options(listing_options);
                    
                    let config = config.infer(&ctx.state()).await?;
                    let table = Arc::new(ListingTable::try_new(config)?);
                    
                    let df = ctx.read_table(table)?;
                    Ok(vec![df.into_optimized_plan()?])
                }
                FileFormat::Json => {
                    let abs_path = std::fs::canonicalize(path)?;
                    let query = format!("SELECT * FROM read_json_auto('{}/**/*.json')", abs_path.to_str().unwrap());
                    
                    let conn = DuckDBHelper::open_connection(":memory:")?;
                    let batches = DuckDBHelper::query_arrow(&conn, &query)?;
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
                _ => Err(anyhow!("Directory scanning not yet supported for {:?}", format)),
            }
        } else {
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
                    let query = format!("SELECT * FROM read_json_auto('{}')", abs_path.to_str().unwrap());
                    
                    let conn = DuckDBHelper::open_connection(":memory:")?;
                    let batches = DuckDBHelper::query_arrow(&conn, &query)?;
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
                _ => Err(anyhow!("Format {:?} not supported in propose_plan", format)),
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
        let driver = Self::select_driver(url);
        
        let ctx = SessionContext::new();
        
        match driver {
            DbDriver::DuckDBPostgres | DbDriver::DuckDBSqlite => {
                let conn = DuckDBHelper::open_connection(":memory:")?;
                let ext = if driver == DbDriver::DuckDBPostgres { "postgres" } else { "sqlite" };
                DuckDBHelper::prepare_connection(&conn, &[ext])?;
                
                // For Postgres, we need to ATTACH. 
                // For SQLite, we can just ATTACH.
                if driver == DbDriver::DuckDBPostgres {
                    conn.execute(&format!("ATTACH '{}' AS ext_db (TYPE postgres);", url), [])?;
                } else {
                    // url is sqlite:///path/to/db.sqlite, DuckDB needs the path
                    let path = url.trim_start_matches("sqlite://");
                    conn.execute(&format!("ATTACH '{}' AS ext_db (TYPE sqlite);", path), [])?;
                }
                
                // Use query_arrow to get the schema reliably
                let batches = DuckDBHelper::query_arrow(&conn, query)?;
                if batches.is_empty() {
                    return Err(anyhow!("Query returned no data for schema inference"));
                }
                let schema = batches[0].schema();
                let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;
                
                let attach_sql = if driver == DbDriver::DuckDBPostgres {
                    Some(format!("ATTACH '{}' AS ext_db (TYPE postgres);", url))
                } else {
                    let path = url.trim_start_matches("sqlite://");
                    Some(format!("ATTACH '{}' AS ext_db (TYPE sqlite);", path))
                };

                Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(crate::core::datasource::DuckDBScanLogical {
                        connection_path: ":memory:".to_string(),
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
                let pool = futures::executor::block_on(sqlx::AnyPool::connect(url))
                    .map_err(|e| anyhow!("SQLx connection failed: {}", e))?;
                
                let query_obj = sqlx::query(query);
                let first_row = futures::executor::block_on(query_obj.fetch_one(&pool))
                    .map_err(|e| anyhow!("SQLx query failed for schema inference: {}", e))?;
                
                let mut fields = Vec::new();
                for col in first_row.columns() {
                    fields.push(Field::new(col.name().to_string(), DataType::Utf8, true));
                }
                
                let schema = Arc::new(Schema::new(fields));
                let df_schema = datafusion::common::DFSchema::try_from(schema)?;

                Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(crate::core::datasource::SqlxScanLogical {
                        url: url.to_string(),
                        query: query.to_string(),
                        schema: Arc::new(df_schema),
                    }),
                })])
            }
        }
    }
}
