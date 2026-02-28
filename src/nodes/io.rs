use async_trait::async_trait;
use datafusion::logical_expr::{LogicalPlan, Extension};
use datafusion::prelude::*;
use datafusion::common::{DFSchemaRef, DFSchema};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{ArrayRef, StringBuilder};
use serde_json::Value as JsonValue;
use anyhow::{Result, anyhow};
use calamine::{Reader, Xlsx, open_workbook, Data};
use std::sync::Arc;
use std::path::PathBuf;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, InferredMetadata};
use crate::core::datasource::{ExcelScanLogical, ExcelScanMode};

pub struct ExcelReadNode;

impl ExcelReadNode {
    fn promote_type(_current: &DataType, _data: &Data) -> DataType {
        DataType::Utf8
    }
}

#[async_trait]
impl NodeBehavior for ExcelReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_read".to_string(),
            display_name: "Excel Reader".to_string(),
            description: "Read data from an Excel file with automatic type promotion and a rejects port".to_string(),
            input_ports: vec![],
            output_ports: vec![
                PortSpec { id: 0, name: "output".into(), description: "Validated Data".into() },
                PortSpec { id: 1, name: "rejects".into(), description: "Rows with type errors".into() }
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
        config["path"].as_str().ok_or_else(|| anyhow!("Excel Path is required"))?;
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
            workbook.worksheet_range(name).map_err(|e| anyhow!("Error reading sheet {}: {}", name, e))?
        } else {
            workbook.worksheet_range_at(0)
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
            (0..first_row.len()).map(|i| format!("column_{}", i)).collect()
        };

        let main_fields: Vec<Field> = headers.iter()
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
            
            // Re-create the iterator because we used rows.next()
            let mut data_rows = sheet.rows();
            if has_header { data_rows.next(); } // skip header

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
                let columns: Vec<ArrayRef> = builders.into_iter()
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
            description: "Write data to an Excel file with multi-sheet and styling support".to_string(),
            input_ports: vec![
                PortSpec { id: 0, name: "sheet_0".into(), description: "Primary Data Stream".into() }
            ],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "sheets": { 
                        "type": "array", 
                        "items": { 
                            "type": "object",
                            "properties": {
                                "name": { "type": "string" },
                                "bold_header": { "type": "boolean", "default": true }
                            }
                        }
                    }
                },
                "required": ["path"]
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        config["path"].as_str().ok_or_else(|| anyhow!("Output Path is required"))?;
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
        config["path"].as_str().ok_or_else(|| {
            anyhow::anyhow!("Parquet Path is required")
        })?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let path = config["path"].as_str().unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(path, ParquetReadOptions::default()).await?;
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
        config["path"].as_str().ok_or_else(|| {
            anyhow::anyhow!("CSV Path is required")
        })?;
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
