use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use arrow::array::{ArrayRef, StringBuilder};
use serde_json::Value as JsonValue;
use anyhow::{Result, anyhow};
use calamine::{Reader, Xlsx, open_workbook, Data};
use std::sync::Arc;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec, InferredMetadata};

pub struct ExcelReadNode;

#[async_trait]
impl NodeBehavior for ExcelReadNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_read".to_string(),
            display_name: "Excel Reader".to_string(),
            description: "Read data from an Excel file (.xlsx, .xls, .xlsb, .ods)".to_string(),
            input_ports: vec![],
            output_ports: vec![
                PortSpec { id: 0, name: "output".into(), description: "Success Data".into() },
                PortSpec { id: 1, name: "rejects".into(), description: "Rejected Rows".into() }
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

        let fields: Vec<Field> = headers.into_iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        
        let schema = Arc::new(Schema::new(fields));
        let df_schema = datafusion::common::DFSchema::try_from(schema.clone())?;

        let mut sample_batches = Vec::new();
        if sample_limit > 0 {
            let mut builders: Vec<StringBuilder> = (0..schema.fields().len())
                .map(|_| StringBuilder::new())
                .collect();
            
            for row in rows.take(sample_limit) {
                for (i, cell) in row.iter().enumerate() {
                    if i < builders.len() {
                        builders[i].append_value(cell.to_string());
                    }
                }
            }
            
            let columns: Vec<ArrayRef> = builders.into_iter()
                .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                .collect();
            
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            sample_batches.push(batch);
        }

        Ok(InferredMetadata {
            schemas: vec![Arc::new(df_schema)],
            sample_data: Some(sample_batches),
        })
    }

    async fn propose_plan(
        &self,
        _inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        let metadata = self.infer_schema(vec![], config).await?;
        Ok(vec![LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: metadata.schemas[0].clone(),
        })])
    }
}

pub struct ExcelWriteNode;

#[async_trait]
impl NodeBehavior for ExcelWriteNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.io.excel_write".to_string(),
            display_name: "Excel Writer".to_string(),
            description: "Write data to an Excel file".to_string(),
            input_ports: vec![PortSpec { id: 0, name: "input".into(), description: "Data to write".into() }],
            output_ports: vec![],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "sheet": { "type": "string", "default": "Sheet1" }
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
