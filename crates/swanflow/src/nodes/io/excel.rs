use anyhow::{anyhow, Result};
use async_trait::async_trait;
use calamine::{open_workbook, Data, Reader, Xlsx};
use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan};
use serde_json::json;
use serde_json::Value as JsonValue;
use std::path::PathBuf;
use std::sync::Arc;

use crate::core::datasource::{ExcelScanLogical, ExcelScanMode, ExcelSinkLogical};
use crate::nodes::config as cfg;
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
        let has_header = cfg::optional_bool(config, "has_header").unwrap_or(true);

        // Read the real header row (or first data row when has_header=false) so
        // downstream SQL can resolve column names at plan time. The previous
        // DFSchema::empty() placeholder made every downstream column reference
        // fail with "No field named ..." (and produced 0-column Arrow batches).
        let mut workbook: Xlsx<_> = open_workbook(&path)
            .map_err(|e| anyhow!("Cannot open Excel file {}: {}", path.display(), e))?;
        let range = if let Some(ref name) = sheet {
            workbook
                .worksheet_range(name)
                .map_err(|e| anyhow!("Error reading sheet {}: {}", name, e))?
        } else {
            workbook
                .worksheet_range_at(0)
                .ok_or_else(|| anyhow!("No sheets found"))?
                .map_err(|e| anyhow!("Error reading first sheet: {}", e))?
        };
        let header_row = range
            .rows()
            .next()
            .ok_or_else(|| anyhow!("Empty sheet"))?;
        let fields: Vec<Field> = if has_header {
            header_row
                .iter()
                .map(|c: &Data| Field::new(&c.to_string(), DataType::Utf8, true))
                .collect()
        } else {
            (0..header_row.len())
                .map(|i| Field::new(&format!("column_{}", i), DataType::Utf8, true))
                .collect()
        };
        let real_schema = Arc::new(DFSchema::try_from(Arc::new(Schema::new(fields)))?);

        let scan_node = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelScanLogical {
                path,
                sheet,
                mode,
                schema: real_schema,
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
                    "convert_to_table": { "type": "boolean", "default": true },
                    "styles": {
                        "type": "array",
                        "description": "Per-column static styles. Each item: {\"column\":\"name\",\"format\":{\"bold\":bool,\"font_color\":\"red|hex\",\"bg_color\":\"green|hex\",\"num_format\":\"0.00\"}}.",
                        "default": []
                    },
                    "conditional_formatting": {
                        "type": "array",
                        "description": "Declarative conditional-format rules applied to the data region. Each item: cell rule {\"column\":\"name\"|null,\"type\":\"cell\",\"operator\":\"equal|not_equal|greater_than|greater_than_equal|less_than|less_than_equal|between|not_between\",\"value\":num|\"min\":num,\"max\":num,\"format\":{\"font_color\":\"hex\",\"bg_color\":\"hex\"}} OR data bar {\"column\":\"name\"|null,\"type\":\"data_bar\",\"fill_color\":\"hex\",\"solid\":bool,\"bar_only\":bool,\"min\":num,\"max\":num}.",
                        "default": []
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
        if inputs.is_empty() {
            anyhow::bail!("ExcelWriteNode requires at least one input (connect an upstream node to its input port)");
        }
        let path = PathBuf::from(cfg::required_string(config, "path")?);
        let sheet = cfg::optional_string(config, "sheet").unwrap_or_else(|| "Sheet1".to_string());
        let convert_to_table = cfg::optional_bool(config, "convert_to_table").unwrap_or(true);
        let styles = config.get("styles").cloned().unwrap_or_else(|| json!([]));
        let conditional_formatting =
            config.get("conditional_formatting").cloned().unwrap_or_else(|| json!([]));

        let sink_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(ExcelSinkLogical {
                path,
                sheets: vec![sheet],
                inputs,
                styles,
                convert_to_table,
                conditional_formatting,
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
