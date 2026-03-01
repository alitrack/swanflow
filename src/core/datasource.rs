use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use calamine::{open_workbook, Data, Reader, Xlsx};
use serde_json::Value as JsonValue;
use datafusion::common::DFSchemaRef;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::Stream;
use futures::StreamExt;
use rust_xlsxwriter::{Chart, ChartType, Workbook, Format, Color};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcelTemplateSinkLogical {
    pub template_path: PathBuf,
    pub output_path: PathBuf,
    pub sheet: String,
    pub markers: HashMap<String, String>,
    pub table_start: Option<String>,
    pub inputs: Vec<LogicalPlan>,
}

impl std::hash::Hash for ExcelTemplateSinkLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.template_path.hash(state);
        self.output_path.hash(state);
        self.sheet.hash(state);
        // HashMap doesn't implement Hash, so we hash sorted entries
        let mut markers: Vec<_> = self.markers.iter().collect();
        markers.sort_by_key(|(k, _)| *k);
        for (k, v) in markers {
            k.hash(state);
            v.hash(state);
        }
        self.table_start.hash(state);
        // LogicalPlans are generally not hashed directly, 
        // they are usually hashed by their IDs or structure in DataFusion
        // but for UserDefinedLogicalNode we often skip them or use a placeholder
    }
}

impl PartialOrd for ExcelTemplateSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExcelTemplateSinkLogical {
    fn name(&self) -> &str {
        "ExcelTemplateSink"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.inputs[0].schema()
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelTemplateSink(template={:?}, output={:?})", self.template_path, self.output_path)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            template_path: self.template_path.clone(),
            output_path: self.output_path.clone(),
            sheet: self.sheet.clone(),
            markers: self.markers.clone(),
            table_start: self.table_start.clone(),
            inputs,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
pub enum ExcelScanMode {
    Validated,
    Rejects,
}

/// Logical node for Excel scanning.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ExcelScanLogical {
    pub path: PathBuf,
    pub sheet: Option<String>,
    pub mode: ExcelScanMode,
    pub schema: DFSchemaRef,
}

impl PartialOrd for ExcelScanLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExcelScanLogical {
    fn name(&self) -> &str {
        "ExcelScan"
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
        write!(f, "ExcelScan(path={:?}, mode={:?})", self.path, self.mode)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(self.clone())
    }
}

/// Physical execution plan for scanning an Excel file.
#[derive(Debug)]
pub struct ExcelScanExec {
    pub path: PathBuf,
    pub sheet: Option<String>,
    pub mode: ExcelScanMode,
    pub project_schema: SchemaRef,
    cache: PlanProperties,
}

impl ExcelScanExec {
    pub fn new(
        path: PathBuf,
        sheet: Option<String>,
        mode: ExcelScanMode,
        project_schema: SchemaRef,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(project_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            path,
            sheet,
            mode,
            project_schema,
            cache,
        }
    }
}

impl DisplayAs for ExcelScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ExcelScanExec(path={:?}, mode={:?})",
            self.path, self.mode
        )
    }
}

impl ExecutionPlan for ExcelScanExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ExcelScanExec"
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
        let mut workbook: Xlsx<_> = open_workbook(&self.path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        let range = if let Some(ref name) = self.sheet {
            workbook
                .worksheet_range(name)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        } else {
            workbook
                .worksheet_range_at(0)
                .ok_or_else(|| datafusion::error::DataFusionError::External("No sheets".into()))?
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        };

        Ok(Box::pin(ExcelStream {
            range,
            mode: self.mode.clone(),
            schema: self.project_schema.clone(),
            row_idx: 0,
            finished: false,
        }))
    }
}

struct ExcelStream {
    range: calamine::Range<Data>,
    mode: ExcelScanMode,
    schema: SchemaRef,
    row_idx: usize,
    finished: bool,
}

impl RecordBatchStream for ExcelStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ExcelStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.finished {
            return std::task::Poll::Ready(None);
        }

        let mut row_count = 0;
        let start_row = self.row_idx + 1;
        let mut current_idx = self.row_idx;

        if self.mode == ExcelScanMode::Validated {
            let mut builders: Vec<StringBuilder> = (0..self.schema.fields().len())
                .map(|_| StringBuilder::new())
                .collect();
            for row in self.range.rows().skip(start_row).take(1000) {
                current_idx += 1;
                row_count += 1;
                for (i, cell) in row.iter().enumerate() {
                    if i < builders.len() {
                        builders[i].append_value(cell.to_string());
                    }
                }
            }
            self.row_idx = current_idx;
            if row_count == 0 {
                self.finished = true;
                return std::task::Poll::Ready(None);
            }
            let columns: Vec<ArrayRef> = builders
                .into_iter()
                .map(|mut b| Arc::new(b.finish()) as ArrayRef)
                .collect();
            let batch = RecordBatch::try_new(self.schema.clone(), columns)
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
            std::task::Poll::Ready(Some(Ok(batch)))
        } else {
            self.finished = true;
            std::task::Poll::Ready(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcelReportSinkLogical {
    pub path: PathBuf,
    pub charts: JsonValue,
    pub inputs: Vec<LogicalPlan>,
}

impl std::hash::Hash for ExcelReportSinkLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.charts.to_string().hash(state);
    }
}

impl PartialOrd for ExcelReportSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExcelReportSinkLogical {
    fn name(&self) -> &str {
        "ExcelReportSink"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.inputs[0].schema()
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelReportSink(path={:?})", self.path)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            path: self.path.clone(),
            charts: self.charts.clone(),
            inputs,
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExcelSinkLogical {
    pub path: PathBuf,
    pub sheets: Vec<String>,
    pub styles: JsonValue,
    pub convert_to_table: bool,
    pub conditional_formatting: JsonValue,
    pub inputs: Vec<LogicalPlan>,
}

impl std::hash::Hash for ExcelSinkLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
        self.sheets.hash(state);
        self.styles.to_string().hash(state);
        self.convert_to_table.hash(state);
        self.conditional_formatting.to_string().hash(state);
    }
}

impl PartialOrd for ExcelSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for ExcelSinkLogical {
    fn name(&self) -> &str {
        "ExcelSink"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }
    fn schema(&self) -> &DFSchemaRef {
        self.inputs[0].schema()
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelSink(path={:?}, sheets={:?})", self.path, self.sheets)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            path: self.path.clone(),
            sheets: self.sheets.clone(),
            styles: self.styles.clone(),
            convert_to_table: self.convert_to_table,
            conditional_formatting: self.conditional_formatting.clone(),
            inputs,
        })
    }
}

/// Physical execution plan for writing to a professional Excel file.
#[derive(Debug)]
pub struct ExcelSinkExec {
    pub path: PathBuf,
    pub sheets: Vec<String>,
    pub styles: JsonValue,
    pub convert_to_table: bool,
    pub conditional_formatting: JsonValue,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    cache: PlanProperties,
}

impl ExcelSinkExec {
    pub fn new(
        path: PathBuf,
        sheets: Vec<String>,
        styles: JsonValue,
        convert_to_table: bool,
        conditional_formatting: JsonValue,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(inputs[0].schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            path,
            sheets,
            styles,
            convert_to_table,
            conditional_formatting,
            inputs,
            cache,
        }
    }
}

impl DisplayAs for ExcelSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelSinkExec(path={:?})", self.path)
    }
}

impl ExecutionPlan for ExcelSinkExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ExcelSinkExec"
    }
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
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
            self.path.clone(),
            self.sheets.clone(),
            self.styles.clone(),
            self.convert_to_table,
            self.conditional_formatting.clone(),
            children,
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
        
        Ok(Box::pin(ExcelWritingStream {
            inputs: streams,
            path: self.path.clone(),
            sheets: self.sheets.clone(),
            styles: self.styles.clone(),
            convert_to_table: self.convert_to_table,
            conditional_formatting: self.conditional_formatting.clone(),
            all_batches: vec![Vec::new(); self.inputs.len()],
            finished_mask: vec![false; self.inputs.len()],
        }))
    }
}

struct ExcelWritingStream {
    inputs: Vec<SendableRecordBatchStream>,
    path: PathBuf,
    sheets: Vec<String>,
    styles: JsonValue,
    convert_to_table: bool,
    conditional_formatting: JsonValue,
    /// Stores batches per input (sheet)
    all_batches: Vec<Vec<RecordBatch>>,
    finished_mask: Vec<bool>,
}

impl RecordBatchStream for ExcelWritingStream {
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
    }
}

impl Stream for ExcelWritingStream {
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
                    self.all_batches[i].push(batch.clone());
                    if i == 0 {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.finished_mask[i] = true;
                }
                Poll::Pending => any_pending = true,
            }
        }

        if any_pending {
            Poll::Pending
        } else if self.finished_mask.iter().all(|&f| f) {
            self.finalize_workbook()?;
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl ExcelWritingStream {
    fn json_to_format(format_json: &JsonValue) -> Format {
        let mut format = Format::new();
        if format_json["bold"].as_bool().unwrap_or(false) {
            format = format.set_bold();
        }
        if let Some(color_str) = format_json["font_color"].as_str() {
            let color = match color_str.to_lowercase().as_str() {
                "red" => Color::Red,
                "blue" => Color::Blue,
                "green" => Color::Green,
                "white" => Color::White,
                "black" => Color::Black,
                _ => Color::Black,
            };
            format = format.set_font_color(color);
        }
        if let Some(color_str) = format_json["bg_color"].as_str() {
            let color = match color_str.to_lowercase().as_str() {
                "red" => Color::Red,
                "blue" => Color::Blue,
                "green" => Color::Green,
                "yellow" => Color::Yellow,
                _ => Color::White,
            };
            format = format.set_background_color(color);
        }
        if let Some(num_format) = format_json["num_format"].as_str() {
            format = format.set_num_format(num_format);
        }
        format
    }

    fn finalize_workbook(&mut self) -> Result<()> {
        if self.all_batches.is_empty() {
            return Ok(());
        }

        let mut workbook = Workbook::new();
        
        // Define some professional styles
        let header_format = Format::new()
            .set_bold()
            .set_font_color(Color::White)
            .set_background_color(Color::RGB(0x4F81BD))
            .set_border(rust_xlsxwriter::FormatBorder::Thin);

        // Pre-parse styles
        let mut col_styles = HashMap::new();
        if let Some(styles_arr) = self.styles.as_array() {
            for s in styles_arr {
                if let (Some(col), Some(fmt_json)) = (s["column"].as_str(), Some(&s["format"])) {
                    col_styles.insert(col.to_string(), Self::json_to_format(fmt_json));
                }
            }
        }

        for (i, sheet_batches) in self.all_batches.iter().enumerate() {
            if sheet_batches.is_empty() {
                continue;
            }

            let sheet_name = self.sheets.get(i).cloned()
                .unwrap_or_else(|| format!("Sheet{}", i + 1));
            
            let worksheet = workbook.add_worksheet();
            if let Err(e) = worksheet.set_name(&sheet_name) {
                tracing::warn!("Failed to set sheet name: {}", e);
            }

            let mut row_offset = 0;
            let schema = sheet_batches[0].schema();
            
            // Map column indices to formats
            let mut col_formats = Vec::new();
            for field in schema.fields() {
                col_formats.push(col_styles.get(field.name()).cloned());
            }

            for (col_idx, field) in schema.fields().iter().enumerate() {
                if let Err(e) = worksheet.write_with_format(0, col_idx as u16, field.name(), &header_format) {
                    tracing::warn!("Failed to write header cell: {}", e);
                }
            }
            row_offset += 1;

            for batch in sheet_batches {
                for row_idx in 0..batch.num_rows() {
                    for col_idx in 0..batch.num_columns() {
                        let array = batch.column(col_idx);
                        let cell_format = col_formats[col_idx].as_ref();
                        
                        let write_res = match array.data_type() {
                            DataType::Int64 => {
                                let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                if a.is_null(row_idx) {
                                    Ok(())
                                } else {
                                    let val = a.value(row_idx);
                                    if let Some(fmt) = cell_format {
                                        worksheet.write_with_format(row_offset as u32, col_idx as u16, val as f64, fmt).map(|_| ())
                                    } else {
                                        worksheet.write(row_offset as u32, col_idx as u16, val as f64).map(|_| ())
                                    }
                                }
                            }
                            DataType::Float64 => {
                                let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                if a.is_null(row_idx) {
                                    Ok(())
                                } else {
                                    let val = a.value(row_idx);
                                    if let Some(fmt) = cell_format {
                                        worksheet.write_with_format(row_offset as u32, col_idx as u16, val, fmt).map(|_| ())
                                    } else {
                                        worksheet.write(row_offset as u32, col_idx as u16, val).map(|_| ())
                                    }
                                }
                            }
                            _ => {
                                let val = array.as_any().downcast_ref::<StringArray>()
                                    .map(|a| a.value(row_idx).to_string())
                                    .unwrap_or_else(|| "".to_string());
                                if let Some(fmt) = cell_format {
                                    worksheet.write_with_format(row_offset as u32, col_idx as u16, val, fmt).map(|_| ())
                                } else {
                                    worksheet.write(row_offset as u32, col_idx as u16, val).map(|_| ())
                                }
                            }
                        };
                        if let Err(e) = write_res {
                            tracing::warn!("Failed to write cell: {}", e);
                        }
                    }
                    row_offset += 1;
                }
            }

            // Task 2.3: Implement Excel Table (ListObject) support
            if self.convert_to_table && row_offset > 1 {
                let mut table = rust_xlsxwriter::Table::new();
                if let Err(e) = worksheet.add_table(0, 0, (row_offset - 1) as u32, (schema.fields().len() - 1) as u16, &table) {
                    tracing::warn!("Failed to add Excel table: {}", e);
                }
            }

            // Only add default chart if NOT a table and has enough data
            if i == 0 && schema.fields().len() >= 2 && row_offset > 2 && !self.convert_to_table {
                let mut chart = Chart::new(ChartType::Column);
                let sheet_name_str = sheet_name.as_str();
                let row_end = (row_offset - 1) as u32;
                chart.add_series()
                    .set_categories((sheet_name_str, 1, 0, row_end, 0))
                    .set_values((sheet_name_str, 1, 1, row_end, 1));
                
                let title = format!("{} Analysis", sheet_name_str);
                chart.title().set_name(&title);
                // Chart creation is non-critical, log at debug level
                if let Err(e) = worksheet.insert_chart(1, (schema.fields().len() + 1) as u16, &chart) {
                    tracing::debug!("Failed to insert chart (non-critical): {}", e);
                }
            }
        }

        workbook.save(&self.path).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}

/// Physical execution plan for template-based Excel writing.
#[derive(Debug)]
pub struct ExcelTemplateExec {
    pub template_path: PathBuf,
    pub output_path: PathBuf,
    pub sheet: String,
    pub markers: HashMap<String, String>,
    pub table_start: Option<String>,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    cache: PlanProperties,
}

impl ExcelTemplateExec {
    pub fn new(
        template_path: PathBuf,
        output_path: PathBuf,
        sheet: String,
        markers: HashMap<String, String>,
        table_start: Option<String>,
        inputs: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(inputs[0].schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            template_path,
            output_path,
            sheet,
            markers,
            table_start,
            inputs,
            cache,
        }
    }
}

impl DisplayAs for ExcelTemplateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelTemplateExec(template={:?})", self.template_path)
    }
}

impl ExecutionPlan for ExcelTemplateExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ExcelTemplateExec"
    }
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
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
            self.template_path.clone(),
            self.output_path.clone(),
            self.sheet.clone(),
            self.markers.clone(),
            self.table_start.clone(),
            children,
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
        
        Ok(Box::pin(ExcelTemplateWritingStream {
            inputs: streams,
            template_path: self.template_path.clone(),
            output_path: self.output_path.clone(),
            sheet: self.sheet.clone(),
            markers: self.markers.clone(),
            table_start: self.table_start.clone(),
            all_batches: vec![Vec::new(); self.inputs.len()],
            finished_mask: vec![false; self.inputs.len()],
        }))
    }
}

struct ExcelTemplateWritingStream {
    inputs: Vec<SendableRecordBatchStream>,
    template_path: PathBuf,
    output_path: PathBuf,
    sheet: String,
    markers: HashMap<String, String>,
    table_start: Option<String>,
    all_batches: Vec<Vec<RecordBatch>>,
    finished_mask: Vec<bool>,
}

impl RecordBatchStream for ExcelTemplateWritingStream {
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
    }
}

impl Stream for ExcelTemplateWritingStream {
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
                    self.all_batches[i].push(batch.clone());
                    if i == 0 {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.finished_mask[i] = true;
                }
                Poll::Pending => any_pending = true,
            }
        }

        if any_pending {
            Poll::Pending
        } else if self.finished_mask.iter().all(|&f| f) {
            self.finalize_workbook()?;
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl ExcelTemplateWritingStream {
    fn finalize_workbook(&mut self) -> Result<()> {
        if self.all_batches.is_empty() {
            return Ok(());
        }

        // 1. Read template using calamine to find markers and current values
        let mut workbook_reader: Xlsx<_> = open_workbook(&self.template_path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        
        let range = workbook_reader.worksheet_range(&self.sheet)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Map markers found in template to their (row, col) coordinates
        let mut marker_coords = HashMap::new();
        for (r, row) in range.rows().enumerate() {
            for (c, cell) in row.iter().enumerate() {
                if let Data::String(s) = cell {
                    for (field, marker) in &self.markers {
                        if s.contains(marker) {
                            marker_coords.insert(field.clone(), (r as u32, c as u16));
                        }
                    }
                }
            }
        }

        // 2. Create new workbook using rust_xlsxwriter
        let mut workbook_writer = Workbook::new();
        let worksheet = workbook_writer.add_worksheet();
        worksheet.set_name(&self.sheet).ok();

        // Copy template content (simplified)
        for (r, row) in range.rows().enumerate() {
            for (c, cell) in row.iter().enumerate() {
                match cell {
                    Data::String(s) => { worksheet.write(r as u32, c as u16, s).ok(); }
                    Data::Float(f) => { worksheet.write(r as u32, c as u16, *f).ok(); }
                    Data::Int(i) => { worksheet.write(r as u32, c as u16, *i as f64).ok(); }
                    Data::Bool(b) => { worksheet.write(r as u32, c as u16, *b).ok(); }
                    _ => {}
                }
            }
        }

        // 3. Inject single values from the FIRST row of the FIRST input batch (standard for markers)
        if let Some(first_batch) = self.all_batches[0].first() {
            if first_batch.num_rows() > 0 {
                for (field, (r, c)) in &marker_coords {
                    if let Some(col_idx) = first_batch.schema().index_of(field).ok() {
                        let array = first_batch.column(col_idx);
                        // Simplified: convert type to string for cell injection
                        let val = if array.is_null(0) {
                            "".to_string()
                        } else {
                            // Basic type handling
                            match array.data_type() {
                                DataType::Int64 => {
                                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                    a.value(0).to_string()
                                }
                                DataType::Float64 => {
                                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                    a.value(0).to_string()
                                }
                                _ => {
                                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                                    a.value(0).to_string()
                                }
                            }
                        };
                        worksheet.write(*r, *c, val).ok();
                    }
                }
            }
        }

        // 4. Inject table data
        if let Some(table_start) = &self.table_start {
            let row_start = table_start.chars().filter(|c| c.is_digit(10)).collect::<String>()
                .parse::<u32>().unwrap_or(1) - 1;
            let col_start = (table_start.chars().filter(|c| c.is_alphabetic()).collect::<String>()
                .to_uppercase().as_bytes()[0] as u16) - (b'A' as u16);

            let mut current_row = row_start;
            for batch in &self.all_batches[0] {
                for r in 0..batch.num_rows() {
                    for c in 0..batch.num_columns() {
                        let array = batch.column(c);
                        if !array.is_null(r) {
                            match array.data_type() {
                                DataType::Int64 => {
                                    let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                    worksheet.write(current_row, col_start + c as u16, a.value(r) as f64).ok();
                                }
                                DataType::Float64 => {
                                    let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                    worksheet.write(current_row, col_start + c as u16, a.value(r)).ok();
                                }
                                _ => {
                                    let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                                    worksheet.write(current_row, col_start + c as u16, a.value(r)).ok();
                                }
                            }
                        }
                    }
                    current_row += 1;
                }
            }
        }

        workbook_writer.save(&self.output_path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        
        Ok(())
    }
}

/// Physical execution plan for professional Excel reports with charts.
#[derive(Debug)]
pub struct ExcelReportExec {
    pub path: PathBuf,
    pub charts: JsonValue,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    cache: PlanProperties,
}

impl ExcelReportExec {
    pub fn new(path: PathBuf, charts: JsonValue, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(inputs[0].schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            path,
            charts,
            inputs,
            cache,
        }
    }
}

impl DisplayAs for ExcelReportExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelReportExec(path={:?})", self.path)
    }
}

impl ExecutionPlan for ExcelReportExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "ExcelReportExec"
    }
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
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
            self.path.clone(),
            self.charts.clone(),
            children,
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
        
        Ok(Box::pin(ExcelReportWritingStream {
            inputs: streams,
            path: self.path.clone(),
            charts: self.charts.clone(),
            all_batches: vec![Vec::new(); self.inputs.len()],
            finished_mask: vec![false; self.inputs.len()],
        }))
    }
}

struct ExcelReportWritingStream {
    inputs: Vec<SendableRecordBatchStream>,
    path: PathBuf,
    charts: JsonValue,
    all_batches: Vec<Vec<RecordBatch>>,
    finished_mask: Vec<bool>,
}

impl RecordBatchStream for ExcelReportWritingStream {
    fn schema(&self) -> SchemaRef {
        self.inputs[0].schema()
    }
}

impl Stream for ExcelReportWritingStream {
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
                    self.all_batches[i].push(batch.clone());
                    if i == 0 {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.finished_mask[i] = true;
                }
                Poll::Pending => any_pending = true,
            }
        }

        if any_pending {
            Poll::Pending
        } else if self.finished_mask.iter().all(|&f| f) {
            self.finalize_workbook()?;
            Poll::Ready(None)
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

impl ExcelReportWritingStream {
    fn finalize_workbook(&mut self) -> Result<()> {
        if self.all_batches.is_empty() {
            return Ok(());
        }

        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name("Data").ok();

        let mut row_offset = 0;
        let schema = self.all_batches[0][0].schema();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            worksheet.write(0, col_idx as u16, field.name()).ok();
        }
        row_offset += 1;

        for batch in &self.all_batches[0] {
            for row_idx in 0..batch.num_rows() {
                for col_idx in 0..batch.num_columns() {
                    let array = batch.column(col_idx);
                    if !array.is_null(row_idx) {
                        match array.data_type() {
                            DataType::Int64 => {
                                let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                worksheet.write(row_offset as u32, col_idx as u16, a.value(row_idx) as f64).ok();
                            }
                            DataType::Float64 => {
                                let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                worksheet.write(row_offset as u32, col_idx as u16, a.value(row_idx)).ok();
                            }
                            _ => {
                                let a = array.as_any().downcast_ref::<StringArray>().unwrap();
                                worksheet.write(row_offset as u32, col_idx as u16, a.value(row_idx)).ok();
                            }
                        }
                    }
                }
                row_offset += 1;
            }
        }

        // Add charts
        if let Some(charts_arr) = self.charts.as_array() {
            for (i, chart_json) in charts_arr.iter().enumerate() {
                let chart_type = match chart_json["type"].as_str().unwrap_or("column") {
                    "line" => ChartType::Line,
                    "pie" => ChartType::Pie,
                    _ => ChartType::Column,
                };
                let mut chart = Chart::new(chart_type);
                
                if let Some(title) = chart_json["title"].as_str() {
                    chart.title().set_name(title);
                }

                // Simplified range parsing (DataFusion doesn't easily give us A1 notation from names)
                // For test, we assume user provides correct ranges like "Data!$B$2:$B$11"
                let data_range = chart_json["data_range"].as_str().unwrap_or("");
                let label_range = chart_json["label_range"].as_str().unwrap_or("");
                
                // Helper to parse "Data!$B$2:$B$11" into (sheet, row_start, col_start, row_end, col_end)
                // rust_xlsxwriter needs tuples or strings. Let's use strings for simplicity.
                chart.add_series()
                    .set_values(data_range)
                    .set_categories(label_range);

                let row_pos = chart_json["offset"]["row"].as_u64().unwrap_or(1) as u32;
                let col_pos = chart_json["offset"]["col"].as_u64().unwrap_or(5) as u16;
                
                worksheet.insert_chart(row_pos, col_pos + (i as u16 * 8), &chart).ok();
            }
        }

        workbook.save(&self.path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        
        Ok(())
    }
}
