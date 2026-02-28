use std::any::Any;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use calamine::{open_workbook, Data, Reader, Xlsx};
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
                .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;
            std::task::Poll::Ready(Some(Ok(batch)))
        } else {
            self.finished = true;
            std::task::Poll::Ready(None)
        }
    }
}

/// Logical node for professional Excel writing (Dashboarding).
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ExcelSinkLogical {
    pub path: PathBuf,
    pub sheets: Vec<String>,
    pub inputs: Vec<LogicalPlan>,
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
            inputs,
        })
    }
}

/// Physical execution plan for writing to a professional Excel file.
#[derive(Debug)]
pub struct ExcelSinkExec {
    pub path: PathBuf,
    pub sheets: Vec<String>,
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    cache: PlanProperties,
}

impl ExcelSinkExec {
    pub fn new(path: PathBuf, sheets: Vec<String>, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(inputs[0].schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            path,
            sheets,
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
            all_batches: vec![Vec::new(); self.inputs.len()],
            finished_mask: vec![false; self.inputs.len()],
        }))
    }
}

struct ExcelWritingStream {
    inputs: Vec<SendableRecordBatchStream>,
    path: PathBuf,
    sheets: Vec<String>,
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

        for (i, sheet_batches) in self.all_batches.iter().enumerate() {
            if sheet_batches.is_empty() {
                continue;
            }

            let sheet_name = self.sheets.get(i).cloned()
                .unwrap_or_else(|| format!("Sheet{}", i + 1));
            
            let worksheet = workbook.add_worksheet();
            worksheet.set_name(&sheet_name).ok();

            let mut row_offset = 0;
            let schema = sheet_batches[0].schema();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                worksheet.write_with_format(0, col_idx as u16, field.name(), &header_format).ok();
            }
            row_offset += 1;

            for batch in sheet_batches {
                for row_idx in 0..batch.num_rows() {
                    for col_idx in 0..batch.num_columns() {
                        let array = batch.column(col_idx);
                        match array.data_type() {
                            DataType::Int64 => {
                                let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                if !a.is_null(row_idx) {
                                    let val = a.value(row_idx);
                                    // Check if value fits in f64 without precision loss
                                    // i64::MAX is ~9.2e18, f64::MAX_PRECISION is ~2^53 (~9e15)
                                    if val > 9_000_000_000_000_000 || val < -9_000_000_000_000_000 {
                                        // Large integers: write as string to preserve precision
                                        worksheet.write(row_offset as u32, col_idx as u16, val.to_string()).ok();
                                    } else {
                                        // Small integers: safe to write as f64
                                        worksheet.write(row_offset as u32, col_idx as u16, val as f64).ok();
                                    }
                                }
                            }
                            DataType::Float64 => {
                                let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
                                if !a.is_null(row_idx) {
                                    worksheet.write(row_offset as u32, col_idx as u16, a.value(row_idx)).ok();
                                }
                            }
                            _ => {
                                let val = array.as_any().downcast_ref::<StringArray>()
                                    .map(|a| a.value(row_idx).to_string())
                                    .unwrap_or_else(|| "val".to_string());
                                worksheet.write(row_offset as u32, col_idx as u16, val).ok();
                            }
                        }
                    }
                    row_offset += 1;
                }
            }

            if i == 0 && schema.fields().len() >= 2 && row_offset > 2 {
                let mut chart = Chart::new(ChartType::Column);
                let sheet_name_str = sheet_name.as_str();
                let row_end = (row_offset - 1) as u32;
                chart.add_series()
                    .set_categories((sheet_name_str, 1, 0, row_end, 0))
                    .set_values((sheet_name_str, 1, 1, row_end, 1));
                
                let title = format!("{} Analysis", sheet_name_str);
                chart.title().set_name(&title);
                worksheet.insert_chart(1, (schema.fields().len() + 1) as u16, &chart).ok();
            }
        }

        workbook.save(&self.path).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(())
    }
}
