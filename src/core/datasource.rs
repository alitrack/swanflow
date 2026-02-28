use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::path::PathBuf;
use std::task::Context;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{SchemaRef, Schema, Field, DataType};
use arrow::array::{Array, ArrayRef, StringBuilder, Int64Array, Float64Array, StringArray};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::common::DFSchemaRef;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datafusion::physical_plan::execution_plan::{EmissionType, Boundedness};
use futures::Stream;
use futures::StreamExt;
use calamine::{Reader, Xlsx, open_workbook, Data};
use rust_xlsxwriter::Workbook;

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
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> { None }
}

impl UserDefinedLogicalNodeCore for ExcelScanLogical {
    fn name(&self) -> &str { "ExcelScan" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![] }
    fn schema(&self) -> &DFSchemaRef { &self.schema }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelScan(path={:?}, mode={:?})", self.path, self.mode)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
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
    pub fn new(path: PathBuf, sheet: Option<String>, mode: ExcelScanMode, project_schema: SchemaRef) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(project_schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { path, sheet, mode, project_schema, cache }
    }
}

impl DisplayAs for ExcelScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelScanExec(path={:?}, mode={:?})", self.path, self.mode)
    }
}

impl ExecutionPlan for ExcelScanExec {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ExcelScanExec" }
    fn schema(&self) -> SchemaRef { self.project_schema.clone() }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
    
    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let mut workbook: Xlsx<_> = open_workbook(&self.path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        
        let range = if let Some(ref name) = self.sheet {
            workbook.worksheet_range(name).map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        } else {
            workbook.worksheet_range_at(0)
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
    fn schema(&self) -> SchemaRef { self.schema.clone() }
}

impl Stream for ExcelStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        if self.finished { return std::task::Poll::Ready(None); }

        let mut row_count = 0;
        let start_row = self.row_idx + 1;
        let mut current_idx = self.row_idx;

        if self.mode == ExcelScanMode::Validated {
            let mut builders: Vec<StringBuilder> = (0..self.schema.fields().len()).map(|_| StringBuilder::new()).collect();
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
            if row_count == 0 { self.finished = true; return std::task::Poll::Ready(None); }
            let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| Arc::new(b.finish()) as ArrayRef).collect();
            let batch = RecordBatch::try_new(self.schema.clone(), columns).map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;
            std::task::Poll::Ready(Some(Ok(batch)))
        } else {
            self.finished = true;
            std::task::Poll::Ready(None)
        }
    }
}

/// Logical node for Excel writing.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct ExcelSinkLogical {
    pub path: PathBuf,
    pub sheet: String,
    pub input: LogicalPlan,
}

impl PartialOrd for ExcelSinkLogical {
    fn partial_cmp(&self, _other: &Self) -> Option<std::cmp::Ordering> { None }
}

impl UserDefinedLogicalNodeCore for ExcelSinkLogical {
    fn name(&self) -> &str { "ExcelSink" }
    fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
    fn schema(&self) -> &DFSchemaRef { self.input.schema() }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> { vec![] }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelSink(path={:?}, sheet={:?})", self.path, self.sheet)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<datafusion::logical_expr::Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self { path: self.path.clone(), sheet: self.sheet.clone(), input: inputs[0].clone() })
    }
}

/// Physical execution plan for writing to an Excel file.
#[derive(Debug)]
pub struct ExcelSinkExec {
    pub path: PathBuf,
    pub sheet: String,
    pub input: Arc<dyn ExecutionPlan>,
    cache: PlanProperties,
}

impl ExcelSinkExec {
    pub fn new(path: PathBuf, sheet: String, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self { path, sheet, input, cache }
    }
}

impl DisplayAs for ExcelSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ExcelSinkExec(path={:?})", self.path)
    }
}

impl ExecutionPlan for ExcelSinkExec {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "ExcelSinkExec" }
    fn schema(&self) -> SchemaRef { self.input.schema() }
    fn properties(&self) -> &PlanProperties { &self.cache }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![&self.input] }
    
    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(self.path.clone(), self.sheet.clone(), children[0].clone())))
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(ExcelWritingStream {
            input: input_stream,
            path: self.path.clone(),
            sheet: self.sheet.clone(),
            batches: Vec::new(),
        }))
    }
}

struct ExcelWritingStream {
    input: SendableRecordBatchStream,
    path: PathBuf,
    sheet: String,
    batches: Vec<RecordBatch>,
}

impl RecordBatchStream for ExcelWritingStream {
    fn schema(&self) -> SchemaRef { self.input.schema() }
}

impl Stream for ExcelWritingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(Ok(batch))) => {
                self.batches.push(batch.clone());
                std::task::Poll::Ready(Some(Ok(batch)))
            }
            std::task::Poll::Ready(None) => {
                if !self.batches.is_empty() {
                    let mut workbook = Workbook::new();
                    let worksheet = workbook.add_worksheet();
                    worksheet.set_name(&self.sheet).ok();
                    let mut row_offset = 0;
                    let schema = self.batches[0].schema();
                    for (i, field) in schema.fields().iter().enumerate() {
                        worksheet.write(0, i as u16, field.name()).ok();
                    }
                    row_offset += 1;
                    for batch in &self.batches {
                        for row_idx in 0..batch.num_rows() {
                            for col_idx in 0..batch.num_columns() {
                                let array = batch.column(col_idx);
                                match array.data_type() {
                                    DataType::Int64 => {
                                        let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
                                        if !a.is_null(row_idx) {
                                            worksheet.write(row_offset as u32, col_idx as u16, a.value(row_idx) as f64).ok();
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
                    workbook.save(&self.path).ok();
                }
                std::task::Poll::Ready(None)
            }
            other => other,
        }
    }
}
