use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::array::{ArrayRef, StringBuilder};
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
use calamine::{Reader, Xlsx, open_workbook, Data};

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
        None // Custom nodes don't strictly need ordering for plan comparison
    }
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

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        if self.finished {
            return std::task::Poll::Ready(None);
        }

        let mut builders: Vec<StringBuilder> = (0..self.schema.fields().len()).map(|_| StringBuilder::new()).collect();
        let mut rows_read = 0;
        
        // Use a temporary variable to hold current row index to avoid double borrow
        let start_row = self.row_idx + 1;
        let mut current_idx = self.row_idx;

        for row in self.range.rows().skip(start_row).take(1000) {
            current_idx += 1;
            rows_read += 1;
            
            if self.mode == ExcelScanMode::Validated {
                for (i, cell) in row.iter().enumerate() {
                    if i < builders.len() {
                        builders[i].append_value(cell.to_string());
                    }
                }
            }
        }

        self.row_idx = current_idx;

        if rows_read == 0 {
            self.finished = true;
            return std::task::Poll::Ready(None);
        }

        let columns: Vec<ArrayRef> = builders.into_iter().map(|mut b| Arc::new(b.finish()) as ArrayRef).collect();
        let batch = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;

        std::task::Poll::Ready(Some(Ok(batch)))
    }
}
