use std::collections::HashMap;

use datafusion::common::DFSchemaRef;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkflowMetadata {
    pub schemas: HashMap<Uuid, Vec<DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<datafusion::arrow::record_batch::RecordBatch>>,
    pub profiles: HashMap<Uuid, datafusion::arrow::record_batch::RecordBatch>,
    pub diagnostics: HashMap<Uuid, Vec<String>>,
}