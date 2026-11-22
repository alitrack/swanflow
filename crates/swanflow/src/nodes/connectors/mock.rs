use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::core::datasource::{ConnConfig, Connector};
use crate::core::error::{CoreResult, SwanFlowError};

/// A mock Connector that runs entirely in memory — no network, no disk.
///
/// Seeded with `with_table(name, schema, batches)` before use.
/// Ideal for unit testing workflow logic without real databases.
pub struct MockConnector {
    name: &'static str,
    tables: HashMap<String, MockTable>,
}

struct MockTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MockConnector {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            tables: HashMap::new(),
        }
    }

    /// Add a table with one or more record batches.
    pub fn with_table(mut self, name: &str, schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        self.tables
            .insert(name.to_string(), MockTable { schema, batches });
        self
    }

    /// Add a table with a single record batch (convenience).
    pub fn with_batch(mut self, name: &str, batch: RecordBatch) -> Self {
        let schema = batch.schema();
        self.tables.insert(
            name.to_string(),
            MockTable {
                schema,
                batches: vec![batch],
            },
        );
        self
    }

    fn project_batch(
        batch: &RecordBatch,
        projection: &[usize],
    ) -> arrow::error::Result<RecordBatch> {
        if projection.is_empty() {
            return Ok(batch.clone());
        }
        let columns: Vec<Arc<dyn arrow::array::Array>> = projection
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        // project() returns Schema, wrap in Arc for RecordBatch::try_new
        let projected_schema = Arc::new(batch.schema().project(projection)?);
        RecordBatch::try_new(projected_schema, columns)
    }
}

#[async_trait]
impl Connector for MockConnector {
    async fn connect(_config: ConnConfig) -> CoreResult<Self>
    where
        Self: Sized,
    {
        Ok(Self::new("mock"))
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        Ok(self.tables.keys().cloned().collect())
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        self.tables
            .get(table)
            .map(|t| t.schema.clone())
            .ok_or_else(|| SwanFlowError::table_not_found(format!("table '{table}' not found")))
    }

    async fn scan(
        &self,
        table: &str,
        projection: &[usize],
    ) -> CoreResult<SendableRecordBatchStream> {
        let table_data = self
            .tables
            .get(table)
            .ok_or_else(|| SwanFlowError::table_not_found(format!("table '{table}' not found")))?;

        // Compute projected schema
        let projected_schema: SchemaRef = if projection.is_empty() {
            table_data.schema.clone()
        } else {
            Arc::new(table_data.schema.project(projection)?)
        };

        // Apply projection to each batch
        let mut projected_batches = Vec::new();
        for batch in &table_data.batches {
            projected_batches.push(Self::project_batch(batch, projection)?);
        }

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 4);
        let tx = builder.tx();

        builder.spawn(async move {
            for batch in projected_batches {
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(());
                }
            }
            Ok(())
        });

        Ok(builder.build())
    }

    fn name(&self) -> &'static str {
        self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use futures::StreamExt;

    #[tokio::test]
    async fn test_mock_connector_basic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .unwrap();

        let connector = MockConnector::new("test").with_batch("users", batch);

        let tables = connector.list_tables().await.unwrap();
        assert_eq!(tables, vec!["users"]);

        let schema_out = connector.get_schema("users").await.unwrap();
        assert_eq!(schema_out.fields().len(), 2);

        let mut stream = connector.scan("users", &[0]).await.unwrap();
        let batch_out = stream.next().await.unwrap().unwrap();
        assert_eq!(batch_out.num_rows(), 3);
        assert_eq!(batch_out.num_columns(), 1); // projection: only column 0
    }

    #[tokio::test]
    async fn test_mock_connector_table_not_found() {
        let connector = MockConnector::new("test");
        assert!(connector.get_schema("nonexistent").await.is_err());
        assert!(connector.scan("nonexistent", &[]).await.is_err());
    }

    #[tokio::test]
    async fn test_mock_connector_multi_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5]))])
                .unwrap();

        let connector = MockConnector::new("test").with_table("data", schema, vec![batch1, batch2]);

        let mut stream = connector.scan("data", &[]).await.unwrap();
        let mut row_count = 0;
        while let Some(Ok(batch)) = stream.next().await {
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 5);
    }
}
