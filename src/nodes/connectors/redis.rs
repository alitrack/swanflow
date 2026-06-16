//! Redis connector — implements the Connector trait for Redis key-value store.
//!
//! Since Redis is not a relational database, the semantics are adapted:
//! - list_tables: returns common key patterns (namespace prefixes)
//! - get_schema: returns a simple KEY / VALUE / TYPE schema
//! - scan: returns key-value pairs as a RecordBatch

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use crate::core::datasource::{ConnConfig, Connector, DynamicArrayBuilder};

/// Redis key-value connector.
pub struct RedisConnector {
    client: redis::Client,
    _config: ConnConfig,
}

#[async_trait]
impl Connector for RedisConnector {
    async fn connect(config: ConnConfig) -> Result<Self> {
        let client = redis::Client::open(&config.url)
            .map_err(|e| anyhow!("Invalid Redis URL: {}", e))?;
        Ok(Self { client, _config: config })
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        let mut con = self.client.get_multiplexed_async_connection().await
            .map_err(|e| anyhow!("Redis connection failed: {}", e))?;

        // Use SCAN to discover key patterns
        use redis::AsyncCommands;
        let mut keys: Vec<String> = con.scan().await
            .map_err(|e| anyhow!("Redis SCAN failed: {}", e))?;
        keys.sort();
        keys.truncate(100);

        // Extract common prefixes as "table" names
        let mut prefixes: Vec<String> = Vec::new();
        for key in &keys {
            if let Some(pos) = key.find(':') {
                let prefix = key[..pos].to_string();
                if !prefixes.contains(&prefix) {
                    prefixes.push(prefix);
                }
            }
        }

        if prefixes.is_empty() && !keys.is_empty() {
            prefixes.push("default".to_string());
        }

        Ok(prefixes)
    }

    async fn get_schema(&self, _table: &str) -> Result<SchemaRef> {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
        ]);
        Ok(Arc::new(schema))
    }

    async fn scan(&self, _table: &str, _projection: &[usize]) -> Result<SendableRecordBatchStream> {
        let mut con = self.client.get_multiplexed_async_connection().await
            .map_err(|e| anyhow!("Redis connection failed: {}", e))?;

        use redis::AsyncCommands;
        let keys: Vec<String> = con.scan().await
            .map_err(|e| anyhow!("Redis SCAN failed: {}", e))?;

        let schema = self.get_schema(_table).await?;
        let schema_clone = schema.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            let mut key_builder = datafusion::arrow::array::StringBuilder::new();
            let mut value_builder = datafusion::arrow::array::StringBuilder::new();
            let mut type_builder = datafusion::arrow::array::StringBuilder::new();

            for key in keys {
                key_builder.append_value(&key);
                value_builder.append_null();
                type_builder.append_value("string");
            }

            let columns: Vec<ArrayRef> = vec![
                Arc::new(key_builder.finish()),
                Arc::new(value_builder.finish()),
                Arc::new(type_builder.finish()),
            ];

            match RecordBatch::try_new(schema_clone, columns) {
                Ok(batch) => {
                    let _ = tx.send(Ok(batch)).await;
                }
                Err(e) => {
                    let _ = tx.send(Err(
                        datafusion::error::DataFusionError::ArrowError(Box::new(e), None),
                    )).await;
                }
            }
        });

        Ok(Box::pin(RecordBatchReceiverStream::new(schema, rx)))
    }

    fn name(&self) -> &'static str {
        "redis"
    }
}
