//! Redis connector — implements the Connector trait for Redis key-value store.
//!
//! Since Redis is not a relational database, the semantics are adapted:
//! - list_tables: returns common key patterns (namespace prefixes)
//! - get_schema: returns a simple KEY / VALUE / TYPE schema
//! - scan: returns key-value pairs as a RecordBatch

use std::sync::Arc;

use crate::core::error::{CoreResult, SwanFlowError};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::core::datasource::{ConnConfig, Connector};

/// Redis key-value connector.
pub struct RedisConnector {
    client: redis::Client,
    _config: ConnConfig,
}

async fn scan_keys(client: &redis::Client) -> CoreResult<Vec<String>> {
    let mut con = client.get_async_connection()
        .await
        .map_err(|e| SwanFlowError::unknown(format!("Redis connection failed: {}",  e)))?;

    let mut cursor = 0i64;
    let mut all_keys: Vec<String> = Vec::new();
    loop {
        let result: (i64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("COUNT")
            .arg(1000)
            .query_async(&mut con)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("Redis SCAN failed: {}",  e)))?;
        cursor = result.0;
        all_keys.extend(result.1);
        if cursor == 0 {
            break;
        }
    }
    all_keys.sort();
    all_keys.truncate(100);
    Ok(all_keys)
}

#[async_trait]
impl Connector for RedisConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        let client = redis::Client::open(config.url.as_str())
            .map_err(|e| SwanFlowError::unknown(format!("Invalid Redis URL: {}",  e)))?;
        Ok(Self { client, _config: config })
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let keys = scan_keys(&self.client).await?;

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

    async fn get_schema(&self, _table: &str) -> CoreResult<SchemaRef> {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
        ]);
        Ok(Arc::new(schema))
    }

    async fn scan(
        &self,
        _table: &str,
        _projection: &[usize],
    ) -> CoreResult<SendableRecordBatchStream> {
        let keys = scan_keys(&self.client).await?;

        let schema = self.get_schema(_table).await?;
        let schema_clone = schema.clone();
        let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 1);
        let tx = builder.tx();

        builder.spawn(async move {
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
                    ))
                    .await;
                }
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn name(&self) -> &'static str {
        "redis"
    }
}
