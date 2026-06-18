//! PostgreSQL connector — uses sqlx::PgPool for database access.

use std::sync::Arc;

use crate::core::error::{CoreResult, SwanFlowError};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;

use sqlx::Row;

use crate::core::datasource::{ConnConfig, Connector, DynamicArrayBuilder};

/// PostgreSQL database connector.
pub struct PostgresConnector {
    pool: sqlx::PgPool,
    _config: ConnConfig,
}

#[async_trait]
impl Connector for PostgresConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        let pool = sqlx::PgPool::connect(&config.url)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("PostgreSQL connection failed: {}", e)))?;
        Ok(Self {
            pool,
            _config: config,
        })
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            "SELECT tablename FROM pg_catalog.pg_tables \
             WHERE schemaname NOT IN ('pg_catalog', 'information_schema') \
             ORDER BY tablename",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SwanFlowError::unknown(format!("Failed to list PostgreSQL tables: {}", e)))?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        let rows = sqlx::query_as::<_, (String, String, String)>(
            "SELECT column_name, data_type, is_nullable \
             FROM information_schema.columns \
             WHERE table_name = $1 \
             ORDER BY ordinal_position",
        )
        .bind(table)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            SwanFlowError::unknown(format!("Failed to get schema for table '{}': {}", table, e))
        })?;

        if rows.is_empty() {
            return Err(SwanFlowError::unknown(format!(
                "Table '{}' not found in PostgreSQL",
                table
            )));
        }

        let fields: Vec<Field> = rows
            .into_iter()
            .map(|(name, pg_type, nullable)| {
                // Map PostgreSQL types to Arrow using the shared type mapper
                let arrow_type = pg_type_to_arrow(&pg_type);
                Field::new(&name, arrow_type, nullable == "YES")
            })
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }

    async fn scan(
        &self,
        table: &str,
        projection: &[usize],
    ) -> CoreResult<SendableRecordBatchStream> {
        let schema = self.get_schema(table).await?;

        let projected_schema: SchemaRef = if projection.is_empty() {
            schema.clone()
        } else {
            let fields: Vec<Field> = projection
                .iter()
                .map(|&i| schema.field(i).clone())
                .collect();
            Arc::new(Schema::new(fields))
        };

        let col_names: Vec<String> = projected_schema
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect();
        let select_cols = col_names.join(", ");
        // Use quoted identifiers to handle reserved words
        let query = format!(
            "SELECT {} FROM \"{}\"",
            select_cols
                .split(", ")
                .map(|c| format!("\"{}\"", c))
                .collect::<Vec<_>>()
                .join(", "),
            table
        );

        let pool = self.pool.clone();
        let proj_schema = projected_schema.clone();

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 1);
        let tx = builder.tx();

        builder.spawn(async move {
            let rows = sqlx::query(&query)
                .fetch_all(&pool)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            if rows.is_empty() {
                let _ = tx
                    .send(Ok(RecordBatch::new_empty(proj_schema.clone())))
                    .await;
                return Ok(());
            }

            let batch_size = 1000;
            for chunk in rows.chunks(batch_size) {
                let mut builders: Vec<DynamicArrayBuilder> = proj_schema
                    .fields()
                    .iter()
                    .map(|f| DynamicArrayBuilder::new(f.data_type(), chunk.len()))
                    .collect();

                for row in chunk {
                    for (i, builder) in builders.iter_mut().enumerate() {
                        // Use column index (i) directly — matches SELECT column order
                        append_pg_value(builder, row, i);
                    }
                }

                let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
                let batch = RecordBatch::try_new(proj_schema.clone(), columns).map_err(|e| {
                    datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                })?;
                let _ = tx.send(Ok(batch)).await;
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn name(&self) -> &'static str {
        "postgres"
    }
}

/// Map PostgreSQL type names to Arrow DataType.
fn pg_type_to_arrow(pg_type: &str) -> DataType {
    match pg_type.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "SMALLINT" | "SMALLSERIAL" | "INT2" => DataType::Int16,
        "INTEGER" | "SERIAL" | "INT4" => DataType::Int32,
        "BIGINT" | "BIGSERIAL" | "INT8" => DataType::Int64,
        "REAL" | "FLOAT4" => DataType::Float32,
        "DOUBLE PRECISION" | "FLOAT8" | "NUMERIC" | "DECIMAL" | "MONEY" => DataType::Float64,
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "DATE" => DataType::Date32,
        "TIME" | "TIME WITHOUT TIME ZONE" => DataType::Utf8,
        "JSON" | "JSONB" => DataType::Utf8,
        "UUID" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

/// Append a value from a PostgreSQL row to a DynamicArrayBuilder by column index.
fn append_pg_value(builder: &mut DynamicArrayBuilder, row: &sqlx::postgres::PgRow, col_idx: usize) {
    match builder {
        DynamicArrayBuilder::Int64(b) => {
            let val: Option<i64> = row.try_get(col_idx).ok().flatten();
            b.append_option(val);
        }
        DynamicArrayBuilder::Float64(b) => {
            let val: Option<f64> = row.try_get(col_idx).ok().flatten();
            b.append_option(val);
        }
        DynamicArrayBuilder::Boolean(b) => {
            let val: Option<bool> = row.try_get(col_idx).ok().flatten();
            b.append_option(val);
        }
        DynamicArrayBuilder::Timestamp(b) => {
            // Try chrono::NaiveDateTime first (no tz), then chrono::DateTime<Utc> (with tz)
            if let Ok(dt) = row.try_get::<chrono::NaiveDateTime, _>(col_idx) {
                b.append_value(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
            } else if let Ok(dt) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(col_idx) {
                b.append_value(dt.timestamp_nanos_opt().unwrap_or(0));
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Utf8(b) => {
            let val: Option<String> = row.try_get(col_idx).ok().flatten();
            b.append_option(val);
        }
    }
}
