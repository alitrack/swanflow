//! MySQL connector — uses sqlx::MySqlPool for database access.

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

/// MySQL database connector.
pub struct MySqlConnector {
    pool: sqlx::MySqlPool,
    _config: ConnConfig,
}

#[async_trait]
impl Connector for MySqlConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        let pool = sqlx::MySqlPool::connect(&config.url)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("MySQL connection failed: {}",  e)))?;
        Ok(Self { pool, _config: config })
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>("SHOW TABLES")
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("Failed to list MySQL tables: {}",  e)))?;
        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        let rows = sqlx::query_as::<_, (String, String, String)>(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
             FROM information_schema.COLUMNS \
             WHERE TABLE_NAME = ? \
             ORDER BY ORDINAL_POSITION",
        )
        .bind(table)
        .fetch_all(&self.pool)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("Failed to get schema for MySQL table '{}': {}",  table, e)))?;

        if rows.is_empty() {
            return Err(SwanFlowError::unknown(format!("Table '{}' not found in MySQL",  table)));
        }

        let fields: Vec<Field> = rows
            .into_iter()
            .map(|(name, mysql_type, nullable)| {
                let arrow_type = mysql_type_to_arrow(&mysql_type);
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

        let col_names: Vec<String> = projected_schema.fields().iter().map(|f| f.name().to_string()).collect();
        let select_cols = col_names.join(", ");
        let query = format!("SELECT {} FROM `{}`", select_cols, table);

        let pool = self.pool.clone();
        let proj_schema = projected_schema.clone();

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 1);
        let tx = builder.tx();

        builder.spawn(async move {
            let rows = sqlx::query(&query).fetch_all(&pool).await.map_err(|e| {
                datafusion::error::DataFusionError::External(Box::new(e))
            })?;

            if rows.is_empty() {
                let _ = tx.send(Ok(RecordBatch::new_empty(proj_schema.clone()))).await;
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
                        append_mysql_value(builder, row, i);
                    }
                }

                let columns: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
                let batch = RecordBatch::try_new(proj_schema.clone(), columns)
                    .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;
                let _ = tx.send(Ok(batch)).await;
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn name(&self) -> &'static str {
        "mysql"
    }
}

/// Map MySQL type names to Arrow DataType.
fn mysql_type_to_arrow(mysql_type: &str) -> DataType {
    match mysql_type.to_uppercase().as_str() {
        "TINYINT" | "TINYINT(1)" => DataType::Boolean,
        "SMALLINT" => DataType::Int16,
        "MEDIUMINT" | "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" | "DECIMAL" | "DEC" | "NUMERIC" => DataType::Float64,
        "DATE" => DataType::Date32,
        "TIMESTAMP" | "DATETIME" => DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
            None,
        ),
        "TIME" => DataType::Utf8,
        "YEAR" => DataType::Int32,
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT"
        | "ENUM" | "SET" | "JSON" => DataType::Utf8,
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => DataType::Utf8,
        _ => DataType::Utf8,
    }
}

/// Append a value from a MySQL row to a DynamicArrayBuilder.
fn append_mysql_value(builder: &mut DynamicArrayBuilder, row: &sqlx::mysql::MySqlRow, col_idx: usize) {
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
            if let Ok(dt) = row.try_get::<chrono::NaiveDateTime, _>(col_idx) {
                b.append_value(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
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
