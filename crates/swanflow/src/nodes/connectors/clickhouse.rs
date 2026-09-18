//! ClickHouse connector — uses reqwest HTTP protocol (native Arrow format).

use std::sync::Arc;

use crate::core::error::{CoreResult, SwanFlowError};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::core::datasource::{ConnConfig, Connector, DynamicArrayBuilder};

/// ClickHouse database connector (HTTP protocol).
///
/// Uses ClickHouse's native HTTP interface which supports Arrow format
/// via the `?format=Arrow` query parameter.
pub struct ClickHouseConnector {
    url: String,
    config: ConnConfig,
    client: reqwest::Client,
}

#[async_trait]
impl Connector for ClickHouseConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        let timeout = std::time::Duration::from_secs(config.timeout_secs);
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|e| {
                SwanFlowError::unknown(format!("Failed to build HTTP client for ClickHouse: {}", e))
            })?;

        // Validate connectivity by sending a ping
        let ping_url = format!("{}/?query=SELECT+1", config.url.trim_end_matches('/'));
        let resp = client
            .get(&ping_url)
            .send()
            .await
            .map_err(|e| SwanFlowError::unknown(format!("ClickHouse ping failed: {}", e)))?;

        if !resp.status().is_success() {
            return Err(SwanFlowError::unknown(format!(
                "ClickHouse ping returned HTTP {}",
                resp.status()
            )));
        }

        Ok(Self {
            url: config.url.trim_end_matches('/').to_string(),
            config,
            client,
        })
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let query_url = format!("{}/?query=SHOW+TABLES", self.url);
        let resp =
            self.client.get(&query_url).send().await.map_err(|e| {
                SwanFlowError::unknown(format!("ClickHouse list tables failed: {}", e))
            })?;

        let text = resp.text().await.map_err(|e| {
            SwanFlowError::unknown(format!("Failed to read ClickHouse response: {}", e))
        })?;

        Ok(text.lines().map(|l| l.trim().to_string()).collect())
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        // URL-encode the query: DESCRIBE TABLE table_name
        let query = format!("DESCRIBE TABLE {}", quote_ident(table));
        let query_url = format!("{}/?query={}", self.url, urlencoding(&query));

        let resp = self.client.get(&query_url).send().await.map_err(|e| {
            SwanFlowError::unknown(format!("ClickHouse DESCRIBE TABLE failed: {}", e))
        })?;

        let text = resp.text().await.map_err(|e| {
            SwanFlowError::unknown(format!("Failed to read ClickHouse response: {}", e))
        })?;

        let fields: Vec<Field> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|line| {
                // Format: name	type	[other...]
                let parts: Vec<&str> = line.split('\t').collect();
                if parts.len() >= 2 {
                    let name = parts[0].trim().to_string();
                    let ch_type = parts[1].trim();
                    Some(Field::new(&name, ch_type_to_arrow(ch_type), true))
                } else {
                    None
                }
            })
            .collect();

        if fields.is_empty() {
            return Err(SwanFlowError::table_not_found(format!(
                "Table '{}' not found or has no columns in ClickHouse",
                table
            )));
        }

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
        // P2-1a: quote each column/table identifier (backtick syntax, escaped).
        let select_cols = col_names
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!("SELECT {} FROM {}", select_cols, quote_ident(table));
        let ch_url = self.url.clone();
        let client = self.client.clone();
        let proj_schema = projected_schema.clone();
        let _col_names_clone = col_names.clone();

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 1);
        let tx = builder.tx();

        builder.spawn(async move {
            // Try Arrow format first (ClickHouse >= 21.12)
            let arrow_url = format!("{}/?query={}&format=Arrow", ch_url, urlencoding(&query));
            match client.get(&arrow_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    if !bytes.is_empty() {
                        let reader = StreamReader::try_new(bytes.as_ref(), None).map_err(|e| {
                            datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                        })?;

                        for batch_result in reader {
                            match batch_result {
                                Ok(batch) => {
                                    let _ = tx.send(Ok(batch)).await;
                                }
                                Err(e) => {
                                    let _ = tx
                                        .send(Err(datafusion::error::DataFusionError::ArrowError(
                                            Box::new(e),
                                            None,
                                        )))
                                        .await;
                                    return Ok(());
                                }
                            }
                        }
                        return Ok(());
                    }
                }
                _ => {}
            }

            // Fallback: TSV format and convert manually
            let tsv_url = format!(
                "{}/?query={}&format=TSVWithNames",
                ch_url,
                urlencoding(&query)
            );
            let resp = client
                .get(&tsv_url)
                .send()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let text = resp
                .text()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let lines: Vec<&str> = text.lines().collect();
            if lines.len() < 2 {
                let _ = tx
                    .send(Ok(RecordBatch::new_empty(proj_schema.clone())))
                    .await;
                return Ok(());
            }

            // First line is headers, skip it
            let data_lines = &lines[1..];
            let batch_size = 1000;

            for chunk in data_lines.chunks(batch_size) {
                let mut builders: Vec<DynamicArrayBuilder> = proj_schema
                    .fields()
                    .iter()
                    .map(|f| DynamicArrayBuilder::new(f.data_type(), chunk.len()))
                    .collect();

                for line in chunk {
                    let values: Vec<&str> = split_tsv(line);
                    for (i, builder) in builders.iter_mut().enumerate() {
                        if i < values.len() {
                            append_tsv_value(builder, values[i]);
                        } else {
                            builder.append_null();
                        }
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
        "clickhouse"
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Quote a ClickHouse identifier with backticks, escaping embedded backticks.
/// P2-1a: closes identifier-injection via table/column names.
fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Map ClickHouse type names to Arrow DataType.
fn ch_type_to_arrow(ch_type: &str) -> DataType {
    let upper = ch_type.to_uppercase();

    if upper == "BOOL" || upper == "BOOLEAN" {
        return DataType::Boolean;
    }
    if upper.starts_with("INT8") || upper == "TINYINT" {
        return DataType::Int8;
    }
    if upper.starts_with("INT16") || upper == "SMALLINT" {
        return DataType::Int16;
    }
    if upper.starts_with("INT32") || upper == "INT" || upper == "INTEGER" {
        return DataType::Int32;
    }
    if upper.starts_with("INT64") || upper == "BIGINT" {
        return DataType::Int64;
    }
    if upper.starts_with("UINT8") {
        return DataType::UInt8;
    }
    if upper.starts_with("UINT16") {
        return DataType::UInt16;
    }
    if upper.starts_with("UINT32") {
        return DataType::UInt32;
    }
    if upper.starts_with("UINT64") {
        return DataType::UInt64;
    }
    if upper.starts_with("FLOAT32") || upper == "FLOAT" {
        return DataType::Float32;
    }
    if upper.starts_with("FLOAT64") || upper == "DOUBLE" {
        return DataType::Float64;
    }
    if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
        return DataType::Float64;
    }
    if upper.starts_with("DATE") {
        return DataType::Date32;
    }
    if upper.starts_with("DATETIME") || upper.starts_with("TIMESTAMP") {
        return DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None);
    }
    // String types
    if upper.starts_with("STRING")
        || upper.starts_with("VARCHAR")
        || upper.starts_with("CHAR")
        || upper.starts_with("TEXT")
        || upper.starts_with("JSON")
        || upper.starts_with("UUID")
        || upper.starts_with("IPV4")
        || upper.starts_with("IPV6")
    {
        return DataType::Utf8;
    }
    if upper.starts_with("FIXEDSTRING") || upper.starts_with("BINARY") {
        return DataType::Utf8;
    }
    // Arrays, Maps, Tuples, Enums, Nested — store as Utf8 for simplicity
    if upper.starts_with("ARRAY")
        || upper.starts_with("MAP")
        || upper.starts_with("TUPLE")
        || upper.starts_with("ENUM")
        || upper.starts_with("NESTED")
        || upper.starts_with("AGGREGATEFUNCTION")
        || upper.starts_with("SIMPLEAGGREGATEFUNCTION")
    {
        return DataType::Utf8;
    }
    if upper == "NOTHING" || upper == "NULLABLE(NOTHING)" {
        return DataType::Null;
    }

    // Fallback
    DataType::Utf8
}

/// Simple URL encoding for ClickHouse queries.
fn urlencoding(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'.' | b'~' => {
                result.push(byte as char);
            }
            b' ' => result.push_str("+"),
            _ => {
                result.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    result
}

/// Simple TSV splitter (handles basic escaping).
fn split_tsv(line: &str) -> Vec<&str> {
    line.split('\t').collect()
}

/// Append a TSV string value to a DynamicArrayBuilder.
fn append_tsv_value(builder: &mut DynamicArrayBuilder, value: &str) {
    match builder {
        DynamicArrayBuilder::Int64(b) => {
            if value.is_empty() || value == "\\N" {
                b.append_null();
            } else if let Ok(v) = value.parse::<i64>() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Float64(b) => {
            if value.is_empty() || value == "\\N" {
                b.append_null();
            } else if let Ok(v) = value.parse::<f64>() {
                b.append_value(v);
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Boolean(b) => {
            if value.is_empty() || value == "\\N" {
                b.append_null();
            } else {
                b.append_value(value == "1" || value.eq_ignore_ascii_case("true"));
            }
        }
        DynamicArrayBuilder::Timestamp(b) => {
            if value.is_empty() || value == "\\N" {
                b.append_null();
            } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S")
            {
                b.append_value(dt.and_utc().timestamp_nanos_opt().unwrap_or(0));
            } else if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(value) {
                b.append_value(dt.timestamp_nanos_opt().unwrap_or(0));
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Utf8(b) => {
            if value == "\\N" {
                b.append_null();
            } else {
                b.append_value(value);
            }
        }
    }
}
