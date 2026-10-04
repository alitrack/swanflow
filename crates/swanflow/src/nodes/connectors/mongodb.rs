//! MongoDB connector — uses the mongodb crate for document database access.
//!
//! list_tables returns collection names.
//! get_schema infers schema by sampling documents from the collection.
//! scan queries the collection and converts BSON documents to Arrow RecordBatches.

use std::sync::Arc;

use crate::core::error::{CoreResult, SwanFlowError};
use async_trait::async_trait;
use bson::{Bson, Document};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use mongodb::options::ClientOptions;
use mongodb::{Client, Collection, Database};

use crate::core::datasource::{ConnConfig, Connector};

/// MongoDB database connector.
pub struct MongoDbConnector {
    client: Client,
    database: Database,
    _config: ConnConfig,
    db_name: String,
}

#[async_trait]
impl Connector for MongoDbConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        // The MongoDB connection URL is like: mongodb://user:pass@host:27017/db
        let mut client_options = ClientOptions::parse(&config.url)
            .await
            .map_err(|e| SwanFlowError::unknown(format!("Failed to parse MongoDB URL: {}", e)))?;

        client_options.app_name = Some("rustflow".to_string());
        client_options.connect_timeout = Some(std::time::Duration::from_secs(config.timeout_secs));

        let client = Client::with_options(client_options).map_err(|e| {
            SwanFlowError::unknown(format!("Failed to create MongoDB client: {}", e))
        })?;

        // Extract database name from URL
        let db_name = extract_db_name(&config.url)?;

        // Ping to verify connectivity
        let database = client.database(&db_name);
        database
            .run_command(bson::doc! { "ping": 1 })
            .await
            .map_err(|e| SwanFlowError::unknown(format!("MongoDB ping failed: {}", e)))?;

        Ok(Self {
            client,
            database,
            _config: config,
            db_name,
        })
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let mut collections = self.database.list_collection_names().await.map_err(|e| {
            SwanFlowError::unknown(format!("Failed to list MongoDB collections: {}", e))
        })?;

        collections.sort();
        Ok(collections)
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        let collection: Collection<Document> = self.database.collection(table);

        // Sample up to 100 documents to infer schema
        let mut cursor = collection
            .find(bson::doc! {})
            .limit(100)
            .await
            .map_err(|e| {
                SwanFlowError::unknown(format!("Failed to query collection '{}': {}", table, e))
            })?;

        let mut fields: std::collections::BTreeMap<String, FieldInfo> =
            std::collections::BTreeMap::new();
        let mut doc_count = 0u64;

        while let Some(doc_result) = cursor.next().await {
            let doc = doc_result.map_err(|e| {
                SwanFlowError::unknown(format!("Failed to read document from '{}': {}", table, e))
            })?;
            doc_count += 1;
            for (key, value) in &doc {
                let entry = fields.entry(key.clone()).or_insert_with(|| FieldInfo {
                    bson_type: BsonType::Null,
                    nullable: true,
                });
                let bson_type = bson_value_to_type(value);
                entry.bson_type = merge_types(&entry.bson_type, &bson_type);
            }
        }

        if fields.is_empty() {
            // Empty collection — return single default field
            return Ok(Arc::new(Schema::new(vec![Field::new(
                "_id",
                DataType::Utf8,
                true,
            )])));
        }

        let arrow_fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, info)| {
                let dt = bson_type_to_arrow(&info.bson_type);
                Field::new(&name, dt, info.nullable)
            })
            .collect();

        Ok(Arc::new(Schema::new(arrow_fields)))
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

        let collection_name = table.to_string();
        let db_name = self.db_name.clone();
        let url = self._config.url.clone();
        let proj_schema = projected_schema.clone();

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 1);
        let tx = builder.tx();

        builder.spawn(async move {
            let mut client_options = ClientOptions::parse(&url)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            client_options.app_name = Some("rustflow-scan".to_string());

            let client = Client::with_options(client_options)
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let database = client.database(&db_name);
            let collection: Collection<Document> = database.collection(&collection_name);

            let mut cursor = collection
                .find(bson::doc! {})
                .batch_size(1000)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let batch_size = 1000;
            let mut doc_buffer: Vec<Document> = Vec::with_capacity(batch_size);

            while let Some(doc_result) = cursor.next().await {
                match doc_result {
                    Ok(doc) => {
                        doc_buffer.push(doc);
                        if doc_buffer.len() >= batch_size {
                            let batch = docs_to_batch(&doc_buffer, &proj_schema, &col_names)?;
                            let _ = tx.send(Ok(batch)).await;
                            doc_buffer.clear();
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(datafusion::error::DataFusionError::External(Box::new(
                                e,
                            ))))
                            .await;
                        return Ok(());
                    }
                }
            }

            // Flush remaining documents
            if !doc_buffer.is_empty() {
                let batch = docs_to_batch(&doc_buffer, &proj_schema, &col_names)?;
                let _ = tx.send(Ok(batch)).await;
            } else if doc_buffer.is_empty() {
                // No documents at all
                let _ = tx
                    .send(Ok(RecordBatch::new_empty(proj_schema.clone())))
                    .await;
            }

            Ok(())
        });

        Ok(builder.build())
    }

    fn name(&self) -> &'static str {
        "mongodb"
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Track a field's BSON type during schema inference.
#[derive(Debug, Clone)]
enum BsonType {
    Null,
    Boolean,
    Int32,
    Int64,
    Float64,
    String,
    Date,
    Array,
    Document, // Nested document — stored as Utf8 (JSON string)
    Mixed,
}

struct FieldInfo {
    bson_type: BsonType,
    nullable: bool,
}

/// Determine the BSON type of a BSON value.
fn bson_value_to_type(value: &Bson) -> BsonType {
    match value {
        Bson::Null
        | Bson::RegularExpression(_)
        | Bson::JavaScriptCode(_)
        | Bson::Symbol(_)
        | Bson::JavaScriptCodeWithScope(_)
        | Bson::Undefined
        | Bson::MinKey
        | Bson::MaxKey
        | Bson::DbPointer(_) => BsonType::Null,
        Bson::Boolean(_) => BsonType::Boolean,
        Bson::Int32(_) => BsonType::Int32,
        Bson::Int64(_) => BsonType::Int64,
        Bson::Double(_) | Bson::Decimal128(_) => BsonType::Float64,
        Bson::String(_) | Bson::ObjectId(_) | Bson::Binary(_) => BsonType::String,
        Bson::DateTime(_) | Bson::Timestamp(_) => BsonType::Date,
        Bson::Array(_) => BsonType::Array,
        Bson::Document(_) => BsonType::Document,
    }
}

/// Merge two BsonTypes, promoting to wider type or Mixed.
fn merge_types(a: &BsonType, b: &BsonType) -> BsonType {
    if std::mem::discriminant(a) != std::mem::discriminant(b) {
        // Different type discriminants — try numeric promotion, else mixed
        match (a, b) {
            // Null merges into any other type
            (BsonType::Null, other) | (other, BsonType::Null) => other.clone(),
            // Numeric promotions
            (BsonType::Int32, BsonType::Int64) | (BsonType::Int64, BsonType::Int32) => {
                BsonType::Int64
            }
            (BsonType::Int32, BsonType::Float64)
            | (BsonType::Float64, BsonType::Int32)
            | (BsonType::Int64, BsonType::Float64)
            | (BsonType::Float64, BsonType::Int64) => BsonType::Float64,
            _ => BsonType::Mixed,
        }
    } else {
        a.clone()
    }
}

/// Map our internal BsonType to Arrow DataType.
fn bson_type_to_arrow(bt: &BsonType) -> DataType {
    match bt {
        BsonType::Null => DataType::Utf8,
        BsonType::Boolean => DataType::Boolean,
        BsonType::Int32 => DataType::Int32,
        BsonType::Int64 => DataType::Int64,
        BsonType::Float64 => DataType::Float64,
        BsonType::String => DataType::Utf8,
        BsonType::Date => DataType::Timestamp(TimeUnit::Nanosecond, None),
        BsonType::Array => DataType::Utf8,
        BsonType::Document => DataType::Utf8,
        BsonType::Mixed => DataType::Utf8,
    }
}

/// Convert a batch of BSON documents into an Arrow RecordBatch.
fn docs_to_batch(
    docs: &[Document],
    schema: &SchemaRef,
    col_names: &[String],
) -> std::result::Result<RecordBatch, datafusion::error::DataFusionError> {
    let num_rows = docs.len();
    let num_cols = col_names.len();

    // Pre-allocate arrays
    let mut bool_cols: Vec<Vec<Option<bool>>> = vec![vec![None; num_rows]; num_cols];
    let mut i32_cols: Vec<Vec<Option<i32>>> = vec![vec![None; num_rows]; num_cols];
    let mut i64_cols: Vec<Vec<Option<i64>>> = vec![vec![None; num_rows]; num_cols];
    let mut f64_cols: Vec<Vec<Option<f64>>> = vec![vec![None; num_rows]; num_cols];
    let mut str_cols: Vec<Vec<Option<String>>> = vec![vec![None; num_rows]; num_cols];
    let mut ts_cols: Vec<Vec<Option<i64>>> = vec![vec![None; num_rows]; num_cols];

    // Track which type each column actually is per the schema
    let col_types: Vec<DataType> = col_names
        .iter()
        .map(|name| {
            let idx = schema.index_of(name).unwrap_or(0);
            schema.field(idx).data_type().clone()
        })
        .collect();

    for (row_idx, doc) in docs.iter().enumerate() {
        for (col_idx, col_name) in col_names.iter().enumerate() {
            let value = doc.get(col_name);
            let dt = &col_types[col_idx];

            match value {
                None => { /* leave as None */ }
                Some(bson_val) => {
                    match dt {
                        DataType::Boolean => {
                            bool_cols[col_idx][row_idx] = bson_val.as_bool();
                        }
                        DataType::Int32 => {
                            i32_cols[col_idx][row_idx] = bson_val.as_i32();
                        }
                        DataType::Int64 => {
                            i64_cols[col_idx][row_idx] = bson_val
                                .as_i64()
                                .or_else(|| bson_val.as_i32().map(|v| v as i64));
                        }
                        DataType::Float64 => {
                            f64_cols[col_idx][row_idx] = bson_val
                                .as_f64()
                                .or_else(|| bson_val.as_i32().map(|v| v as f64))
                                .or_else(|| bson_val.as_i64().map(|v| v as f64));
                        }
                        DataType::Timestamp(_, _) => {
                            ts_cols[col_idx][row_idx] = bson_val
                                .as_datetime()
                                .map(|dt| dt.timestamp_millis() * 1_000_000);
                        }
                        _ => {
                            // Utf8 or other — serialize to string
                            str_cols[col_idx][row_idx] = Some(bson_val_to_string(bson_val));
                        }
                    }
                }
            }
        }
    }

    // Build Arrow arrays
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_cols);
    for col_idx in 0..num_cols {
        let dt = &col_types[col_idx];
        let arr: ArrayRef = match dt {
            DataType::Boolean => Arc::new(BooleanArray::from(
                bool_cols[col_idx].iter().map(|v| *v).collect::<Vec<_>>(),
            )),
            DataType::Int32 => Arc::new(Int64Array::from(
                i32_cols[col_idx]
                    .iter()
                    .map(|v| v.map(|x| x as i64))
                    .collect::<Vec<_>>(),
            )),
            DataType::Int64 => Arc::new(Int64Array::from(i64_cols[col_idx].clone())),
            DataType::Float64 => Arc::new(Float64Array::from(f64_cols[col_idx].clone())),
            DataType::Timestamp(_, _) => Arc::new(Int64Array::from(ts_cols[col_idx].clone())),
            _ => Arc::new(StringArray::from(
                str_cols[col_idx]
                    .iter()
                    .map(|v| v.as_deref())
                    .collect::<Vec<_>>(),
            )),
        };
        arrays.push(arr);
    }

    RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

/// Serialize a Bson value to a string representation.
fn bson_val_to_string(val: &Bson) -> String {
    match val {
        Bson::Null => String::new(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Int32(i) => i.to_string(),
        Bson::Int64(i) => i.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::String(s) => s.clone(),
        Bson::Array(arr) => {
            let items: Vec<String> = arr.iter().map(bson_val_to_string).collect();
            format!("[{}]", items.join(", "))
        }
        Bson::Document(doc) => {
            // Use BSON's extended JSON for nested documents
            bson::to_bson(doc)
                .map(|b| format!("{}", b))
                .unwrap_or_else(|_| "{...}".to_string())
        }
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::DateTime(dt) => dt.to_chrono().format("%Y-%m-%d %H:%M:%S").to_string(),
        Bson::Binary(bin) => hex::encode(&bin.bytes),
        Bson::Decimal128(d) => d.to_string(),
        Bson::RegularExpression(re) => format!("/{}/{}", re.pattern, re.options),
        Bson::JavaScriptCode(code) => code.clone(),
        Bson::JavaScriptCodeWithScope(js) => js.code.clone(),
        Bson::Symbol(sym) => sym.clone(),
        _ => format!("{}", val),
    }
}

/// Extract the database name from a MongoDB connection URL.
///
/// URL format: mongodb://[user:pass@]host[:port][/db][?options]
fn extract_db_name(url: &str) -> CoreResult<String> {
    // Remove scheme
    let rest = url
        .strip_prefix("mongodb://")
        .or_else(|| url.strip_prefix("mongodb+srv://"))
        .ok_or_else(|| {
            SwanFlowError::unknown("Invalid MongoDB URL: missing 'mongodb://' prefix")
        })?;

    // Split off query string
    let without_query = rest.split('?').next().unwrap_or(rest);

    // Split off userinfo@
    let after_at = if let Some(pos) = without_query.rfind('@') {
        &without_query[pos + 1..]
    } else {
        without_query
    };

    // Get the path (database name)
    if let Some(pos) = after_at.find('/') {
        let db = after_at[pos + 1..].to_string();
        if db.is_empty() || db == "admin" || db == "test" {
            Ok("rustflow".to_string())
        } else {
            Ok(db)
        }
    } else {
        // No database in URL, use default
        Ok("rustflow".to_string())
    }
}
