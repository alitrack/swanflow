//! SQL Server connector — uses tiberius crate for TDS protocol access.
//!
//! tiberius is a pure Rust TDS (Tabular Data Stream) implementation
//! that connects to Microsoft SQL Server via tokio async I/O.
//! It uses TLS via rustls for secure connections.

use std::sync::Arc;
use std::time::Duration;

use crate::core::error::{CoreResult, SwanFlowError};
use async_trait::async_trait;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::stream::RecordBatchReceiverStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use tiberius::{AuthMethod, Client, Config, Query};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::core::datasource::{ConnConfig, Connector, DynamicArrayBuilder};

/// SQL Server database connector using tiberius.
pub struct SqlServerConnector {
    _config: ConnConfig,
    server: String,
    port: u16,
    database: String,
    username: String,
    password: String,
    _timeout_secs: u64,
}

impl SqlServerConnector {
    /// Parse connection URL of the form:
    /// mssql://username:password@host:port/database
    /// or
    /// Server=host,port;Database=db;User Id=user;Password=pass;
    fn parse_url(url: &str) -> Result<(String, u16, String, String, String)> {
        if url.starts_with("mssql://") || url.starts_with("sqlserver://") {
            Self::parse_jdbc_url(url)
        } else {
            Self::parse_odbc_url(url)
        }
    }

    fn parse_jdbc_url(url: &str) -> Result<(String, u16, String, String, String)> {
        let without_scheme = url
            .strip_prefix("mssql://")
            .or_else(|| url.strip_prefix("sqlserver://"))
            .unwrap_or(url);

        let (userinfo, rest) = without_scheme.split_once('@').ok_or_else(|| {
            SwanFlowError::unknown("SQL Server URL missing '@': expected user:pass@host:port/db")
        })?;

        let (username, password) = userinfo
            .split_once(':')
            .ok_or_else(|| SwanFlowError::unknown("SQL Server URL missing ':' in user:password"))?;

        let (host_part, database) = rest
            .split_once('/')
            .ok_or_else(|| SwanFlowError::unknown("SQL Server URL missing database path"))?;

        let (server, port) = if let Some((h, p)) = host_part.split_once(':') {
            let port: u16 = p
                .parse()
                .map_err(|e| SwanFlowError::unknown(format!("Invalid port: {}", e)))?;
            (h.to_string(), port)
        } else {
            (host_part.to_string(), 1433)
        };

        Ok((
            server,
            port,
            database.to_string(),
            username.to_string(),
            password.to_string(),
        ))
    }

    fn parse_odbc_url(url: &str) -> Result<(String, u16, String, String, String)> {
        let mut server = String::new();
        let mut port = 1433u16;
        let mut database = String::new();
        let mut username = String::new();
        let mut password = String::new();

        for part in url.split(';') {
            let part = part.trim();
            if let Some((key, value)) = part.split_once('=') {
                match key.trim().to_lowercase().as_str() {
                    "server" => {
                        if let Some((h, p)) = value.trim().split_once(',') {
                            server = h.trim().to_string();
                            port = p.trim().parse().unwrap_or(1433);
                        } else {
                            server = value.trim().to_string();
                        }
                    }
                    "database" => database = value.trim().to_string(),
                    "user id" | "uid" => username = value.trim().to_string(),
                    "password" | "pwd" => password = value.trim().to_string(),
                    _ => {}
                }
            }
        }

        if server.is_empty() || database.is_empty() {
            return Err(SwanFlowError::unknown(
                "ODBC-style URL must contain Server and Database",
            ));
        }

        Ok((server, port, database, username, password))
    }

    /// Create a tiberius client connection.
    async fn connect_client(&self) -> Result<Client<tokio_util::compat::Compat<TcpStream>>> {
        let mut config = Config::new();
        config.host(&self.server);
        config.port(self.port);
        config.database(&self.database);
        config.authentication(AuthMethod::sql_server(&self.username, &self.password));
        config.trust_cert();

        let tcp = TcpStream::connect(format!("{}:{}", self.server, self.port))
            .await
            .map_err(|e| {
                SwanFlowError::unknown(format!("TCP connection to SQL Server failed: {}", e))
            })?;

        let client = Client::connect(config, tcp.compat())
            .await
            .map_err(|e| SwanFlowError::unknown(format!("SQL Server connection failed: {}", e)))?;

        Ok(client)
    }
}

#[async_trait]
impl Connector for SqlServerConnector {
    async fn connect(config: ConnConfig) -> CoreResult<Self> {
        let _timeout = Duration::from_secs(config.timeout_secs);
        let (server, port, database, username, password) = Self::parse_url(&config.url)?;

        let connector = Self {
            _config: config,
            server,
            port,
            database,
            username,
            password,
            _timeout_secs: _timeout.as_secs(),
        };

        // Validate connection
        let _client = connector.connect_client().await?;

        Ok(connector)
    }

    async fn list_tables(&self) -> CoreResult<Vec<String>> {
        let mut client = self.connect_client().await?;

        let stream = client
            .simple_query(
                "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
                 WHERE TABLE_TYPE = 'BASE TABLE' \
                 ORDER BY TABLE_NAME",
            )
            .await
            .map_err(|e| {
                SwanFlowError::unknown(format!("Failed to list SQL Server tables: {}", e))
            })?;

        let results = stream.into_results().await.map_err(|e| {
            SwanFlowError::unknown(format!("Failed to read SQL Server table list: {}", e))
        })?;

        let mut tables = Vec::new();
        for row in results.iter().flat_map(|r| r.iter()) {
            if let Some(name) = row.get::<&str, _>(0) {
                tables.push(name.to_string());
            }
        }

        Ok(tables)
    }

    async fn get_schema(&self, table: &str) -> CoreResult<SchemaRef> {
        let mut client = self.connect_client().await?;

        let stream = client
            .simple_query(&format!(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME = '{}' \
                 ORDER BY ORDINAL_POSITION",
                table.replace('\'', "''")
            ))
            .await
            .map_err(|e| {
                SwanFlowError::unknown(format!("Failed to get schema for table '{}': {}", table, e))
            })?;

        let results = stream.into_results().await.map_err(|e| {
            SwanFlowError::unknown(format!(
                "Failed to read schema for table '{}': {}",
                table, e
            ))
        })?;

        let mut fields = Vec::new();
        for row in results.iter().flat_map(|r| r.iter()) {
            let name = row
                .get::<&str, _>(0)
                .ok_or_else(|| SwanFlowError::unknown("Missing column name"))?
                .to_string();
            let sql_type = row
                .get::<&str, _>(1)
                .ok_or_else(|| {
                    SwanFlowError::unknown(format!("Missing data type for column '{}'", name))
                })?
                .to_string();
            let nullable_str = row.get::<&str, _>(2).unwrap_or("YES");
            let nullable = nullable_str.eq_ignore_ascii_case("YES");

            let arrow_type = sqlserver_type_to_arrow(&sql_type);
            fields.push(Field::new(&name, arrow_type, nullable));
        }

        if fields.is_empty() {
            return Err(SwanFlowError::unknown(format!(
                "Table '{}' not found in SQL Server",
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
        // P2-1a: quote each column/table identifier (bracket syntax, ] escaped).
        let select_cols = col_names
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        let query_str = format!("SELECT {} FROM {}", select_cols, quote_ident(table));

        let server = self.server.clone();
        let port = self.port;
        let database = self.database.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let proj_schema = projected_schema.clone();

        let mut builder = RecordBatchReceiverStream::builder(projected_schema, 1);
        let tx = builder.tx();

        builder.spawn(async move {
            // Build a fresh connection for this scan
            let mut config = Config::new();
            config.host(&server);
            config.port(port);
            config.database(&database);
            config.authentication(AuthMethod::sql_server(&username, &password));
            config.trust_cert();

            let tcp = TcpStream::connect(format!("{}:{}", server, port))
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let mut client = Client::connect(config, tcp.compat())
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let stream = client
                .simple_query(&query_str)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let results = stream
                .into_results()
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            let rows: Vec<&tiberius::Row> = results.iter().flat_map(|r| r.iter()).collect();
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
                        append_sqlserver_value(builder, row, i);
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
        "sqlserver"
    }
}

/// Quote a SQL Server identifier with brackets, escaping embedded `]` as `]]`.
/// P2-1a: closes identifier-injection via table/column names.
fn quote_ident(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Map SQL Server type names to Arrow DataType.
fn sqlserver_type_to_arrow(sql_type: &str) -> DataType {
    let upper = sql_type.to_uppercase();
    match upper.as_str() {
        "BIT" => DataType::Boolean,
        "TINYINT" => DataType::UInt8,
        "SMALLINT" => DataType::Int16,
        "INT" | "INTEGER" => DataType::Int32,
        "BIGINT" => DataType::Int64,
        "REAL" => DataType::Float32,
        "FLOAT" | "DOUBLE" | "DECIMAL" | "NUMERIC" | "MONEY" | "SMALLMONEY" => DataType::Float64,
        "DATE" => DataType::Date32,
        "DATETIME" | "DATETIME2" | "SMALLDATETIME" | "TIMESTAMP" => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "CHAR" | "VARCHAR" | "TEXT" | "NCHAR" | "NVARCHAR" | "NTEXT" | "XML"
        | "UNIQUEIDENTIFIER" | "SQL_VARIANT" => DataType::Utf8,
        "BINARY" | "VARBINARY" | "IMAGE" => DataType::Utf8,
        _ => {
            if upper.starts_with("DECIMAL") || upper.starts_with("NUMERIC") {
                DataType::Float64
            } else if upper.starts_with("VARCHAR")
                || upper.starts_with("NVARCHAR")
                || upper.starts_with("CHAR")
                || upper.starts_with("NCHAR")
            {
                DataType::Utf8
            } else {
                DataType::Utf8
            }
        }
    }
}

/// Append a value from a tiberius Row to a DynamicArrayBuilder by column index.
fn append_sqlserver_value(builder: &mut DynamicArrayBuilder, row: &tiberius::Row, col_idx: usize) {
    match builder {
        DynamicArrayBuilder::Int64(b) => {
            if let Some(val) = row.get::<i64, _>(col_idx) {
                b.append_value(val);
            } else if let Some(val) = row.get::<i32, _>(col_idx) {
                b.append_value(val as i64);
            } else if let Some(val) = row.get::<i16, _>(col_idx) {
                b.append_value(val as i64);
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Float64(b) => {
            if let Some(val) = row.get::<f64, _>(col_idx) {
                b.append_value(val);
            } else if let Some(val) = row.get::<f32, _>(col_idx) {
                b.append_value(val as f64);
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Boolean(b) => {
            if let Some(val) = row.get::<bool, _>(col_idx) {
                b.append_value(val);
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Timestamp(b) => {
            if let Some(val) = row.get::<chrono::NaiveDateTime, _>(col_idx) {
                b.append_value(val.and_utc().timestamp_nanos_opt().unwrap_or(0));
            } else {
                b.append_null();
            }
        }
        DynamicArrayBuilder::Utf8(b) => {
            if let Some(val) = row.get::<&str, _>(col_idx) {
                b.append_value(val);
            } else {
                b.append_null();
            }
        }
    }
}
