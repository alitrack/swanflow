//! DBX-compatible connection configuration format.
//!
//! This module defines a connection config schema that mirrors DBX's
//! `ConnectionConfig` struct, enabling config reuse between SwanFlow and DBX.
//!
//! ## DBX Compatibility
//! - Same schema shape: `name`, `db_type`, `host`, `port`, `user`, `password`, `database`
//! - Same database type names (lowercase enum serialization)
//! - Can read `.dbx/config/profiles/*.yaml` files
//! - Can write SwanFlow configs that DBX can import
//!
//! Usage:
//!   ```yaml
//!   # swanflow-connection.yaml
//!   name: "Production PG"
//!   db_type: postgres
//!   host: localhost
//!   port: 5432
//!   user: analyst
//!   password: "***"
//!   database: analytics
//!   ssl: true
//!   connect_timeout: 30
//!   query_timeout: 60
//!   ```

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::core::datasource::ConnConfig;

/// SwanFlow connection config, compatible with DBX's shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwanFlowConnection {
    /// Unique identifier (auto-generated if empty)
    #[serde(default)]
    pub id: String,

    /// Human-readable name
    pub name: String,

    /// Database type — matches DBX's `DatabaseType` enum (lowercase)
    pub db_type: String,

    /// Hostname
    #[serde(default = "default_host")]
    pub host: String,

    /// Port
    pub port: u16,

    /// Username
    #[serde(default)]
    pub user: String,

    /// Password
    #[serde(default)]
    pub password: String,

    /// Database name
    pub database: Option<String>,

    /// SSL/TLS enabled
    #[serde(default)]
    pub ssl: bool,

    /// Connect timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: u64,

    /// Query timeout in seconds
    #[serde(default = "default_query_timeout")]
    pub query_timeout: u64,

    /// Read-only mode (prevents writes)
    #[serde(default)]
    pub read_only: bool,

    /// Extra connection parameters (driver-specific)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extra: Option<HashMap<String, String>>,
}

fn default_host() -> String {
    "localhost".to_string()
}
fn default_connect_timeout() -> u64 {
    30
}
fn default_query_timeout() -> u64 {
    60
}

impl SwanFlowConnection {
    /// Build a connection URL from the config fields.
    pub fn build_url(&self) -> Result<String> {
        let host = &self.host;
        let port = self.port;
        let user = &self.user;
        let password = &self.password;
        let database = self.database.as_deref().unwrap_or("");

        let url = match self.db_type.to_lowercase().as_str() {
            "postgres" | "postgresql" => {
                format!(
                    "postgres://{}:{}@{}:{}/{}",
                    user, password, host, port, database
                )
            }
            "mysql" | "mariadb" => {
                format!(
                    "mysql://{}:{}@{}:{}/{}",
                    user, password, host, port, database
                )
            }
            "clickhouse" | "clickhouse_http" => {
                let scheme = if self.ssl { "https" } else { "http" };
                format!("{}://{}:{}@{}:{}", scheme, user, password, host, port)
            }
            "sqlite" => {
                format!("sqlite:{}", database)
            }
            "sqlserver" => {
                format!(
                    "sqlserver://{}:{}@{}:{};database={}",
                    user, password, host, port, database
                )
            }
            "mongodb" => {
                let extra = self.extra.as_ref();
                let auth_source = extra
                    .and_then(|e| e.get("auth_source"))
                    .map(|s| format!("/?authSource={}", s))
                    .unwrap_or_default();
                let db_part = if database.is_empty() {
                    String::new()
                } else {
                    format!("/{}", database)
                };
                format!("mongodb://{}:{}{}{}", host, port, db_part, auth_source)
            }
            "redis" => {
                format!(
                    "redis://{}:{}@{}:{}/{}",
                    user, password, host, port, database
                )
            }
            "duckdb" => database.to_string(),
            "elasticsearch" => {
                let scheme = if self.ssl { "https" } else { "http" };
                format!("{}://{}:{}@{}:{}", scheme, user, password, host, port)
            }
            _ => {
                // Generic JDBC-style fallback
                format!(
                    "{}://{}:{}@{}:{}/{}",
                    self.db_type, user, password, host, port, database
                )
            }
        };

        Ok(url)
    }

    /// Convert to SwanFlow's internal ConnConfig.
    pub fn to_conn_config(&self) -> Result<ConnConfig> {
        let url = self.build_url()?;
        Ok(ConnConfig {
            url,
            driver: self.db_type.clone(),
            max_connections: 10,
            timeout_secs: self.connect_timeout,
        })
    }

    /// Save this config as a YAML file.
    pub fn save_to_yaml(&self, path: &Path) -> Result<()> {
        let yaml = serde_yaml::to_string(self)?;
        std::fs::write(path, yaml)?;
        Ok(())
    }

    /// Load from a YAML file.
    pub fn load_from_yaml(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: SwanFlowConnection = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Load multiple connection files from a directory.
    pub fn load_dir(dir: &Path) -> Result<Vec<Self>> {
        let mut configs = Vec::new();
        if !dir.exists() {
            return Ok(configs);
        }
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path
                .extension()
                .map(|e| e == "yaml" || e == "yml")
                .unwrap_or(false)
            {
                match Self::load_from_yaml(&path) {
                    Ok(cfg) => configs.push(cfg),
                    Err(e) => {
                        eprintln!(
                            "Warning: failed to load connection '{}': {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }
        Ok(configs)
    }

    /// Load DBX profiles from `~/.dbx/config/profiles/` directory.
    pub fn load_dbx_profiles() -> Result<Vec<Self>> {
        let home = std::env::var("HOME").unwrap_or_else(|_| "~".to_string());
        let dbx_profiles = Path::new(&home)
            .join(".dbx")
            .join("config")
            .join("profiles");
        Self::load_dir(&dbx_profiles)
    }
}

/// All supported database types (mirrors DBX's DatabaseType enum).
pub const SUPPORTED_DATABASES: &[(&str, &str)] = &[
    ("postgres", "PostgreSQL"),
    ("postgresql", "PostgreSQL (alias)"),
    ("mysql", "MySQL / MariaDB"),
    ("clickhouse", "ClickHouse"),
    ("sqlite", "SQLite"),
    ("sqlserver", "SQL Server"),
    ("mongodb", "MongoDB"),
    ("redis", "Redis"),
    ("duckdb", "DuckDB (local file)"),
    ("elasticsearch", "Elasticsearch"),
    ("influxdb", "InfluxDB"),
    ("oracle", "Oracle (via JDBC agent)"),
    ("bigquery", "Google BigQuery"),
    ("snowflake", "Snowflake"),
    ("redshift", "Amazon Redshift"),
    ("trino", "Trino / Presto"),
    ("hive", "Apache Hive"),
    ("cassandra", "Apache Cassandra"),
    ("neo4j", "Neo4j"),
    ("doris", "Apache Doris"),
    ("starrocks", "StarRocks"),
    ("databend", "Databend"),
    ("saphana", "SAP HANA"),
    ("teradata", "Teradata"),
    ("vertica", "Vertica"),
    ("db2", "IBM DB2"),
    ("tdengine", "TDengine"),
    ("questdb", "QuestDB"),
];

/// Get the default port for a database type.
pub fn default_port(db_type: &str) -> u16 {
    match db_type {
        "postgres" | "postgresql" => 5432,
        "mysql" | "mariadb" => 3306,
        "clickhouse" => 8123,
        "sqlserver" => 1433,
        "mongodb" => 27017,
        "redis" => 6379,
        "elasticsearch" => 9200,
        "influxdb" => 8086,
        "oracle" => 1521,
        "bigquery" => 443,
        "snowflake" => 443,
        "redshift" => 5439,
        "trino" => 8080,
        "hive" => 10000,
        "cassandra" => 9042,
        "neo4j" => 7687,
        "doris" => 9030,
        "starrocks" => 9030,
        "databend" => 8000,
        "saphana" => 39013,
        "teradata" => 1025,
        "vertica" => 5433,
        "db2" => 50000,
        "tdengine" => 6030,
        "questdb" => 9000,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_postgres_url() {
        let cfg = SwanFlowConnection {
            id: String::new(),
            name: "test".into(),
            db_type: "postgres".into(),
            host: "localhost".into(),
            port: 5432,
            user: "admin".into(),
            password: "secret".into(),
            database: Some("mydb".into()),
            ssl: false,
            connect_timeout: 30,
            query_timeout: 60,
            read_only: false,
            extra: None,
        };
        assert_eq!(
            cfg.build_url().unwrap(),
            "postgres://admin:secret@localhost:5432/mydb"
        );
    }

    #[test]
    fn test_mysql_url() {
        let cfg = SwanFlowConnection {
            id: String::new(),
            name: "test".into(),
            db_type: "mysql".into(),
            host: "db.example.com".into(),
            port: 3306,
            user: "root".into(),
            password: "pass".into(),
            database: Some("analytics".into()),
            ssl: true,
            connect_timeout: 30,
            query_timeout: 60,
            read_only: false,
            extra: None,
        };
        assert_eq!(
            cfg.build_url().unwrap(),
            "mysql://root:pass@db.example.com:3306/analytics"
        );
    }

    #[test]
    fn test_clickhouse_url() {
        let cfg = SwanFlowConnection {
            id: String::new(),
            name: "test".into(),
            db_type: "clickhouse".into(),
            host: "ch.example.com".into(),
            port: 8123,
            user: "default".into(),
            password: "".into(),
            database: None,
            ssl: false,
            connect_timeout: 30,
            query_timeout: 60,
            read_only: false,
            extra: None,
        };
        assert_eq!(
            cfg.build_url().unwrap(),
            "http://default:@ch.example.com:8123"
        );
    }

    #[test]
    fn test_sqlite_url() {
        let cfg = SwanFlowConnection {
            id: String::new(),
            name: "test".into(),
            db_type: "sqlite".into(),
            host: "localhost".into(),
            port: 0,
            user: String::new(),
            password: String::new(),
            database: Some("/data/mydb.sqlite".into()),
            ssl: false,
            connect_timeout: 30,
            query_timeout: 60,
            read_only: false,
            extra: None,
        };
        assert_eq!(cfg.build_url().unwrap(), "sqlite:/data/mydb.sqlite");
    }

    #[test]
    fn test_default_port() {
        assert_eq!(default_port("postgres"), 5432);
        assert_eq!(default_port("mysql"), 3306);
        assert_eq!(default_port("mongodb"), 27017);
        assert_eq!(default_port("sqlserver"), 1433);
        assert_eq!(default_port("unknown"), 0);
    }

    #[test]
    fn test_roundtrip_yaml() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let path = dir.path().join("test-conn.yaml");

        let cfg = SwanFlowConnection {
            id: String::new(),
            name: "Test PG".into(),
            db_type: "postgres".into(),
            host: "10.0.0.1".into(),
            port: 5432,
            user: "analyst".into(),
            password: "s3cret".into(),
            database: Some("warehouse".into()),
            ssl: true,
            connect_timeout: 15,
            query_timeout: 120,
            read_only: false,
            extra: None,
        };
        cfg.save_to_yaml(&path).unwrap();
        let loaded = SwanFlowConnection::load_from_yaml(&path).unwrap();
        assert_eq!(loaded.name, "Test PG");
        assert_eq!(loaded.host, "10.0.0.1");
        assert_eq!(loaded.build_url().unwrap(), cfg.build_url().unwrap());
    }
}
