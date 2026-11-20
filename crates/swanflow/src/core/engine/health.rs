//! Health and metrics for the SwanFlow engine.
//!
//! Provides a snapshot of engine state for observability and health checks.


/// A snapshot of the engine's current health and metrics.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EngineHealth {
    /// Whether the engine is ready to execute workflows.
    pub ready: bool,
    /// Number of registered node types.
    pub registered_node_count: usize,
    /// Current cache directory.
    pub cache_dir: String,
    /// Current cache size in MB.
    pub cache_size_mb: f64,
    /// Max cache size in MB.
    pub max_cache_size_mb: u64,
    /// Node execution timeout in seconds.
    pub node_timeout_seconds: u64,
    /// Max rows per node.
    pub max_rows_per_node: u64,
    /// Engine mode (DataFusion, DuckDB, Auto).
    pub engine_mode: String,
    /// Version string from Cargo.toml.
    pub version: String,
    /// Number of completed checkpoint runs.
    pub completed_runs: usize,
    /// Number of incomplete (in-progress) checkpoint runs.
    pub in_progress_runs: usize,
}

impl EngineHealth {
    /// JSON representation for HTTP responses.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::json!({ "error": "serialization failed" }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_health_serialization() {
        let health = EngineHealth {
            ready: true,
            registered_node_count: 42,
            cache_dir: "/tmp/swanflow".to_string(),
            cache_size_mb: 12.5,
            max_cache_size_mb: 1024,
            node_timeout_seconds: 3600,
            max_rows_per_node: 10_000_000,
            engine_mode: "DuckDB".to_string(),
            version: "0.1.0".to_string(),
            completed_runs: 10,
            in_progress_runs: 1,
        };

        let json = health.to_json();
        assert_eq!(json["ready"], true);
        assert_eq!(json["registered_node_count"], 42);
        assert_eq!(json["engine_mode"], "DuckDB");
        assert_eq!(json["cache_size_mb"], 12.5);
    }

    #[test]
    fn test_engine_health_default_not_ready() {
        let health = EngineHealth {
            ready: false,
            registered_node_count: 0,
            cache_dir: String::new(),
            cache_size_mb: 0.0,
            max_cache_size_mb: 0,
            node_timeout_seconds: 0,
            max_rows_per_node: 0,
            engine_mode: "unknown".to_string(),
            version: String::new(),
            completed_runs: 0,
            in_progress_runs: 0,
        };

        assert!(!health.ready);
        assert_eq!(health.to_json()["ready"], false);
    }
}
