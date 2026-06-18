use serde::{Deserialize, Serialize};

/// Describes a SwanFlow plugin package.
///
/// A plugin package can contain one or more node types.
/// This manifest is used by SwanFlow's plugin loader for
/// discovery, validation, and registration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Unique plugin identifier (e.g. "myorg/my-postgres-connector").
    pub name: String,

    /// Human-readable display name.
    pub display_name: String,

    /// Semantic version of the plugin.
    pub version: String,

    /// Short description of what the plugin provides.
    pub description: String,

    /// Author / organization.
    #[serde(default)]
    pub author: String,

    /// URL to homepage or repository.
    #[serde(default)]
    pub homepage: String,

    /// Minimum SwanFlow engine version required.
    #[serde(default = "default_min_version")]
    pub min_swanflow_version: String,

    /// List of node type_ids provided by this plugin.
    #[serde(default)]
    pub nodes: Vec<PluginNodeEntry>,
}

fn default_min_version() -> String {
    "0.1.0".to_string()
}

/// A single node type entry in the plugin manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginNodeEntry {
    /// The unique type_id (e.g. "myorg.connectors.bigquery").
    pub type_id: String,

    /// Human-readable display name.
    pub display_name: String,

    /// Short description.
    pub description: String,

    /// JSON Schema for the node's configuration.
    #[serde(default)]
    pub config_schema: serde_json::Value,
}

impl PluginManifest {
    /// Create a new manifest with required fields.
    pub fn new(name: &str, display_name: &str, version: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            display_name: display_name.to_string(),
            version: version.to_string(),
            description: description.to_string(),
            author: String::new(),
            homepage: String::new(),
            min_swanflow_version: default_min_version(),
            nodes: Vec::new(),
        }
    }

    /// Add a node entry to this manifest.
    pub fn with_node(mut self, type_id: &str, display_name: &str, description: &str) -> Self {
        self.nodes.push(PluginNodeEntry {
            type_id: type_id.to_string(),
            display_name: display_name.to_string(),
            description: description.to_string(),
            config_schema: serde_json::Value::Object(Default::default()),
        });
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_serialization() {
        let manifest = PluginManifest::new(
            "alitrack/postgres-advanced",
            "PostgreSQL Advanced Connector",
            "0.2.0",
            "Advanced Postgres connector with query pushdown",
        )
        .with_node(
            "alitrack.connectors.postgres_advanced",
            "Postgres Advanced",
            "PostgreSQL connector with materialized views",
        );

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("alitrack/postgres-advanced"));
        assert!(json.contains("0.2.0"));

        let parsed: PluginManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.nodes.len(), 1);
        assert_eq!(parsed.nodes[0].type_id, "alitrack.connectors.postgres_advanced");
    }
}
