pub mod compiler;
pub mod connection_config;
pub mod datasource;
pub mod engine;
pub mod error;
pub mod graph;
pub mod kernel;
pub mod observer;
pub mod repair;
pub mod retry;
pub mod templating;

/// Canonical in-context table name for a node's materialization.
///
/// Uses the FULL node uuid — an 8-char prefix collides whenever node ids
/// share a prefix (e.g. hand-written yaml ids like a0000000-...-0101/0102),
/// which caused `register_parquet` "table already exists" failures and
/// `sync_metadata_to_context` silently overwriting views.
pub fn node_table_name(node_id: uuid::Uuid) -> String {
    format!("node_{}", node_id)
}
