//! SwanFlow Connectors — formal database connectors implementing the Connector trait.
//!
//! Each connector provides `list_tables`, `get_schema`, and `scan` operations
//! backed by Arrow record batches, enabling seamless integration with the
//! DataFusion query engine.

pub mod clickhouse;
pub mod mock;
#[cfg(feature = "extended-connectors")]
pub mod redis;
#[cfg(feature = "extended-connectors")]
pub mod sqlserver;

// Re-export the Connector trait and ConnConfig from core for convenience.
pub use crate::core::datasource::{ConnConfig, Connector};
