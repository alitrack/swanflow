//! SwanFlow KNIME Integration
//!
//! Two-way compatibility:
//! 1. **Import**: `.knwf` → SwanFlow `Workflow` — use KNIME as SwanFlow's visual editor
//! 2. **Export**: SwanFlow `Workflow` → `.knwf` — share workflows with KNIME users
//!
//! # Architecture
//!
//! ```text
//! KNIME .knwf (ZIP)          SwanFlow
//! ┌──────────────────┐       ┌──────────────────┐
//! │ workflow.knime    │ ───→  │ KnwfParser       │
//! │ Node #0/          │       │   ↓              │
//! │   settings.xml    │       │ KnimeNodeMapper  │
//! │ Node #1/          │       │   ↓              │
//! │   settings.xml    │       │ Workflow builder │
//! │ ...               │       │   ↓              │
//! └──────────────────┘       │ SwanFlowEngine   │
//!                            └──────────────────┘
//! ```

pub mod error;
pub mod mapper;
pub mod parser;
pub mod types;

pub use error::{KnimeError, KnimeResult};
pub use parser::KnwfParser;
pub use mapper::KnimeNodeMapper;

use swanflow::core::graph::Workflow;

/// Import a .knwf file and convert to SwanFlow Workflow.
pub async fn import_knwf(path: &str) -> KnimeResult<Workflow> {
    let knwf = KnwfParser::parse_file(path)?;
    let mapper = KnimeNodeMapper::new();
    mapper.convert(knwf).await
}

/// Import .knwf bytes directly (e.g., from HTTP upload).
pub async fn import_knwf_bytes(data: &[u8]) -> KnimeResult<Workflow> {
    let knwf = KnwfParser::parse_bytes(data)?;
    let mapper = KnimeNodeMapper::new();
    mapper.convert(knwf).await
}
