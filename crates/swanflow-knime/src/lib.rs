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
pub mod shaper;
pub mod types;

pub use error::{KnimeError, KnimeResult};
pub use mapper::KnimeNodeMapper;
pub use mapper::KnimeNodeMapper as Mapper;
pub use mapper::MappingEntry;
pub use parser::KnwfParser;
pub use types::KnwfArchive;

use swanflow_core::graph::Workflow;

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

/// Parse a .knar workflow group archive and return all contained workflows.
pub fn parse_knar(path: &str) -> KnimeResult<Vec<(String, crate::types::KnwfArchive)>> {
    let file = std::fs::File::open(path).map_err(KnimeError::Io)?;
    let mut archive = zip::ZipArchive::new(file).map_err(KnimeError::Zip)?;

    // Find all top-level workflow directories (depth=3 within group root)
    let wf_paths: Vec<String> = (0..archive.len())
        .filter_map(|i| {
            let f = archive.by_index(i).ok()?;
            let name = f.name().to_string();
            if name.ends_with("workflow.knime") && name.matches('/').count() == 3 {
                Some(name)
            } else {
                None
            }
        })
        .collect();

    let mut results = Vec::new();
    let mut errors = Vec::new();

    for wf_path in &wf_paths {
        let prefix = wf_path
            .strip_suffix("workflow.knime")
            .unwrap_or("")
            .to_string();

        match KnwfParser::parse_zip_at(&mut archive, &prefix) {
            Ok(knwf) => {
                let name = prefix
                    .trim_end_matches('/')
                    .rsplit('/')
                    .next()
                    .unwrap_or("unknown")
                    .to_string();
                results.push((name, knwf));
            }
            Err(e) => {
                errors.push(format!("{}: {}", wf_path, e));
            }
        }
    }

    if results.is_empty() && !errors.is_empty() {
        return Err(KnimeError::Xml(format!(
            "All {} workflows failed: {}",
            errors.len(),
            errors.join("; ")
        )));
    }

    if !errors.is_empty() {
        tracing::warn!(
            "{} workflow(s) failed to parse out of {}: {:?}",
            errors.len(),
            wf_paths.len(),
            errors
        );
    }

    Ok(results)
}
