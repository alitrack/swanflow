use thiserror::Error;

#[derive(Error, Debug)]
pub enum KnimeError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("ZIP error: {0}")]
    Zip(#[from] zip::result::ZipError),

    #[error("XML parse error: {0}")]
    Xml(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Missing workflow.knime in archive")]
    MissingWorkflow,

    #[error("Missing settings.xml for node {node_id}")]
    MissingSettings { node_id: String },

    #[error("Unknown node type: {type_id} — no SwanFlow mapping")]
    UnknownNodeType { type_id: String },

    #[error("SwanFlow error: {0}")]
    SwanFlow(#[from] anyhow::Error),

    #[error("Unsupported KNIME feature: {0}")]
    Unsupported(String),
}

pub type KnimeResult<T> = Result<T, KnimeError>;
