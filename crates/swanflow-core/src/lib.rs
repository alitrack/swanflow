// swanflow-core — foundational types
pub mod config;
pub mod error;
pub mod graph;
pub mod kernel;
pub mod missing;
pub mod nodes;
pub mod registry;

// Re-export commonly used types
pub use error::SwanFlowError;
pub use graph::{EdgeInstance, ErrorMode, NodeInstance, StepMode, Workflow};
pub use nodes::{
    GraphSchemas, InferredMetadata, NodeBehavior, NodeCapability, NodeDiagnostic,
    NodeErrorClass, NodeEvent, NodeSpec, NodeState, PortSpec,
};
