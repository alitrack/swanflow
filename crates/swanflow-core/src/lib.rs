// swanflow-core — foundational types
pub mod config;
pub mod error;
pub mod graph;
pub mod kernel;
pub mod missing;
pub mod nodes;
pub mod registry;
pub mod validation;

// Re-export commonly used types
pub use error::SwanFlowError;
pub use graph::{EdgeInstance, ErrorMode, NodeInstance, StepMode, Workflow};
pub use nodes::{
    DesignerMetadata, GraphSchemas, InferredMetadata, NodeBehavior, NodeCapability,
    NodeCategory, NodeDiagnostic, NodeErrorClass, NodeEvent, NodeSpec, NodeState, OptionChoice,
    OptionDesignMetadata, OptionValueType, Plugin, PortDesignMetadata, PortSpec, TypedNodeBehavior,
};
pub use registry::NodeRegistry;
pub use validation::SchemaValidator;
