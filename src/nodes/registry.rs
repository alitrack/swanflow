use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{Result, anyhow};
use crate::nodes::NodeBehavior;

/// A thread-safe registry for node implementations.
/// Node types are indexed by their unique type_id.
pub struct NodeRegistry {
    nodes: HashMap<String, Arc<dyn NodeBehavior>>,
}

impl NodeRegistry {
    /// Create a new, empty node registry.
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Register a node implementation.
    pub fn register_node(&mut self, behavior: Arc<dyn NodeBehavior>) {
        let type_id = behavior.spec().type_id;
        self.nodes.insert(type_id, behavior);
    }

    /// Retrieve a node implementation by its type identifier.
    pub fn get_node(&self, type_id: &str) -> Result<Arc<dyn NodeBehavior>> {
        self.nodes
            .get(type_id)
            .cloned()
            .ok_or_else(|| anyhow!("Node type '{}' not found in registry", type_id))
    }

    /// List all registered node specifications.
    pub fn list_specs(&self) -> Vec<crate::nodes::NodeSpec> {
        self.nodes.values().map(|n| n.spec()).collect()
    }

    /// Register all fundamental SwanFlow nodes.
    /// 
    /// Note: AI nodes (ConfigWizard, McpNode) are not registered here as they
    /// require runtime configuration (API keys, tool names) and are used as
    /// helpers rather than direct workflow nodes.
    pub fn register_core_nodes(&mut self) {
        use crate::nodes::sql::SqlNode;
        use crate::nodes::io::{CsvReadNode, ParquetReadNode, UniversalReadNode, DbConnectorNode, ExcelReadNode, ExcelWriteNode, ExcelTemplateNode, ExcelReportNode, ExcelSheetNamesNode, DuckDBReadNode, DuckDBWriteNode, HttpFetchNode, GraphIndexWriterNode};
        use crate::nodes::boundary::{InputNode, OutputNode, DuckDBExtensionNode};
        use crate::nodes::transform::{FilterNode, ProjectNode, AggregateNode, WindowNode, ConditionNode, ForEachNode, ProfilerNode, VariableCreatorNode, TableToVariableNode, GraphMergerNode, TableSchemaValidatorNode, SemanticCleanerNode, AiRouterNode};
        use crate::nodes::script::python::{PythonNode, PythonUdfNode};
        use crate::nodes::ai::mcp::McpInvokeNode;
        use crate::nodes::ai::graph::{GraphRetrieverNode, ContextFuserNode, EntityExtractorNode, ResearchLoopMetaNode};
        use crate::nodes::ai::providers::{OpenAiProvider, LlmConfig, ProviderKind};

        self.register_node(Arc::new(SqlNode));
        self.register_node(Arc::new(CsvReadNode));
        self.register_node(Arc::new(ParquetReadNode));
        self.register_node(Arc::new(UniversalReadNode));
        self.register_node(Arc::new(DbConnectorNode));
        self.register_node(Arc::new(ExcelReadNode));
        self.register_node(Arc::new(ExcelWriteNode));
        self.register_node(Arc::new(ExcelTemplateNode));
        self.register_node(Arc::new(ExcelReportNode));
        self.register_node(Arc::new(ExcelSheetNamesNode));
        self.register_node(Arc::new(DuckDBReadNode));
        self.register_node(Arc::new(DuckDBWriteNode));
        self.register_node(Arc::new(HttpFetchNode));
        self.register_node(Arc::new(GraphIndexWriterNode));
        self.register_node(Arc::new(InputNode));
        self.register_node(Arc::new(OutputNode));
        self.register_node(Arc::new(DuckDBExtensionNode));
        self.register_node(Arc::new(FilterNode));
        self.register_node(Arc::new(ProjectNode));
        self.register_node(Arc::new(AggregateNode));
        self.register_node(Arc::new(WindowNode));
        self.register_node(Arc::new(ConditionNode));
        self.register_node(Arc::new(ForEachNode));
        self.register_node(Arc::new(ProfilerNode));
        self.register_node(Arc::new(VariableCreatorNode));
        self.register_node(Arc::new(TableToVariableNode));
        self.register_node(Arc::new(GraphMergerNode));
        self.register_node(Arc::new(TableSchemaValidatorNode));
        self.register_node(Arc::new(SemanticCleanerNode));
        self.register_node(Arc::new(AiRouterNode));
        self.register_node(Arc::new(PythonNode));
        // Note: PythonUdfNode is registered but currently returns empty plan (stub implementation)
        self.register_node(Arc::new(PythonUdfNode));
        self.register_node(Arc::new(GraphRetrieverNode));
        self.register_node(Arc::new(ContextFuserNode));
        self.register_node(Arc::new(McpInvokeNode));
        self.register_node(Arc::new(ResearchLoopMetaNode::new()));
        
        // Register EntityExtractor if environment variables are set
        if let Ok(model) = std::env::var("LLM_MODEL") {
            println!("[DEBUG] Found LLM_MODEL: {}", model);
            let config = LlmConfig {
                provider: ProviderKind::OpenAI,
                api_key: std::env::var("LLM_API_KEY").ok(),
                model,
                endpoint: std::env::var("LLM_API_BASE").ok(),
            };
            match OpenAiProvider::new(config) {
                Ok(provider) => {
                    println!("[DEBUG] Registering EntityExtractorNode");
                    self.register_node(Arc::new(EntityExtractorNode { provider: Arc::new(provider) }));
                }
                Err(e) => println!("[DEBUG] Failed to create OpenAiProvider: {}", e),
            }
        } else if let Ok(provider) = OpenAiProvider::new(LlmConfig {
            provider: ProviderKind::OpenAI,
            api_key: std::env::var("OPENAI_API_KEY").ok(), 
            model: "gpt-4o".to_string(),
            endpoint: None,
        }) {
             println!("[DEBUG] Registering EntityExtractorNode (Fallback)");
             self.register_node(Arc::new(EntityExtractorNode { provider: Arc::new(provider) }));
        } else {
            println!("[DEBUG] No LLM_MODEL or OPENAI_API_KEY found, EntityExtractor skip");
        }
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut registry = Self::new();
        registry.register_core_nodes();
        registry
    }
}
