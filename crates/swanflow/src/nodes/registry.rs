use crate::nodes::NodeBehavior;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::Arc;

/// A thread-safe registry for node implementations.
/// Node types are indexed by their unique type_id.
pub struct NodeRegistry {
    nodes: HashMap<String, Arc<dyn NodeBehavior>>,
}

/// Prefixes owned by plugins that ship in this workspace but may not be
/// installed (compiled in). Used only to turn "node not found" into "plugin
/// not installed" — the distinction an old workflow needs to understand WHY
/// its quality node vanished after P1 moved them out of the core.
const PLUGIN_PREFIXES: &[(&str, &str)] = &[("core.quality.", "quality")];

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
        self.nodes.get(type_id).cloned().ok_or_else(|| {
            // Distinguish a typo'd id from a plugin that simply is not in
            // this build: same failure, different remedy.
            if let Some((_, plugin)) = PLUGIN_PREFIXES
                .iter()
                .find(|(prefix, _)| type_id.starts_with(prefix))
            {
                anyhow!(
                    "Node type '{}' belongs to the '{}' plugin, which is not installed \
                         in this build (build with --features {} to get it)",
                    type_id,
                    plugin,
                    plugin
                )
            } else {
                anyhow!("Node type '{}' not found in registry", type_id)
            }
        })
    }

    /// List all registered node specifications.
    pub fn list_specs(&self) -> Vec<crate::nodes::NodeSpec> {
        self.nodes.values().map(|n| n.spec()).collect()
    }

    /// Register multiple node types at once (plugin-style bulk load).
    /// Returns count of registered nodes.
    pub fn load_plugin_nodes(&mut self, name: &str, nodes: Vec<Arc<dyn NodeBehavior>>) -> usize {
        let count = nodes.len();
        for behavior in nodes {
            let type_id = behavior.spec().type_id.clone();
            self.nodes.insert(type_id, behavior);
        }
        tracing::info!("Plugin '{}' registered {} node types", name, count);
        count
    }

    /// Number of registered node types.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Register all fundamental SwanFlow nodes.
    ///
    /// Note: AI nodes (ConfigWizard, McpNode) are not registered here as they
    /// require runtime configuration (API keys, tool names) and are used as
    /// helpers rather than direct workflow nodes.
    pub fn register_core_nodes(&mut self) {
        use crate::nodes::ai::communities::CommunityBatchesNode;
        use crate::nodes::ai::contracts::{CitationTrackerNode, StructuredIoContractNode};
        use crate::nodes::ai::graph::{
            ContextFuserNode, EntityExtractorNode, GraphRetrieverNode, ResearchLoopMetaNode,
        };
        use crate::nodes::ai::mcp::McpInvokeNode;
        use crate::nodes::ai::privacy::{PiiDetectorNode, PiiMaskerNode, PolicyPrivacyGateNode};
        use crate::nodes::ai::providers::{LlmConfig, OpenAiProvider, ProviderKind};
        use crate::nodes::ai::retrieval::{
            EmbeddingGeneratorNode, HybridRetrieverNode, QueryPlanningNode, RerankerNode,
            SemanticChunkerNode, VectorIndexWriterNode, VectorRetrieverNode,
        };
        use crate::nodes::ai::runtime::{
            AgentLoopOrchestratorNode, CheckpointResumeControllerNode,
            ObservabilityAuditLedgerNode, RetryBackoffGuardNode, RunStateStoreNode,
        };
        use crate::nodes::ai::tool_executor::ToolExecutorNode;
        use crate::nodes::ai::verification::FactClaimVerifierNode;
        use crate::nodes::boundary::{DuckDBExtensionNode, InputNode, OutputNode};
        use crate::nodes::io::{
            CsvReadNode, DbConnectorNode, DuckDBReadNode, DuckDBWriteNode, ExcelReadNode,
            ExcelReportNode, ExcelSheetNamesNode, ExcelTemplateNode, ExcelWriteNode,
            GraphIndexWriterNode, HttpFetchNode, OssieExportNode, OssieReadNode, ParquetReadNode,
            SemanticExtractNode, SemanticFkInferNode, UniversalReadNode,
        };
        use crate::nodes::io::{CsvWriteNode, ParquetWriteNode};
        use crate::nodes::luajit::LuaJitNode;
        use crate::nodes::mcp::McpToolNode;

        #[cfg(feature = "wasm")]
        use crate::nodes::plugin_transform::PluginTransformNode;
        use crate::nodes::relational::{
            CastNode, DistinctNode, JoinNode, LimitNode, SortNode, UnionNode,
        };
        use crate::nodes::script::boxed_python::BoxedPythonNode;
        #[cfg(feature = "python")]
        use crate::nodes::script::python::{PythonNode, PythonUdfNode};
        use crate::nodes::sql::SqlNode;
        use crate::nodes::transform::{
            AggregateNode, AiRouterNode, ConditionNode, FilterNode, ForEachNode, GraphMergerNode,
            PivotNode, ProfilerNode, ProjectNode, SemanticCleanerNode, TableSchemaValidatorNode,
            TableToVariableNode, VariableCreatorNode, WindowNode,
        };
        #[cfg(feature = "wasm")]
        use crate::nodes::wasm::WasmNode;

        self.register_node(Arc::new(SqlNode));
        self.register_node(Arc::new(LuaJitNode));

        // Auto-register duckdb-luajit-libs as canvas nodes ("self-evolution"):
        // every lib in the INDEX becomes a luajit.<lib> node the agent can add
        // directly. Missing INDEX is fine — just the generic shell node remains.
        if let Some(index_path) = crate::nodes::luajit::default_luajit_libs_index() {
            let libs = crate::nodes::luajit::luajit_libs_from_index(&index_path);
            let lib_count = libs.len();
            for lib in libs {
                self.register_node(Arc::new(lib));
            }
            tracing::info!(
                "Auto-registered {} luajit lib nodes from {}",
                lib_count,
                index_path
            );
        }

        // Second auto-registration source: DuckDB extension functions
        // (implementation and rationale live in `nodes/extfn.rs`).
        // Reads the materialized `ext_functions` catalog produced by
        // `scripts/build_ext_functions.py`. Node bloat is guarded by the
        // catalog's `enabled` column (tier-1 `table` functions of non-core
        // extensions are pre-enabled; everything else needs human curation)
        // plus the filter inside `extfn_nodes_from_table`. A missing catalog is
        // fine — no ext nodes appear and the rest of the registry is unaffected.
        // Design record: openspec/changes/extfn-auto-nodes/design.md
        if let Some(db_path) = crate::nodes::extfn::default_ext_functions_db() {
            let fns = crate::nodes::extfn::extfn_nodes_from_table(&db_path);
            let fn_count = fns.len();
            for f in fns {
                self.register_node(Arc::new(f));
            }
            if fn_count > 0 {
                tracing::info!(
                    "Auto-registered {} DuckDB extension function nodes from {}",
                    fn_count,
                    db_path
                );
            }
        }
        self.register_node(Arc::new(CsvReadNode));
        self.register_node(Arc::new(ParquetReadNode));
        self.register_node(Arc::new(OssieReadNode));
        self.register_node(Arc::new(OssieExportNode));
        self.register_node(Arc::new(SemanticFkInferNode));
        self.register_node(Arc::new(SemanticExtractNode));
        self.register_node(Arc::new(UniversalReadNode));
        self.register_node(Arc::new(DbConnectorNode));
        self.register_node(Arc::new(ExcelReadNode));
        self.register_node(Arc::new(ExcelWriteNode));
        self.register_node(Arc::new(ExcelTemplateNode));
        self.register_node(Arc::new(ExcelReportNode));
        self.register_node(Arc::new(ExcelSheetNamesNode));
        self.register_node(Arc::new(DuckDBReadNode));
        self.register_node(Arc::new(DuckDBWriteNode));
        self.register_node(Arc::new(CsvWriteNode));
        self.register_node(Arc::new(ParquetWriteNode));
        self.register_node(Arc::new(HttpFetchNode));
        self.register_node(Arc::new(GraphIndexWriterNode));
        self.register_node(Arc::new(InputNode));
        self.register_node(Arc::new(OutputNode));
        self.register_node(Arc::new(DuckDBExtensionNode));
        self.register_node(Arc::new(FilterNode));
        self.register_node(Arc::new(ProjectNode));
        self.register_node(Arc::new(SortNode));
        self.register_node(Arc::new(LimitNode));
        self.register_node(Arc::new(DistinctNode));
        self.register_node(Arc::new(CastNode));
        self.register_node(Arc::new(UnionNode));
        self.register_node(Arc::new(JoinNode));
        self.register_node(Arc::new(AggregateNode));
        self.register_node(Arc::new(WindowNode));
        self.register_node(Arc::new(ConditionNode));
        self.register_node(Arc::new(ForEachNode));
        self.register_node(Arc::new(ProfilerNode));
        self.register_node(Arc::new(PivotNode));
        self.register_node(Arc::new(VariableCreatorNode));
        self.register_node(Arc::new(TableToVariableNode));
        self.register_node(Arc::new(GraphMergerNode));
        self.register_node(Arc::new(TableSchemaValidatorNode));
        // Quality nodes live in the `swanflow-plugin-quality` crate since P1
        // (plugin-architecture.md §P1). Registered through the swanflow_core
        // bridge when the feature is on; without it, workflows referencing
        // `core.quality.*` get a "plugin not installed" error (see get_node)
        // instead of a bare "node not found".
        #[cfg(feature = "quality")]
        {
            // The trait must be in scope to call name()/node_types().
            use swanflow_core::Plugin as _;
            let plugin = swanflow_plugin_quality::QualityPlugin;
            let nodes = plugin.node_types();
            self.load_plugin_nodes(
                plugin.name(),
                crate::nodes::core_bridge::CoreNodeAdapter::wrap_all(nodes),
            );
        }
        self.register_node(Arc::new(SemanticCleanerNode));
        self.register_node(Arc::new(AiRouterNode));
        // BoxedPython is a pure-DataFusion placeholder (no pyo3) — stays
        // registered in every build.
        self.register_node(Arc::new(BoxedPythonNode));
        #[cfg(feature = "python")]
        {
            self.register_node(Arc::new(PythonNode));
            // Note: PythonUdfNode is registered but currently returns empty plan (stub implementation)
            self.register_node(Arc::new(PythonUdfNode));
        }
        #[cfg(feature = "wasm")]
        self.register_node(Arc::new(WasmNode));
        #[cfg(feature = "wasm")]
        self.register_node(Arc::new(PluginTransformNode));
        self.register_node(Arc::new(GraphRetrieverNode));
        self.register_node(Arc::new(ContextFuserNode));
        self.register_node(Arc::new(McpInvokeNode));
        self.register_node(Arc::new(McpToolNode));
        self.register_node(Arc::new(crate::nodes::motherduck::MotherDuckFlightNode));
        self.register_node(Arc::new(StructuredIoContractNode));
        self.register_node(Arc::new(CitationTrackerNode));
        self.register_node(Arc::new(CommunityBatchesNode));
        self.register_node(Arc::new(EmbeddingGeneratorNode));
        self.register_node(Arc::new(QueryPlanningNode));
        self.register_node(Arc::new(SemanticChunkerNode));
        self.register_node(Arc::new(HybridRetrieverNode));
        self.register_node(Arc::new(RerankerNode));
        self.register_node(Arc::new(VectorIndexWriterNode));
        self.register_node(Arc::new(VectorRetrieverNode));
        self.register_node(Arc::new(ToolExecutorNode));
        self.register_node(Arc::new(PiiDetectorNode));
        self.register_node(Arc::new(PiiMaskerNode));
        self.register_node(Arc::new(PolicyPrivacyGateNode));
        self.register_node(Arc::new(RetryBackoffGuardNode));
        self.register_node(Arc::new(CheckpointResumeControllerNode::default()));
        self.register_node(Arc::new(RunStateStoreNode::default()));
        self.register_node(Arc::new(ObservabilityAuditLedgerNode::default()));
        self.register_node(Arc::new(AgentLoopOrchestratorNode));
        self.register_node(Arc::new(FactClaimVerifierNode));
        self.register_node(Arc::new(ResearchLoopMetaNode::new()));

        // Backward-compatible type aliases used by existing workflow JSON/YAML.
        if let Some(csv) = self.nodes.get("core.io.csv_read").cloned() {
            self.nodes.insert("core.io.csv".to_string(), csv);
        }
        if let Some(parquet) = self.nodes.get("core.io.parquet_read").cloned() {
            self.nodes.insert("core.io.parquet".to_string(), parquet);
        }

        // Register the DuckDB-dialect SQL node (json/icu/fuzzy/lists/PIVOT).
        self.register_node(Arc::new(crate::nodes::sql_duckdb::DuckSqlNode));
        if let Ok(model) = std::env::var("LLM_MODEL") {
            eprintln!("[DEBUG] Found LLM_MODEL: {}", model);
            let config = LlmConfig {
                provider: ProviderKind::OpenAI,
                api_key: std::env::var("LLM_API_KEY").ok(),
                model,
                endpoint: std::env::var("LLM_API_BASE").ok(),
            };
            match OpenAiProvider::new(config) {
                Ok(provider) => {
                    eprintln!("[DEBUG] Registering EntityExtractorNode");
                    let provider = Arc::new(provider);
                    self.register_node(Arc::new(EntityExtractorNode {
                        provider: provider.clone(),
                    }));
                    self.register_node(Arc::new(crate::nodes::io::SemanticAnnotateNode {
                        provider,
                    }));
                }
                Err(e) => eprintln!("[DEBUG] Failed to create OpenAiProvider: {}", e),
            }
        } else if let Ok(provider) = OpenAiProvider::new(LlmConfig {
            provider: ProviderKind::OpenAI,
            api_key: std::env::var("OPENAI_API_KEY").ok(),
            model: "gpt-4o".to_string(),
            endpoint: None,
        }) {
            eprintln!("[DEBUG] Registering EntityExtractorNode (Fallback)");
            let provider = Arc::new(provider);
            self.register_node(Arc::new(EntityExtractorNode {
                provider: provider.clone(),
            }));
            self.register_node(Arc::new(crate::nodes::io::SemanticAnnotateNode {
                provider,
            }));
        } else {
            eprintln!("[DEBUG] No LLM_MODEL or OPENAI_API_KEY found, EntityExtractor skip");
        }

        // Vector nodes: real /embeddings endpoint (config-resolved, env fallback
        // EMBED_MODEL / EMBED_API_BASE / EMBED_API_KEY). Registered always —
        // validation fails loudly at workflow-load time if the endpoint config
        // is unusable (no silent pass-through behavior).
        self.register_node(Arc::new(crate::nodes::ai::vector::EmbedderNode));
        self.register_node(Arc::new(crate::nodes::ai::vector::VectorSearchNode));
    }
}

impl Default for NodeRegistry {
    fn default() -> Self {
        let mut registry = Self::new();
        registry.register_core_nodes();
        registry
    }
}

#[cfg(test)]
mod shell_marker_tests {
    use super::*;

    /// The 8 AI governance nodes audited 2026-09-28 as pure pass-through shells
    /// (propose_plan == identity clone; helpers dead code — see
    /// swanflow-workflows/docs/shell_nodes_audit.md). Their spec descriptions
    /// must carry the shell prefix so registry consumers can flag them.
    const SHELL_PREFIX: &str = "[SHELL NODE — no runtime behavior] ";

    #[test]
    #[cfg_attr(
        not(feature = "python"),
        ignore = "python_udf node absent without the python feature"
    )]
    fn shell_node_descriptions_carry_marker() {
        let registry = NodeRegistry::default();
        let shells = [
            "core.ai.agent_loop_orchestrator",
            "core.ai.retry_backoff_idempotency_guard",
            "core.ai.checkpoint_resume_controller",
            "core.ai.run_state_store",
            "core.ai.observability_audit_ledger",
            "core.ai.query_planning",
            // core.ai.structured_io_contract: real node since UA port batch 2
            // (2026-10-16) — the 4-layer LLM-output validation gate.
            "core.ai.tool_executor",
            "core.flow.python_udf",
        ];
        for type_id in shells {
            let spec = registry
                .get_node(type_id)
                .unwrap_or_else(|e| panic!("{type_id} not registered: {e}"))
                .spec();
            assert!(
                spec.description.starts_with(SHELL_PREFIX),
                "{type_id} description must start with shell marker, got: {}",
                spec.description
            );
        }
    }

    #[test]
    fn real_nodes_do_not_carry_shell_marker() {
        let registry = NodeRegistry::default();
        for spec in registry.list_specs() {
            if spec.description.starts_with("[SHELL NODE") {
                continue; // shells are asserted by the other test
            }
            // spot-check a few known-real nodes are NOT marked
        }
        let sql = registry.get_node("core.sql").unwrap().spec();
        assert!(!sql.description.starts_with("[SHELL NODE"));
        let excel = registry.get_node("core.io.excel_read").unwrap().spec();
        assert!(!excel.description.starts_with("[SHELL NODE"));
    }

    #[test]
    fn deprecated_shells_carry_disposition_guidance() {
        // M2 audit disposition: these shells duplicate real engine machinery
        // or a real sibling node and must say so in their description.
        let registry = NodeRegistry::default();
        let deprecated = [
            ("core.ai.checkpoint_resume_controller", "CheckpointManager"),
            ("core.ai.run_state_store", "engine"),
            ("core.ai.agent_loop_orchestrator", "research_loop"),
            ("core.ai.tool_executor", "mcp_invoke"),
        ];
        for (type_id, hint) in deprecated {
            let spec = registry.get_node(type_id).unwrap().spec();
            assert!(
                spec.description.contains("Deprecated"),
                "{type_id} description must carry deprecation guidance"
            );
            assert!(
                spec.description.contains(hint),
                "{type_id} description must point at the real alternative ({hint})"
            );
        }
    }
}

/// P1 (plugin-architecture.md): `core.quality.*` left the core for the
/// `swanflow-plugin-quality` crate. A workflow written before that split must
/// learn *why* its node vanished — "plugin not installed" tells the operator
/// what to do, while a bare "not found" reads like a typo and sends them
/// hunting for a misspelling that does not exist.
#[cfg(test)]
mod plugin_prefix_tests {
    use super::*;

    /// `get_node` returns `Result<Arc<dyn NodeBehavior>>`, and `dyn
    /// NodeBehavior` is not `Debug`, so `unwrap_err()` is unavailable here.
    fn error_for(type_id: &str) -> String {
        match NodeRegistry::new().get_node(type_id) {
            Ok(_) => panic!("'{type_id}' must not resolve in an empty registry"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn a_known_plugin_prefix_names_the_plugin_and_the_remedy() {
        let err = error_for("core.quality.null_check");
        assert!(
            err.contains("'quality' plugin"),
            "must name the plugin that owns the id, got: {err}"
        );
        assert!(
            err.contains("not installed"),
            "must say the plugin is not installed rather than that the node is unknown, got: {err}"
        );
        assert!(
            err.contains("--features quality"),
            "must tell the operator how to get the node back, got: {err}"
        );
    }

    #[test]
    fn an_unknown_id_is_not_blamed_on_a_plugin() {
        let err = error_for("core.nope.definitely_missing");
        assert!(
            err.contains("not found"),
            "an unknown id keeps the original message, got: {err}"
        );
        assert!(
            !err.contains("plugin"),
            "a typo must not be blamed on a plugin, got: {err}"
        );
    }
}
