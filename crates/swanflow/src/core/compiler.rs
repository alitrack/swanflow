use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::*;
use petgraph::algo::toposort;
use petgraph::visit::EdgeRef;
use serde_json::json;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::core::graph::Workflow;
use crate::core::observer::{MaterializerLogical, StateProbeLogical};
use crate::core::templating::TemplateEngine;
use crate::nodes::missing::MissingNode;
use crate::nodes::registry::NodeRegistry;
use crate::nodes::{NodeDiagnostic, NodeErrorClass, NodeEvent};

#[derive(Debug, Clone)]
pub struct CompiledStagePlan {
    pub node_id: Uuid,
    pub node_type: String,
    pub port: usize,
    pub plan: LogicalPlan,
    /// Conditional mode: run this stage only if any predecessor cell
    /// contains this substring. None = unconditional.
    pub conditional_when: Option<String>,
    /// Error handling for this stage: fail / skip / retry(n).
    pub error_mode: crate::core::graph::ErrorMode,
}

pub struct WorkflowCompiler {
    pub compiled_outputs: HashMap<Uuid, HashMap<usize, LogicalPlan>>,
    pub inferred_schemas: HashMap<Uuid, Vec<datafusion::common::DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<datafusion::arrow::record_batch::RecordBatch>>,
    pub diagnostics: HashMap<Uuid, Vec<String>>,
    pub node_hashes: HashMap<Uuid, String>,
    pub inject_probes: bool,
    pub dynamic_context: HashMap<String, JsonValue>,
    pub event_tx: Option<mpsc::UnboundedSender<NodeEvent>>,
    pub cache_dir: String,
    pub ctx: Arc<SessionContext>,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self::with_cache_dir(inject_probes, "target/swanflow_cache".to_string())
    }

    pub fn with_cache_dir(inject_probes: bool, cache_dir: String) -> Self {
        let ctx = Arc::new(SessionContext::new());
        Self {
            compiled_outputs: HashMap::new(),
            inferred_schemas: HashMap::new(),
            sample_data: HashMap::new(),
            diagnostics: HashMap::new(),
            node_hashes: HashMap::new(),
            inject_probes,
            dynamic_context: HashMap::new(),
            event_tx: None,
            cache_dir,
            ctx,
        }
    }

    pub fn set_event_tx(&mut self, tx: mpsc::UnboundedSender<NodeEvent>) {
        self.event_tx = Some(tx);
    }

    pub async fn compile(
        &mut self,
        workflow: &Workflow,
        registry: &NodeRegistry,
        ctx_override: Option<Arc<SessionContext>>,
    ) -> Result<Vec<Vec<CompiledStagePlan>>> {
        let _ctx = ctx_override.unwrap_or_else(|| self.ctx.clone());

        // 1. Topological Sort
        let sorted_indices = toposort(&workflow.graph, None).map_err(|_| {
            datafusion::error::DataFusionError::Plan("Cycle detected in workflow".to_string())
        })?;

        // We will group nodes into levels for staged execution
        let mut node_levels: HashMap<Uuid, usize> = HashMap::new();
        let mut stages: Vec<Vec<CompiledStagePlan>> = Vec::new();

        // 2. Process nodes in topological order
        for node_idx in sorted_indices {
            let node = workflow.graph.node_weight(node_idx).unwrap();
            let mut is_placeholder = false;

            // ── Loop mode: embed sub_workflow as nested stages ──────────
            // The loop node's own compiled output is the parent feed; the
            // sub_workflow's terminal outputs are appended at deeper levels.
            if node.mode == crate::core::graph::StepMode::Loop {
                if let Some(sub) = &node.sub_workflow {
                    let inner_levels = self
                        .compile_inner(sub, registry, Some(&node.id.to_string()))
                        .await;
                    if let Ok(inner) = inner_levels {
                        let base = stages.len();
                        for (offset, inner_stage) in inner.into_iter().enumerate() {
                            let lvl = base + offset;
                            while stages.len() <= lvl {
                                stages.push(Vec::new());
                            }
                            stages[lvl].extend(inner_stage);
                        }
                        // record inner terminal outputs under the loop node id
                        if let Some(last) = stages.last() {
                            let mut outs: HashMap<usize, LogicalPlan> = HashMap::new();
                            for sp in last {
                                if sp.node_type != "core.placeholder" {
                                    outs.insert(sp.port, sp.plan.clone());
                                }
                            }
                            if !outs.is_empty() {
                                self.compiled_outputs
                                    .insert(node.id, outs);
                            }
                        }
                    }
                }
            }

            // Calculate level
            let mut level = 0;
            for edge in workflow
                .graph
                .edges_directed(node_idx, petgraph::Direction::Incoming)
            {
                let source_node = workflow.graph.node_weight(edge.source()).unwrap();
                if let Some(&src_level) = node_levels.get(&source_node.id) {
                    level = level.max(src_level + 1);
                }
            }
            node_levels.insert(node.id, level);

            // Ensure stages vec is large enough
            while stages.len() <= level {
                stages.push(Vec::new());
            }

            // Collect inputs for this node
            let mut input_plans = Vec::new();
            let mut incoming_edges: Vec<_> = workflow
                .graph
                .edges_directed(node_idx, petgraph::Direction::Incoming)
                .collect();
            incoming_edges.sort_by_key(|e| e.weight().target_port);

            for edge in incoming_edges {
                let source_node = workflow.graph.node_weight(edge.source()).unwrap();
                if let Some(outputs) = self.compiled_outputs.get(&source_node.id) {
                    if let Some(plan) = outputs.get(&(edge.weight().source_port as usize)) {
                        input_plans.push(plan.clone());
                    }
                }
            }

            // Resolve node behavior, falling back to placeholder for unknown types.
            let spec = match registry.get_node(&node.type_id) {
                Ok(spec) => spec,
                Err(_) => {
                    is_placeholder = true;
                    self.diagnostics.entry(node.id).or_default().push(format!(
                        "Loaded as placeholder: original type '{}' is not available",
                        node.type_id
                    ));
                    Arc::new(MissingNode::new(node.id, node.type_id.clone()))
                }
            };

            let preflight_config = self
                .run_preflight(
                    workflow,
                    node.id,
                    &node.type_id,
                    spec.as_ref(),
                    &node.config,
                )
                .map_err(|diagnostic| {
                    self.diagnostics
                        .entry(node.id)
                        .or_default()
                        .push(diagnostic.message.clone());
                    self.emit_diagnostic(&diagnostic);
                    datafusion::error::DataFusionError::Plan(diagnostic.to_event_message())
                })?;

            // Propose plan from node
            let compiled_vec = spec
                .propose_plan(input_plans, &preflight_config)
                .await
                .map_err(|e| {
                    let diagnostic = NodeDiagnostic::new(
                        node.id,
                        node.type_id.clone(),
                        NodeErrorClass::Planning,
                        format!("Plan proposal failed: {}", e),
                    );
                    self.diagnostics
                        .entry(node.id)
                        .or_default()
                        .push(diagnostic.message.clone());
                    self.emit_diagnostic(&diagnostic);
                    datafusion::error::DataFusionError::Plan(diagnostic.to_event_message())
                })?;

            self.inferred_schemas.insert(
                node.id,
                compiled_vec.iter().map(|p| p.schema().clone()).collect(),
            );

            let mut compiled_node_outputs: HashMap<usize, LogicalPlan> =
                compiled_vec.into_iter().enumerate().collect();

            // Apply probes and materializers
            if self.inject_probes && node.type_id != "core.output" && !is_placeholder {
                for (port, plan) in compiled_node_outputs.iter_mut() {
                    let table_name = format!("node_{}", &node.id.to_string()[..8]);
                    let cache_key = format!("{}_{}", node.id, port);

                    if self.event_tx.is_some() {
                        *plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(MaterializerLogical {
                                node_id: node.id,
                                table_name: table_name.clone(),
                                cache_key: cache_key.clone(),
                                cache_dir: self.cache_dir.clone(),
                                input: plan.clone(),
                            }),
                        });

                        *plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(StateProbeLogical {
                                node_id: node.id,
                                input: plan.clone(),
                            }),
                        });
                    }
                }
            }

            self.compiled_outputs
                .insert(node.id, compiled_node_outputs.clone());

            // Add to its corresponding stage
            for (port, plan) in compiled_node_outputs {
                stages[level].push(CompiledStagePlan {
                    node_id: node.id,
                    node_type: node.type_id.clone(),
                    port,
                    plan,
                    conditional_when: if node.mode
                        == crate::core::graph::StepMode::Conditional
                    {
                        node.when.clone()
                    } else {
                        None
                    },
                    error_mode: node.error_mode.clone(),
                });
            }
        }

        Ok(stages)
    }


    /// Compile a sub_workflow's own graph into stages (used by loop mode).
    /// Recursion is boxed to satisfy the async recursion rule.
    async fn compile_inner(
        &mut self,
        sub: &crate::core::graph::Workflow,
        registry: &crate::nodes::registry::NodeRegistry,
        _tag: Option<&str>,
    ) -> Result<Vec<Vec<CompiledStagePlan>>> {
        let sub_clone = sub.clone();
        let mut inner_stages = Box::pin(self.compile(&sub_clone, registry, None)).await?;
        // Prefix inner node ids' stage entries are appended as-is; caller merges.
        let _ = &mut inner_stages;
        Ok(inner_stages)
    }

    pub async fn execute_plan(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let ctx = Arc::new(SessionContext::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tx,
            ctx: ctx.clone(),
            cache_dir: self.cache_dir.clone(),
        };
        let physical_planner = Arc::new(
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(planner),
            ]),
        );

        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner {
                physical_planner,
            }))
            .build();

        let physical_plan = state.create_physical_plan(plan).await?;
        datafusion::physical_plan::collect(physical_plan, state.task_ctx()).await
    }

    fn emit_diagnostic(&self, diagnostic: &NodeDiagnostic) {
        if let Some(tx) = &self.event_tx {
            let _ = tx.send(NodeEvent::Error {
                node_id: diagnostic.node_id,
                message: diagnostic.to_event_message(),
            });
        }
    }

    #[allow(clippy::result_large_err)]
    fn parse_version(
        &self,
        node_id: Uuid,
        node_type: &str,
        config: &serde_json::Map<String, JsonValue>,
        key: &str,
        default: u32,
    ) -> std::result::Result<u32, NodeDiagnostic> {
        match config.get(key) {
            None => Ok(default),
            Some(value) => {
                if let Some(version) = value.as_u64() {
                    Ok(version as u32)
                } else {
                    Err(NodeDiagnostic::new(
                        node_id,
                        node_type.to_string(),
                        NodeErrorClass::Migration,
                        format!("'{}' must be an unsigned integer", key),
                    ))
                }
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn run_preflight(
        &mut self,
        workflow: &Workflow,
        node_id: Uuid,
        node_type: &str,
        behavior: &dyn crate::nodes::NodeBehavior,
        raw_config: &JsonValue,
    ) -> std::result::Result<JsonValue, NodeDiagnostic> {
        // Template resolution runs first so migration/validation sees stable config values.
        let rendered_config = TemplateEngine::render_json(raw_config, &workflow.context);
        let config_obj = rendered_config.as_object().ok_or_else(|| {
            NodeDiagnostic::new(
                node_id,
                node_type.to_string(),
                NodeErrorClass::Validation,
                "node config must be a JSON object",
            )
        })?;

        let source_impl_version =
            self.parse_version(node_id, node_type, config_obj, "impl_version", 1)?;
        let source_config_version =
            self.parse_version(node_id, node_type, config_obj, "config_version", 1)?;

        let target_impl_version = behavior.impl_version();
        let target_config_version = behavior.config_version();

        if source_impl_version > target_impl_version {
            return Err(NodeDiagnostic::new(
                node_id,
                node_type.to_string(),
                NodeErrorClass::Migration,
                format!(
                    "config impl_version={} is newer than runtime impl_version={}",
                    source_impl_version, target_impl_version
                ),
            )
            .with_versions(source_impl_version, target_impl_version));
        }

        if source_config_version > target_config_version {
            return Err(NodeDiagnostic::new(
                node_id,
                node_type.to_string(),
                NodeErrorClass::Migration,
                format!(
                    "config_version={} is newer than supported target={}",
                    source_config_version, target_config_version
                ),
            )
            .with_versions(source_config_version, target_config_version));
        }

        let mut migrated = rendered_config.clone();
        let mut current = source_config_version;
        while current < target_config_version {
            let next = current + 1;
            migrated = behavior
                .migrate_config(migrated, current, next)
                .map_err(|e| {
                    NodeDiagnostic::new(
                        node_id,
                        node_type.to_string(),
                        NodeErrorClass::Migration,
                        format!("migration failed: {}", e),
                    )
                    .with_versions(current, next)
                    .with_failed_step(format!("{}->{}", current, next))
                })?;
            current = next;
        }

        if let Some(obj) = migrated.as_object_mut() {
            obj.insert("impl_version".to_string(), json!(target_impl_version));
            obj.insert("config_version".to_string(), json!(target_config_version));
        }

        behavior.validate(&migrated).map_err(|e| {
            NodeDiagnostic::new(
                node_id,
                node_type.to_string(),
                NodeErrorClass::Validation,
                format!("validation failed: {}", e),
            )
            .with_versions(source_config_version, target_config_version)
            .with_metadata(json!({
                "impl_version": {
                    "source": source_impl_version,
                    "target": target_impl_version
                }
            }))
        })?;

        Ok(migrated)
    }
}
