use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use datafusion::prelude::*;
use datafusion::logical_expr::{LogicalPlan, Extension};
use datafusion::error::Result;
use datafusion::execution::session_state::SessionStateBuilder;
use uuid::Uuid;
use serde_json::Value as JsonValue;
use petgraph::visit::EdgeRef;
use petgraph::algo::toposort;

use crate::core::graph::Workflow;
use crate::nodes::registry::NodeRegistry;
use crate::nodes::NodeEvent;
use crate::core::observer::{StateProbeLogical, MaterializerLogical};

pub struct WorkflowCompiler {
    pub compiled_outputs: HashMap<Uuid, HashMap<usize, LogicalPlan>>,
    pub inferred_schemas: HashMap<Uuid, Vec<datafusion::common::DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<datafusion::arrow::record_batch::RecordBatch>>,
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
    ) -> Result<Vec<Vec<(usize, LogicalPlan)>>> {
        let ctx = ctx_override.unwrap_or_else(|| self.ctx.clone());
        
        // 1. Topological Sort
        let sorted_indices = toposort(&workflow.graph, None)
            .map_err(|_| datafusion::error::DataFusionError::Plan("Cycle detected in workflow".to_string()))?;
        
        // We will group nodes into levels for staged execution
        let mut node_levels: HashMap<Uuid, usize> = HashMap::new();
        let mut stages: Vec<Vec<(usize, LogicalPlan)>> = Vec::new();

        // 2. Process nodes in topological order
        for node_idx in sorted_indices {
            let node = workflow.graph.node_weight(node_idx).unwrap();
            
            // Calculate level
            let mut level = 0;
            for edge in workflow.graph.edges_directed(node_idx, petgraph::Direction::Incoming) {
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
            let mut incoming_edges: Vec<_> = workflow.graph
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

            // Propose plan from node
            let spec = registry.get_node(&node.type_id)
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("Unknown node type {}: {}", node.type_id, e)))?;
            
            let compiled_vec = spec.propose_plan(input_plans, &node.config).await
                .map_err(|e| datafusion::error::DataFusionError::Plan(format!("Plan proposal failed for node {}: {}", node.id, e)))?;
            
            let mut compiled_node_outputs: HashMap<usize, LogicalPlan> = compiled_vec.into_iter().enumerate().collect();
            
            // Apply probes and materializers
            if self.inject_probes && node.type_id != "core.output" {
                for (port, plan) in compiled_node_outputs.iter_mut() {
                    let table_name = format!("node_{}", &node.id.to_string()[..8]);
                    let cache_key = format!("{}_{}", node.id, port);
                    
                    *plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(MaterializerLogical {
                            node_id: node.id,
                            table_name: table_name.clone(),
                            cache_key: cache_key.clone(),
                            cache_dir: self.cache_dir.clone(),
                            input: plan.clone(),
                            ctx: ctx.clone(),
                            event_tx: self.event_tx.clone().unwrap(),
                        }),
                    });
                    
                    *plan = LogicalPlan::Extension(Extension { 
                        node: Arc::new(StateProbeLogical { 
                            node_id: node.id, 
                            input: plan.clone() 
                        }) 
                    });
                }
            }

            self.compiled_outputs.insert(node.id, compiled_node_outputs.clone());
            
            // Add to its corresponding stage
            for (port, plan) in compiled_node_outputs {
                stages[level].push((port, plan));
            }
        }

        Ok(stages)
    }

    pub async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let ctx = Arc::new(SessionContext::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tx,
            ctx: ctx.clone(),
            cache_dir: self.cache_dir.clone(),
        };
        let physical_planner = Arc::new(datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(planner)]));
        
        let state = SessionStateBuilder::new_from_existing(ctx.state())
            .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner { physical_planner }))
            .build();
            
        let physical_plan = state.create_physical_plan(plan).await?;
        datafusion::physical_plan::collect(physical_plan, state.task_ctx()).await
    }
}
