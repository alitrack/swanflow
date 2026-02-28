use std::collections::HashMap;
use std::sync::Arc;
use petgraph::algo::toposort;
use petgraph::prelude::*;
use uuid::Uuid;
use datafusion::logical_expr::{LogicalPlan, Extension};
use anyhow::{Result, anyhow};
use async_recursion::async_recursion;

use crate::core::graph::Workflow;
use crate::nodes::registry::NodeRegistry;
use crate::core::observer::StateProbeLogical;

/// The WorkflowCompiler is responsible for transforming a logical Workflow DAG
/// into a DataFusion LogicalPlan.
pub struct WorkflowCompiler {
    /// Cache of compiled outputs for each node instance (node_id -> HashMap<port_id, LogicalPlan>)
    compiled_outputs: HashMap<Uuid, HashMap<u32, LogicalPlan>>,
    /// Whether to inject state probes for observation (Traffic Lights)
    inject_probes: bool,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self {
            compiled_outputs: HashMap::new(),
            inject_probes,
        }
    }

    /// Performs a topological sort on the workflow graph to determine the execution order.
    pub fn get_execution_order(&self, workflow: &Workflow) -> Result<Vec<NodeIndex>> {
        toposort(&workflow.graph, None)
            .map_err(|_| anyhow!("Workflow contains cycles and cannot be compiled"))
    }

    /// Main entry point for compilation.
    /// Supports recursive MetaNode expansion via `injected_inputs`.
    #[async_recursion]
    pub async fn compile(
        &mut self,
        workflow: &Workflow,
        registry: &NodeRegistry,
        injected_inputs: Option<HashMap<u32, LogicalPlan>>,
    ) -> Result<HashMap<u32, LogicalPlan>> {
        let order = self.get_execution_order(workflow)?;
        
        for &node_idx in &order {
            let node = workflow.get_node(node_idx)
                .ok_or_else(|| anyhow!("Node at index {:?} not found", node_idx))?;
            
            // 1. Collect inputs for this node
            let inputs = self.collect_inputs(node_idx, workflow, &injected_inputs)?;

            // 2. Compile node logic
            let mut outputs = if let Some(ref sub_workflow) = node.sub_workflow {
                // MetaNode Logic: Recursive expansion
                let mut sub_compiler = WorkflowCompiler::new(self.inject_probes);
                sub_compiler.compile(sub_workflow, registry, Some(inputs)).await?
            } else if node.type_id == "core.input" {
                // Special handling for boundary nodes inside sub-workflows
                inputs
            } else {
                // Standard Node Logic
                let behavior = registry.get_node(&node.type_id)?;
                behavior.validate(&node.config)?;
                
                // Convert HashMap<u32, LogicalPlan> to Vec<LogicalPlan> sorted by port ID
                let mut input_vec: Vec<(u32, LogicalPlan)> = inputs.into_iter().collect();
                input_vec.sort_by_key(|(port, _)| *port);
                let ordered_inputs: Vec<LogicalPlan> = input_vec.into_iter().map(|(_, p)| p).collect();

                let output_plans = behavior.propose_plan(ordered_inputs, &node.config).await?;
                
                // Map Vec back to HashMap based on NodeSpec (assume 0..N ports)
                let mut output_map = HashMap::new();
                for (i, plan) in output_plans.into_iter().enumerate() {
                    output_map.insert(i as u32, plan);
                }
                output_map
            };

            // 3. Inject Probes if enabled
            if self.inject_probes {
                for plan in outputs.values_mut() {
                    let probe = StateProbeLogical {
                        node_id: node.id,
                        input: plan.clone(),
                    };
                    *plan = LogicalPlan::Extension(Extension {
                        node: Arc::new(probe),
                    });
                }
            }

            // 4. Store compiled outputs
            self.compiled_outputs.insert(node.id, outputs);
        }

        // Return the terminal nodes' outputs
        let last_node_id = workflow.graph[order.last().cloned().ok_or_else(|| anyhow!("Empty workflow"))?].id;
        Ok(self.compiled_outputs.get(&last_node_id).cloned().unwrap_or_default())
    }

    /// Collects LogicalPlans from upstream nodes based on graph edges.
    fn collect_inputs(
        &self, 
        node_idx: NodeIndex, 
        workflow: &Workflow,
        injected_inputs: &Option<HashMap<u32, LogicalPlan>>
    ) -> Result<HashMap<u32, LogicalPlan>> {
        let mut inputs = HashMap::new();

        // If this is a root node and we have injected inputs (e.g. inside a MetaNode), use them.
        let incoming_count = workflow.graph.edges_directed(node_idx, Incoming).count();
        if incoming_count == 0 {
            if let Some(ref injected) = injected_inputs {
                return Ok(injected.clone());
            }
        }

        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let source_idx = edge_ref.source();
            let edge = edge_ref.weight();
            let source_node = &workflow.graph[source_idx];

            if let Some(source_outputs) = self.compiled_outputs.get(&source_node.id) {
                if let Some(plan) = source_outputs.get(&edge.source_port) {
                    inputs.insert(edge.target_port, plan.clone());
                } else {
                    return Err(anyhow!("Source port {} not found for node {}", edge.source_port, source_node.id));
                }
            } else {
                return Err(anyhow!("Upstream node {} not yet compiled", source_node.id));
            }
        }

        Ok(inputs)
    }
}
