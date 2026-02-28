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
                // Map parent input port to this input node's output
                let port_idx = node.config["port_index"].as_u64().unwrap_or(0) as u32;
                let mut map = HashMap::new();
                if let Some(ref injected) = injected_inputs {
                    if let Some(plan) = injected.get(&port_idx) {
                        map.insert(0, plan.clone());
                    }
                }
                map
            } else if node.type_id == "core.output" {
                // Pass-through the input to the output, to be collected by parent
                inputs
            } else {
                // Standard Node Logic
                let behavior = registry.get_node(&node.type_id)?;
                behavior.validate(&node.config)?;
                
                let mut input_vec: Vec<(u32, LogicalPlan)> = inputs.into_iter().collect();
                input_vec.sort_by_key(|(port, _)| *port);
                let ordered_inputs: Vec<LogicalPlan> = input_vec.into_iter().map(|(_, p)| p).collect();

                let output_plans = behavior.propose_plan(ordered_inputs, &node.config).await?;
                
                let mut output_map = HashMap::new();
                for (i, plan) in output_plans.into_iter().enumerate() {
                    output_map.insert(i as u32, plan);
                }
                output_map
            };

            // 3. Inject Probes if enabled (don't probe boundary nodes)
            if self.inject_probes && node.type_id != "core.input" && node.type_id != "core.output" {
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

        // 5. Final Result Collection
        // If this is a sub-workflow, we look for all 'core.output' nodes and map them to ports.
        let mut final_outputs = HashMap::new();
        let mut has_output_nodes = false;

        for &node_idx in &order {
            let node = &workflow.graph[node_idx];
            if node.type_id == "core.output" {
                has_output_nodes = true;
                let port_idx = node.config["port_index"].as_u64().unwrap_or(0) as u32;
                if let Some(node_outputs) = self.compiled_outputs.get(&node.id) {
                    if let Some(plan) = node_outputs.get(&0) {
                        final_outputs.insert(port_idx, plan.clone());
                    }
                }
            }
        }

        if !has_output_nodes {
            // Fallback for simple workflows: return last node's output at port 0
            if let Some(&last_idx) = order.last() {
                let last_id = workflow.graph[last_idx].id;
                if let Some(node_outputs) = self.compiled_outputs.get(&last_id) {
                    return Ok(node_outputs.clone());
                }
            }
        }

        Ok(final_outputs)
    }

    /// Collects LogicalPlans from upstream nodes based on graph edges.
    fn collect_inputs(
        &self, 
        node_idx: NodeIndex, 
        workflow: &Workflow,
        _injected_inputs: &Option<HashMap<u32, LogicalPlan>>
    ) -> Result<HashMap<u32, LogicalPlan>> {
        let mut inputs = HashMap::new();

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
