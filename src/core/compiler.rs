use std::collections::HashMap;
use std::sync::Arc;
use petgraph::algo::toposort;
use petgraph::prelude::*;
use uuid::Uuid;
use datafusion::logical_expr::{LogicalPlan, Extension};
use datafusion::prelude::*;
use datafusion::common::DFSchemaRef;
use arrow::record_batch::RecordBatch;
use anyhow::{Result, anyhow};
use async_recursion::async_recursion;
use sha2::{Sha256, Digest};

use crate::core::graph::{Workflow, NodeInstance};
use crate::nodes::registry::NodeRegistry;
use crate::core::observer::{StateProbeLogical, MaterializerLogical};

/// The WorkflowCompiler is responsible for transforming a logical Workflow DAG
/// into a DataFusion LogicalPlan.
pub struct WorkflowCompiler {
    /// Cache of compiled outputs for each node instance (node_id -> HashMap<port_id, LogicalPlan>)
    pub compiled_outputs: HashMap<Uuid, HashMap<u32, LogicalPlan>>,
    /// Inferred schemas for each node instance (node_id -> Vec<schema>)
    pub inferred_schemas: HashMap<Uuid, Vec<DFSchemaRef>>,
    /// Optional sample data for each node instance (node_id -> Vec<RecordBatch>)
    pub sample_data: HashMap<Uuid, Vec<RecordBatch>>,
    /// Cache keys for each node instance (node_id -> hash_string)
    pub node_hashes: HashMap<Uuid, String>,
    /// Whether to inject state probes for observation (Traffic Lights)
    inject_probes: bool,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self {
            compiled_outputs: HashMap::new(),
            inferred_schemas: HashMap::new(),
            sample_data: HashMap::new(),
            node_hashes: HashMap::new(),
            inject_probes,
        }
    }

    pub fn get_execution_order(&self, workflow: &Workflow) -> Result<Vec<NodeIndex>> {
        toposort(&workflow.graph, None)
            .map_err(|_| anyhow!("Workflow contains cycles and cannot be compiled"))
    }

    fn generate_cache_key(&self, node: &NodeInstance, input_hashes: &HashMap<u32, String>) -> String {
        let mut hasher = Sha256::new();
        hasher.update(node.type_id.as_bytes());
        let config_str = serde_json::to_string(&node.config).unwrap_or_default();
        hasher.update(config_str.as_bytes());
        let mut input_ports: Vec<_> = input_hashes.keys().cloned().collect();
        input_ports.sort_unstable();
        for port in input_ports {
            hasher.update(port.to_be_bytes());
            if let Some(hash) = input_hashes.get(&port) {
                hasher.update(hash.as_bytes());
            }
        }
        format!("{:x}", hasher.finalize())
    }

    #[async_recursion]
    pub async fn compile(
        &mut self,
        workflow: &Workflow,
        registry: &NodeRegistry,
        injected_inputs: Option<HashMap<u32, LogicalPlan>>,
    ) -> Result<HashMap<u32, LogicalPlan>> {
        let order = self.get_execution_order(workflow)?;
        let injected_hashes = HashMap::new(); 

        for &node_idx in &order {
            let node = workflow.get_node(node_idx)
                .ok_or_else(|| anyhow!("Node at index {:?} not found", node_idx))?;
            
            let inputs = self.collect_inputs(node_idx, workflow, &injected_inputs)?;
            let input_hashes = self.collect_input_hashes(node_idx, workflow, &injected_hashes)?;

            let cache_key = self.generate_cache_key(node, &input_hashes);
            self.node_hashes.insert(node.id, cache_key.clone());

            let cache_path = format!("target/swanflow_cache/{}.parquet", cache_key);
            let mut outputs = if node.materialize && std::path::Path::new(&cache_path).exists() {
                let ctx = SessionContext::new();
                let df = ctx.read_parquet(&cache_path, ParquetReadOptions::default()).await?;
                let mut map = HashMap::new();
                map.insert(0, df.into_optimized_plan()?);
                map
            } else {
                let compiled_node_outputs = if let Some(ref sub_workflow) = node.sub_workflow {
                    let mut sub_compiler = WorkflowCompiler::new(self.inject_probes);
                    let res = sub_compiler.compile(sub_workflow, registry, Some(inputs)).await?;
                    for (id, schemas) in sub_compiler.inferred_schemas {
                        self.inferred_schemas.insert(id, schemas);
                    }
                    for (id, samples) in sub_compiler.sample_data {
                        self.sample_data.insert(id, samples);
                    }
                    res
                } else if node.type_id == "core.input" {
                    let port_idx = node.config["port_index"].as_u64().unwrap_or(0) as u32;
                    let mut map = HashMap::new();
                    if let Some(ref injected) = injected_inputs {
                        if let Some(plan) = injected.get(&port_idx) {
                            map.insert(0, plan.clone());
                        }
                    }
                    map
                } else if node.type_id == "core.output" {
                    inputs
                } else {
                    let behavior = registry.get_node(&node.type_id)?;
                    behavior.validate(&node.config)?;
                    
                    // NEW: Use infer_schema to get metadata
                    let input_schemas = inputs.values().map(|p| p.schema().clone()).collect();
                    let metadata = behavior.infer_schema(input_schemas, &node.config).await?;
                    
                    if let Some(samples) = metadata.sample_data {
                        self.sample_data.insert(node.id, samples);
                    }

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

                let mut wrapped_outputs = compiled_node_outputs;
                if node.materialize {
                    for plan in wrapped_outputs.values_mut() {
                        *plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(MaterializerLogical {
                                cache_key: cache_key.clone(),
                                input: plan.clone(),
                            }),
                        });
                    }
                }
                wrapped_outputs
            };

            let schemas: Vec<DFSchemaRef> = outputs.values().map(|p| p.schema().clone()).collect();
            self.inferred_schemas.insert(node.id, schemas);

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

            self.compiled_outputs.insert(node.id, outputs);
        }

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
            if let Some(&last_idx) = order.last() {
                let last_id = workflow.graph[last_idx].id;
                if let Some(node_outputs) = self.compiled_outputs.get(&last_id) {
                    return Ok(node_outputs.clone());
                }
            }
        }
        Ok(final_outputs)
    }

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

    fn collect_input_hashes(
        &self,
        node_idx: NodeIndex,
        workflow: &Workflow,
        _injected_hashes: &HashMap<u32, String>
    ) -> Result<HashMap<u32, String>> {
        let mut hashes = HashMap::new();
        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let source_idx = edge_ref.source();
            let edge = edge_ref.weight();
            let source_node = &workflow.graph[source_idx];
            if let Some(hash) = self.node_hashes.get(&source_node.id) {
                hashes.insert(edge.target_port, hash.clone());
            } else {
                return Err(anyhow!("Upstream node {} hash not found", source_node.id));
            }
        }
        Ok(hashes)
    }
}
