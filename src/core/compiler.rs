use anyhow::{anyhow, Result};
use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_recursion::async_recursion;
use datafusion::common::DFSchemaRef;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::prelude::*;
use petgraph::algo::toposort;
use petgraph::prelude::*;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::core::graph::{NodeInstance, Workflow};
use crate::core::observer::{MaterializerLogical, StateProbeLogical};
use crate::core::templating::TemplateEngine;
use crate::nodes::registry::NodeRegistry;

use tokio::sync::mpsc;
use crate::nodes::{NodeEvent, NodeState};

/// The WorkflowCompiler is responsible for transforming a logical Workflow DAG
/// into a DataFusion LogicalPlan.
pub struct WorkflowCompiler {
    pub compiled_outputs: HashMap<Uuid, HashMap<u32, LogicalPlan>>,
    pub inferred_schemas: HashMap<Uuid, Vec<DFSchemaRef>>,
    pub sample_data: HashMap<Uuid, Vec<RecordBatch>>,
    pub node_hashes: HashMap<Uuid, String>,
    inject_probes: bool,
    pub dynamic_context: HashMap<String, JsonValue>,
    // Cache for serialized config to avoid repeated JSON serialization
    config_cache: HashMap<Uuid, String>,
    pub event_tx: Option<mpsc::UnboundedSender<NodeEvent>>,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self {
            compiled_outputs: HashMap::new(),
            inferred_schemas: HashMap::new(),
            sample_data: HashMap::new(),
            node_hashes: HashMap::new(),
            inject_probes,
            dynamic_context: HashMap::new(),
            config_cache: HashMap::new(),
            event_tx: None,
        }
    }

    pub fn with_event_tx(mut self, tx: mpsc::UnboundedSender<NodeEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }
    
    /// Get or compute serialized config string for a node.
    fn get_config_str(&mut self, node: &NodeInstance) -> String {
        if let Some(cached) = self.config_cache.get(&node.id) {
            return cached.clone();
        }
        let config_str = serde_json::to_string(&node.config).unwrap_or_default();
        self.config_cache.insert(node.id, config_str.clone());
        config_str
    }

    pub fn get_execution_order(&mut self, workflow: &Workflow) -> Result<Vec<NodeIndex>> {
        let mut aug_graph = workflow.graph.clone();
        let node_indices: Vec<NodeIndex> = workflow.graph.node_indices().collect();

        // Step 1: Pre-index variable producers (which variables does each node produce?)
        let mut variable_producers: HashMap<String, NodeIndex> = HashMap::new();
        for &idx in &node_indices {
            let node = &workflow.graph[idx];
            if node.type_id == "core.flow.table_to_variable" {
                // table_to_variable produces variables based on input schema mapping
                // For order inference, we treat it as a general variable producer
                // Since we don't know the exact variable names yet without compilation,
                // we'll use a special marker to indicate it produces "context" variables.
                // However, for simplicity, let's just mark it by its type.
            } else if node.type_id == "core.flow.variable_creator" {
                if let Some(vars) = node.config["variables"].as_object() {
                    for var in vars.keys() {
                        variable_producers.insert(var.clone(), idx);
                    }
                }
            }
        }

        // Step 2: Add implicit edges for variable dependencies
        for &consumer_idx in &node_indices {
            let consumer = &workflow.graph[consumer_idx];
            let config_str = self.get_config_str(consumer);
            
            // Check for ${context.*} dependencies
            if config_str.contains("${context.") {
                // If consumer uses ${context.VAR}, add edge from producer of VAR
                for (var, &producer_idx) in &variable_producers {
                    if consumer_idx != producer_idx && config_str.contains(&format!("${{context.{}}}", var)) {
                        aug_graph.add_edge(
                            producer_idx,
                            consumer_idx,
                            crate::core::graph::EdgeInstance {
                                source_port: 0,
                                target_port: 999, // Special port for variable dependency
                            },
                        );
                    }
                }

                // Add edges from ALL table_to_variable nodes since they produce dynamic variables
                for &producer_idx in &node_indices {
                    if consumer_idx != producer_idx && workflow.graph[producer_idx].type_id == "core.flow.table_to_variable" {
                        aug_graph.add_edge(
                            producer_idx,
                            consumer_idx,
                            crate::core::graph::EdgeInstance {
                                source_port: 0,
                                target_port: 999,
                            },
                        );
                    }
                }
            }
        }

        toposort(&aug_graph, None)
            .map_err(|_| anyhow!("Workflow contains cycles (possibly via variable dependencies) and cannot be compiled"))
    }

    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tokio::sync::mpsc::unbounded_channel().0,
        };
        let physical_planner = Arc::new(
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(planner),
            ]),
        );
        let query_planner =
            Arc::new(crate::core::engine::SwanFlowQueryPlanner { physical_planner });

        let state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
            ctx.state(),
        )
        .with_query_planner(query_planner)
        .build();

        let physical_plan = state.create_physical_plan(plan).await?;
        let task_ctx = Arc::new(datafusion::execution::context::TaskContext::from(&state));
        datafusion::physical_plan::collect(physical_plan, task_ctx)
            .await
            .map_err(|e| anyhow!(e))
    }
    fn generate_cache_key(
        &mut self,
        node: &NodeInstance,
        input_hashes: &HashMap<u32, String>,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(node.type_id.as_bytes());
        
        // Use cached config string to avoid repeated serialization
        let config_str = self.get_config_str(node);
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
        self.dynamic_context = workflow.context.clone();

        for &node_idx in &order {
            let node = workflow
                .get_node(node_idx)
                .ok_or_else(|| anyhow!("Node at index {:?} not found", node_idx))?;

            let resolved_config = TemplateEngine::render_json(&node.config, &self.dynamic_context);
            let inputs = self.collect_inputs(node_idx, workflow, &injected_inputs)?;
            let input_hashes = self.collect_input_hashes(node_idx, workflow, &injected_hashes)?;

            let cache_key = self.generate_cache_key(node, &input_hashes);
            self.node_hashes.insert(node.id, cache_key.clone());

            if node.type_id == "core.flow.variable_creator" {
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(NodeEvent::StateChanged { node_id: node.id, state: NodeState::Running });
                }
                if let Some(vars) = resolved_config["variables"].as_object() {
                    for (k, v) in vars {
                        self.dynamic_context.insert(k.clone(), v.clone());
                    }
                }
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(NodeEvent::StateChanged { node_id: node.id, state: NodeState::Executed });
                }
            } else if node.type_id == "core.flow.table_to_variable" {
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(NodeEvent::StateChanged { node_id: node.id, state: NodeState::Running });
                }
                if let Some(input_plan) = inputs.get(&0) {
                    let results = self.execute_plan(input_plan).await?;
                    if let Some(batch) = results.first() {
                        if batch.num_rows() > 0 {
                            let mapping = resolved_config["mapping"].as_object();
                            let schema = batch.schema();
                            for i in 0..batch.num_columns() {
                                let col_name = schema.field(i).name();
                                let var_name = mapping
                                    .and_then(|m| m.get(col_name).and_then(|v| v.as_str()))
                                    .unwrap_or(col_name);
                                let array = batch.column(i);
                                let value = if array.is_null(0) {
                                    JsonValue::Null
                                } else {
                                    match array.data_type() {
                                        arrow::datatypes::DataType::Utf8 => JsonValue::String(
                                            array
                                                .as_any()
                                                .downcast_ref::<StringArray>()
                                                .unwrap()
                                                .value(0)
                                                .to_string(),
                                        ),
                                        arrow::datatypes::DataType::Int64 => JsonValue::Number(
                                            array
                                                .as_any()
                                                .downcast_ref::<Int64Array>()
                                                .unwrap()
                                                .value(0)
                                                .into(),
                                        ),
                                        arrow::datatypes::DataType::Float64 => {
                                            serde_json::Number::from_f64(
                                                array
                                                    .as_any()
                                                    .downcast_ref::<Float64Array>()
                                                    .unwrap()
                                                    .value(0),
                                            )
                                            .map(JsonValue::Number)
                                            .unwrap_or(JsonValue::Null)
                                        }
                                        arrow::datatypes::DataType::Boolean => JsonValue::Bool(
                                            array
                                                .as_any()
                                                .downcast_ref::<BooleanArray>()
                                                .unwrap()
                                                .value(0),
                                        ),
                                        _ => JsonValue::String(format!("{:?}", array)),
                                    }
                                };
                                self.dynamic_context.insert(var_name.to_string(), value);
                            }
                        }
                    }
                }
                if let Some(ref tx) = self.event_tx {
                    let _ = tx.send(NodeEvent::StateChanged { node_id: node.id, state: NodeState::Executed });
                }
            }

            let cache_path = format!("target/swanflow_cache/{}.parquet", cache_key);
            let mut outputs = if node.materialize && std::path::Path::new(&cache_path).exists() {
                let ctx = SessionContext::new();
                let df = ctx
                    .read_parquet(&cache_path, ParquetReadOptions::default())
                    .await?;
                let mut map = HashMap::new();
                map.insert(0, df.into_optimized_plan()?);
                map
            } else {
                let compiled_node_outputs = if let Some(ref sub_workflow) = node.sub_workflow {
                    let mut sub_compiler = WorkflowCompiler::new(self.inject_probes);
                    let res = sub_compiler
                        .compile(sub_workflow, registry, Some(inputs))
                        .await?;
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
                } else if node.type_id == "core.flow.variable_creator"
                    || node.type_id == "core.flow.table_to_variable"
                {
                    HashMap::new()
                } else {
                    let behavior = registry.get_node(&node.type_id)?;
                    behavior.validate(&resolved_config)?;
                    let input_schemas = inputs.values().map(|p| p.schema().clone()).collect();
                    let metadata = behavior
                        .infer_schema(input_schemas, &resolved_config)
                        .await?;
                    if let Some(samples) = metadata.sample_data {
                        self.sample_data.insert(node.id, samples);
                    }
                    let mut input_vec: Vec<(u32, LogicalPlan)> = inputs.into_iter().collect();
                    input_vec.sort_by_key(|(port, _)| *port);
                    let ordered_inputs: Vec<LogicalPlan> =
                        input_vec.into_iter().map(|(_, p)| p).collect();
                    let output_plans = behavior
                        .propose_plan(ordered_inputs, &resolved_config)
                        .await?;
                    let mut output_map = HashMap::new();
                    for (i, plan) in output_plans.into_iter().enumerate() {
                        output_map.insert(i as u32, plan);
                    }
                    output_map
                };

                let mut wrapped_outputs = compiled_node_outputs;
                if node.materialize && !wrapped_outputs.is_empty() {
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

            if self.inject_probes
                && !outputs.is_empty()
                && node.type_id != "core.input"
                && node.type_id != "core.output"
            {
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
            // Collect all terminal nodes (nodes with no outgoing edges)
            let mut port_counter = 0;
            for &node_idx in &order {
                if workflow.graph.edges_directed(node_idx, Outgoing).count() == 0 {
                    let node_id = workflow.graph[node_idx].id;
                    if let Some(node_outputs) = self.compiled_outputs.get(&node_id) {
                        for plan in node_outputs.values() {
                            final_outputs.insert(port_counter, plan.clone());
                            port_counter += 1;
                        }
                    }
                }
            }
        }
        Ok(final_outputs)
    }

    fn collect_inputs(
        &self,
        node_idx: NodeIndex,
        workflow: &Workflow,
        _injected_inputs: &Option<HashMap<u32, LogicalPlan>>,
    ) -> Result<HashMap<u32, LogicalPlan>> {
        let mut inputs = HashMap::new();
        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let edge = edge_ref.weight();
            if edge.target_port == 999 {
                continue;
            }
            let source_node = &workflow.graph[edge_ref.source()];
            if let Some(source_outputs) = self.compiled_outputs.get(&source_node.id) {
                if let Some(plan) = source_outputs.get(&edge.source_port) {
                    inputs.insert(edge.target_port, plan.clone());
                } else if source_node.type_id != "core.flow.variable_creator"
                    && source_node.type_id != "core.flow.table_to_variable"
                {
                    return Err(anyhow!(
                        "Source port {} not found for node {}",
                        edge.source_port,
                        source_node.id
                    ));
                }
            }
        }
        Ok(inputs)
    }

    fn collect_input_hashes(
        &self,
        node_idx: NodeIndex,
        workflow: &Workflow,
        _injected_hashes: &HashMap<u32, String>,
    ) -> Result<HashMap<u32, String>> {
        let mut hashes = HashMap::new();
        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let edge = edge_ref.weight();
            if edge.target_port == 999 {
                continue;
            }
            let source_node = &workflow.graph[edge_ref.source()];
            if let Some(hash) = self.node_hashes.get(&source_node.id) {
                hashes.insert(edge.target_port, hash.clone());
            }
        }
        Ok(hashes)
    }
}
