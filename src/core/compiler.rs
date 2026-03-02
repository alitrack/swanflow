use anyhow::{anyhow, Result};
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
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
use crate::nodes::sql::SqlProxyLogical;

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
    pub cache_dir: String,
}

impl WorkflowCompiler {
    pub fn new(inject_probes: bool) -> Self {
        Self::with_cache_dir(inject_probes, "target/swanflow_cache".to_string())
    }

    pub fn with_cache_dir(inject_probes: bool, cache_dir: String) -> Self {
        Self {
            compiled_outputs: HashMap::new(),
            inferred_schemas: HashMap::new(),
            sample_data: HashMap::new(),
            node_hashes: HashMap::new(),
            inject_probes,
            dynamic_context: HashMap::new(),
            config_cache: HashMap::new(),
            event_tx: None,
            cache_dir,
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

        // Step 1: Pre-index variable producers and materialized table names
        let mut variable_producers: HashMap<String, NodeIndex> = HashMap::new();
        let mut table_producers: HashMap<String, NodeIndex> = HashMap::new();
        for &idx in &node_indices {
            let node = &workflow.graph[idx];
            if node.materialize {
                let table_name = format!("node_{}", &node.id.to_string()[..8]);
                table_producers.insert(table_name, idx);
            }
            if node.type_id == "core.flow.table_to_variable" {
                // ... (logic for table_to_variable)
            } else if node.type_id == "core.flow.variable_creator" {
                if let Some(vars) = node.config["variables"].as_object() {
                    for var in vars.keys() {
                        variable_producers.insert(var.clone(), idx);
                    }
                }
            }
        }

        // Step 2: Add implicit edges for variable and SQL table dependencies
        for &consumer_idx in &node_indices {
            let consumer = &workflow.graph[consumer_idx];
            let config_str = self.get_config_str(consumer);
            
            // Check for node_xxx table references in SQL nodes
            if consumer.type_id == "core.sql" {
                if let Some(query) = consumer.config["query"].as_str() {
                    for (table_name, &producer_idx) in &table_producers {
                        // Use word boundary check to avoid partial matches
                        let pattern = format!(r"\b{}\b", table_name);
                        let re = regex::Regex::new(&pattern).unwrap();
                        if consumer_idx != producer_idx && re.is_match(query) {
                            aug_graph.add_edge(
                                producer_idx,
                                consumer_idx,
                                crate::core::graph::EdgeInstance {
                                    source_port: 0,
                                    target_port: 888, // Special port for SQL table dependency
                                },
                            );
                        }
                    }
                }
            }

            // Check for ${context.*} dependencies
            if config_str.contains("${context.") {
                for (var, &producer_idx) in &variable_producers {
                    if consumer_idx != producer_idx && config_str.contains(&format!("${{context.{}}}", var)) {
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
            .map_err(|_| anyhow!("Workflow contains cycles and cannot be compiled"))
    }

    async fn execute_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let ctx = SessionContext::new();
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: tokio::sync::mpsc::unbounded_channel().0,
            ctx: ctx.clone(),
            cache_dir: self.cache_dir.clone(),
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
    ) -> Result<Vec<(u32, LogicalPlan)>> {
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
                    for (k, v) in vars { self.dynamic_context.insert(k.clone(), v.clone()); }
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
                                let var_name = mapping.and_then(|m| m.get(col_name).and_then(|v| v.as_str())).unwrap_or(col_name);
                                let array = batch.column(i);
                                let value = if array.is_null(0) { JsonValue::Null } else {
                                    match array.data_type() {
                                        datafusion::arrow::datatypes::DataType::Utf8 => JsonValue::String(array.as_any().downcast_ref::<StringArray>().unwrap().value(0).to_string()),
                                        datafusion::arrow::datatypes::DataType::Utf8View => JsonValue::String(array.as_any().downcast_ref::<datafusion::arrow::array::StringViewArray>().unwrap().value(0).to_string()),
                                        datafusion::arrow::datatypes::DataType::Int64 => JsonValue::Number(array.as_any().downcast_ref::<Int64Array>().unwrap().value(0).into()),
                                        datafusion::arrow::datatypes::DataType::Float64 => serde_json::Number::from_f64(array.as_any().downcast_ref::<Float64Array>().unwrap().value(0)).map(JsonValue::Number).unwrap_or(JsonValue::Null),
                                        datafusion::arrow::datatypes::DataType::Boolean => JsonValue::Bool(array.as_any().downcast_ref::<BooleanArray>().unwrap().value(0)),
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
                let df = ctx.read_parquet(&cache_path, ParquetReadOptions::default()).await?;
                let mut map = HashMap::new();
                map.insert(0, df.into_optimized_plan()?);
                map
            } else {
                let mut compiled_node_outputs = if let Some(ref sub_workflow) = node.sub_workflow {
                    let mut sub_compiler = WorkflowCompiler::new(self.inject_probes);
                    let res = sub_compiler.compile(sub_workflow, registry, Some(inputs)).await?;
                    let mut res_map = HashMap::new();
                    for (i, (_, plan)) in res.into_iter().enumerate() { res_map.insert(i as u32, plan); }
                    for (id, schemas) in sub_compiler.inferred_schemas { self.inferred_schemas.insert(id, schemas); }
                    for (id, samples) in sub_compiler.sample_data { self.sample_data.insert(id, samples); }
                    res_map
                } else if node.type_id == "core.input" {
                    let port_idx = node.config["port_index"].as_u64().unwrap_or(0) as u32;
                    let mut map = HashMap::new();
                    if let Some(ref injected) = injected_inputs { if let Some(plan) = injected.get(&port_idx) { map.insert(0, plan.clone()); } }
                    map
                } else if node.type_id == "core.output" {
                    inputs
                } else if node.type_id == "core.flow.variable_creator" || node.type_id == "core.flow.table_to_variable" {
                    HashMap::new()
                } else {
                    let behavior = registry.get_node(&node.type_id)?;
                    behavior.validate(&resolved_config)?;
                    let input_schemas = inputs.values().map(|p| p.schema().clone()).collect();
                    let metadata = behavior.infer_schema(input_schemas, &resolved_config).await?;
                    if let Some(samples) = metadata.sample_data { self.sample_data.insert(node.id, samples); }
                    let mut input_vec: Vec<(u32, LogicalPlan)> = inputs.into_iter().collect();
                    input_vec.sort_by_key(|(port, _)| *port);
                    let ordered_inputs: Vec<LogicalPlan> = input_vec.into_iter().map(|(_, p)| p).collect();
                    let output_plans = behavior.propose_plan(ordered_inputs, &resolved_config).await?;
                    let mut output_map = HashMap::new();
                    for (i, plan) in output_plans.into_iter().enumerate() { output_map.insert(i as u32, plan); }
                    output_map
                };

                // NEW: Update node_id in SqlProxyLogical if present
                for plan in compiled_node_outputs.values_mut() {
                    if let LogicalPlan::Extension(ref mut ext) = plan {
                        if let Some(proxy) = ext.node.as_any().downcast_ref::<SqlProxyLogical>() {
                            let mut updated_proxy = proxy.clone();
                            updated_proxy.node_id = node.id;
                            ext.node = Arc::new(updated_proxy);
                        }
                    }
                }

                if node.materialize && !compiled_node_outputs.is_empty() {
                    let table_name = format!("node_{}", &node.id.to_string()[..8]);
                    for plan in compiled_node_outputs.values_mut() {
                        *plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(MaterializerLogical {
                                node_id: node.id,
                                table_name: table_name.clone(),
                                cache_key: cache_key.clone(),
                                cache_dir: self.cache_dir.clone(),
                                input: plan.clone(),
                            }),
                        });
                    }
                }
                compiled_node_outputs
            };

            let schemas: Vec<DFSchemaRef> = outputs.values().map(|p| p.schema().clone()).collect();
            self.inferred_schemas.insert(node.id, schemas);

            if self.inject_probes && !outputs.is_empty() && node.type_id != "core.input" && node.type_id != "core.output" {
                for plan in outputs.values_mut() {
                    *plan = LogicalPlan::Extension(Extension { node: Arc::new(StateProbeLogical { node_id: node.id, input: plan.clone() }) });
                }
            }
            self.compiled_outputs.insert(node.id, outputs);
        }

        let mut final_outputs_vec = Vec::new();
        let mut port_counter = 0;

        // Collect all terminal nodes in topological order.
        // A terminal node is an explicit Output node OR a materialized node with no consumers.
        for &node_idx in &order {
            let node = &workflow.graph[node_idx];
            let has_no_outgoing = workflow.graph.edges_directed(node_idx, Outgoing).count() == 0;
            let is_output = node.type_id == "core.output";
            let is_materialized_orphan = node.materialize && has_no_outgoing;

            if is_output || is_materialized_orphan {
                if let Some(node_outputs) = self.compiled_outputs.get(&node.id) {
                    if is_output {
                        let port_idx = node.config["port_index"].as_u64().unwrap_or(0) as u32;
                        if let Some(plan) = node_outputs.get(&0) {
                            final_outputs_vec.push((port_idx, plan.clone()));
                            if port_idx >= port_counter { port_counter = port_idx + 1; }
                        }
                    } else {
                        for plan in node_outputs.values() {
                            final_outputs_vec.push((port_counter, plan.clone()));
                            port_counter += 1;
                        }
                    }
                }
            }
        }

        if final_outputs_vec.is_empty() {
            for &node_idx in &order {
                if workflow.graph.edges_directed(node_idx, Outgoing).count() == 0 {
                    if let Some(node_outputs) = self.compiled_outputs.get(&workflow.graph[node_idx].id) {
                        for plan in node_outputs.values() {
                            final_outputs_vec.push((port_counter, plan.clone()));
                            port_counter += 1;
                        }
                    }
                }
            }
        }
        Ok(final_outputs_vec)
    }

    fn collect_inputs(&self, node_idx: NodeIndex, workflow: &Workflow, _injected_inputs: &Option<HashMap<u32, LogicalPlan>>) -> Result<HashMap<u32, LogicalPlan>> {
        let mut inputs = HashMap::new();
        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let edge = edge_ref.weight();
            if edge.target_port == 999 || edge.target_port == 888 { continue; }
            let source_node = &workflow.graph[edge_ref.source()];
            if let Some(source_outputs) = self.compiled_outputs.get(&source_node.id) {
                if let Some(plan) = source_outputs.get(&edge.source_port) { inputs.insert(edge.target_port, plan.clone()); }
            }
        }
        Ok(inputs)
    }

    fn collect_input_hashes(&self, node_idx: NodeIndex, workflow: &Workflow, _injected_hashes: &HashMap<u32, String>) -> Result<HashMap<u32, String>> {
        let mut hashes = HashMap::new();
        for edge_ref in workflow.graph.edges_directed(node_idx, Incoming) {
            let edge = edge_ref.weight();
            if edge.target_port == 999 || edge.target_port == 888 { continue; }
            let source_node = &workflow.graph[edge_ref.source()];
            if let Some(hash) = self.node_hashes.get(&source_node.id) { hashes.insert(edge.target_port, hash.clone()); }
        }
        Ok(hashes)
    }
}
