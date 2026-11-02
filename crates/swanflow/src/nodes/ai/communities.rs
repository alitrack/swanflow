//! `core.ai.community_batches` — REAL since 2026-10-16 (UA port batch 3).
//!
//! Port of the Understand-Anything community-batching strategy: large
//! entity/node graphs are cut into communities of at most `max_community_size`
//! nodes (default 35) so a downstream LLM can summarize them one batch at a
//! time. This node is the **purely deterministic** core of that strategy —
//! no LLM, no network, no clock:
//!
//!   1. collect the node identifiers of `input_column` (default: first
//!      column), order-preserving dedup;
//!   2. build the undirected adjacency from `edges` (array of `"a|b"`
//!      strings, or object `{a: [b, ...]}`); no `edges` → empty graph;
//!   3. Leiden (leiden-rs, `seed` fixed, default 42, sequential paths only)
//!      → membership vector;
//!   4. communities larger than `max_community_size` are split into
//!      sub-batches in input (node) order; singleton communities (size 1)
//!      are merged into ONE batch named by `singletons` (default `"misc"`)
//!      or kept as standalone batches with `singletons: "keep"`;
//!   5. one output row per batch:
//!      `{batch_id, community, members, size}` where `batch_id` is shaped
//!      `c0b0`, `c0b1`, ... (community id + sub-batch index) or the misc
//!      name, and `members` is the JSON array of member node ids in input
//!      order.
//!
//! No edges ⇒ every node is a singleton ⇒ (default) all nodes land in the
//! single `misc` batch — the trivial case is the natural limit, not a
//! special path. Same input + same seed ⇒ byte-identical output.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, Int64Builder, RecordBatch, StringBuilder, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Extension, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream};
use futures::Stream;
use leiden_rs::{GraphDataBuilder, Leiden, LeidenConfig, Partition};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};

// ============================================================================
// Configuration (parsed once at plan time, shared by Logical + Exec)
// ============================================================================

/// Compiled `community_batches` config — validated at plan time so Exec never
/// re-parses (and planning fails loudly on bad config).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommunityBatchesConfig {
    /// Upper bound on nodes per output batch. A Leiden community larger than
    /// this is split into input-order sub-batches (never merged into misc).
    pub max_community_size: usize,
    /// RNG seed for leiden-rs — fixing it makes runs reproducible.
    pub seed: u64,
    /// `"keep"` → singleton communities stay standalone batches; any other
    /// string is the NAME of the single misc batch that collects all
    /// singletons (default `"misc"`).
    pub singletons: String,
    /// Name of the Utf8 input column holding node identifiers.
    /// None = first column.
    pub input_column: Option<String>,
    /// Undirected edge endpoints as node identifiers (deduped downstream;
    /// self-loops are ignored; endpoints not present in the node list are a
    /// hard error).
    pub edges: Vec<(String, String)>,
}

impl CommunityBatchesConfig {
    pub const DEFAULT_MAX_COMMUNITY_SIZE: usize = 35;
    pub const DEFAULT_SEED: u64 = 42;
    pub const DEFAULT_SINGLETONS: &'static str = "misc";

    /// True when singleton communities are kept as standalone batches.
    pub fn keep_singletons(&self) -> bool {
        self.singletons == "keep"
    }

    /// Parse + validate a node config.
    pub fn from_config(config: &JsonValue) -> Result<Self> {
        let max_community_size = match config.get("max_community_size") {
            None => Self::DEFAULT_MAX_COMMUNITY_SIZE,
            Some(v) => {
                let n = v.as_u64().ok_or_else(|| {
                    anyhow!(
                        "community_batches: max_community_size must be a non-negative integer"
                    )
                })?;
                if n == 0 {
                    return Err(anyhow!(
                        "community_batches: max_community_size must be >= 1"
                    ));
                }
                n as usize
            }
        };

        let seed = match config.get("seed") {
            None => Self::DEFAULT_SEED,
            Some(v) => v.as_u64().ok_or_else(|| {
                anyhow!("community_batches: seed must be a non-negative integer")
            })?,
        };

        if let Some(v) = config.get("singletons") {
            if v.as_str().is_none() {
                return Err(anyhow!(
                    "community_batches: singletons must be a string ('keep' or the misc batch name)"
                ));
            }
        }
        let singletons = config
            .get("singletons")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Self::DEFAULT_SINGLETONS.to_string());

        if let Some(v) = config.get("input_column") {
            if v.as_str().is_none() {
                return Err(anyhow!(
                    "community_batches: input_column must be a string"
                ));
            }
        }
        let input_column = config["input_column"].as_str().map(|s| s.to_string());

        let edges = match config.get("edges") {
            None => Vec::new(),
            Some(v) => parse_edges(v)?,
        };

        Ok(Self {
            max_community_size,
            seed,
            singletons,
            input_column,
            edges,
        })
    }
}

/// `edges` accepts two shapes (see module docs): an array of `"a|b"`
/// strings, or an object mapping node → array of neighbor nodes.
fn parse_edges(v: &JsonValue) -> Result<Vec<(String, String)>> {
    let mut edges: Vec<(String, String)> = Vec::new();
    match v {
        JsonValue::Array(items) => {
            for item in items {
                let s = item.as_str().ok_or_else(|| {
                    anyhow!(
                        "community_batches: edges array items must be 'a|b' strings"
                    )
                })?;
                edges.push(split_edge(s)?);
            }
        }
        JsonValue::Object(map) => {
            for (a, neighbours) in map {
                let arr = neighbours.as_array().ok_or_else(|| {
                    anyhow!(
                        "community_batches: edges object values must be arrays of neighbor strings (key '{a}')"
                    )
                })?;
                for b in arr {
                    let bs = b.as_str().ok_or_else(|| {
                        anyhow!(
                            "community_batches: edges object values must be arrays of neighbor strings (key '{a}')"
                        )
                    })?;
                    edges.push((a.clone(), bs.to_string()));
                }
            }
        }
        other => {
            return Err(anyhow!(
                "community_batches: edges must be an array of 'a|b' strings or an object mapping node -> neighbors (got {})",
                json_type_name(other)
            ))
        }
    }
    Ok(edges)
}

fn split_edge(s: &str) -> Result<(String, String)> {
    let parts: Vec<&str> = s.split('|').collect();
    if parts.len() != 2 {
        return Err(anyhow!(
            "community_batches: edge '{s}' must be exactly 'a|b' (one pipe separator)"
        ));
    }
    let (a, b) = (parts[0].trim(), parts[1].trim());
    if a.is_empty() || b.is_empty() {
        return Err(anyhow!("community_batches: edge '{s}' has an empty endpoint"));
    }
    Ok((a.to_string(), b.to_string()))
}

fn json_type_name(v: &JsonValue) -> &'static str {
    match v {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

// ============================================================================
// Pure batch computation (no DataFusion — unit-testable in isolation)
// ============================================================================

/// One output batch of the community partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommunityBatch {
    /// `c{community}b{sub}` for Leiden communities (sub-batch index after
    /// oversized splitting), or the misc name for the merged singleton batch.
    pub batch_id: String,
    /// Community label: the renumbered Leiden community id, or the misc name.
    pub community: String,
    /// Member node identifiers in input (first-seen) order.
    pub members: Vec<String>,
    pub size: usize,
}

impl CommunityBatch {
    fn new(batch_id: String, community: String, members: Vec<String>) -> Self {
        let size = members.len();
        Self {
            batch_id,
            community,
            members,
            size,
        }
    }
}

/// Run Leiden on an anonymous `node_count`-node graph (nodes 0..n) and return
/// the raw membership vector. Deterministic: `seed` is fixed and the
/// sequential (non-rayon) algorithm paths are forced regardless of graph
/// size, so identical inputs yield identical memberships.
pub fn run_leiden_membership(
    node_count: usize,
    edges: &[(usize, usize)],
    seed: u64,
) -> Result<Vec<usize>> {
    if node_count == 0 {
        return Ok(Vec::new());
    }
    let mut builder = GraphDataBuilder::new(node_count);
    for &(s, t) in edges {
        builder
            .add_edge(s, t, 1.0)
            .map_err(|e| anyhow!("community_batches: {e}"))?;
    }
    let graph = builder
        .build()
        .map_err(|e| anyhow!("community_batches: {e}"))?;
    let config = LeidenConfig {
        seed: Some(seed),
        // Force the sequential paths: the parallel local-moving/aggregation
        // passes (rayon feature) may converge differently for the same seed.
        parallel_local_moving_threshold: None,
        parallel_aggregation_threshold: None,
        ..LeidenConfig::default()
    };
    let output = Leiden::new(config)
        .run(&graph)
        .map_err(|e| anyhow!("community_batches: leiden run failed: {e}"))?;
    Ok(output.partition.as_slice().to_vec())
}

/// The full deterministic pipeline over an already-indexed graph:
///
/// `node_ids` — node identifiers in input order (position i = graph node i);
/// `edge_pairs` — undirected edges as (node index, node index), any
/// normalization/dedup the caller's choosing.
///
/// Leiden (seeded) → [compute_batches_from_membership].
pub fn compute_batches(
    node_ids: &[String],
    edge_pairs: &[(usize, usize)],
    config: &CommunityBatchesConfig,
) -> Result<Vec<CommunityBatch>> {
    if node_ids.is_empty() {
        return Ok(Vec::new());
    }
    if config.max_community_size == 0 {
        return Err(anyhow!("community_batches: max_community_size must be >= 1"));
    }
    let membership = run_leiden_membership(node_ids.len(), edge_pairs, config.seed)?;
    Ok(compute_batches_from_membership(node_ids, &membership, config)?.0)
}

/// Split a Leiden membership vector into output batches:
/// renumber → oversized communities split into input-order sub-batches →
/// singletons merged (default) or kept → output rows: Leiden communities
/// first (id order, sub-batches in input order), the misc batch last.
///
/// Returns `(batches, num_communities)` where `num_communities` counts the
/// Leiden communities BEFORE singleton merging (exact for diagnostics).
pub fn compute_batches_from_membership(
    node_ids: &[String],
    membership: &[usize],
    config: &CommunityBatchesConfig,
) -> Result<(Vec<CommunityBatch>, usize)> {
    if node_ids.is_empty() {
        return Ok((Vec::new(), 0));
    }
    if config.max_community_size == 0 {
        return Err(anyhow!("community_batches: max_community_size must be >= 1"));
    }
    let mut partition = Partition::from_membership(membership.to_vec());
    partition.renumber();
    let num_communities = partition.num_communities();

    let mut out: Vec<CommunityBatch> = Vec::new();
    let mut misc: Vec<(usize, String)> = Vec::new(); // (node index, id)
    for (comm, members) in partition.communities() {
        let ids: Vec<String> = members.iter().map(|&i| node_ids[i].clone()).collect();
        if ids.len() == 1 {
            if config.keep_singletons() {
                out.push(CommunityBatch::new(
                    format!("c{comm}b0"),
                    comm.to_string(),
                    ids,
                ));
            } else {
                misc.push((members[0], ids.into_iter().next().unwrap()));
            }
            continue;
        }
        // Oversized (or exactly-sized) communities: split in input order.
        for (sub, chunk) in ids.chunks(config.max_community_size).enumerate() {
            out.push(CommunityBatch::new(
                format!("c{comm}b{sub}"),
                comm.to_string(),
                chunk.to_vec(),
            ));
        }
    }
    if !misc.is_empty() {
        misc.sort_by_key(|(i, _)| *i); // restore input order across communities
        let ids: Vec<String> = misc.into_iter().map(|(_, id)| id).collect();
        out.push(CommunityBatch::new(
            config.singletons.clone(),
            config.singletons.clone(),
            ids,
        ));
    }
    Ok((out, num_communities))
}

/// Per-execution counters for the diagnostic report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommunityBatchesStats {
    pub input_rows: usize,
    pub nodes: usize,
    pub edges: usize,
    pub communities: usize,
    pub batches: usize,
}

/// Run the whole node over buffered input batches: collect node identifiers
/// (order-preserving dedup), resolve `edges` endpoints against the node
/// list, run the deterministic pipeline, materialize the output batch.
pub fn process_communities(
    batches: &[RecordBatch],
    config: &CommunityBatchesConfig,
    out_schema: SchemaRef,
) -> Result<(RecordBatch, CommunityBatchesStats), DataFusionError> {
    let mut stats = CommunityBatchesStats::default();

    // 1. Collect node identifiers (order-preserving dedup), any string-family
    //    physical flavor cast to plain Utf8.
    let mut node_ids: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (bi, batch) in batches.iter().enumerate() {
        if batch.num_columns() == 0 {
            return Err(DataFusionError::Execution(
                "community_batches: input has no columns (expected one string column of node identifiers)"
                    .into(),
            ));
        }
        let idx = match &config.input_column {
            Some(name) => batch.schema().index_of(name).map_err(|e| {
                DataFusionError::Execution(format!(
                    "community_batches: input column '{name}' not found: {e}"
                ))
            })?,
            None => 0,
        };
        let col = datafusion::arrow::compute::cast(batch.column(idx), &DataType::Utf8)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "community_batches: cannot cast input column {idx} to Utf8: {e}"
                ))
            })?;
        let ids = col
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "community_batches: input column is not string-like".into(),
                )
            })?;
        for i in 0..ids.len() {
            stats.input_rows += 1;
            if ids.is_null(i) {
                return Err(DataFusionError::Execution(format!(
                    "community_batches: input row {i} (batch {bi}) is null — node identifiers cannot be null"
                )));
            }
            let id = ids.value(i);
            if seen.insert(id.to_string()) {
                node_ids.push(id.to_string());
            }
        }
    }
    stats.nodes = node_ids.len();

    // 2. Resolve edge endpoints against the node list; undirected,
    //    normalized to (min, max) and deduped; self-loops are ignored.
    let index_of: HashMap<&str, usize> = node_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    let mut edge_pairs: BTreeSet<(usize, usize)> = BTreeSet::new();
    for (a, b) in &config.edges {
        if a == b {
            continue; // self-loop: no community signal
        }
        let ia = *index_of
            .get(a.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "community_batches: edge endpoint '{a}' is not in the node list"
                ))
            })?;
        let ib = *index_of
            .get(b.as_str())
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "community_batches: edge endpoint '{b}' is not in the node list"
                ))
            })?;
        edge_pairs.insert(if ia < ib { (ia, ib) } else { (ib, ia) });
    }
    let edges: Vec<(usize, usize)> = edge_pairs.into_iter().collect();
    stats.edges = edges.len();

    // 3. Deterministic Leiden + splitting.
    let membership = run_leiden_membership(node_ids.len(), &edges, config.seed)
        .map_err(|e| DataFusionError::Execution(format!("community_batches: {e}")))?;
    let (community_batches, num_communities) =
        compute_batches_from_membership(&node_ids, &membership, config).map_err(
            |e| DataFusionError::Execution(format!("community_batches: {e}")),
        )?;
    stats.batches = community_batches.len();
    stats.communities = num_communities;

    // 4. Materialize: {batch_id, community, members (JSON array), size}.
    let mut bid_b = StringBuilder::new();
    let mut comm_b = StringBuilder::new();
    let mut members_b = StringBuilder::new();
    let mut size_b = Int64Builder::with_capacity(community_batches.len());
    for b in &community_batches {
        let members_json = serde_json::to_string(&b.members)
            .unwrap_or_else(|_| "[]".to_string());
        bid_b.append_value(&b.batch_id);
        comm_b.append_value(&b.community);
        members_b.append_value(&members_json);
        size_b.append_value(b.size as i64);
    }
    let out = RecordBatch::try_new(
        out_schema,
        vec![
            Arc::new(bid_b.finish()),
            Arc::new(comm_b.finish()),
            Arc::new(members_b.finish()),
            Arc::new(size_b.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok((out, stats))
}

/// The fixed output schema (one row per batch).
pub fn community_batches_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("batch_id", DataType::Utf8, false),
        Field::new("community", DataType::Utf8, false),
        Field::new("members", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
    ]))
}

// ============================================================================
// Node behavior
// ============================================================================

pub struct CommunityBatchesNode;

#[async_trait]
impl NodeBehavior for CommunityBatchesNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.community_batches".to_string(),
            display_name: "Community Batches (Leiden)".to_string(),
            description: "Deterministic Leiden community batching (UA port): collects node identifiers from `input_column` (default first column, order-preserving dedup), builds the undirected adjacency from `edges` (array of 'a|b' strings or object mapping node -> neighbors; absent = edgeless graph), runs Leiden (leiden-rs, seed fixed by `seed`, default 42) and emits one row per batch with columns batch_id, community, members (JSON array of node ids in input order), size. Communities above `max_community_size` (default 35) are split into input-order sub-batches (batch_id c{community}b{sub}); singleton communities are merged into one batch named by `singletons` (default 'misc') or kept standalone with singletons='keep'. With no edges every node is a singleton, so all nodes land in the single misc batch. Purely deterministic: same input and seed produce identical output — no LLM, no network, no clock."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "One string column of node identifiers — one node per row (default: first column; override with `input_column`); duplicate ids are deduped in first-seen order".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "One row per batch: `batch_id` (c0b0 / c0b1 / misc), `community` (renumbered Leiden id or misc name), `members` (JSON array of node ids in input order), `size`".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_community_size": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 35,
                        "description": "Upper bound on nodes per output batch; larger communities are split into input-order sub-batches"
                    },
                    "seed": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 42,
                        "description": "RNG seed for Leiden — fixed by default so runs are reproducible"
                    },
                    "singletons": {
                        "type": "string",
                        "default": "misc",
                        "description": "'keep' keeps size-1 communities as standalone batches; any other string is the name of the single batch that collects all singletons"
                    },
                    "input_column": {
                        "type": "string",
                        "description": "String column holding the node identifiers (default: first column)"
                    },
                    "edges": {
                        "description": "Undirected edges: an array of 'a|b' strings, or an object mapping a node id to an array of its neighbor ids; absent means no edges (all nodes become a single misc batch)"
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        CommunityBatchesConfig::from_config(config)?;
        Ok(())
    }

    /// Pure in-memory graph computation — no external side effects.
    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        mut inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Community Batches requires one input plan"));
        }
        let config = CommunityBatchesConfig::from_config(config)?;
        let out_schema = Arc::new(DFSchema::try_from(community_batches_schema().as_ref().clone())?);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(CommunityBatchesLogical {
                config,
                schema: out_schema,
                input: Arc::new(inputs.remove(0)),
            }),
        })])
    }
}

// --- Extension wiring (real implementation, 2026-10-16; UA port batch 3) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommunityBatchesLogical {
    pub config: CommunityBatchesConfig,
    pub schema: Arc<DFSchema>,
    /// The (single) upstream plan — carried so DataFusion lowers the input.
    pub input: Arc<LogicalPlan>,
}

impl PartialOrd for CommunityBatchesLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(
            self.config
                .max_community_size
                .cmp(&other.config.max_community_size)
                .then(self.config.seed.cmp(&other.config.seed))
                .then_with(|| self.config.singletons.cmp(&other.config.singletons)),
        )
    }
}

impl UserDefinedLogicalNodeCore for CommunityBatchesLogical {
    fn name(&self) -> &str {
        "CommunityBatches"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CommunityBatches(max={}, seed={}, singletons={}, edges={})",
            self.config.max_community_size,
            self.config.seed,
            self.config.singletons,
            self.config.edges.len()
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        let input = inputs
            .into_iter()
            .next()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "CommunityBatches requires its input".into(),
                )
            })?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct CommunityBatchesExec {
    pub config: CommunityBatchesConfig,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
    pub input: Arc<dyn ExecutionPlan>,
}

impl CommunityBatchesExec {
    pub fn new(
        config: CommunityBatchesConfig,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        // CRITICAL: coalesce input partitions to 1 (same partition-drop hazard
        // as the batch-2 gate) — a UNION ALL / multi-statement SQL source
        // yields several partitions; the graph needs the WHOLE node set, so
        // executing only the caller's partition index would silently drop
        // nodes and corrupt the partitioning.
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            config,
            schema,
            cache,
            input,
        }
    }
}

impl DisplayAs for CommunityBatchesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CommunityBatchesExec(max={}, seed={})",
            self.config.max_community_size, self.config.seed
        )
    }
}

impl ExecutionPlan for CommunityBatchesExec {
    fn name(&self) -> &str {
        "CommunityBatchesExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(CommunityBatchesExec::new(
            self.config.clone(),
            self.schema.clone(),
            children.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal(
                    "CommunityBatchesExec requires its input child".into(),
                )
            })?,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(CommunityBatchesStream {
            schema: self.schema.clone(),
            config: self.config.clone(),
            input,
            buffered: Vec::new(),
            done: false,
            reported: false,
        }))
    }
}

/// The community graph needs the ENTIRE input before any output row can be
/// produced, so the stream buffers every input batch and materializes the
/// output once at end-of-stream.
pub struct CommunityBatchesStream {
    schema: SchemaRef,
    config: CommunityBatchesConfig,
    input: SendableRecordBatchStream,
    buffered: Vec<RecordBatch>,
    done: bool,
    /// Guard so exactly one diagnostic report is emitted per execution.
    reported: bool,
}

impl CommunityBatchesStream {
    fn report_diagnostic(&mut self, stats: Option<&CommunityBatchesStats>) {
        if self.reported {
            return;
        }
        self.reported = true;
        match stats {
            Some(s) => eprintln!(
                "[community_batches] rows {} / nodes {} / edges {} / communities {} / batches {}",
                s.input_rows, s.nodes, s.edges, s.communities, s.batches
            ),
            None => eprintln!("[community_batches] failed before partitioning"),
        }
    }
}

impl Stream for CommunityBatchesStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut self.input).poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.buffered.push(batch);
                    continue;
                }
                Poll::Ready(Some(Err(e))) => {
                    self.done = true;
                    self.report_diagnostic(None);
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    let result = process_communities(
                        &self.buffered,
                        &self.config,
                        self.schema.clone(),
                    );
                    match result {
                        Ok((out, stats)) => {
                            self.report_diagnostic(Some(&stats));
                            return Poll::Ready(Some(Ok(out)));
                        }
                        Err(e) => {
                            self.report_diagnostic(None);
                            return Poll::Ready(Some(Err(e)));
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl RecordBatchStream for CommunityBatchesStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod community_batches_tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;

    fn default_config() -> CommunityBatchesConfig {
        CommunityBatchesConfig::from_config(&serde_json::json!({})).unwrap()
    }

    /// Two triangles weakly coupled by the c-d bridge — the exact graph of
    /// the /tmp/leiden-verify smoke project (seed=42 → membership
    /// [0,0,0,1,1,1], 2 communities).
    fn two_triangle_edges() -> Vec<(String, String)> {
        vec![
            ("a".into(), "b".into()),
            ("a".into(), "c".into()),
            ("b".into(), "c".into()),
            ("d".into(), "e".into()),
            ("d".into(), "f".into()),
            ("e".into(), "f".into()),
            ("c".into(), "d".into()),
        ]
    }

    fn resolve(
        node_ids: &[&str],
        edges: Vec<(String, String)>,
        config: &CommunityBatchesConfig,
    ) -> Result<Vec<CommunityBatch>> {
        let ids: Vec<String> = node_ids.iter().map(|s| s.to_string()).collect();
        let index: HashMap<&str, usize> = ids
            .iter()
            .enumerate()
            .map(|(i, id)| (id.as_str(), i))
            .collect();
        let pairs: Vec<(usize, usize)> = edges
            .iter()
            .filter_map(|(a, b)| {
                if a == b {
                    return None;
                }
                Some(match (index.get(a.as_str()), index.get(b.as_str())) {
                    (Some(&ia), Some(&ib)) => (ia, ib),
                    _ => (usize::MAX, usize::MAX),
                })
            })
            .collect();
        compute_batches(&ids, &pairs, config)
    }

    fn members_json(b: &CommunityBatch) -> String {
        serde_json::to_string(&b.members).unwrap()
    }

    /// ① Two triangles → 2 batches of 3 (cross-checked against the
    /// /tmp/leiden-verify smoke run: community 0 = a,b,c; community 1 = d,e,f).
    #[test]
    fn two_triangles_two_communities_of_three() {
        let cfg = default_config();
        let out = resolve(
            &["a", "b", "c", "d", "e", "f"],
            two_triangle_edges(),
            &cfg,
        )
        .unwrap();
        assert_eq!(out.len(), 2, "two triangles -> 2 batches: {out:?}");
        assert_eq!(out[0].batch_id, "c0b0");
        assert_eq!(out[0].community, "0");
        assert_eq!(members_json(&out[0]), "[\"a\",\"b\",\"c\"]");
        assert_eq!(out[0].size, 3);
        assert_eq!(out[1].batch_id, "c1b0");
        assert_eq!(out[1].community, "1");
        assert_eq!(members_json(&out[1]), "[\"d\",\"e\",\"f\"]");
        assert_eq!(out[1].size, 3);
    }

    /// ② No edges → every node is a singleton → one misc batch holding all
    /// nodes in input order (the trivial case is a natural limit, not a
    /// special path).
    #[test]
    fn no_edges_all_nodes_single_misc_batch() {
        let cfg = default_config();
        let out = resolve(&["n1", "n2", "n3", "n4"], Vec::new(), &cfg).unwrap();
        assert_eq!(out.len(), 1, "edgeless graph -> single misc batch: {out:?}");
        assert_eq!(out[0].batch_id, "misc");
        assert_eq!(out[0].community, "misc");
        assert_eq!(
            out[0].members,
            vec!["n1", "n2", "n3", "n4"],
            "misc members keep input order"
        );
        assert_eq!(out[0].size, 4);
    }

    /// ③ A community larger than max_community_size splits into input-order
    /// sub-batches (split fragments are NOT merged into misc).
    #[test]
    fn oversized_community_split_by_max_size() {
        let mut cfg = default_config();
        cfg.max_community_size = 2;
        // 3-node triangle: one fully-connected community of size 3.
        let out = resolve(
            &["x", "y", "z"],
            vec![
                ("x".into(), "y".into()),
                ("x".into(), "z".into()),
                ("y".into(), "z".into()),
            ],
            &cfg,
        )
        .unwrap();
        assert_eq!(out.len(), 2, "size-3 community with max 2 -> 2 sub-batches: {out:?}");
        assert_eq!(out[0].batch_id, "c0b0");
        assert_eq!(out[0].members, vec!["x", "y"]);
        assert_eq!(out[1].batch_id, "c0b1");
        assert_eq!(out[1].members, vec!["z"]);
    }

    /// ④ Determinism: same input + seed, two independent runs → identical
    /// membership and identical batches.
    #[test]
    fn deterministic_identical_membership_across_runs() {
        let cfg = default_config();
        let ids = ["a", "b", "c", "d", "e", "f"];
        let pairs = [(0usize, 1usize), (0, 2), (1, 2), (3, 4), (3, 5), (4, 5), (2, 3)];
        let m1 = run_leiden_membership(6, &pairs, cfg.seed).unwrap();
        let m2 = run_leiden_membership(6, &pairs, cfg.seed).unwrap();
        assert_eq!(m1, m2, "seeded leiden must be run-to-run stable");
        assert_eq!(m1, vec![0, 0, 0, 1, 1, 1], "smoke-project membership");

        let node_strs: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
        let o1 = compute_batches(&node_strs, &pairs, &cfg).unwrap();
        let o2 = compute_batches(&node_strs, &pairs, &cfg).unwrap();
        assert_eq!(o1, o2, "batches must be byte-identical across runs");
        assert_eq!(o1.len(), 2);
    }

    /// ⑤ singletons="keep": a size-1 community stays a standalone batch
    /// (contrast: default merges it into misc).
    #[test]
    fn singletons_keep_keeps_isolated_nodes_standalone() {
        let cfg: CommunityBatchesConfig =
            CommunityBatchesConfig::from_config(&serde_json::json!({ "singletons": "keep" }))
                .unwrap();
        // Triangle a,b,c + isolated d: leiden gives community 0 = a,b,c and
        // community 1 = d (singleton).
        let out = resolve(
            &["a", "b", "c", "d"],
            vec![
                ("a".into(), "b".into()),
                ("a".into(), "c".into()),
                ("b".into(), "c".into()),
            ],
            &cfg,
        )
        .unwrap();
        assert_eq!(out.len(), 2, "keep: singleton d is its own batch: {out:?}");
        assert_eq!(out[0].members, vec!["a", "b", "c"]);
        assert_eq!(out[1].members, vec!["d"]);
        assert_eq!(out[1].size, 1);
        assert!(out[1].community != "misc", "kept singletons are not misc");

        // Contrast: the same graph under the default policy folds d into misc.
        let out_default = resolve(
            &["a", "b", "c", "d"],
            vec![
                ("a".into(), "b".into()),
                ("a".into(), "c".into()),
                ("b".into(), "c".into()),
            ],
            &default_config(),
        )
        .unwrap();
        assert_eq!(out_default.len(), 2);
        assert_eq!(out_default[1].batch_id, "misc");
        assert_eq!(out_default[1].members, vec!["d"]);
    }

    /// ⑥ Empty node list → zero batches, no error (empty misc is not emitted).
    #[test]
    fn empty_input_yields_zero_batches() {
        let cfg = default_config();
        let out = compute_batches(&[], &[], &cfg).unwrap();
        assert!(out.is_empty());
        let membership = run_leiden_membership(0, &[], cfg.seed).unwrap();
        assert!(membership.is_empty());
    }

    /// ⑦ Config surface: defaults are right and malformed configs fail loudly
    /// at plan time.
    #[test]
    fn config_validation_defaults_and_errors() {
        let cfg = default_config();
        assert_eq!(cfg.max_community_size, 35);
        assert_eq!(cfg.seed, 42);
        assert_eq!(cfg.singletons, "misc");
        assert!(cfg.input_column.is_none());
        assert!(cfg.edges.is_empty());

        let array_edges: CommunityBatchesConfig =
            CommunityBatchesConfig::from_config(&serde_json::json!({
                "edges": ["a|b", "c|d"],
                "input_column": "node",
                "max_community_size": 10,
                "seed": 7
            }))
            .unwrap();
        assert_eq!(
            array_edges.edges,
            vec![
                ("a".to_string(), "b".to_string()),
                ("c".to_string(), "d".to_string())
            ]
        );
        assert_eq!(array_edges.input_column.as_deref(), Some("node"));
        assert_eq!(array_edges.max_community_size, 10);
        assert_eq!(array_edges.seed, 7);

        let obj_edges: CommunityBatchesConfig =
            CommunityBatchesConfig::from_config(&serde_json::json!({
                "edges": { "a": ["b", "c"], "d": ["e"] }
            }))
            .unwrap();
        assert_eq!(
            obj_edges.edges,
            vec![
                ("a".to_string(), "b".to_string()),
                ("a".to_string(), "c".to_string()),
                ("d".to_string(), "e".to_string())
            ]
        );

        for (cfg, expected) in [
            (
                serde_json::json!({ "max_community_size": 0 }),
                "max_community_size",
            ),
            (
                serde_json::json!({ "max_community_size": "big" }),
                "max_community_size",
            ),
            (serde_json::json!({ "seed": "42" }), "seed"),
            (serde_json::json!({ "singletons": 7 }), "singletons"),
            (serde_json::json!({ "input_column": 3 }), "input_column"),
            (serde_json::json!({ "edges": ["abc"] }), "exactly 'a|b'"),
            (serde_json::json!({ "edges": ["a|"] }), "empty endpoint"),
            (serde_json::json!({ "edges": [42] }), "'a|b' strings"),
            (serde_json::json!({ "edges": { "a": "b" } }), "arrays of neighbor"),
            (serde_json::json!({ "edges": { "a": ["b", 1] } }), "neighbor strings"),
            (serde_json::json!({ "edges": "a|b" }), "array of 'a|b'"),
        ] {
            let err = CommunityBatchesConfig::from_config(&cfg)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains(expected),
                "config {cfg} should mention '{expected}', got: {err}"
            );
        }
    }

    /// ⑧ process_communities: RecordBatch in → output batch out, exercising
    /// input_column selection, dedup, object-form edges and the endpoint
    /// validation.
    #[test]
    fn process_communities_materializes_output_batch() {
        let mut node_b = StringBuilder::new();
        for n in ["a", "b", "c", "d", "e", "f", "a"] {
            node_b.append_value(n); // duplicate "a" on purpose: dedup
        }
        let mut extra = StringBuilder::new();
        for _ in 0..7 {
            extra.append_value("ignored");
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("extra", DataType::Utf8, true),
            Field::new("node", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(extra.finish()), Arc::new(node_b.finish())],
        )
        .unwrap();

        let cfg: CommunityBatchesConfig = CommunityBatchesConfig::from_config(
            &serde_json::json!({
                "input_column": "node",
                "edges": { "a": ["b", "c"], "b": ["c"], "c": ["d"], "d": ["e", "f"], "e": ["f"] }
            }),
        )
        .unwrap();
        let (out, stats) = process_communities(&[batch], &cfg, community_batches_schema())
            .unwrap();
        assert_eq!(stats.nodes, 6, "duplicate node ids dedup");
        assert_eq!(stats.edges, 7);
        assert_eq!(out.num_rows(), 2, "two triangles -> 2 rows: {out:?}");
        let bid = out
            .column_by_name("batch_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let members = out
            .column_by_name("members")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let size = out
            .column_by_name("size")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(bid.value(0), "c0b0");
        assert_eq!(members.value(0), "[\"a\",\"b\",\"c\"]");
        assert_eq!(size.value(0), 3);
        assert_eq!(bid.value(1), "c1b0");
        assert_eq!(members.value(1), "[\"d\",\"e\",\"f\"]");
        assert_eq!(size.value(1), 3);
    }

    /// ⑨ Edge endpoints not present in the node list are a hard error (no
    /// silent node invention).
    #[test]
    fn unknown_edge_endpoint_errors() {
        let mut node_b = StringBuilder::new();
        node_b.append_value("a");
        node_b.append_value("b");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "node",
            DataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(node_b.finish())]).unwrap();
        let cfg: CommunityBatchesConfig = CommunityBatchesConfig::from_config(
            &serde_json::json!({ "edges": ["a|z"] }),
        )
        .unwrap();
        let err = process_communities(&[batch], &cfg, community_batches_schema())
            .unwrap_err()
            .to_string();
        assert!(err.contains("'z'"), "unknown endpoint must be named: {err}");
    }

    /// ⑩ A null node identifier is a hard error (identifiers are the graph
    /// contract).
    #[test]
    fn null_node_identifier_errors() {
        let mut node_b = StringBuilder::new();
        node_b.append_value("a");
        node_b.append_null();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "node",
            DataType::Utf8,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(node_b.finish())]).unwrap();
        let err = process_communities(&[batch], &default_config(), community_batches_schema())
            .unwrap_err()
            .to_string();
        assert!(err.contains("null"), "null identifier must fail loudly: {err}");
    }
}
