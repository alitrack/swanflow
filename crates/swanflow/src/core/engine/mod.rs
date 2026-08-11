pub mod backend;
pub mod cache;
pub mod checkpoint;
pub mod daemon;
pub mod flow_vars;
pub mod health;
pub mod incremental;
pub mod datafusion_backend;
pub mod duckdb_backend;

pub use backend::{EngineCapabilities, QueryEngine, QueryEngineRef, RegisteredTable, TableSource};
pub use datafusion_backend::DataFusionEngine;
pub use duckdb_backend::DuckDBEngine;

use async_trait::async_trait;
use datafusion::common::{DFSchema, DFSchemaRef, ScalarValue};
use datafusion::error::Result;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{self, UnboundedSender};
use uuid::Uuid;

use crate::core::compiler::WorkflowCompiler;
use crate::core::datasource::{
    DuckDBScanExec, DuckDBScanLogical, ExcelReportExec, ExcelReportSinkLogical, ExcelScanExec,
    ExcelScanLogical, ExcelSinkExec, ExcelSinkLogical, ExcelTemplateExec, ExcelTemplateSinkLogical,
    SqlxScanExec, SqlxScanLogical,
};
use crate::core::graph::Workflow;
use crate::core::observer::{
    MaterializerExec, MaterializerLogical, StateProbeExec, StateProbeLogical,
};
use crate::nodes::missing::{MissingNodeExec, MissingNodeLogical};
use crate::nodes::registry::NodeRegistry;
use crate::nodes::script::python::{PythonExec, PythonLogical};
use crate::nodes::sql::SqlProxyLogical;
use crate::nodes::{NodeDiagnostic, NodeErrorClass, NodeEvent};

use duckdb::Connection;
use serde::Deserialize;
use tokio::time::{timeout, Duration as TokioDuration};

/// A single lineage record — tracks which node produced which table.
#[derive(Debug, Clone, Deserialize)]
pub struct LineageRecord {
    pub node_id: String,
    pub table_name: String,
    pub parquet_path: String,
    pub schema_json: String,
}

/// Query lineage records for a specific table or all tables.
/// Returns an empty vec if the metadata DB doesn't exist.
pub fn get_lineage(cache_dir: &str, table_name: Option<&str>) -> Vec<LineageRecord> {
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    let Ok(conn) = Connection::open(&metadata_path) else {
        return vec![];
    };
    let sql = match table_name {
        Some(name) => format!(
            "SELECT node_id, table_name, parquet_path, schema_json FROM flow_metadata WHERE table_name = '{}'",
            name.replace('\'', "''")
        ),
        None => "SELECT node_id, table_name, parquet_path, schema_json FROM flow_metadata".to_string(),
    };
    let Ok(mut stmt) = conn.prepare(&sql) else {
        return vec![];
    };
    let Ok(rows) = stmt.query_map([], |row| {
        Ok(LineageRecord {
            node_id: row.get(0)?,
            table_name: row.get(1)?,
            parquet_path: row.get(2)?,
            schema_json: row.get(3)?,
        })
    }) else {
        return vec![];
    };
    rows.flatten().collect()
}

/// Wraps an async node execution with timeout.
/// If the timeout fires, returns an error with a clear message.
async fn with_node_timeout<F, T>(
    node_id: Uuid,
    timeout_seconds: u64,
    fut: F,
) -> Result<T, datafusion::error::DataFusionError>
where
    F: std::future::Future<Output = Result<T, datafusion::error::DataFusionError>>,
{
    if timeout_seconds == 0 {
        // No timeout configured
        return fut.await;
    }
    match timeout(TokioDuration::from_secs(timeout_seconds), fut).await {
        Ok(result) => result,
        Err(_elapsed) => Err(datafusion::error::DataFusionError::Execution(format!(
            "Node {} timed out after {} seconds",
            node_id, timeout_seconds
        ))),
    }
}

/// Hooks for injecting AI intelligence into the execution flow.
#[async_trait]
pub trait IntelligenceHook: Send + Sync {
    /// Called when a node execution fails.
    /// Returns Some(new_config) to retry, or None to fail.
    async fn on_error(
        &self,
        node_id: Uuid,
        type_id: &str,
        config: &serde_json::Value,
        error: &str,
    ) -> Option<serde_json::Value>;

    /// Called after a node completes to inspect data quality.
    async fn on_data_quality(
        &self,
        node_id: Uuid,
        sample: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Option<serde_json::Value>;
}

/// Engine execution mode for SwanFlow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineMode {
    /// Default — use DataFusion for all execution (backward compatible).
    DataFusion,
    /// Use DuckDB as the primary execution engine.
    DuckDB,
    /// Auto-select based on pipeline node types.
    Auto,
}

pub struct SwanFlowEngine {
    pub ctx: Arc<SessionContext>,
    pub registry: Arc<NodeRegistry>,
    pub cache_dir: String,
    pub hook: Option<Arc<dyn IntelligenceHook>>,
    /// Optional query engine for DuckDB/Auto modes.
    pub engine: Option<QueryEngineRef>,
    /// Active engine mode.
    pub engine_mode: EngineMode,
    /// Max seconds per node execution before timeout (0 = no timeout).
    /// Applied to both DataFusion and DuckDB execution paths.
    pub node_timeout_seconds: u64,
    /// Max rows returned per node. Prevents OOM on large datasets (0 = unlimited).
    pub max_rows_per_node: u64,
    /// Max cache directory size in MB before eviction (0 = unlimited).
    pub max_cache_size_mb: u64,
}

async fn sync_metadata_to_context(ctx: &Arc<SessionContext>, cache_dir: &str) {
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    let tables = {
        let mut tables_data = Vec::new();
        if let Ok(conn) = Connection::open(&metadata_path) {
            if let Ok(mut stmt) = conn.prepare("SELECT table_name, parquet_path FROM flow_metadata")
            {
                if let Ok(rows) = stmt.query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                }) {
                    for res in rows.flatten() {
                        tables_data.push(res);
                    }
                }
            }
        }
        tables_data
    };

    for (table_name, path) in tables {
        let _ = ctx.deregister_table(&table_name);
        ctx.register_parquet(&table_name, &path, ParquetReadOptions::default())
            .await
            .ok();
    }
}

fn should_invoke_self_healing(diagnostic: &NodeDiagnostic) -> bool {
    diagnostic.class.is_recoverable()
        && !diagnostic
            .message
            .to_ascii_lowercase()
            .contains("missing node implementation")
}

/// Detect whether a node type performs external I/O — LLM API calls, Python subprocess,
/// remote DB queries, or MCP tool invocations. These nodes should report `Waiting` while
/// blocked on external resources (Herdr-inspired state model).
fn is_external_io_node(node_type: &str) -> bool {
    node_type.contains("python")
        || node_type.contains("ai.")
        || node_type.contains("llm")
        || node_type.contains("sqlx_scan")
        || node_type.contains("mcp.")
        || node_type.contains("wasm")
}

/// Count completed and in-progress checkpoint runs from the metadata store.
fn count_checkpoint_runs(cache_dir: &str) -> (usize, usize) {
    let metadata_path = format!("{}/metadata.duckdb", cache_dir);
    let Ok(conn) = duckdb::Connection::open(&metadata_path) else {
        return (0, 0);
    };
    let mut completed = 0usize;
    let mut in_progress = 0usize;

    if let Ok(mut stmt) = conn.prepare(
        "SELECT checkpoint_json FROM __execution_checkpoints",
    ) {
        if let Ok(rows) = stmt.query_map([], |row| {
            let json: String = row.get(0)?;
            Ok(json)
        }) {
            for row in rows.flatten() {
                if let Ok(cp) = serde_json::from_str::<checkpoint::EngineCheckpoint>(&row) {
                    if cp.completed_at.is_some() {
                        completed += 1;
                    } else {
                        in_progress += 1;
                    }
                }
            }
        }
    }
    (completed, in_progress)
}

impl SwanFlowEngine {
    pub fn new(registry: Arc<NodeRegistry>) -> Self {
        Self::with_cache_dir(registry, "target/swanflow_cache".to_string())
    }

    pub fn with_cache_dir(registry: Arc<NodeRegistry>, cache_dir: String) -> Self {
        std::fs::create_dir_all(&cache_dir).ok();

        let ctx = Arc::new(SessionContext::new());

        // Initialize metadata table in DuckDB directly
        let metadata_path = format!("{}/metadata.duckdb", cache_dir);
        let conn = Connection::open(&metadata_path).expect("Failed to open DuckDB metadata");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS flow_metadata (
                node_id TEXT PRIMARY KEY,
                table_name TEXT,
                parquet_path TEXT,
                schema_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )
        .expect("Failed to create metadata table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS __graph_nodes (
                id BIGINT PRIMARY KEY,
                label TEXT,
                properties TEXT
            )",
            [],
        )
        .expect("Failed to create graph nodes table");

        conn.execute(
            "CREATE TABLE IF NOT EXISTS __graph_edges (
                source_id BIGINT,
                target_id BIGINT,
                type TEXT,
                weight DOUBLE,
                PRIMARY KEY (source_id, target_id, type)
            )",
            [],
        )
        .expect("Failed to create graph edges table");

        Self {
            ctx,
            registry,
            cache_dir,
            hook: None,
            engine: None,
            engine_mode: EngineMode::DataFusion,
            node_timeout_seconds: 3600, // default 1 hour timeout
            max_rows_per_node: 10_000_000, // default 10M rows per node
            max_cache_size_mb: 1024, // default 1GB cache limit
        }
    }

    /// Create an engine with DuckDB as the query backend.
    pub fn with_engine(mut self, engine: QueryEngineRef) -> Self {
        self.engine = Some(engine);
        self.engine_mode = EngineMode::DuckDB;
        self
    }

    /// Set the engine mode explicitly.
    pub fn with_engine_mode(mut self, mode: EngineMode) -> Self {
        self.engine_mode = mode;
        self
    }

    /// Return a health snapshot of the engine.
    pub fn get_health(&self) -> health::EngineHealth {
        let cm = cache::CacheManager::new(&self.cache_dir, self.max_cache_size_mb);

        // Count completed and in-progress checkpoints from the metadata DB
        let (completed_runs, in_progress_runs) = count_checkpoint_runs(&self.cache_dir);

        health::EngineHealth {
            ready: true,
            registered_node_count: self.registry.list_specs().len(),
            cache_dir: self.cache_dir.clone(),
            cache_size_mb: cm.total_size_mb(),
            max_cache_size_mb: self.max_cache_size_mb,
            node_timeout_seconds: self.node_timeout_seconds,
            max_rows_per_node: self.max_rows_per_node,
            engine_mode: format!("{:?}", self.engine_mode),
            version: env!("CARGO_PKG_VERSION").to_string(),
            completed_runs,
            in_progress_runs,
        }
    }

    /// Resolve Auto mode to a concrete engine based on pipeline node types.
    pub fn resolve_mode(&mut self, workflow: &Workflow) -> EngineMode {
        if self.engine_mode != EngineMode::Auto {
            return self.engine_mode;
        }
        // Auto: scan nodes — if any require DataFusion (python, wasm, sqlx_scan), use DF
        for node_idx in workflow.graph.node_indices() {
            if let Some(node) = workflow.graph.node_weight(node_idx) {
                let tid = &node.type_id;
                if tid.contains("python") || tid.contains("wasm") || tid.contains("sqlx_scan") {
                    self.engine_mode = EngineMode::DataFusion;
                    return EngineMode::DataFusion;
                }
            }
        }
        // Default to DuckDB for pure SQL/transform pipelines
        self.engine_mode = EngineMode::DuckDB;
        EngineMode::DuckDB
    }

    /// Execute a workflow using the DuckDB SQL compilation pipeline.
    /// Compiles each node's LogicalPlan to SQL, executes in DuckDB, materializes results.
    pub async fn execute_workflow_duckdb(
        &self,
        workflow: &Workflow,
    ) -> Result<mpsc::UnboundedReceiver<NodeEvent>> {
        let engine = self
            .engine
            .as_ref()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(
                    "DuckDB engine not configured. Use with_engine() to set one.".to_string(),
                )
            })?;

        let (tx, rx) = mpsc::unbounded_channel();

        // Compile workflow to LogicalPlans (same as DataFusion path)
        let mut compiler = WorkflowCompiler::with_cache_dir(true, self.cache_dir.clone());
        compiler.ctx = self.ctx.clone();
        compiler.event_tx = Some(tx.clone());

        let stages = compiler
            .compile(workflow, &self.registry, Some(self.ctx.clone()))
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Workflow compilation failed: {}",
                    e
                ))
            })?;

        let engine_clone = engine.arc_clone();
        let tx_err = tx.clone();
        let duckdb_timeout_secs = self.node_timeout_seconds;
        let duckdb_max_rows = self.max_rows_per_node;
        let duckdb_cache_dir = self.cache_dir.clone();
        let duckdb_run_id = Uuid::new_v4().to_string();
        let duckdb_wf_name = workflow.name.clone();
        let duckdb_total_levels = stages.len();
        let duckdb_max_cache = self.max_cache_size_mb;

        tokio::spawn(async move {
            let cm = checkpoint::CheckpointManager::new(&duckdb_cache_dir);

            // Check for resumable checkpoint — skip already-completed levels
            let resume_from = cm.find_resumable(&duckdb_wf_name)
                .ok()
                .flatten()
                .and_then(|cp| cp.completed_levels.iter().max().map(|l| l + 1))
                .unwrap_or(0);

            for (level, terminals) in stages.into_iter().enumerate() {
                // Skip levels already completed in a previous interrupted run
                if level < resume_from {
                    continue;
                }
                // Emit Pending for all nodes in this level (Herdr-inspired state model)
                for stage_plan in &terminals {
                    let _ = tx_err.send(NodeEvent::StateChanged {
                        node_id: stage_plan.node_id,
                        state: crate::nodes::NodeState::Pending,
                    });
                }
                for stage_plan in terminals {
                    let node_id = stage_plan.node_id;
                    let node_type = stage_plan.node_type.clone();
                    let logical_plan = stage_plan.plan;

                    // Emit Running transition
                    let _ = tx_err.send(NodeEvent::StateChanged {
                        node_id,
                        state: crate::nodes::NodeState::Running,
                    });

                    // Emit Waiting for external-I/O nodes
                    if is_external_io_node(&node_type) {
                        let _ = tx_err.send(NodeEvent::StateChanged {
                            node_id,
                            state: crate::nodes::NodeState::Waiting,
                        });
                    }

                    // Compile LogicalPlan to SQL
                    let sql = match engine_clone.compile_to_sql(&logical_plan) {
                        Ok(sql) => sql,
                        Err(e) => {
                            let _ = tx_err.send(NodeEvent::Error {
                                node_id,
                                message: format!("SQL compilation failed: {}", e),
                            });
                            continue;
                        }
                    };

                    // Apply row limit to prevent OOM on large datasets
                    let sql = if duckdb_max_rows > 0 {
                        let trimmed = sql.trim();
                        if trimmed.to_uppercase().starts_with("SELECT") {
                            format!(
                                "SELECT * FROM ({}) AS _sw_limit LIMIT {}",
                                trimmed, duckdb_max_rows
                            )
                        } else {
                            sql
                        }
                    } else {
                        sql
                    };

                    // Execute SQL in DuckDB
                    let duckdb_node_id = node_id;
                    match with_node_timeout(duckdb_node_id, duckdb_timeout_secs, async {
                        engine_clone.execute_sql(&sql).await
                    }).await {
                        Ok(batches) => {
                            if !batches.is_empty() {
                                // Materialize result as a registered table
                                let table_name =
                                    format!("node_{}", &node_id.to_string()[..8]);
                                let schema = batches[0].schema();
                                if let Err(e) = engine_clone
                                    .register_table(RegisteredTable {
                                        name: table_name.clone(),
                                        schema,
                                        source: TableSource::InMemory {
                                            batches: batches.clone(),
                                        },
                                    })
                                    .await
                                {
                                    let _ = tx_err.send(NodeEvent::Error {
                                        node_id,
                                        message: format!(
                                            "Materialization failed: {}",
                                            e
                                        ),
                                    });
                                    continue;
                                }
                                let _ = tx_err.send(NodeEvent::Materialized {
                                    node_id,
                                    table_name,
                                    parquet_path: String::new(),
                                });
                            }
                            let _ = tx_err.send(NodeEvent::StateChanged {
                                node_id,
                                state: crate::nodes::NodeState::Executed,
                            });
                        }
                        Err(e) => {
                            let _ = tx_err.send(NodeEvent::Error {
                                node_id,
                                message: format!(
                                    "DuckDB execution failed (level {}): {}",
                                    level, e
                                ),
                            });
                        }
                    }
                }
                // Save checkpoint after each level
                let _ = cm.save_checkpoint(&checkpoint::EngineCheckpoint {
                    run_id: duckdb_run_id.clone(),
                    workflow_hash: duckdb_wf_name.clone(),
                    workflow_name: duckdb_wf_name.clone(),
                    completed_levels: (0..=level).collect(),
                    total_levels: duckdb_total_levels,
                    materialized_tables: std::collections::HashMap::new(),
                    flow_vars: std::collections::HashMap::new(),
                    started_at: "".to_string(),
                    completed_at: None,
                });
            }
            // Mark workflow as completed
            let _ = cm.save_checkpoint(&checkpoint::EngineCheckpoint {
                run_id: duckdb_run_id,
                workflow_hash: duckdb_wf_name.clone(),
                workflow_name: duckdb_wf_name.clone(),
                completed_levels: (0..duckdb_total_levels).collect(),
                total_levels: duckdb_total_levels,
                materialized_tables: std::collections::HashMap::new(),
                flow_vars: std::collections::HashMap::new(),
                started_at: "".to_string(),
                completed_at: Some("".to_string()),
            });
            let cache_mgr = cache::CacheManager::new(&duckdb_cache_dir, duckdb_max_cache);
            cache_mgr.evict();
            drop(tx_err);
        });

        Ok(rx)
    }

    pub async fn execute_workflow(
        &self,
        workflow: &Workflow,
    ) -> Result<mpsc::UnboundedReceiver<NodeEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();

        let mut compiler = WorkflowCompiler::with_cache_dir(true, self.cache_dir.clone());
        compiler.ctx = self.ctx.clone(); // CRITICAL: Share the same context!
        compiler.event_tx = Some(tx.clone());

        let stages = match compiler
            .compile(workflow, &self.registry, Some(self.ctx.clone()))
            .await
        {
            Ok(stages) => stages,
            Err(e) => {
                let message = e.to_string();
                if !message.contains("\"class\"") {
                    let fallback = NodeDiagnostic::new(
                        Uuid::nil(),
                        "workflow".to_string(),
                        NodeErrorClass::Planning,
                        message,
                    );
                    let _ = tx.send(NodeEvent::Error {
                        node_id: Uuid::nil(),
                        message: fallback.to_event_message(),
                    });
                }
                drop(tx);
                return Ok(rx);
            }
        };

        if stages.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "Workflow has no outputs".to_string(),
            ));
        }

        let planner = SwanFlowPlanner {
            event_tx: tx.clone(),
            ctx: self.ctx.clone(),
            cache_dir: self.cache_dir.clone(),
        };
        let physical_planner = Arc::new(DefaultPhysicalPlanner::with_extension_planners(vec![
            Arc::new(planner.clone()),
        ]));
        let query_planner = Arc::new(SwanFlowQueryPlanner { physical_planner });

        // Use shared context directly in the task
        let ctx_shared = self.ctx.clone();
        let cache_dir_clone = self.cache_dir.clone();
        let query_planner_clone = query_planner.clone();
        let hook_shared = self.hook.clone();
        let _registry_shared = self.registry.clone();
        let node_timeout_seconds = self.node_timeout_seconds;
        let df_cache_dir = self.cache_dir.clone();
        let df_run_id = Uuid::new_v4().to_string();
        let df_wf_name = workflow.name.clone();
        let df_total_levels = stages.len();
        let df_max_cache = self.max_cache_size_mb;

        tokio::spawn(async move {
            let cm = checkpoint::CheckpointManager::new(&df_cache_dir);

            // Check for resumable checkpoint — skip already-completed levels
            let resume_from = cm.find_resumable(&df_wf_name)
                .ok()
                .flatten()
                .and_then(|cp| cp.completed_levels.iter().max().map(|l| l + 1))
                .unwrap_or(0);

            // STAGED TOPOLOGICAL EXECUTION: run each level stage in order
            for (level, mut terminals) in stages.into_iter().enumerate() {
                // Skip levels already completed in a previous interrupted run
                if level < resume_from {
                    continue;
                }
                println!("DEBUG: Executing Stage Level {}", level);
                terminals
                    .sort_by(|a, b| a.node_id.cmp(&b.node_id).then_with(|| a.port.cmp(&b.port)));

                // 1. Initial Sync for this level
                sync_metadata_to_context(&ctx_shared, &cache_dir_clone).await;

                // Emit Pending for all nodes in this level (Herdr-inspired state model)
                for stage_plan in &terminals {
                    let _ = tx.send(NodeEvent::StateChanged {
                        node_id: stage_plan.node_id,
                        state: crate::nodes::NodeState::Pending,
                    });
                }

                // 2. Planning and execution for all terminals at this level
                for stage_plan in terminals {
                    let node_id = stage_plan.node_id;
                    let node_type = stage_plan.node_type.clone();
                    let port = stage_plan.port;
                    let logical_plan = stage_plan.plan;
                    println!(
                        "DEBUG: Stage Execution for level {}, node {}, port {}",
                        level, node_id, port
                    );

                    // Emit Running (StateProbeExec also sends this during stream, but
                    // emit early here for immediate feedback before planning starts)
                    let _ = tx.send(NodeEvent::StateChanged {
                        node_id,
                        state: crate::nodes::NodeState::Running,
                    });

                    // Emit Waiting for external-I/O nodes (LLM, Python, remote DB, MCP)
                    if is_external_io_node(&node_type) {
                        let _ = tx.send(NodeEvent::StateChanged {
                            node_id,
                            state: crate::nodes::NodeState::Waiting,
                        });
                    }

                    let state = ctx_shared.state();
                    let state = SessionStateBuilder::new_from_existing(state)
                        .with_query_planner(query_planner_clone.clone())
                        .build();

                    match state.create_physical_plan(&logical_plan).await {
                        Ok(physical_plan) => {
                            let task_ctx = ctx_shared.task_ctx();
                            let node_id_exec = node_id;
                            let physical_plan_clone = physical_plan.clone();
                            match with_node_timeout(node_id_exec, node_timeout_seconds, async {
                                datafusion::physical_plan::collect(physical_plan_clone, task_ctx).await
                            }).await
                            {
                                Ok(batches) => {
                                    if let Some(hook) = &hook_shared {
                                        if !batches.is_empty() {
                                            let _ =
                                                hook.on_data_quality(node_id, &batches[0]).await;
                                        }
                                    }

                                    // MANUAL MATERIALIZATION: Register as table so it's queryable
                                    if !batches.is_empty() {
                                        let table_name =
                                            format!("node_{}", &node_id.to_string()[..8]);
                                        if let Ok(df) = ctx_shared.read_batches(batches.clone()) {
                                            let _ = ctx_shared
                                                .register_table(&table_name, df.into_view());
                                            let _ = tx.send(NodeEvent::Materialized {
                                                node_id,
                                                table_name,
                                                parquet_path: "".to_string(), // In-memory for now
                                            });
                                        }
                                    }

                                    let _ = tx.send(NodeEvent::StateChanged {
                                        node_id,
                                        state: crate::nodes::NodeState::Executed,
                                    });
                                }
                                Err(e) => {
                                    let diagnostic = NodeDiagnostic::new(
                                        node_id,
                                        node_type.clone(),
                                        NodeErrorClass::Execution,
                                        format!("Execution error: {}", e),
                                    );
                                    let mut handled = false;
                                    if let Some(hook) = &hook_shared {
                                        if should_invoke_self_healing(&diagnostic) {
                                            if let Some(_fix) = hook
                                                .on_error(
                                                    node_id,
                                                    &node_type,
                                                    &json!({}),
                                                    &diagnostic.message,
                                                )
                                                .await
                                            {
                                                println!("DEBUG: IntelligenceHook proposed a fix for node {}", node_id);
                                                handled = true;
                                            }
                                        }
                                    }
                                    if !handled {
                                        let _ = tx.send(NodeEvent::Error {
                                            node_id,
                                            message: diagnostic.to_event_message(),
                                        });
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            // RETRY after Sync
                            println!("DEBUG: Level {} node {} port {} planning failed, retrying after sync...", level, node_id, port);
                            sync_metadata_to_context(&ctx_shared, &cache_dir_clone).await;
                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

                            let state = ctx_shared.state();
                            let state = SessionStateBuilder::new_from_existing(state)
                                .with_query_planner(query_planner_clone.clone())
                                .build();

                            match state.create_physical_plan(&logical_plan).await {
                                Ok(physical_plan) => {
                                    let task_ctx = ctx_shared.task_ctx();
                                    let retry_node_id = node_id;
                                    let physical_plan_clone2 = physical_plan.clone();
                                    match with_node_timeout(retry_node_id, node_timeout_seconds, async {
                                        datafusion::physical_plan::collect(physical_plan_clone2, task_ctx).await
                                    }).await
                                    {
                                        Ok(_batches) => {
                                            let _ = tx.send(NodeEvent::StateChanged {
                                                node_id,
                                                state: crate::nodes::NodeState::Executed,
                                            });
                                        }
                                        Err(e) => {
                                            let diagnostic = NodeDiagnostic::new(
                                                node_id,
                                                node_type.clone(),
                                                NodeErrorClass::Execution,
                                                format!("Execution error: {}", e),
                                            );
                                            let mut handled = false;
                                            if let Some(hook) = &hook_shared {
                                                if should_invoke_self_healing(&diagnostic) {
                                                    if let Some(_fix) = hook
                                                        .on_error(
                                                            node_id,
                                                            &node_type,
                                                            &json!({}),
                                                            &diagnostic.message,
                                                        )
                                                        .await
                                                    {
                                                        handled = true;
                                                    }
                                                }
                                            }
                                            if !handled {
                                                let _ = tx.send(NodeEvent::Error {
                                                    node_id,
                                                    message: diagnostic.to_event_message(),
                                                });
                                            }
                                        }
                                    }
                                }
                                Err(e2) => {
                                    let diagnostic = NodeDiagnostic::new(
                                        node_id,
                                        node_type.clone(),
                                        NodeErrorClass::Planning,
                                        format!(
                                            "Planning error after sync: {} (Original: {})",
                                            e2, e
                                        ),
                                    );
                                    let mut handled = false;
                                    if let Some(hook) = &hook_shared {
                                        if should_invoke_self_healing(&diagnostic) {
                                            if let Some(_fix) = hook
                                                .on_error(
                                                    node_id,
                                                    &node_type,
                                                    &json!({}),
                                                    &diagnostic.message,
                                                )
                                                .await
                                            {
                                                println!("DEBUG: IntelligenceHook proposed a fix for planning error on node {}", node_id);
                                                handled = true;
                                            }
                                        }
                                    }
                                    if !handled {
                                        let _ = tx.send(NodeEvent::Error {
                                            node_id,
                                            message: diagnostic.to_event_message(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }

                // Final Sync after this level to be absolutely sure
                sync_metadata_to_context(&ctx_shared, &cache_dir_clone).await;

                // Save checkpoint after each level
                let _ = cm.save_checkpoint(&checkpoint::EngineCheckpoint {
                    run_id: df_run_id.clone(),
                    workflow_hash: df_wf_name.clone(),
                    workflow_name: df_wf_name.clone(),
                    completed_levels: (0..=level).collect(),
                    total_levels: df_total_levels,
                    materialized_tables: std::collections::HashMap::new(),
                    flow_vars: std::collections::HashMap::new(),
                    started_at: "".to_string(),
                    completed_at: None,
                });
            }

            // Final Sync to ensure results are visible to main thread
            sync_metadata_to_context(&ctx_shared, &cache_dir_clone).await;

            // Mark workflow as completed
            let _ = cm.save_checkpoint(&checkpoint::EngineCheckpoint {
                run_id: df_run_id,
                workflow_hash: df_wf_name.clone(),
                workflow_name: df_wf_name.clone(),
                completed_levels: (0..df_total_levels).collect(),
                total_levels: df_total_levels,
                materialized_tables: std::collections::HashMap::new(),
                flow_vars: std::collections::HashMap::new(),
                started_at: "".to_string(),
                completed_at: Some("".to_string()),
            });
            let cache_mgr = cache::CacheManager::new(&df_cache_dir, df_max_cache);
            cache_mgr.evict();
        });

        Ok(rx)
    }

    pub async fn infer_metadata(&self, workflow: &Workflow) -> Result<WorkflowMetadata> {
        let mut compiler = WorkflowCompiler::with_cache_dir(true, self.cache_dir.clone());
        compiler.ctx = self.ctx.clone();
        let registry = self.registry.clone();
        let _ = compiler
            .compile(workflow, &registry, Some(self.ctx.clone()))
            .await
            .map_err(|e| datafusion::error::DataFusionError::Plan(e.to_string()))?;

        Ok(WorkflowMetadata {
            schemas: compiler.inferred_schemas,
            sample_data: compiler.sample_data,
            profiles: HashMap::new(),
            diagnostics: compiler.diagnostics,
        })
    }

    /// Execute a YAML workflow with step-mode awareness.
    /// Orchestrates fan_out, collect, conditional, and loop modes.
    pub async fn execute_yaml_workflow(
        &self,
        workflow: &Workflow,
    ) -> Result<mpsc::UnboundedReceiver<NodeEvent>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let registry = self.registry.clone();
        let cache_dir = self.cache_dir.clone();

        // 1. Build a linear task list in topological order with mode annotations
        let mut compiler = WorkflowCompiler::with_cache_dir(true, cache_dir.clone());
        compiler.ctx = self.ctx.clone();

        let ctx_shared = self.ctx.clone();
        let hook_shared = self.hook.clone();
        let registry_clone = registry.clone();
        let workflow = workflow.clone();
        let yaml_timeout_secs = self.node_timeout_seconds;
        let yaml_cache_dir = self.cache_dir.clone();
        let yaml_run_id = Uuid::new_v4().to_string();
        let yaml_wf_name = workflow.name.clone();
        let yaml_max_cache = self.max_cache_size_mb;

        tokio::spawn(async move {
            let cm = checkpoint::CheckpointManager::new(&yaml_cache_dir);
            // Collect topological order
            let sorted_indices = match petgraph::algo::toposort(&workflow.graph, None) {
                Ok(v) => v,
                Err(_) => {
                    let _ = tx.send(NodeEvent::Error {
                        node_id: Uuid::nil(),
                        message: serde_json::json!({
                            "message": "Cycle detected in workflow",
                            "class": "Planning"
                        })
                        .to_string(),
                    });
                    return;
                }
            };

            // Build a node lookup: node_id -> NodeInstance
            let node_map: HashMap<Uuid, &crate::core::graph::NodeInstance> = sorted_indices
                .iter()
                .filter_map(|idx| {
                    let node = workflow.graph.node_weight(*idx)?;
                    Some((node.id, node))
                })
                .collect();

            // Store context variables ({{var_name}})
            let mut step_context: std::collections::HashMap<String, serde_json::Value> =
                workflow.context.clone();

            for (_i, &node_idx) in sorted_indices.iter().enumerate() {
                let node = match workflow.graph.node_weight(node_idx) {
                    Some(n) => n.clone(),
                    None => continue,
                };

                // Resolve and store variable context for this step
                if let Some(output_var) = &node.output_var {
                    step_context.insert(output_var.clone(), serde_json::Value::Null);
                }

                // Handle loop nodes by iterating their sub-workflow
                let result = if node.mode == crate::core::graph::StepMode::Loop
                    && node.sub_workflow.is_some()
                {
                    let sub_wf = node.sub_workflow.as_ref().unwrap();
                    let max_iter = node.max_iterations.max(1);
                    let mut all_events = Vec::new();
                    let mut last_err: Option<String> = None;

                    for iter in 0..max_iter {
                        let _ = tx.send(NodeEvent::StateChanged {
                            node_id: node.id,
                            state: crate::nodes::NodeState::Running,
                        });
                        step_context
                            .insert("iteration".to_string(), serde_json::json!(iter + 1));

                        // Clone engine pieces for sub-workflow execution
                        let sub_engine = SwanFlowEngine {
                            ctx: ctx_shared.clone(),
                            registry: registry_clone.clone(),
                            cache_dir: cache_dir.clone(),
                            hook: hook_shared.clone(),
                            engine: None,
                            engine_mode: crate::core::engine::EngineMode::Auto,
                            node_timeout_seconds: yaml_timeout_secs,
                            max_rows_per_node: 0, // unlimited for sub-workflow (parent applies limits)
                            max_cache_size_mb: 0, // unlimited for sub-workflow
                        };

                        match sub_engine.execute_workflow(sub_wf).await {
                            Ok(mut sub_rx) => {
                                while let Some(evt) = sub_rx.recv().await {
                                    all_events.push(evt);
                                }
                            }
                            Err(e) => {
                                last_err = Some(e.to_string());
                                break;
                            }
                        }

                        // Check until condition
                        if let Some(until) = &node.until {
                            let combined: String = all_events
                                .iter()
                                .filter_map(|e| match e {
                                    NodeEvent::Materialized { .. } => Some("done"),
                                    _ => None,
                                })
                                .collect::<Vec<_>>()
                                .join("");
                            if combined.contains(until) {
                                break;
                            }
                        }
                    }

                    if let Some(err) = last_err {
                        Err(datafusion::error::DataFusionError::Plan(err))
                    } else {
                        Ok("loop_complete".to_string())
                    }
                } else {
                    // Execute based on mode
                    Self::execute_node_with_mode(
                    &node,
                    &node_map,
                    &workflow,
                    node_idx,
                    &mut step_context,
                    &ctx_shared,
                    &cache_dir,
                    &registry_clone,
                    &tx,
                    &hook_shared,
                )
                .await
                };

                let _ = tx.send(NodeEvent::StateChanged {
                    node_id: node.id,
                    state: crate::nodes::NodeState::Executed,
                });

                // Handle error modes
                if let Err(e) = result {
                    let should_fail = match &node.error_mode {
                        crate::core::graph::ErrorMode::Fail => true,
                        crate::core::graph::ErrorMode::Skip => false,
                        crate::core::graph::ErrorMode::Retry(max_retries) => {
                            let mut retried = false;
                            for attempt in 1..=*max_retries {
                                println!(
                                    "DEBUG: Retrying node {} (attempt {}/{})",
                                    node.id, attempt, max_retries
                                );
                                let retry_result = Self::execute_node_with_mode(
                                    &node,
                                    &node_map,
                                    &workflow,
                                    node_idx,
                                    &mut step_context,
                                    &ctx_shared,
                                    &cache_dir,
                                    &registry_clone,
                                    &tx,
                                    &hook_shared,
                                )
                                .await;
                                if retry_result.is_ok() {
                                    retried = true;
                                    break;
                                }
                            }
                            !retried
                        }
                    };
                    if should_fail {
                        let _ = tx.send(NodeEvent::Error {
                            node_id: node.id,
                            message: e.to_string(),
                        });
                    }
                }
            }

            sync_metadata_to_context(&ctx_shared, &cache_dir).await;

            // Mark YAML workflow as completed
            let _ = cm.save_checkpoint(&checkpoint::EngineCheckpoint {
                run_id: yaml_run_id,
                workflow_hash: yaml_wf_name.clone(),
                workflow_name: yaml_wf_name.clone(),
                completed_levels: vec![0],
                total_levels: 1,
                materialized_tables: std::collections::HashMap::new(),
                flow_vars: std::collections::HashMap::new(),
                started_at: "".to_string(),
                completed_at: Some("".to_string()),
            });
            let cache_mgr = cache::CacheManager::new(&yaml_cache_dir, yaml_max_cache);
            cache_mgr.evict();
            drop(tx);
        });

        Ok(rx)
    }

    /// Execute a single node, respecting its mode (fan_out, collect, conditional, loop).
    async fn execute_node_with_mode(
        node: &crate::core::graph::NodeInstance,
        _node_map: &std::collections::HashMap<Uuid, &crate::core::graph::NodeInstance>,
        workflow: &Workflow,
        node_idx: petgraph::stable_graph::NodeIndex,
        step_context: &mut std::collections::HashMap<String, serde_json::Value>,
        ctx: &Arc<SessionContext>,
        cache_dir: &str,
        registry: &Arc<NodeRegistry>,
        tx: &mpsc::UnboundedSender<NodeEvent>,
        hook: &Option<Arc<dyn IntelligenceHook>>,
    ) -> Result<String> {
        use petgraph::visit::EdgeRef;
        use petgraph::Direction;
        if node.mode == crate::core::graph::StepMode::Conditional {
            // Find previous step output
            let mut prev_output = String::new();
            for edge in workflow.graph.edges_directed(node_idx, Direction::Incoming) {
                let src_node = workflow.graph.node_weight(edge.source()).unwrap();
                let table_name = format!("node_{}", &src_node.id.to_string()[..8]);
                if let Ok(df) = ctx
                    .sql(&format!("SELECT * FROM \"{}\" LIMIT 1", table_name))
                    .await
                {
                    if let Ok(batches) = df.collect().await {
                        for batch in &batches {
                            use datafusion::arrow::util::display::ArrayFormatter;
                            use datafusion::arrow::util::display::FormatOptions;
                            let opts = FormatOptions::default();
                            for col_idx in 0..batch.num_columns() {
                                let col = batch.column(col_idx);
                                if let Ok(fmt) = ArrayFormatter::try_new(col, &opts) {
                                    for row in 0..batch.num_rows() {
                                        prev_output.push_str(&format!("{}", fmt.value(row)));
                                        prev_output.push('\n');
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Check condition
            let condition_met = if let Some(cond) = &node.condition {
                prev_output.contains(cond)
            } else if let Some(when_expr) = &node.when {
                // Simple when expression: check if {{input}} contains the when value
                let input_val = step_context
                    .get("input")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                input_val.contains(when_expr)
            } else {
                true
            };

            if !condition_met {
                println!(
                    "DEBUG: Conditional node {} skipped (condition not met)",
                    node.id
                );
                return Ok(String::new());
            }
        }

        // Handle loop mode
        if node.mode == crate::core::graph::StepMode::Loop {
            let until = node.until.clone().unwrap_or_default();
            let max_iter = node.max_iterations.max(1);

            for iteration in 0..max_iter {
                println!(
                    "DEBUG: Loop node {} iteration {}/{}",
                    node.id,
                    iteration + 1,
                    max_iter
                );

                // Update context with iteration count
                step_context.insert("iteration".to_string(), serde_json::json!(iteration + 1));

                let output = Self::compile_and_execute_node(
                    node,
                    workflow,
                    node_idx,
                    step_context,
                    ctx,
                    cache_dir,
                    registry,
                    tx,
                    hook,
                )
                .await?;

                // Check if loop should stop
                if !until.is_empty() && output.contains(&until) {
                    println!(
                        "DEBUG: Loop node {} stopping at iteration {}: found '{}'",
                        node.id,
                        iteration + 1,
                        until
                    );
                    break;
                }
            }
            return Ok(format!("Loop completed ({} iterations)", max_iter));
        }

        // Handle fan_out mode: find children and execute them in parallel
        if node.mode == crate::core::graph::StepMode::FanOut {
            let children: Vec<_> = workflow
                .graph
                .edges_directed(node_idx, Direction::Outgoing)
                .map(|edge| {
                    let child_node = workflow.graph.node_weight(edge.target()).unwrap();
                    child_node.clone()
                })
                .collect();

            if children.is_empty() {
                // No fan-out children, just execute this node normally
                return Self::compile_and_execute_node(
                    node,
                    workflow,
                    node_idx,
                    step_context,
                    ctx,
                    cache_dir,
                    registry,
                    tx,
                    hook,
                )
                .await;
            }

            // Execute children in parallel, honoring each child's error_mode
            // (elastic fan-out, borrowed from Forge research #2026081102).
            let mut handles = Vec::new();
            for child in &children {
                let child_ctx = ctx.clone();
                let child_cache = cache_dir.to_string();
                let child_registry = registry.clone();
                let child_tx = tx.clone();
                let child_hook = hook.clone();
                let child_node = child.clone();
                let child_workflow = workflow.clone();
                let child_idx = workflow
                    .graph
                    .node_indices()
                    .find(|i| workflow.graph[*i].id == child.id)
                    .unwrap_or(node_idx);
                let mut child_step_context = step_context.clone();

                handles.push(tokio::spawn(async move {
                    // Execute once, or retry up to max_retries when the
                    // child's error_mode is Retry (matches the single-node
                    // retry loop semantics).
                    let result = match &child_node.error_mode {
                        crate::core::graph::ErrorMode::Retry(max_retries) => {
                            let mut last_err: Option<anyhow::Error> = None;
                            let mut ok_output: Option<String> = None;
                            for attempt in 1..=*max_retries {
                                println!(
                                    "DEBUG: Retrying node {} (attempt {}/{})",
                                    child_node.id, attempt, max_retries
                                );
                                match Self::compile_and_execute_node(
                                    &child_node,
                                    &child_workflow,
                                    child_idx,
                                    &mut child_step_context,
                                    &child_ctx,
                                    &child_cache,
                                    &child_registry,
                                    &child_tx,
                                    &child_hook,
                                )
                                .await
                                {
                                    Ok(out) => {
                                        ok_output = Some(out);
                                        break;
                                    }
                                    Err(e) => {
                                        last_err = Some(anyhow::Error::from(e));
                                    }
                                }
                            }
                            match ok_output {
                                Some(out) => Ok(out),
                                None => Err(last_err
                                    .unwrap_or_else(|| anyhow::anyhow!("retries exhausted"))),
                            }
                        }
                        _ => Self::compile_and_execute_node(
                            &child_node,
                            &child_workflow,
                            child_idx,
                            &mut child_step_context,
                            &child_ctx,
                            &child_cache,
                            &child_registry,
                            &child_tx,
                            &child_hook,
                        )
                        .await
                        .map_err(anyhow::Error::from),
                    };
                    let outcome = classify_child_outcome(&child_node.error_mode, result, false);
                    (child_node.id, outcome)
                }));
            }

            let mut outputs = Vec::new();
            let mut hard_errors: Vec<String> = Vec::new();
            for handle in handles {
                match handle.await {
                    Ok((child_id, outcome)) => {
                        aggregate_child_outcome(
                            &mut outputs,
                            &mut hard_errors,
                            &tx,
                            child_id,
                            outcome,
                        );
                    }
                    Err(join_err) => {
                        // Panicked child task: always a hard error (R4).
                        let outcome = classify_child_outcome(
                            &crate::core::graph::ErrorMode::Fail,
                            Err(anyhow::anyhow!("{}", join_err)),
                            true,
                        );
                        if let ChildOutcome::HardError(msg) = outcome {
                            hard_errors.push(msg);
                        }
                    }
                }
            }

            if !hard_errors.is_empty() {
                let _ = tx.send(NodeEvent::StateChanged {
                    node_id: node.id,
                    state: crate::nodes::NodeState::Error(format!(
                        "fan-out step failed: {}",
                        hard_errors.join("; ")
                    )),
                });
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "fan-out step {} failed: {}",
                    node.id,
                    hard_errors.join("; ")
                )));
            }

            // Collect mode: if there's a subsequent collect node, it will be processed separately
            // For now, just return joined outputs
            return Ok(outputs.join("\n---\n"));
        }

        // Handle collect mode: gather outputs from fan-out predecessors
        if node.mode == crate::core::graph::StepMode::Collect {
            let predecessors: Vec<_> = workflow
                .graph
                .edges_directed(node_idx, Direction::Incoming)
                .map(|edge| {
                    let src_node = workflow.graph.node_weight(edge.source()).unwrap();
                    src_node.clone()
                })
                .collect();

            let mut collected = Vec::new();
            for pred in &predecessors {
                let table_name = format!("node_{}", &pred.id.to_string()[..8]);
                if let Ok(df) = ctx.sql(&format!("SELECT * FROM \"{}\"", table_name)).await {
                    if let Ok(batches) = df.collect().await {
                        for batch in &batches {
                            use datafusion::arrow::util::display::ArrayFormatter;
                            use datafusion::arrow::util::display::FormatOptions;
                            let opts = FormatOptions::default();
                            for col_idx in 0..batch.num_columns() {
                                let col = batch.column(col_idx);
                                if let Ok(fmt) = ArrayFormatter::try_new(col, &opts) {
                                    for row in 0..batch.num_rows() {
                                        collected.push(fmt.value(row).to_string());
                                    }
                                } else {
                                    collected.push(format!("{:?}", col));
                                }
                            }
                        }
                    }
                }
            }
            let joined = collected.join("\n---\n");
            step_context.insert("input".to_string(), serde_json::json!(joined));
            return Ok(joined);
        }

        // Default sequential mode: compile and execute
        Self::compile_and_execute_node(
            node,
            workflow,
            node_idx,
            step_context,
            ctx,
            cache_dir,
            registry,
            tx,
            hook,
        )
        .await
    }

    /// Compile and execute a single node, returning its text output.
    async fn compile_and_execute_node(
        node: &crate::core::graph::NodeInstance,
        workflow: &Workflow,
        node_idx: petgraph::stable_graph::NodeIndex,
        step_context: &mut std::collections::HashMap<String, serde_json::Value>,
        ctx: &Arc<SessionContext>,
        cache_dir: &str,
        registry: &Arc<NodeRegistry>,
        _tx: &mpsc::UnboundedSender<NodeEvent>,
        _hook: &Option<Arc<dyn IntelligenceHook>>,
    ) -> Result<String> {
        use crate::core::templating::TemplateEngine;
        use petgraph::visit::EdgeRef;
        use petgraph::Direction;

        // 1. Render config with template engine (supports {{input}}, {{var_name}})
        let rendered_config = TemplateEngine::render_json(&node.config, step_context);

        // 2. Collect input plans from predecessors
        let mut input_plans = Vec::new();
        let mut incoming_edges: Vec<_> = workflow
            .graph
            .edges_directed(node_idx, Direction::Incoming)
            .collect();
        incoming_edges.sort_by_key(|e| e.weight().target_port);

        for edge in &incoming_edges {
            let src_node = workflow.graph.node_weight(edge.source()).unwrap();
            let table_name = format!("node_{}", &src_node.id.to_string()[..8]);
            if let Ok(df) = ctx.sql(&format!("SELECT * FROM \"{}\"", table_name)).await {
                input_plans.push(df.into_optimized_plan()?);
            }
        }

        // 3. Compile the node
        let spec = match registry.get_node(&node.type_id) {
            Ok(spec) => spec,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "Unknown node type '{}': {}",
                    node.type_id, e
                )));
            }
        };

        // Capability gate: log the node's capability for auditability
        match spec.capability() {
            crate::nodes::NodeCapability::ReadOnly => {
                // Pure transform — no side effects expected
            }
            crate::nodes::NodeCapability::ReadWrite => {
                println!(
                    "CAPABILITY: ReadWrite node {} ({}) — may access external systems",
                    node.id,
                    spec.spec().type_id
                );
            }
            crate::nodes::NodeCapability::WriteOnly => {
                println!(
                    "CAPABILITY: WriteOnly node {} ({}) — may send data externally",
                    node.id,
                    spec.spec().type_id
                );
            }
        }

        // 3a. Apply declarative middleware stack (borrowed from Forge's
        // middleware compiler, research #2026081102 — see ADR-012).
        let middleware_stack =
            crate::nodes::middleware::compile_middleware_stack(&rendered_config).map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Middleware compilation failed for {}: {}",
                    node.id, e
                ))
            })?;
        let input_plans = if middleware_stack.is_empty() {
            input_plans
        } else {
            crate::nodes::middleware::apply_stack_all(&middleware_stack, input_plans).map_err(
                |e| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Middleware execution failed for {}: {}",
                        node.id, e
                    ))
                },
            )?
        };

        let compiled_plans = spec
            .propose_plan(input_plans, &rendered_config)
            .await
            .map_err(|e| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Plan proposal failed for {}: {}",
                    node.id, e
                ))
            })?;

        // 4. Execute the plan
        if compiled_plans.is_empty() {
            return Ok(String::new());
        }

        let plan = &compiled_plans[0];
        let planner = crate::core::engine::SwanFlowPlanner {
            event_tx: mpsc::unbounded_channel().0,
            ctx: ctx.clone(),
            cache_dir: cache_dir.to_string(),
        };
        let physical_planner = Arc::new(
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::new(planner),
            ]),
        );

        let state = datafusion::execution::session_state::SessionStateBuilder::new_from_existing(
            ctx.state(),
        )
        .with_query_planner(Arc::new(crate::core::engine::SwanFlowQueryPlanner {
            physical_planner,
        }))
        .build();

        let physical_plan = state.create_physical_plan(plan).await?;
        let batches = datafusion::physical_plan::collect(physical_plan, ctx.task_ctx()).await?;

        // 5. Materialize: register as table for downstream nodes
        if !batches.is_empty() {
            let table_name = format!("node_{}", &node.id.to_string()[..8]);
            if let Ok(df) = ctx.read_batches(batches.clone()) {
                let _ = ctx.register_table(&table_name, df.into_view());
            }
        }

        // 6. Convert batches to text output for context
        let mut text_output = String::new();
        for batch in &batches {
            use datafusion::arrow::util::display::ArrayFormatter;
            use datafusion::arrow::util::display::FormatOptions;
            let opts = FormatOptions::default();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if let Ok(fmt) = ArrayFormatter::try_new(col, &opts) {
                    for row in 0..batch.num_rows() {
                        text_output.push_str(&format!("{}", fmt.value(row)));
                        text_output.push('\n');
                    }
                } else {
                    text_output.push_str(&format!("{:?}\n", col));
                }
            }
        }

        // 7. Store output in step context for {{input}} and {{output_var}} resolution
        step_context.insert("input".to_string(), serde_json::json!(text_output));

        if let Some(var_name) = &node.output_var {
            step_context.insert(var_name.clone(), serde_json::json!(text_output));
        }

        Ok(text_output)
    }
}

/// Outcome of a single fan-out child task, classified by the child's
/// `error_mode` (borrowed from Forge's elastic fan-out semantics,
/// research #2026081102 — see ADR-013).
#[derive(Debug, Clone, PartialEq)]
enum ChildOutcome {
    /// Child succeeded; holds its output.
    Ok(String),
    /// Child failed but policy is Skip — execution continues, the error
    /// string is embedded in the joined output. A `NodeEvent::Error`
    /// must be emitted by the caller.
    ContinueWithError(String),
    /// Child failed under Fail policy, retries exhausted, or the task
    /// panicked (join error). The fan-out step must fail.
    HardError(String),
}

/// Classify a child task result against the child's error mode.
///
/// `is_join_error` marks a tokio join failure (panicked task) — always a
/// hard error regardless of policy (R4).
fn classify_child_outcome(
    error_mode: &crate::core::graph::ErrorMode,
    result: anyhow::Result<String>,
    is_join_error: bool,
) -> ChildOutcome {
    if is_join_error {
        return match result {
            Ok(_) => ChildOutcome::HardError("fan-out child task panicked".to_string()),
            Err(e) => ChildOutcome::HardError(format!("JoinError: {}", e)),
        };
    }
    match (error_mode, result) {
        (_, Ok(output)) => ChildOutcome::Ok(output),
        // Retry policy reaching classification means retries were
        // exhausted → falls back to Fail (matches single-node path).
        (crate::core::graph::ErrorMode::Retry(_), Err(e))
        | (crate::core::graph::ErrorMode::Fail, Err(e)) => {
            ChildOutcome::HardError(e.to_string())
        }
        (crate::core::graph::ErrorMode::Skip, Err(e)) => {
            ChildOutcome::ContinueWithError(format!("Error: {}", e))
        }
    }
}

/// Fold one child outcome into the fan-out aggregates. Emits
/// `NodeEvent::Error` for failed children (both Skip and Fail policies),
/// so failures are always observable (Forge principle: log loudly).
fn aggregate_child_outcome(
    outputs: &mut Vec<String>,
    hard_errors: &mut Vec<String>,
    tx: &mpsc::UnboundedSender<NodeEvent>,
    child_id: uuid::Uuid,
    outcome: ChildOutcome,
) {
    match outcome {
        ChildOutcome::Ok(output) => outputs.push(output),
        ChildOutcome::ContinueWithError(msg) => {
            let _ = tx.send(NodeEvent::Error {
                node_id: child_id,
                message: msg.clone(),
            });
            outputs.push(msg);
        }
        ChildOutcome::HardError(msg) => {
            let _ = tx.send(NodeEvent::Error {
                node_id: child_id,
                message: msg.clone(),
            });
            hard_errors.push(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::graph::ErrorMode;

    fn err_msg() -> anyhow::Error {
        anyhow::anyhow!("boom")
    }

    #[test]
    fn ok_result_is_ok_for_all_policies() {
        for mode in [
            ErrorMode::Fail,
            ErrorMode::Skip,
            ErrorMode::Retry(3),
        ] {
            assert_eq!(
                classify_child_outcome(&mode, Ok("data".to_string()), false),
                ChildOutcome::Ok("data".to_string())
            );
        }
    }

    #[test]
    fn fail_policy_hard_errors() {
        let outcome = classify_child_outcome(&ErrorMode::Fail, Err(err_msg()), false);
        assert!(matches!(outcome, ChildOutcome::HardError(m) if m == "boom"));
    }

    #[test]
    fn skip_policy_continues_with_error() {
        let outcome = classify_child_outcome(&ErrorMode::Skip, Err(err_msg()), false);
        assert!(matches!(outcome, ChildOutcome::ContinueWithError(m) if m == "Error: boom"));
    }

    #[test]
    fn retry_exhausted_falls_back_to_fail() {
        let outcome = classify_child_outcome(&ErrorMode::Retry(2), Err(err_msg()), false);
        assert!(matches!(outcome, ChildOutcome::HardError(m) if m == "boom"));
    }

    #[test]
    fn join_error_always_hard() {
        for mode in [ErrorMode::Fail, ErrorMode::Skip, ErrorMode::Retry(1)] {
            let outcome = classify_child_outcome(&mode, Err(err_msg()), true);
            assert!(
                matches!(outcome, ChildOutcome::HardError(m) if m.contains("JoinError")),
                "mode {:?} should hard-fail on join error",
                mode
            );
        }
    }

    #[test]
    fn aggregate_ok_and_skip_push_outputs_emit_error_event() {
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<crate::nodes::NodeEvent>();
        let mut outputs = Vec::new();
        let mut hard_errors = Vec::new();
        let id_ok = uuid::Uuid::new_v4();
        let id_skip = uuid::Uuid::new_v4();

        aggregate_child_outcome(
            &mut outputs,
            &mut hard_errors,
            &tx,
            id_ok,
            ChildOutcome::Ok("good".to_string()),
        );
        aggregate_child_outcome(
            &mut outputs,
            &mut hard_errors,
            &tx,
            id_skip,
            ChildOutcome::ContinueWithError("Error: boom".to_string()),
        );

        assert_eq!(outputs, vec!["good", "Error: boom"]);
        assert!(hard_errors.is_empty());
        // Skip failure must still be observable via the event bus.
        let evt = rx.try_recv().expect("error event for skipped child");
        assert!(matches!(evt, crate::nodes::NodeEvent::Error { node_id, .. } if node_id == id_skip));
    }

    #[test]
    fn aggregate_hard_error_collects_and_emits() {
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<crate::nodes::NodeEvent>();
        let mut outputs = Vec::new();
        let mut hard_errors = Vec::new();
        let id_fail = uuid::Uuid::new_v4();

        aggregate_child_outcome(
            &mut outputs,
            &mut hard_errors,
            &tx,
            id_fail,
            ChildOutcome::HardError("boom".to_string()),
        );

        assert!(outputs.is_empty());
        assert_eq!(hard_errors, vec!["boom"]);
        let evt = rx.try_recv().expect("error event for failed child");
        assert!(matches!(evt, crate::nodes::NodeEvent::Error { node_id, .. } if node_id == id_fail));
    }
}

pub mod metadata;
pub mod planner;

pub use metadata::WorkflowMetadata;
pub use planner::SwanFlowPlanner;
pub use planner::SwanFlowQueryPlanner;
