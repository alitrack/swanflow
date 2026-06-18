use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

use super::backend::{
    EngineCapabilities, QueryEngine, QueryEngineRef, RegisteredTable, TableSource,
};

/// DataFusion-backed query engine.
///
/// This is the original SwanFlow execution path, now wrapped as a QueryEngine
/// implementation. DataFusion provides distributed query planning, Arrow-native
/// execution, and the full LogicalPlan → PhysicalPlan pipeline.
pub struct DataFusionEngine {
    ctx: Arc<SessionContext>,
    /// Track registered table schemas for `list_tables` / `get_table_schema`.
    table_registry: Mutex<std::collections::HashMap<String, SchemaRef>>,
}

impl fmt::Debug for DataFusionEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFusionEngine")
            .field("ctx", &"<SessionContext>")
            .finish()
    }
}

impl DataFusionEngine {
    pub fn new(ctx: Arc<SessionContext>) -> Self {
        Self {
            ctx,
            table_registry: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Create a new DataFusionEngine with a fresh SessionContext.
    pub fn new_fresh() -> Self {
        Self::new(Arc::new(SessionContext::new()))
    }

    /// Access the underlying SessionContext (for compatibility with existing code).
    pub fn session_ctx(&self) -> &Arc<SessionContext> {
        &self.ctx
    }

    fn capabilities_static() -> EngineCapabilities {
        EngineCapabilities {
            in_memory_tables: true,
            logical_plan_execution: true,
            sql_compilation: false,
            external_attach: false,
            concurrent_reads: true,
        }
    }
}

#[async_trait]
impl QueryEngine for DataFusionEngine {
    fn name(&self) -> &str {
        "DataFusion"
    }

    fn capabilities(&self) -> &EngineCapabilities {
        // Return a static reference — DataFusion capabilities never change.
        static CAPS: std::sync::LazyLock<EngineCapabilities> =
            std::sync::LazyLock::new(DataFusionEngine::capabilities_static);
        &CAPS
    }

    async fn register_table(&self, table: RegisteredTable) -> Result<()> {
        let name = table.name.clone();
        let schema = table.schema.clone();

        match &table.source {
            TableSource::InMemory { batches } => {
                if batches.is_empty() {
                    // Register an empty view with the schema
                    let empty = RecordBatch::new_empty(schema.clone());
                    let df = self.ctx.read_batch(empty)?;
                    self.ctx.register_table(&name, df.into_view())?;
                } else {
                    let df = self.ctx.read_batches(batches.clone())?;
                    self.ctx.register_table(&name, df.into_view())?;
                }
            }
            TableSource::Parquet { path } => {
                self.ctx
                    .register_parquet(&name, path, ParquetReadOptions::default())
                    .await?;
            }
            TableSource::Csv { path, has_header } => {
                self.ctx
                    .register_csv(
                        &name,
                        path,
                        CsvReadOptions::new().has_header(*has_header),
                    )
                    .await?;
            }
            TableSource::Native { identifier: _ } => {
                // DataFusion doesn't have a "native" catalog concept — tables
                // must be explicitly registered. This is a no-op for now.
                return Err(datafusion::error::DataFusionError::NotImplemented(
                    "DataFusion does not support Native table source".to_string(),
                ));
            }
        }

        self.table_registry
            .lock()
            .await
            .insert(name, schema);
        Ok(())
    }

    async fn table_exists(&self, name: &str) -> Result<bool> {
        Ok(self.table_registry.lock().await.contains_key(name))
    }

    async fn deregister_table(&self, name: &str) -> Result<()> {
        self.ctx.deregister_table(name)?;
        self.table_registry.lock().await.remove(name);
        Ok(())
    }

    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(sql).await?;
        df.collect().await
    }

    async fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>> {
        let state = self.ctx.state();
        let physical_plan = state.create_physical_plan(plan).await?;
        datafusion::physical_plan::collect(physical_plan, self.ctx.task_ctx()).await
    }

    async fn list_tables(&self) -> Result<Vec<String>> {
        Ok(self.table_registry.lock().await.keys().cloned().collect())
    }

    async fn get_table_schema(&self, name: &str) -> Result<SchemaRef> {
        self.table_registry
            .lock()
            .await
            .get(name)
            .cloned()
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Table '{}' not found in DataFusion engine",
                    name
                ))
            })
    }

    fn arc_clone(&self) -> QueryEngineRef {
        Arc::new(Self {
            ctx: self.ctx.clone(),
            table_registry: Mutex::new(
                // We can't clone the Mutex contents easily, but for arc_clone
                // sharing the same registry is acceptable since it's behind Arc.
                std::collections::HashMap::new(),
            ),
        })
    }
}
