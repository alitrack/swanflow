use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result;
use datafusion::logical_expr::LogicalPlan;
use std::fmt::Debug;
use std::sync::Arc;

/// Represents a registered table in the query engine.
#[derive(Debug, Clone)]
pub struct RegisteredTable {
    pub name: String,
    pub schema: SchemaRef,
    /// Backend-specific table reference (e.g. parquet path, DuckDB table name, in-memory view).
    pub source: TableSource,
}

/// Describes how a table is stored and accessed by the backend.
#[derive(Debug, Clone)]
pub enum TableSource {
    /// Table registered from Arrow RecordBatches in memory.
    InMemory { batches: Vec<RecordBatch> },
    /// Table backed by a Parquet file on disk.
    Parquet { path: String },
    /// Table already present in the engine's native catalog (e.g. DuckDB attached table).
    Native { identifier: String },
    /// Table registered via CSV file.
    Csv { path: String, has_header: bool },
}

/// Capability flags for a query engine backend.
#[derive(Debug, Clone, Default)]
pub struct EngineCapabilities {
    /// Whether this engine supports registering Arrow RecordBatches as tables.
    pub in_memory_tables: bool,
    /// Whether this engine can execute a DataFusion LogicalPlan directly.
    pub logical_plan_execution: bool,
    /// Whether this engine supports SQL compilation (turning LogicalPlan → SQL).
    pub sql_compilation: bool,
    /// Whether this engine supports external table attachment (e.g. DuckDB ATTACH).
    pub external_attach: bool,
    /// Whether this engine supports concurrent reads.
    pub concurrent_reads: bool,
}

/// The unified query engine abstraction.
///
/// SwanFlow originally coupled everything to DataFusion's SessionContext.
/// This trait decouples the workflow compiler/executor from any single engine,
/// enabling:
/// - Pure DuckDB mode (fast local ETL, SQL-compiled pipelines)
/// - DataFusion mode (distributed query planning, Arrow-native)
/// - Future backends (SlothDB, Polars, etc.)
#[async_trait]
pub trait QueryEngine: Send + Sync + Debug {
    /// Return a human-readable name for this engine (e.g. "DataFusion", "DuckDB").
    fn name(&self) -> &str;

    /// Return the capabilities of this engine.
    fn capabilities(&self) -> &EngineCapabilities;

    /// Register a table with the engine. Subsequent queries can reference it by name.
    async fn register_table(&self, table: RegisteredTable) -> Result<()>;

    /// Check whether a table with the given name exists.
    async fn table_exists(&self, name: &str) -> Result<bool>;

    /// Deregister (drop) a previously registered table.
    async fn deregister_table(&self, name: &str) -> Result<()>;

    /// Execute a SQL query and return the results as Arrow RecordBatches.
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>>;

    /// Execute a prepared query with parameterized values.
    async fn execute_parameterized(
        &self,
        sql: &str,
        params: &[datafusion::common::ScalarValue],
    ) -> Result<Vec<RecordBatch>> {
        // Default: ignore params (engines that support parameterized queries override this).
        self.execute_sql(sql).await
    }

    /// Execute a DataFusion LogicalPlan directly.
    /// Only engines with `logical_plan_execution` capability support this.
    async fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>>;

    /// Compile a DataFusion LogicalPlan to this engine's native SQL dialect.
    /// Only engines with `sql_compilation` capability support this.
    /// Returns the generated SQL string.
    fn compile_to_sql(&self, plan: &LogicalPlan) -> Result<String> {
        Err(datafusion::error::DataFusionError::NotImplemented(
            format!("{} does not support SQL compilation", self.name()),
        ))
    }

    /// Get the list of currently registered table names.
    async fn list_tables(&self) -> Result<Vec<String>>;

    /// Get the Arrow schema for a registered table.
    async fn get_table_schema(&self, name: &str) -> Result<SchemaRef>;

    /// Return a clone of this engine (if the engine is clonable via Arc).
    fn arc_clone(&self) -> Arc<dyn QueryEngine>;
}

/// Type alias for an Arc-wrapped query engine.
pub type QueryEngineRef = Arc<dyn QueryEngine>;
