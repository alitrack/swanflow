# Design: Engine Abstraction Layer + SQL Compilation Pipeline

## Architecture Overview

```
┌─────────────────────────────────────────────────┐
│                  SwanFlowEngine                   │
│  engine_mode: DataFusion | DuckDB | Auto         │
│  engine: QueryEngineRef                           │
└────────────┬────────────────────────────────────┘
             │
    ┌────────▼────────┐
    │ WorkflowCompiler │──► compile() ──► LogicalPlans (DF mode)
    │  engine: QERef   │              ──► SQL strings  (DB mode)
    └────────┬────────┘
             │
    ┌────────▼────────────────────────────────────┐
    │              QueryEngine trait                │
    │  register_table / execute_sql /              │
    │  execute_logical_plan / compile_to_sql       │
    └────┬─────────────────────┬──────────────────┘
         │                     │
  ┌──────▼──────┐      ┌───────▼───────┐
  │DataFusion   │      │  DuckDB       │
  │ Engine      │      │  Engine       │
  │             │      │               │
  │ SessionCtx  │      │ duckdb::Conn  │
  │ LogicalPlan │      │ SQL execution │
  │ →Physical   │      │ SQL compile   │
  └─────────────┘      └───────────────┘
```

## Data Structures

### QueryEngine trait

```rust
#[async_trait]
pub trait QueryEngine: Send + Sync + Debug {
    fn name(&self) -> &str;
    fn capabilities(&self) -> &EngineCapabilities;

    async fn register_table(&self, table: RegisteredTable) -> Result<()>;
    async fn table_exists(&self, name: &str) -> Result<bool>;
    async fn deregister_table(&self, name: &str) -> Result<()>;
    async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>>;
    async fn execute_logical_plan(&self, plan: &LogicalPlan) -> Result<Vec<RecordBatch>>;
    fn compile_to_sql(&self, plan: &LogicalPlan) -> Result<String>;
    async fn list_tables(&self) -> Result<Vec<String>>;
    async fn get_table_schema(&self, name: &str) -> Result<SchemaRef>;
    fn arc_clone(&self) -> QueryEngineRef;
}
```

### EngineCapabilities

```rust
pub struct EngineCapabilities {
    pub in_memory_tables: bool,
    pub logical_plan_execution: bool,  // DataFusion: true, DuckDB: false
    pub sql_compilation: bool,         // DataFusion: false, DuckDB: true
    pub external_attach: bool,         // DuckDB ATTACH support
    pub concurrent_reads: bool,
}
```

### RegisteredTable / TableSource

```rust
pub struct RegisteredTable {
    pub name: String,
    pub schema: SchemaRef,
    pub source: TableSource,
}

pub enum TableSource {
    InMemory { batches: Vec<RecordBatch> },
    Parquet { path: String },
    Csv { path: String, has_header: bool },
    Native { identifier: String },
}
```

### EngineMode (in SwanFlowEngine)

```rust
pub enum EngineMode {
    DataFusion,   // Default — backward compatible
    DuckDB,       // Pure DuckDB execution
    Auto,         // Auto-select based on pipeline node types
}
```

## Implementation Notes

### 1. SQL Compilation Priority

`DuckDBEngine::compile_to_sql()` 按递归下降实现：

| LogicalPlan variant | SQL generation |
|---|---|
| Projection | `SELECT exprs FROM (input) AS _sub` |
| Filter | `SELECT * FROM (input) AS _sub WHERE predicate` |
| Aggregate | `SELECT groups, aggs FROM (input) AS _sub GROUP BY groups` |
| Sort | `SELECT * FROM (input) AS _sub ORDER BY exprs` |
| Limit | `SELECT * FROM (input) AS _sub LIMIT n OFFSET m` |
| TableScan | `SELECT * FROM "table_name"` |
| Join | `SELECT * FROM (left) AS _l JOIN (right) AS _r ON cond` |
| Union | `(part1) UNION ALL (part2)` |
| CrossJoin | `SELECT * FROM (left) AS _l CROSS JOIN (right) AS _r` |
| Distinct | `SELECT DISTINCT * FROM (input) AS _sub` |
| Subquery/SubqueryAlias | Transparent pass-through |
| Extension (MF/SProbe) | Pass-through to inner plan |
| EmptyRelation | `SELECT 1` or `... WHERE FALSE` |

未覆盖的 variant（如 Window、Repartition、Dml 等）返回 `NotImplemented`。SwanFlow 的典型 pipeline 节点不会生成这些 plan 类型。

### 2. Auto Engine Selection

`EngineMode::Auto` 的决策逻辑：

```
遍历 workflow 的所有节点 type_id：
  if 任何节点是 "python" / "wasm" / UnknownType → DataFusion
  if 任何节点包含 "sqlx_scan" → DataFusion（需要 sqlx 运行时）
  if 任何节点包含 "duckdb_scan" → DuckDB
  else → DuckDB（纯 SQL/transform pipeline）
```

### 3. Metadata 存储

当前 `SwanFlowEngine::with_cache_dir()` 创建 DuckDB metadata 数据库（`metadata.duckdb`）存储 flow_metadata / graph_nodes / graph_edges。引擎抽象层不改变这个行为——metadata DuckDB 是独立于执行引擎的持久化层，不属于 `QueryEngine` trait 的职责范围。

### 4. 向后兼容

- `SwanFlowEngine::new(registry)` 不变 → 内部用 `EngineMode::DataFusion`
- 新增 `SwanFlowEngine::with_engine(registry, engine)` builder
- `WorkflowCompiler::ctx: Arc<SessionContext>` 保留但标记 deprecated，新增 `engine: Option<QueryEngineRef>`
- 所有现有 `connectors/` 节点的 `propose_plan()` 返回 `LogicalPlan`——在 DataFusion 模式下不变，在 DuckDB 模式下由 `compile_to_sql()` 转换

### 5. DuckDB Connection 管理

`DuckDBEngine` 内部持有 `Arc<Mutex<duckdb::Connection>>`，DuckDB 的 crate 本身不是 `Send` 的（Connection 不是 Send），所以用 `tokio::task::spawn_blocking` 将所有 DuckDB 操作移到阻塞线程池。这是 DuckDB Rust binding 的标准模式。

### 6. 测试策略

- DuckDB in-memory 模式——不需要外部数据库，CI 友好
- 测试 register_table + execute_sql 的往返
- 测试 SQL 编译的每个 LogicalPlan variant
- 测试 EngineMode::Auto 的选择逻辑
- 测试向后兼容：DataFusion 模式下现有 workflow 执行不变
