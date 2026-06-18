# Proposal: Engine Abstraction Layer + SQL Compilation Pipeline

## Why

SwanFlow 目前对 DataFusion 是硬耦合的。所有执行路径——编译、规划、物理执行——都直接依赖 `SessionContext`、`LogicalPlan`、`ExecutionPlan`。DuckDB 仅作为旁路扫描源（`DuckDBScanLogical/Exec`），无法独立承接完整 pipeline 执行。

对 Duckle 的深度调研（#2026061811）揭示了两个 SwanFlow 可以借鉴的关键架构决策：

1. **引擎抽象层**：Duckle 在 `duckdb-engine` / `slothdb-engine` 之上抽象了统一的引擎接口，使上层 pipeline 编译器和执行器无需感知底层引擎。加一个新引擎不需要改编译器和执行器。

2. **SQL 编译管道**：Duckle 将 pipeline 编译为 SQL 而非自定义 DSL，获得可调试性、可审计性、引擎可移植性。SwanFlow 编译出 DataFusion LogicalPlan，调试困难（需看懂 plan 树），且锁死在 DataFusion 生态。

这两个改动一起解决了一个长期痛点：SwanFlow 对于"纯 ETL"场景过度设计——简单 CSV→filter→aggregate→parquet 的 pipeline 需要走完整的 DataFusion LogicalPlan→PhysicalPlan 链路，而 DuckDB 的 SQL 引擎可以更直接高效地完成。

## What Changes

### New Capabilities

- **`QueryEngine` trait**：统一的查询引擎抽象，定义 `register_table` / `execute_sql` / `execute_logical_plan` / `compile_to_sql` 等接口
- **`DataFusionEngine`**：现有 DataFusion 路径的 trait 实现，保持向后兼容
- **`DuckDBEngine`**：DuckDB 原生执行路径，支持 SQL 编译和执行
- **SQL 编译管道**：`DuckDBEngine::compile_to_sql()` 将 DataFusion LogicalPlan 编译为 DuckDB SQL
- **`TableSource` 枚举**：统一表注册语义（InMemory / Parquet / Csv / Native），取代当前分散的表注册方式
- **`EngineCapabilities`**：引擎能力声明，让上层逻辑在编译时选择最优路径

### Modified Capabilities

- **`SwanFlowEngine`**：从硬编码 `Arc<SessionContext>` 迁移到 `QueryEngineRef`，加 `engine_mode` 选择逻辑
- **`WorkflowCompiler`**：添加 `set_engine()` 方法，支持在编译时选择执行引擎
- **`connectors/` 节点**：thin connector 的 `PathA`（DuckDB ATTACH）与 `PathB`（外部 adapter）在引擎抽象层下自然归位

### Non-goals

- ❌ 不替换 DataFusion —— 保留双引擎共存，DataFusion 仍是默认引擎
- ❌ 不实现 Polars / SlothDB / 其他引擎 —— Duckle 的 SlothDB 是规划中的，SwanFlow 暂不跟进
- ❌ 不修改 YAML workflow 格式 —— 模式选择通过 CLI flag / API param，不侵入 DSL
- ❌ 不在此 change 中做多 Crate 拆分 —— 那是 P1 的独立 change

## Capabilities

### New Capabilities

1. **引擎抽象层 (`QueryEngine` trait)**
   - 统一接口：register / deregister / execute_sql / execute_logical_plan / compile_to_sql
   - 能力声明：EngineCapabilities 让编译器决策引擎选择
   - 两个实现：DataFusionEngine（向后兼容）、DuckDBEngine（新路径）

2. **SQL 编译管道 (`compile_to_sql`)**
   - LogicalPlan → DuckDB SQL 的递归编译
   - 覆盖 Projection / Filter / Aggregate / Sort / Limit / Join / Union / TableScan / Subquery / Distinct / CrossJoin
   - Extension 节点透传（MaterializerLogical → 剥离，只编译内层 plan）
   - 未覆盖的 plan 类型返回 NotImplemented

3. **纯 DuckDB 执行路径**
   - `DuckDBEngine::execute_sql()` 直接在 DuckDB 中执行并返回 Arrow RecordBatches
   - DuckDB → Arrow 的类型映射（包括 Timestamp、Date 等）
   - 利用 DuckDB 的 `ATTACH` 能力连接外部数据库（Postgres、SQLite 等）

### Modified Capabilities

4. **SwanFlowEngine 引擎选择**
   - 构造时接受 `engine_mode: EngineMode`（DataFusion / DuckDB / Auto）
   - Auto 模式：检测 pipeline 中的节点类型，纯 SQL/transform 走 DuckDB，含 Python/WASM/自定义节点走 DataFusion
   - 向后兼容：不传 engine_mode 默认 DataFusion

5. **WorkflowCompiler 引擎感知**
   - `set_engine(engine: QueryEngineRef)` 替代硬编码 `self.ctx: Arc<SessionContext>`
   - 编译产物可选 SQL strings（DuckDB 模式）或 LogicalPlans（DataFusion 模式）

## Impact

- **Affected modules**: `core/engine/` (new submodule), `core/compiler.rs`, `nodes/connectors/`
- **API changes**: `SwanFlowEngine::new()` 签名不变（默认 DataFusion），新增 `with_engine()` builder
- **Dependencies**: 已有 `duckdb = "1.10503.1"` 和 `datafusion = "52.1.0"`，无需新依赖
- **Breaking changes**: 无——所有现有 API 保持兼容
- **Performance**: DuckDB 路径对纯 SQL pipeline 预期更优（利用 DuckDB 的向量化执行和查询优化器）
- **Testing**: 新增 DuckDB 引擎的集成测试（in-memory DuckDB，不需要外部依赖）
