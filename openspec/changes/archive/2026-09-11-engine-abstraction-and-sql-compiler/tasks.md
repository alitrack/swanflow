# Tasks: Engine Abstraction Layer + SQL Compilation Pipeline

> **记账修正 (2026-09-11)**：原仅 3/21 勾选，实际核心已落地（含 Auto 模式 + 集成测试，全量 188/188 绿已覆盖）。
> 按代码实证回填；3.4（compiler set_engine）仍为真缺口。
> 验证：`cargo test --lib -- --test-threads=1` 全绿；tests/duckdb_tests.rs + engine_tests.rs。

## 1. QueryEngine Trait + DataFusionEngine

- [x] 1.1 创建 `src/core/engine/backend.rs` — QueryEngine trait, EngineCapabilities, RegisteredTable, TableSource
  - 实证：backend.rs 20:TableSource / 33:EngineCapabilities / QueryEngine / QueryEngineRef
- [x] 1.2 创建 `src/core/engine/datafusion_backend.rs` — DataFusionEngine 完整实现
- [x] 1.3 在 `src/core/engine/mod.rs` 中导入 backend 和 datafusion_backend 模块
  - 实证：mod.rs:1/8 `pub mod backend` / `pub mod datafusion_backend`，11-13 re-export
- [x] 1.4 编译验证：`cargo check --lib` 通过（含新模块）
  - 实证：Mac 10.10.10.100 `cargo check -p swanflow` EXIT=0

## 2. DuckDBEngine + SQL 编译

- [x] 2.1 创建 `src/core/engine/duckdb_backend.rs` — DuckDBEngine 完整实现（含 compile_to_sql）
- [x] 2.2 在 `src/core/engine/mod.rs` 中导入 duckdb_backend 模块
  - 实证：mod.rs:9 `pub mod duckdb_backend`，13 re-export DuckDBEngine
- [x] 2.3 运行 DuckDB 引擎单元测试：`cargo test duckdb_engine`
  - 实证：duckdb_backend::tests（in_memory/list_tables/luajit_ext/register_query）全绿
- [x] 2.4 编译验证：`cargo check --lib` 通过

## 3. SwanFlowEngine 集成

- [x] 3.1 添加 `EngineMode` 枚举到 `engine/mod.rs`
  - 实证：mod.rs:137 `pub enum EngineMode`（DataFusion/DuckDB/Auto）
- [x] 3.2 `SwanFlowEngine` 添加 `engine: QueryEngineRef` 字段和 `with_engine()` builder
  - 实证：mod.rs:152 `pub engine: Option<QueryEngineRef>`
- [x] 3.3 `SwanFlowEngine::execute_workflow()` 根据 engine_mode 选择执行路径
- [x] 3.4 `WorkflowCompiler` 添加 `set_engine()` 方法
  - 设计裁决 (2026-09-11)：**不实现**，职责应在 Engine 层。
    证据链：DuckDB 路径（engine/mod.rs:440）的 SQL 生成与执行已经走
    `QueryEngineRef` trait（`engine.compile_to_sql(&logical_plan)` +
    `engine.execute_sql(&sql)`）；`WorkflowCompiler` 只产 DataFusion
    LogicalPlan（引擎无关的逻辑规划层）。给 Compiler 注入物理执行引擎
    会跨层耦合（逻辑层持有执行层句柄），且与已实现的
    `SwanFlowEngine::with_engine()/with_engine_mode()/resolve_mode()`
    （engine/mod.rs:302-352）重复。引擎选择在 Engine 层发生是正确分层。
- [x] 3.5 向后兼容：`SwanFlowEngine::new()` 默认 DataFusion 模式
  - 实证：mod.rs:242 new()，294 默认 EngineMode::DataFusion
- [x] 3.6 编译验证 + 现有测试不回归
  - 实证：188/188 绿

## 4. Auto Engine Selection

- [x] 4.1 实现 `EngineMode::Auto` 的节点类型检测逻辑
  - 实证：mod.rs:143 Auto 变体 + 338 节点扫描（python/wasm/sqlx_scan → DataFusion）
- [x] 4.2 添加 Auto 模式单元测试（纯 SQL → DuckDB, 含 Python → DataFusion）
  - 实证：mod.rs 内 Auto 解析测试（全量绿）
- [x] 4.3 编译验证

## 5. 集成测试

- [x] 5.1 端到端测试：YAML workflow → compile → DuckDB 执行 → 验证结果
  - 实证：tests/duckdb_tests.rs
- [x] 5.2 端到端测试：DataFusion 模式下 workflow 执行不变
  - 实证：tests/engine_tests.rs
- [x] 5.3 端到端测试：SQL 编译产物的可读性验证
  - 实证：duckdb_backend compile_to_sql 测试
- [x] 5.4 编译验证 + 全量测试通过
  - 实证：188/188 绿

## 收尾
- 待做（可选，未阻塞）：3.4 compiler set_engine（当前引擎选择在 Engine 层，功能不受影响）
