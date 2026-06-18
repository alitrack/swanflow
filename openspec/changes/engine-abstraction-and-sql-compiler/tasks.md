# Tasks: Engine Abstraction Layer + SQL Compilation Pipeline

## 1. QueryEngine Trait + DataFusionEngine

- [x] 1.1 创建 `src/core/engine/backend.rs` — QueryEngine trait, EngineCapabilities, RegisteredTable, TableSource
- [x] 1.2 创建 `src/core/engine/datafusion_backend.rs` — DataFusionEngine 完整实现
- [ ] 1.3 在 `src/core/engine/mod.rs` 中导入 backend 和 datafusion_backend 模块
- [ ] 1.4 编译验证：`cargo check --lib` 通过（包含新模块）

## 2. DuckDBEngine + SQL 编译

- [x] 2.1 创建 `src/core/engine/duckdb_backend.rs` — DuckDBEngine 完整实现（含 compile_to_sql）
- [ ] 2.2 在 `src/core/engine/mod.rs` 中导入 duckdb_backend 模块
- [ ] 2.3 运行 DuckDB 引擎单元测试：`cargo test duckdb_engine`
- [ ] 2.4 编译验证：`cargo check --lib` 通过

## 3. SwanFlowEngine 集成

- [ ] 3.1 添加 `EngineMode` 枚举到 `engine/mod.rs`
- [ ] 3.2 `SwanFlowEngine` 添加 `engine: QueryEngineRef` 字段和 `with_engine()` builder
- [ ] 3.3 `SwanFlowEngine::execute_workflow()` 根据 engine_mode 选择执行路径
- [ ] 3.4 `WorkflowCompiler` 添加 `set_engine()` 方法
- [ ] 3.5 向后兼容：`SwanFlowEngine::new()` 默认 DataFusion 模式
- [ ] 3.6 编译验证 + 现有测试不回归

## 4. Auto Engine Selection

- [ ] 4.1 实现 `EngineMode::Auto` 的节点类型检测逻辑
- [ ] 4.2 添加 Auto 模式单元测试（纯 SQL → DuckDB, 含 Python → DataFusion）
- [ ] 4.3 编译验证

## 5. 集成测试

- [ ] 5.1 端到端测试：YAML workflow → compile → DuckDB 执行 → 验证结果
- [ ] 5.2 端到端测试：DataFusion 模式下 workflow 执行不变
- [ ] 5.3 端到端测试：SQL 编译产物的可读性验证（生成的 SQL 可直接在 duckdb CLI 运行）
- [ ] 5.4 编译验证 + 全量测试通过
