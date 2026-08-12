# Tasks: Graph Repair Operators

**Status**: Implemented ✅（2026-08-12 实现完成，待集成测试通过后提交）
**Change**: graph-repair-operators

## 实现修订（vs 原 proposal）

1. **类型/执行分层**：`repair.rs` 拆两层——
   - **类型层** `crates/swanflow-core/src/repair.rs`：`RepairOperator`/`RepairNodeSpec`/`RepairEdgeSpec`/`EdgeRef`/`RepairOutcome`/`RepairResponse`（serde 解析）。
     原因：`swanflow-ai`（hook 返回类型）依赖 core，core 不能反向依赖 ai。
   - **执行层** `crates/swanflow/src/core/repair.rs`：`apply_repair`。原因：engine 用的是 **swanflow 本地 `Workflow`**（`crates/swanflow/src/core/graph.rs`，与 swanflow-core 平行重复的类型系统），
     apply_repair 必须操作它；本地 graph.rs 补齐 `remove_edge`/`find_node_by_id`/`upstream_of`/`downstream_of`/`downstream_up_to_h`。
2. **`NodeSpec` 改名 `RepairNodeSpec`**：core 已有 `nodes::NodeSpec`，避免撞名。
3. **`apply_repair` 签名**：`apply_repair(&mut Workflow, failed_node_id: &Uuid, op) -> Result<RepairOutcome>`——REBIND 目标节点由调用方传入。
4. **`PromptContext.graph_topology` 类型**：`Vec<String>`（"source→target" 边描述），不用强类型边结构。
5. **engine 接入约束**：`execute_workflow(&Workflow)` 只读 + 事件流单向，streaming 中无法 `&mut Workflow` 应用图变换。
   接入 = `IntelligenceHook::on_error` 返回 `Option<RepairOperator>` + 三处调用点打印算子并 handled=true；修复失败走 HITL 不变。
   完整闭环（apply → 重跑）由 `apply_repair` API + 集成测试覆盖；streaming 内图变换重构记入 Non-Goals。
6. **`DefaultIntelligenceHook`（规则版）诚实化**：规则引擎无真实修复能力 → 打印建议后返回 `None`（走 HITL）。
   删除了"返回建议 JSON + handled=true 静默吞错且不重试"的旧坏行为。
7. **BYPASS = 图短路**（本地 NodeState 无 Stopped/Skipped 变体）：移除失败节点 + 上游直连下游，真正 bypass 语义。
8. **测试隔离教训（重要）**：多个集成测试并行执行时，**必须用独立 cache_dir**（`SwanFlowEngine::with_cache_dir`），
   否则共享 `target/swanflow_cache/metadata.duckdb` 并发写触发 DuckDB C++ 异常
   `MarkBlockAsModified called with already modified block id` → SIGABRT。
9. **engine planning 行为（测试断言依据）**：planning 失败发 `NodeEvent::Error` 但**不改节点 state**（保持 Configured）；
   `core.io.csv` 读不存在文件**不报错**（返回空表）——失败场景要选 SQL 引用缺失表/输入（planning 错误）。

## Task 1: repair.rs 数据结构

- [x] 新建 `crates/swanflow-core/src/repair.rs`：`RepairOperator` enum（serde tag SCREAMING_SNAKE_CASE）+ `RepairNodeSpec`/`RepairEdgeSpec`/`EdgeRef` + `RepairOutcome` + `RepairResponse`（untagged，兼容旧格式）
- [x] 单元测试：五个算子 serde 往返 + 旧格式 `{"fixed_config":...}` 向后兼容
- [x] lib.rs 导出 `pub mod repair`
- [x] Workflow 补 API：`remove_edge`/`find_node_by_id`/`upstream_of`/`downstream_of`/`downstream_up_to_h`

## Task 2: hook.rs 升级

- [x] `FixResponse` → `RepairResponse`（untagged），`try_fix` 返回 `Option<RepairOperator>`，签名加 `graph_topology: &[String]`
- [x] `PromptContext.graph_topology` 填充（hook 传入）
- [x] `fix_instructions` + `build_fix_prompt` 输出格式更新：五算子 + 选择规则 + Graph topology 段落
- [x] 解析验证：core serde 测试覆盖五种算子 + 旧格式

## Task 3: core 图变换应用

- [x] `apply_repair(&mut Workflow, failed_node_id: &Uuid, op)` 五算子完整实现
- [x] 修复边界：h=2（affected 限定）/ |ΔV|≤3 / |ΔE|≤5（REWIRE 拒绝超界）
- [x] 单测：14 个（serde 5 + 兼容 2 + apply_repair 7），每算子应用后 DAG 有效（acyclic 断言）、环拒绝、重复 id 拒绝、节点缺失拒绝

## Task 4: 集成 + 验证

- [x] engine 接入：`IntelligenceHook::on_error` 签名升级 + 三处调用点（打印算子 + handled） + `DefaultIntelligenceHook` 诚实化（None → HITL）
- [x] 失败传播验证：INSERT_PREREQ 后受影响子图（新节点 + failed）在 affected_nodes 中
- [x] 集成测试 `crates/swanflow/tests/repair_integration_tests.rs`（3 个全过）：
  - INSERT_PREREQ 闭环（sql 引用 input_0 无上游 → 插入 csv 源节点 → 重跑成功）
  - REBIND 闭环（错误 query → 修正 → 重跑成功）
  - REWIRE 图级契约（断边重连 → DAG 有效 → good 节点继续执行）
- [x] 回归：swanflow-core 88 + swanflow-ai 42 + swanflow repair 单测 10 + 集成 3 全绿

## 验收标准

- [x] `try_fix` 可返回五种算子之一，全部可应用且保持 DAG 有效（core 单测 + 集成测试）
- [x] 旧格式 REBIND 响应仍可解析（RepairResponse untagged）
- [x] 修复失败后 HITL 升级路径不变（on_error None → NodeEvent::Error；DefaultIntelligenceHook 行为保持错误可见）
