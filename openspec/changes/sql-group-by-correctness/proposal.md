# Proposal: SQL GROUP BY 正确性（core.sql 分组坍缩）

## 问题描述

SwanFlow 的 `core.sql` 节点（`SqlProxyLogical` UDLN，`planner.rs` 对共享 `input_0` view 做 late-binding `ctx.sql()`）历史上报告过 **string 列 GROUP BY 坍缩**：本应聚成 N 组的结果坍成 1 行（或单 NULL/空键）。此前仅记录在 `swanflow-dev` skill 里，没有 OpenSpec 修复单，也没有可复现的验收基准——这正是本次 change 要补的。

**本轮实跑探针的关键发现（必须写进 design）**：

- 用 `MemTable` + `into_view("input_0")` + `ctx.sql("GROUP BY")` **精确镜像 planner 的 late-binding 形状**后，坍缩**不复现**——string（含 arrow 57 的 `Utf8View`，DuckDB 读回的形态）与 int 都正确聚成 3 组（a=2,b=2,c=1）。
- 证据只支持：坍缩**不是裸 string 列属性**。它**是否 workflow 形状依赖**（stage 物化顺序 / 共享 ctx 在 fan_out 竞争下被重新注册 / `input_0` view 跨 stage 生命周期）仍待验证——该单元 fixture 绕过了 `SqlProxyLogical` 元数据同步、stage 物化与共享 ctx 重注册，5 行样本不足以排除数据相关缺陷。单元级复现不出来是事实，但"形状依赖"是**候选假设**而非结论。

这意味着修复单不能"直接改一行"了事，必须先把**触发条件**钉死，否则修错地方。

## 动机

1. **正确性红线**：GROUP BY 坍缩 = 用户拿到的聚合结果是错的（行数、计数全错），且静默——比报错更危险。
2. **无验收基准**：skill 里的坍缩描述是孤证，没有回归测试能证明"修好了"或"没修坏"。
3. **修复方向未定**：坍缩是设计/形状问题（改 MemTable 物化 / 改注册方式 / 改 fan_out 竞争 / 升级 DataFusion 各有取舍），需 OpenSpec 决策而非 `fix(...)` 直接改。

## 影响范围

- 受影响能力（新增）：`sql-group-by-correctness`（SQL 分组/聚合结果的正确性保证）
- 受影响代码：`crates/swanflow/src/core/engine/planner.rs`（late-binding `ctx.sql()` / `input_0` view 注册）、`crates/swanflow/src/nodes/sql.rs`（`SqlProxyLogical` UDLN）、`crates/swanflow/src/core/executor/`（stage 物化 / fan_out 竞争）
- 验收护栏已就位：`crates/swanflow/src/core/sql_collapse_probe.rs`（`cargo test -p swanflow --lib sql_collapse_probe`），单元级 string 与 int 都必须 =3 组、无空键，坍缩即红。
- 兼容性：修复目标是让 GROUP BY 结果**始终**按 SQL 语义分组；不改变已正确分组的结果。

## 明确不做的事

- **不**在触发条件钉死前改 `planner.rs` 的注册逻辑（避免修错地方）。
- **不**把 `sql_collapse_probe.rs` 从"单元护栏"升级成"端到端 workflow 复现"直到第 1 个任务（复现）完成。
- **不**为这个 change 升级 DataFusion 版本（除非 design 证明是版本相关）。

## 成功标准

1. 一个**能在 workflow 级复现坍缩**的最小 workflow YAML（真实 `core.sql` 节点 + 触发形状），复现前 assert 红、修复后 assert 绿——这是修复完成的硬门槛。
2. 单元护栏 `sql_collapse_probe` 保持绿并纳入常规回归。
3. `cargo test -p swanflow --lib` 全绿，`-D warnings` 无新增告警。

## 相关能力

- 新增：`sql-group-by-correctness`
- 关联（不修改其 spec，仅引用）：`query-engine`（`core.sql` 节点所在）、`async-staged-execution`（stage 物化 / 共享 ctx 的并发模型，坍缩的可能成因域）
