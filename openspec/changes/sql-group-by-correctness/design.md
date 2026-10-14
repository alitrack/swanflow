# Design: SQL GROUP BY 正确性

## 背景与本轮实跑发现（决定性事实）

修复前先做了探针，结论**改变了修复策略**：

| 复现路径 | 分组键 | 结果 |
|----------|--------|------|
| `MemTable` + `into_view("input_0")` + `ctx.sql(GROUP BY <string Utf8>)` | string | ✅ 3 组（正确） |
| 同上，但 string 换成 `Utf8View`（arrow 57 / DuckDB 读回形态） | string | ✅ 3 组（正确） |
| 同上，整数键 | int | ✅ 3 组（正确） |

探针：`crates/swanflow/src/core/sql_collapse_probe.rs`
（`cargo test -p swanflow --lib sql_collapse_probe -- --nocapture`），
镜像 `planner.rs:341-365` 的 late-binding 形状。

**推论**：skill 里"string GROUP BY 坍缩成 1 行 NULL"的复现**不成立**于裸 `ctx.sql` 路径。
这排除了"纯 string 列属性"，但**不能**断定成因（探针绕过了 stage 物化 / 共享 ctx 重注册 /
`SqlProxyLogical` 元数据同步，5 行样本不足以排除数据相关缺陷）。坍缩**若**存在，更可能由
**workflow 执行形状**触发，候选成因（按怀疑度排序，均需 A1-A3 钉死）：

1. **stage 物化**：`core.sql` 节点的输入不是直接 `MemTable`，而是上游 stage 物化后
   经 `input_0` view 读回；若物化阶段把 batch 合并/重投影改变了 `Utf8View` 的
   logical 视图，`ctx.sql` 的 `input_0` 重解析可能拿到退化 schema。
2. **共享 ctx 生命周期**：多个 stage / fan_out 分支**并发**对同一个
   `SessionContext` 注册/覆盖 `input_0` view（`planner.rs:345` `register_table`），
   竞争下 `ctx.sql` 可能解析到**另一个分支的** view 或其空状态。
3. **fan_out 竞争**：`fan_out` 的 `tokio::join!` 里多个 `core.sql` 实例共享 ctx，
   与 2 叠加。
4. DataFusion 版本相关的 `Utf8View` 聚合 bug（**最低**怀疑度——裸探针已排除纯聚合路径）。

**为什么不能直接改 planner.rs**：在触发条件钉死之前改注册逻辑 = 盲改，极易修错地方
（改完单元探针仍绿，workflow 仍坍缩，且引入新回归）。

## 决策：分阶段，先复现后修复

### 阶段 A — 钉死触发条件（本 change 的前置硬门槛）

- **A1**：写一个**最小 workflow YAML**，真实 `core.sql` 节点 + 触发形状（多 stage /
  fan_out / 上游物化），跑 `swanflow run`，确认 GROUP BY **确实坍缩**（复现 assert 红）。
- **A2**：用**控制变量**二分定位触发形状——逐个去掉（单 stage / 无 fan_out / 直连
  MemTable 不物化），找到**最小坍缩形状**。
- **A3**：把 A2 的最小形状固化成**端到端复现测试**（或一个 `swanflow run` 的
  fixture + 脚本断言），这是修复完成的**硬门槛**：复现前红、修复后绿。

> 若 A1 跑不出坍缩（即 skill 记录的是旧版本行为、现已消失）：本 change 降级为
> "确认已修复 + 加永久回归护栏"，`sql_collapse_probe` 转正为正式测试，change 关闭。
> **不臆造**一个不存在的问题去"修"。

### 阶段 B — 按 A2 的根因修复（A 完成前不动 planner.rs）

- B1：针对 A2 钉死的根因，选最小改动：
  - 若是**共享 ctx 竞争** → 每个 stage/fan_out 分支用**独立** `SessionContext`
    （或 `Arc<Mutex<>>` 串行化 `input_0` 注册），不再跨分支共享。
  - 若是**物化退化** → 物化路径强制 `Utf8View`→`Utf8` 归一，或物化后 re-derive
    `input_0` view。
  - 若是 **DataFusion 版本 bug** → 评估升版本（走独立 change，不混入本 change）。
- B2：修复后**同一次提交**里 A3 的端到端测试转绿。

### 阶段 C — 护栏转正

- C1：`sql_collapse_probe.rs` 从 `#[cfg(test)]` 探针模块转正为永久回归测试
  （保留 string 与 int 双键 =3 组的护栏断言）。
- C2：`cargo test -p swanflow --lib` 全绿，`-D warnings` 无新增告警（尤其别给探针
  留 unused import）。

## 备选方案与否决

| 方案 | 否决原因 |
|------|----------|
| 直接改 `planner.rs` 的 `register_table` 串行化 | 未复现就盲改，A2 还没钉死根因；且可能引入死锁（ctx 持锁期间 `ctx.sql` 阻塞） |
| 把 `core.sql` 全部改走 DuckDB 执行（不走 `SqlProxyLogical`） | 大改，改变节点语义（DataFusion 侧 vs DuckDB 侧），超出本 bug 范围，需独立 ADR |
| 升 DataFusion 版本 | 裸探针已排除纯聚合 bug，版本相关怀疑度最低；若 A2 指向它再单开 change |
| 只加单元护栏不修 | 单元探针不触发坍缩，护栏绿≠修复，会留下"假安全感" |

## 风险

- **A1 复现不出来** → 最可能结局之一是"历史 bug 已被某次无关改动修掉"。此时正确动作是
  确认 + 加护栏 + 关 change，**不是**硬造问题。
- **共享 ctx 是设计意图**（stage 间数据依赖）→ 阶段 B1 的"独立 ctx"可能破坏依赖传递；
  需 A2 确认 ctx 共享范围后再定 B 的具体改法。
- **fan_out 竞争**属并发 bug，偶发难复现 → A3 的复现测试需跑足够多次 / 带 `-j` 压力
  才稳定，否则回归会 flaky。

## 与现有能力的边界

- 不修改 `query-engine`（`core.sql` 节点的注册/契约）与 `async-staged-execution`
  （stage 调度）的既有 spec；本 change 新增 `sql-group-by-correctness` 能力，
  只对"分组/聚合结果的正确性"这一横切性质负责。
