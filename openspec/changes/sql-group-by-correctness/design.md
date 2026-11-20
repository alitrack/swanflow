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

## 阶段 A 实测（2026-11-20，main @ 7eace18）

### 0. 环境事实：默认 staged 执行路径在 main 上整体挂死（与 GROUP BY 无关，但阻塞 A1 命令）

- 现象：`swanflow run <yaml>`（默认 `execute_workflow`，`crates/swanflow/src/core/engine/mod.rs:668`）
  在**任何** workflow 上永不结束：节点首次执行成功后，`'retry_loop`（`mod.rs:1149-1497`，
  由 `b0f9c85` 2026-10-19 P2-15 批次引入）成功分支**没有 `break`**——循环下一轮重跑同一节点，
  第二次 `register_table(node_...)` 报 `The table ... already exists`（`mod.rs:1185-1204`，硬错误后
  是裸 `continue`），无限重试；retry 耗尽/Fail 分支（`:1314-1338`）同样没有 `break`
  （全文只有 2 处 `continue 'retry_loop`，0 处 `break`）。
- 最小复现（仓库自带 example）：`timeout 60 target/debug/swanflow run examples/etl_flow.yaml` →
  2 节点 workflow 挂死，日志 `.campaign/pi-sql/run/etlcheck/run_etl.log`。
- 量化证据：A1 YAML 的 staged run 在 300s 内产生 **4584 次** "Recording materialization"、
  **4583 次** "already exists" 错误（`.campaign/pi-sql/run/a1/run_a1.log`，约 15 次/秒空转）。
- 可用路径：`--legacy-yaml`（`execute_yaml_workflow` → `compile_and_execute_node`，`mod.rs:2218`，
  每节点恰执行一次、无 retry 循环）**可以完成**，且驱动的是同一个 `SwanFlowPlanner` 晚绑定
  （`mod.rs:2339-2358`）。下表 CLI 证据均由 `--legacy-yaml` 采集。
- 该挂死是独立于本 change 的既有缺陷（P2-15 实现缺 `break`），**不在阶段 A 修复范围**，
  已在 REPORT 中单独上报协调者。

### 1. 形状矩阵（真实数字，全部 2026-11-20 实测）

| 形状 | 执行路径 | 组数 | 坍缩? | 证据文件 |
|---|---|---|---|---|
| A1 基线：duckdb_read(Utf8View) → core.sql GROUP BY → duckdb_write | legacy | 3（a=3,b=2,c=1） | 否 | `.campaign/pi-sql/run/a1/run_a1_legacy.log`；`duckdb repro_out.duckdb "SELECT * FROM groupby_out"` |
| A1 同形状走默认 staged run | staged（execute_workflow） | — | N/A：P2-15 挂死，SqlProxy 节点从未被执行（§0） | `.campaign/pi-sql/run/a1/run_a1.log` |
| A2a 物化：seed → duckdb_write → duckdb_read 回读 → GROUP BY | legacy | 3 | 否 | `.campaign/pi-sql/run/a2a_materialize/run_a2a_materialize.log` |
| A2b **fan-out**：两个 core.sql 共享同一上游（P1-6 历史形状） | legacy | 每分支 3，两分支一致 | 否 | `.campaign/pi-sql/run/a2b_fanout/run_a2b_fanout.log` |
| A2c DataFusion 字面量源（Utf8，SqlProxy→SqlProxy） | legacy | 3 | 否 | `.campaign/pi-sql/run/a2c_literal_source/run_a2c_literal_source.log` |
| A2d 规模：1000 行 / 50 键 | legacy | 50（每组 n=20，总 1000，无异常组） | 否 | `.campaign/pi-sql/run/a2d_big/run_a2d_big.log` |
| staged s1：duckdb_read(Utf8View) → GROUP BY（2 级） | staged 机器（测试驱动） | 3 | 否 | `tests/sql_group_by_collapse.rs::shape1_duckdb_source_groupby` |
| staged s2：duckdb_read → filter → GROUP BY（3 级，stage 边界落在 SqlProxy 前、中间节点经 flow_metadata 物化） | 同上 | 3 | 否 | `::shape2_multistage_filter_before_groupby` |
| staged s3：fan-out 两个 GROUP BY 同层（P1-6 形状，staged 顺序执行版） | 同上 | 各 3，分支一致 | 否 | `::shape3_fanout_two_sql_same_level` |
| staged s4：字面量 Utf8 源（SqlProxy→SqlProxy） | 同上 | 3 | 否 | `::shape4_literal_utf8_sqlproxy_source` |
| staged s5：parquet 文件源（真 Utf8） → GROUP BY | 同上 | 3 | 否 | `::shape5_parquet_utf8_source` |
| staged s6：1000 行 / 50 键 | 同上 | 50×20 | 否 | `::shape6_big_1000_rows_50_keys` |
| 既有单元探针（MemTable + into_view 镜像，非本阶段新增） | unit | 3（string+int） | 否 | `src/core/sql_collapse_probe.rs`（`cargo test -p swanflow --lib sql_collapse_probe`） |

"staged 机器（测试驱动）"＝`tests/sql_group_by_collapse.rs::run_staged`：
`WorkflowCompiler::with_cache_dir(true, ..).compile(..)`（同 `mod.rs:673-682`）＋每层
`sync_metadata_to_context` 等价逻辑（同 `mod.rs:158-182`）＋与 `mod.rs:708-716` 逐字相同的
`DefaultPhysicalPlanner::with_extension_planners([SwanFlowPlanner])`/`SwanFlowQueryPlanner`/
`SessionStateBuilder` 栈＋`create_physical_plan`+`collect`（同 `mod.rs:1151-1158`）——
即 `execute_workflow` 的每节点代码本体，唯一缺的是会挂死的 P2-15 循环本身。

### 2. 一句话判定

**历史 string GROUP BY 坍缩在 7eace18（2026-11-20）上不可复现**，排除的形状覆盖：
DuckDB 回读（Utf8View）/ DataFusion 字面量（Utf8）/ parquet（Utf8）三种字符串源 ×
2 级 / 3 级（中间物化）/ 同层 fan-out（P1-6 形状）/ 1000 行×50 键规模，
CLI（legacy 路径）与 staged 编译器机器双路径均验证。最可能的历史触发器——
**共享 ctx 的 `input_0` 别名互踩**（同 stage 多个 SqlProxy 注册同一别名）——已被
**P1-6（`19868e1`，2026-10-19）的 per-proxy scratch `SessionContext` 修复**
（`planner.rs:369-378`）移除：现在每个 proxy 的 `input_0` 注册在独立 scratch ctx，
别名共享在构造上不可能，坍缩失去可观察形状。

### 3. 未排除项（诚实边界）

1. **默认 staged 路径的端到端 run**：因 P2-15 挂死（§0）无法在 main 上完整跑完；
   staged 侧证据来自逐字相同的编译器机器测试驱动。若该挂死被独立修复，应把 s3
   fan-out 形状放回真实 staged 循环重跑一次（含未来若引入 `tokio::join` 真并发版本）。
2. **真并发 fan-out**（多个 SqlProxy 并行注册）：当前 staged 循环是层内顺序执行
   （`mod.rs:854` for 循环，非 join），且 P1-6 后无共享别名，故无可测的竞争窗口；
   这不是"验证过并发安全"，而是"当前代码没有竞争表面"。
3. **DataFusion 版本相关 Utf8View 聚合 bug**（候选成因 4，最低怀疑度）：
   单元探针 + 6 个 workflow 形状全绿，无观察到的触发条件，维持不可判定（无形状可挂）。

### 4. A3 处置（A-出口）

坍缩未复现 → 按 tasks.md A-出口，本 change 的 A3 交付物
`crates/swanflow/tests/sql_group_by_collapse.rs` 定位为**永久护栏**而非红复现
（文件头已写明 STATUS；任一形状再现坍缩会以显式 "COLLAPSE DETECTED" 失败）。
C1（"探针转正为永久回归"）由既有单元探针 + 该 e2e 护栏共同满足。
