# Proposal: DAG → Flights Compiler（把 swanflow DAG 编译成 MotherDuck Flights）

- **日期**: 2026-11-02
- **来源调研**: `wiki/raw/articles/research/swanflow-motherduck-flight-connector-20261030.md` §7（反向方向）
- **前序**: `2026-11-02-motherduck-flight-connector`（Flight 节点，第一阶段，已实现 19/19 测试绿）
- **状态**: 待审批

## Why

第一阶段解决「swanflow 调 MotherDuck」。客户接下来必然问的是反向问题：**「能不能把这套管线部署到 MotherDuck 上，让它自己按 cron 跑？」**——那时 swanflow 从运行时变成**编译器/部署器**：

- 用户继续在 swanflow 画布上设计、本地调试 DAG；
- `swanflow deploy --target motherduck` 把 DAG 变成 MotherDuck 侧的 Flights + 调度；
- 部署后每步独立可观测、可重跑、可换实例尺寸（F4/F16/F32），不依赖本地进程存活。

三个为什么是现在：

1. **实现成本比第一阶段还低**：编译方向只写不读——只需 `create_flight`/`update_flight`，连轮询都不用做。
2. **用户价值更清晰**：「一句话把管线部署上云」比「在桌面工具里点运行一个云端作业」值钱。
3. **厂商自己让出了这块地**：Flights 文档原话 "the coordination logic is yours to write" + "a dedicated orchestrator…is still the better tool"——跨 Flight 编排正是空缺。

## What Changes

新增 CLI 子命令 `swanflow deploy --target motherduck`（暂定 `motherduck` 一种 target，架构留 target trait 扩展位），三种编译模式：

| 模式 | 产物 | 适用 |
|---|---|---|
| **inline**（默认，MVP） | 整个 DAG 编译成 1 个 Flight（拓扑序执行的 Python 脚本） | 小管线；单日志单版本 |
| **per-node** | 每个非 IO 边界节点一个 Flight + 1 个调度 Flight（按拓扑序 `MD_RUN_FLIGHT` 扇出） | 每步独立重跑/换尺寸 |
| **hybrid** | 重活（ingest/ML）各一个 Flight，胶水逻辑内联 | 真实用法（后期） |

本 change 只交付 **inline 模式 + per-node 模式**，hybrid 留 Non-goals。

**inline 编译产物**（单 Flight 内的 Python 脚本）：
- 支持的节点类型白名单逐个降级编译：`core.io.csv_read`/`parquet_read` → Python duckdb `read_csv_auto`/`read_parquet`；`core.sql` → 直接嵌入 SQL；`core.io.duckdb_write` → `ATTACH 'md:'` 后 `CREATE TABLE AS`；`connector.motherduck.flight` → `MD_RUN_FLIGHT`（嵌套触发，DAG 里嵌 DAG）；
- 白名单外节点（LLM/luajit/python 节点等）→ **编译期硬失败**，错误列出全部不可编译节点（宁拒勿骗——绝不静默跳过节点后宣称部署成功）；
- 数据传递：节点间表引用翻译为脚本内共享 DuckDB 表名（与引擎 `node_<id>` 命名一致）；
- 产出元数据：每节点 `run_key`、flight config 默认值表、部署清单（flight_id/version/创建时间）。

**per-node 模式**：调度 Flight 持久化在 MotherDuck 表中的运行状态（`deploy_<dag_id>_state`），逐节点 `MD_RUN_FLIGHT` + 轮询 + 失败即停（继承第一阶段验证过的轮询/退避参数）。

**部署清单与回滚**：`deploy` 输出 `swanflow-deploy-<dag_id>.json`（flight_id × N、version、模式、时间戳）；`swanflow deploy --rollback` 用 `MD_UPDATE_FLIGHT` 恢复上一版清单。编译产物同时落本地 `flights/` 目录（纯文本可 diff、可入 git——对冲 Flight 源码专有不可移植的风险）。

## Capabilities

### New Capabilities

- `dag-flight-compiler` — DAG → Flight Python 脚本的编译器核心（纯函数：Workflow + 模式 → 脚本文本 + 部署清单），零 IO，全量单测
- `flight-deployer` — MCP `create_flight`/`update_flight` 写侧客户端 + 部署/回滚 CLI

### Modified Capabilities

- 无（不动引擎、不动现有节点；`main.rs` 加 CLI 子命令）。

### Non-goals

- **不做 hybrid 模式**（切分决策需要成本模型，等前两模式实证后立项）。
- **不做增量部署/diff 部署**（MVP 整图重编；清单里记 version 只是留回滚锚点）。
- **不做 cron 调度配置下发**（Flight 侧 schedule 由用户在 MotherDuck 控制台自配；我们只部署不托管调度——避免替用户决定计费节奏）。
- **不做 LLM/AI 节点、Python 节点、luajit 节点的跨编译**——这些依赖本地运行时，硬失败并列明（白名单严格化是特性不是缺陷）。
- **不做多 MotherDuck 账号/多 endpoint 管理**（复用第一阶段 `MOTHERDUCK_TOKEN` 约定）。
- **不对接真实 API 实测**（与第一阶段一致：mock MCP server 验证协议层；live 验证需要 token，明确放弃——mock 已覆盖 create/update 调用形状）。

## Impact

- **新增文件**: `crates/swanflow/src/deploy/mod.rs`（target trait + CLI 接线）、`src/deploy/motherduck.rs`（编译器 + 部署器）、`tests/motherduck_flight_deploy.rs`（mock server E2E）
- **修改文件**: `crates/swanflow/src/main.rs`（`deploy` 子命令）、`docs/`（部署文档）
- **依赖**: 零新增（复用第一阶段 `mcp/http.rs`）
- **硬约束继承**: 第一阶段三条红字全部适用且**编译期显式化**——inline 脚本头部注释写入"runs as owner/at-least-once/version lock"三行警告；部署清单记录 owner 账号标识
- **风险**: inline 模式下节点语义漂移（swanflow 引擎行为 vs 生成的 Python 行为）——靠白名单严格化 + 每节点编译单测钉住；per-node 模式的调度 Flight 自身成为单点（文档写明）
