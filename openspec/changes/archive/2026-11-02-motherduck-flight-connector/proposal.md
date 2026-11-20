# Proposal: MotherDuck Flight Connector（`connector.motherduck.flight`）

- **日期**: 2026-11-02
- **来源调研**: `wiki/raw/articles/research/swanflow-motherduck-flight-connector-20261030.md`（19.3KB，swanflow 本仓 file:line 实测 + MotherDuck 官方 docs 一手取证）
- **状态**: 待审批

## Why

MotherDuck 官方在 Flights 概念页明确把「编排」让了出来：

> "One Flight is one process. A run isn't a managed worker pool with a built-in queue or distributed state. … the coordination logic is yours to write."
> "…for workflows that span many external systems, a dedicated orchestrator like Airflow or Prefect is still the better tool."

客户在 Flights 上写了第 5 个、第 10 个 Flight 之后，必然撞上「A 成功才跑 B / B 失败重试 / C 等 D 数据齐」——那时他们需要一个 DAG 引擎。**这个空缺是厂商文档写明的，不是我们猜的需求。**

本仓的接缝也现成：`McpToolNode`（`crates/swanflow/src/nodes/mcp.rs:37`）已经实现了「执行期调外部服务 → 吐一行 RecordBatch」的完整范式（`UserDefinedLogicalNodeCore` + 自定义 `ExecutionPlan` + `RecordBatchStream`）。Flight 节点照抄该范式即可，不是从零发明。`reqwest 0.13.2` 已在依赖（`Cargo.toml:56`），零新 crate。

不做这个节点的代价：swanflow 用户要触发 MotherDuck 作业只能 shell-out 到 CLI（脆、破坏单二进制叙事），或者手工在 SQL 节点里拼 `MD_RUN_FLIGHT`（拿不到状态轮询与超时取消）。

## What Changes

1. **新节点类型 `connector.motherduck.flight`**（capability: `ReadWrite`，见缺口①）：把一个 MotherDuck Flight run 作为 DAG 节点执行——触发、轮询、超时取消、输出固定 8 列状态行。
2. **远程 MCP HTTP 传输层**（`crates/swanflow/src/mcp/` 新增 `http.rs`，约 250–400 行）：现 `McpClient` 只有 stdio（`mcp/client.rs:22`，全仓 `text/event-stream`/`mcp-session-id` 零命中）。新写 streamable HTTP 传输（含 SSE 解析、`Mcp-Protocol-Version` 头），复用现有 JSON-RPC 层。端点 `https://api.motherduck.com/mcp`，`Authorization: Bearer $MOTHERDUCK_TOKEN` 免 OAuth（官方 Gemini CLI 配置即此法）。
3. **run ledger（幂等与可复现）**：Flight 节点每次触发落一行 `(dag_run_id, node_id, flight_id, flight_version, run_number, run_id, status)`。目的：resume 时先查 ledger 再决定是否触发（缓解 level 级重放的重复副作用），并钉住版本快照（`MD_RUN_FLIGHT` 无 version 参数，只锁 current）。
4. **文档红字**：Flight 节点语义是 **at-least-once**；执行身份是 Flight owner（"The Flight runs as its owner, with the owner's token and secrets"）——swanflow 是触发器不是写入者。

## Capabilities

### New Capabilities

- `motherduck-flight-node` — Flight 触发/轮询/取消节点（本 change 的主体）
- `mcp-http-transport` — 远程 MCP streamable HTTP 传输（Flight 节点的底层，也是未来一切远程 MCP 工具节点的公共底座）

### Modified Capabilities

- 无。（`NodeCapability::Action` 档位是缺口①，但**本 change 不做**——动枚举是横切变更，单独立 change，见 Non-goals。）

### Non-goals

- **不做 `NodeCapability` 新增 `Action` 档**——`WriteOnly` 定义是 "no output port"（`nodes/mod.rs:81-88`），Flight 节点只能暂声明 `ReadWrite`。枚举改动是横切的，单独开 change。
- **不做 DAG → Flights 编译（反向部署）**——调研判定这是第二阶段差异化方向，实现成本更低（只写不读），但依赖第一阶段验证语义，单独立项。
- **不做 DuckDB `md:` 直连路线（路 B）**——版本耦合太紧（bundled 1.5.6 是否有对应 `motherduck` 扩展构建未实测，需 MotherDuck token 的 S1 spike），不把产品绑死在某个扩展构建上。若后续拿到 token 验证通过，可作为传输层的备选实现。
- **不做 CLI shell-out（路 C）**——最脆，仅 PoC 兜底，不进产品。
- **不做 OAuth 流程**——Bearer token 已覆盖服务账号场景（swanflow 是服务账号触发，不是人交互登录）。
- **不做多租户 token 管理 / vault 集成**——token 从环境变量 `MOTHERDUCK_TOKEN` 读，与现有 MCP stdio 客户端同级别即可。

## Impact

- **新增文件**: `crates/swanflow/src/nodes/motherduck.rs`（节点）、`crates/swanflow/src/mcp/http.rs`（HTTP 传输）、ledger 存储（挂 run 目录，schema 见 design.md）。
- **修改文件**: `crates/swanflow/src/nodes/mod.rs`（mod 声明 + registry 注册一行）、`crates/swanflow/src/mcp/mod.rs`（pub 模块声明）。
- **依赖**: 零新增 crate（reqwest 已有 json/rustls/blocking features）。
- **受影响系统**: 节点注册表 +1；MCP 模块新增传输实现（不动现有 stdio 客户端）；引擎零改动（照 McpToolNode 范式，不需要动 engine）。
- **风险**: 语义三条（执行身份 / 版本漂移 / at-least-once）已写进设计文档的「硬约束」节，实现必须遵守，文档必须红字。
