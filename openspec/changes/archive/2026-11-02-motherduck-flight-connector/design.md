# Design: MotherDuck Flight Connector

## 0. 判词（来自调研 #2026103003）

可行性 **A-**（一天出原型），语义风险 **C**——三条硬约束不是代码能解决的，必须写进产品契约。

## 1. 传输层：MCP over streamable HTTP（路 A，正式实现路线）

```
FlightNode (nodes/motherduck.rs)
    │  执行期构造一次性连接
    ▼
HttpMcpTransport (mcp/http.rs)          ← 本 change 新写（250–400 行）
    │  POST https://api.motherduck.com/mcp
    │  Authorization: Bearer $MOTHERDUCK_TOKEN
    │  Accept: application/json, text/event-stream
    │  Mcp-Protocol-Version: 2025-06-18   (初始化协商后回填)
    ▼
复用 McpClient 的 JSON-RPC 层 (mcp/client.rs)   ← 零改动
    │  tools/call: run_flight / get_flight_run / get_flight_logs / cancel_flight_run
    ▼
MotherDuck 侧
```

关键实现点：

- **传输 trait 抽象**：现有 `McpClient` 的 `ContentLengthTransport`（stdio）与新的 HTTP 传输实现同一抽象，`McpClient` 泛型化或 trait-object 化。最小改动：只把「读一条 JSON-RPC 消息」抽出来。
- **SSE 解析**：响应可能是 `application/json`（单响应）或 `text/event-stream`（流）。两种都要处理：SSE 分帧按 `event:`/`data:` 行解析，取 `data:` 载荷为 JSON-RPC 响应。不引 crate，手写 ~80 行解析器（事件面小：message/ping）。
- **协议头**：`initialize` 响应的 header 里带 `Mcp-Session-Id`，后续请求回带；协议版本协商后每个请求带 `Mcp-Protocol-Version`。**关键未知**：MotherDuck 端点是否强制 SSE、是否强制 session——实现第一步先写集成探针脚本（带 token 手跑一次 initialize）确认，再定稿传输层。
- **同步 vs 异步**：`duckdb::Connection` 是同步的，但本节点不用 duckdb 走传输；`reqwest` 已有 blocking feature，节点执行在 DataFusion `ExecutionPlan::execute` 内是同步上下文 → 用 `reqwest::blocking` + `spawn_blocking` 包裹（与 McpToolNode 现有做法一致）。

## 2. 节点：`connector.motherduck.flight`

**capability**: `ReadWrite`（当前枚举无「有副作用的输出节点」档；缺口记录在 proposal Non-goals）

**配置 schema**（8 项，serde，全部有默认值除 flight_id/flight_name）：

```jsonc
{
  "flight_id": "uuid",              // 与 flight_name 二选一，优先 id
  "flight_name": "string",          // 名字解析仅当调用者是 Flight owner 时可靠（MD_LIST_FLIGHTS 只列自己的）
  "config": { "KEY": "VALUE" },     // 逐 run 覆盖，只能覆盖已定义 key，值必须字符串
  "wait": true,                     // false = 发射即走（fire-and-forget）
  "poll_interval_secs": 5,          // Pulse 按查询计费，最小间隔 5s
  "timeout_secs": 3600,             // 上限 8h（Business），默认 1h
  "cancel_on_timeout": true,
  "fail_on_nonzero_exit": true      // exit_code != 0 → 节点失败
}
```

**输出 schema**（固定 8 列，恰一行）：

```
run_id: Utf8, run_number: UInt64, flight_name: Utf8, flight_version: UInt32,
status: Utf8, exit_code: Int32, duration_ms: Int64, logs_tail: Utf8
```

**执行状态机**：

```
[idle] --propose_plan--> FlightLogical (无输入计划；或接收上游计划)
   └─ ExecutionPlan::execute
        ├─ ledger 查重: (dag_run_id, node_id) 已有 SUCCEEDED 行? → 直接 emit 旧行(幂等)
        ├─ run_flight(flight_id, config + RUN_KEY) → run_id, run_number   (立即返回)
        ├─ loop: get_flight_run(run_number)
        │     ├─ PENDING/RUNNING → sleep(poll_interval, 指数退避上限 15s)
        │     └─ SUCCEEDED/FAILED/CANCELLED → get_flight_logs → break
        ├─ timeout → cancel_flight_run → 按 cancel_on_timeout emit FAILED 行或 Err
        └─ ledger 落行 → emit 1 行 → Boundedness::Bounded
```

**注册**：`nodes/registry.rs` `register_core_nodes` 加一行 `register_node::<FlightNode>("connector.motherduck.flight")`。

**可观测**：`run_id`/`run_number`/`flight_version` 写进节点事件与 lineage properties，DAG 侧可反查 MotherDuck 侧 run 日志。

## 3. Run Ledger（幂等 + 版本钉住）

存储：run 目录下 `motherduck_ledger.jsonl`（append-only，一行一个触发记录）。

```jsonc
{"dag_run_id":"…","node_id":"…","flight_id":"uuid","flight_version":3,
 "run_number":17,"run_id":"uuid","status":"SUCCEEDED","exit_code":0,
 "duration_ms":42000,"triggered_at":"2026-11-02T10:00:00Z","run_key":"sha256前16位"}
```

- **幂等查重**：节点执行先按 `(dag_run_id, node_id)` 查 ledger，已有 terminal 行则不重触发（resume 场景整层重跑时的副作用防线）。
- **版本快照**：触发前调 `MD_GET_FLIGHT_VERSION`（MCP `get_flight`）把当前版本号记入 ledger——`MD_RUN_FLIGHT` 只锁 current 无 version 参数，重跑可复现性靠 ledger 钉住。
- **RUN_KEY**：`RUN_KEY = sha256(dag_run_id + node_id)[:16]` 塞进 Flight config。Flight 模板侧按此键去重（官方 cookbook `flight-hubspot-list-sync` 的 reconcile-by-diff 范式）——这是双保险的第二层，Flight 侧的去重不是我们能强制的，但键必须传。

## 4. 三条硬约束（实现与文档都必须遵守）

| # | 约束 | 官方依据 | 实现动作 |
|---|---|---|---|
| 1 | **执行身份 = Flight owner** | "The Flight runs as its owner, with the owner's token and secrets." | swanflow 只是触发器；部署清单写明需逐 Flight `GRANT RUN ON FLIGHT`；文档红字。反面是卖点：给 swanflow 窄账号（只能触发这 N 个 Flight）比全库 token 安全 |
| 2 | **版本只锁 current，无 version 参数** | "The new run locks to the Flight's current version." | ledger 记 `flight_version` + 触发前快照；文档写明「Flight 是外部依赖，跨版本重跑语义由 ledger 快照界定」 |
| 3 | **checkpoint 是 level 粒度 → at-least-once** | 本仓 `core/engine/checkpoint.rs:20` `completed_levels: Vec<usize>` | RUN_KEY 幂等键强制传；ledger 查重；文档红字：**Flight 节点是 at-least-once，不是 exactly-once** |

## 5. 测试策略

- **单元**：SSE 解析器（分帧/半包/多事件）、ledger 查重逻辑、config 校验（wait=false 时 timeout 无意义等）。
- **集成（无 token 门）**：`#[ignore]` 标注的 live 测试——`MOTHERDUCK_TOKEN` 环境变量存在才跑（`run_flight` → 轮询 → SUCCEEDED 全链路、超时取消、exit_code=3 故障注入）。CI 不跑，本地有 token 跑。
- **E2E**：2 节点 DAG `duckdb_scan → flight_node`，输出 8 列一行（用 mock MCP server：本地 axum 起一个假 `api.motherduck.com`，脚本化状态迁移 PENDING→RUNNING→SUCCEEDED）——这样 E2E 不需要 token，CI 可跑。

## 6. 里程碑

| M | 内容 | 验收 |
|---|---|---|
| M1 | `mcp/http.rs` 传输层 + SSE 解析器 + mock server 单测 | 单测全绿；`initialize` 握手对 mock server 通过 |
| M2 | `nodes/motherduck.rs` 节点 + 注册 + ledger | E2E（mock server）2 节点 DAG 出 8 列一行 |
| M3 | 故障注入三连（exit_code≠0 / timeout+cancel / resume 二次触发查 ledger 拦截） | 三条行为与设计一致；resume 场景 ledger 拦截二次触发实证 |
| M4 | 文档红字 + `#[ignore]` live 测试骨架 | docs 审阅；live 测试骨架带 TODO(token) 说明 |

## 7. 开放问题（实现期解决，不阻塞审批）

1. MotherDuck MCP 端点是否强制 SSE 响应（vs 单 JSON）——M1 第一天用带 token 的探针脚本确认。
2. `Mcp-Session-Id` 生命周期：一次 DAG run 一个 session 还是长连接复用——倾向前者（节点执行期构造一次性连接，简单且无状态）。
3. ledger 文件并发写（同 DAG 多节点并行触发）——append-only jsonl + 文件锁，或每节点独立文件（倾向后者：`motherduck_ledger_<node_id>.jsonl`）。
