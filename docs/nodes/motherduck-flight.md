# MotherDuck Flight Node (`connector.motherduck.flight`)

Trigger a MotherDuck Flight run as a swanflow DAG node: trigger → poll →
emit one status row. Speaks MCP over streamable HTTP
(`https://api.motherduck.com/mcp`, Bearer token) — no CLI, no external
processes.

## ⚠️ 三条红字（接线前必读）

1. **执行身份是 Flight owner。** MotherDuck 官方文档原话：
   *"The Flight runs as its owner, with the owner's token and secrets."*
   swanflow 只是**触发器**——Flight 写任何数据用的是它 owner 的权限。
   部署要求：对每个目标 Flight 执行 `GRANT RUN ON FLIGHT`（给 swanflow
   的服务账号）。反过来这是安全卖点：给 swanflow 一个「只能触发这 N
   个 Flight」的窄账号，比给它全库 token 安全得多。

2. **版本只锁 current。** `MD_RUN_FLIGHT` 没有 version 参数——触发时锁
   定 Flight 的**当前**版本。同一 DAG 今天跑和下周跑可能执行两份不同
   的 Flight 代码。Ledger 会记录每次触发的 `flight_version`，可审计，
   但**不能**钉住版本。需要严格可复现时，把 Flight 源码纳入仓管。

3. **At-least-once，不是 exactly-once。** swanflow checkpoint 按 level
   重放（`core/engine/checkpoint.rs`），恢复时整层重跑，本节点可能被
   二次执行。缓解（双保险，非保证）：
   - 节点把 `RUN_KEY`（16 hex 字符）注入 Flight config，Flight 模板侧
     按 `RUN_KEY` 去重（reconcile-by-diff 范式，参考官方 cookbook
     `flight-hubspot-list-sync`）；
   - 配置 `ledger_dir` 后，已 SUCCEEDED 的触发直接重放 ledger 行，
     不再发起新 run。

   **绝不在 Flight 侧无幂等契约的情况下，把本节点接到不可重复的副作用
   上（退款、下单、发信）。**

## 配置

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `flight_id` | string | — | Flight UUID（推荐） |
| `flight_name` | string | — | 按 name 解析（走 `list_flights`，只列自己拥有的 Flight） |
| `config` | object | `{}` | 逐 run 覆盖，**值必须是字符串**（MotherDuck 要求） |
| `wait` | bool | `true` | `false` = 发射即走，立即输出 RUNNING 行 |
| `poll_interval_secs` | int | `5` | 最小 5——Pulse 按查询计费，更快的轮询烧钱 |
| `timeout_secs` | int | `3600` | 上限 28800（8h，Business 档单次上限） |
| `cancel_on_timeout` | bool | `true` | 超时后调 `cancel_flight_run` |
| `fail_on_nonzero_exit` | bool | `true` | 终态非 SUCCEEDED 时节点失败 |
| `run_scope` | string | 派生 | 幂等键作用域。**模板渲染 `{{dag_run_id}}` 在此**，让每次 DAG run 一个键 |
| `endpoint` | string | MotherDuck | MCP 端点覆盖（测试/自建中继） |
| `token` | string | env | Bearer token；缺省读 `MOTHERDUCK_TOKEN` |
| `ledger_dir` | string | — | 幂等账本目录；不配则无重放保护 |

## 输出（8 列，一行）

```
run_id, run_number, flight_name, flight_version, status, exit_code, duration_ms, logs_tail
```

## YAML 示例

```yaml
nodes:
  - id: nightly-sync
    type: connector.motherduck.flight
    config:
      flight_id: 5f3a…-uuid
      run_scope: "nightly/{{dag_run_id}}"   # 若引擎上下文提供 dag_run_id
      config: { "REGION": "eu-central-1" }
      poll_interval_secs: 5
      timeout_secs: 1800
      ledger_dir: ./runs/motherduck
```

## 成本与延迟（定位是作业，不是交互）

- F16 跑 5 秒 ≈ $0.0008——算力可忽略；
- **真成本是延迟**：每次 run 现装 requirements，冷启动 10–60 秒；
- 轮询按 Pulse 最小 1 CU·s 计费，间隔固定下限 5s、退避上限 15s。

UI/编排上不要把本节点当「点一下等结果」的交互节点用。
