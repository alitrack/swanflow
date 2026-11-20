# Tasks: MotherDuck Flight Connector

## M1: MCP HTTP 传输层
- [x] 1.1 抽象传输 trait（读一条 JSON-RPC 消息），stdio 与 HTTP 共用，stdio 零行为变更
- [x] 1.2 `mcp/http.rs`：POST + Bearer token + 双 content-type 处理（单 JSON / SSE）
- [x] 1.3 手写 SSE 分帧解析器（~80 行），单测覆盖半包/多事件/ping
- [x] 1.4 mock MCP server（本地 hyper），单测：initialize 握手 / session 回带 / error 传播

## M2: Flight 节点 + Ledger
- [x] 2.1 `nodes/motherduck.rs`：FlightLogical + ExecutionPlan + RecordBatchStream（照 McpToolNode 范式）
- [x] 2.2 配置 8 项 serde + validate（flight_id/flight_name 二选一、poll≥5s、timeout≤8h、config 值必须字符串）
- [x] 2.3 执行状态机：run → poll（指数退避上限 15s）→ terminal → logs → emit 8 列一行
- [x] 2.4 run ledger：`motherduck_ledger_<run_key>.jsonl` append-only；触发前查重；RUN_KEY 计算与注入
- [x] 2.5 registry 注册 + mod 声明；E2E（mock server）出 8 列一行

## M3: 故障注入三连
- [x] 3.1 exit_code=3 → fail_on_nonzero_exit=true 节点失败（错误含 run_id）
- [x] 3.2 timeout=5s + cancel_on_timeout → cancel_flight_run 被调用且节点失败
- [x] 3.3 resume 场景：ledger 已有 SUCCEEDED 行 → 二次执行不触发新 run，re-emit 旧行

## M4: 文档与 live 测试骨架
- [x] 4.1 文档红字：at-least-once / 执行身份=Flight owner / 版本漂移由 ledger 钉住 / GRANT RUN ON FLIGHT 部署清单（`docs/nodes/motherduck-flight.md`）
- [~] 4.2 ~~live 测试骨架（MOTHERDUCK_TOKEN）~~ **已放弃（用户指令：需要 token 的全部放弃）**
- [~] 4.3 ~~SSE 强制与否探针脚本~~ **已放弃（同上，需真实 token 对 api.motherduck.com 探测）**

## 验证记录（2026-11-02）

- `cargo check -p swanflow --lib` → 0 errors
- `cargo test -p swanflow --test motherduck_flight_e2e` → **5 passed / 0 failed**（happy path / 非零退出 / 超时取消+cancel 计数 / ledger 重放不二次触发+run 计数 / RUN_KEY 注入）
- `cargo test -p swanflow --lib mcp::http` → **8 passed / 0 failed**（SSE 分帧/半包/多事件/多行 data/CRLF/注释与 ping/无尾空行 flush/id 匹配）
- `cargo test -p swanflow --lib nodes::motherduck` → **6 passed / 0 failed**（配置校验×4 / run_key 稳定性与敏感性 / ledger 往返+重放）

## 设计修正（相对原提案，实现期发现）

- 引擎不向节点注入 `dag_run_id`/`node_id`（`compile_and_execute_node` 只渲染用户 step_context）→ ledger 幂等键改为 `run_scope` 可配置（模板变量挂此），缺省派生 `sha256(flight+config)` 前 16 hex。
- reqwest 未启用 `stream` feature → SSE 响应整读 body 后喂解析器（POST 响应体有限，行为等价，零新 feature）。
- E2E mock server 用 hyper 1.x 手搭（零新依赖，CI 可跑，不需要 token）。
