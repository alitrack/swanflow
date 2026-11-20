# motherduck-flight-node Specification

## ADDED Requirements

### Requirement: Flight 触发节点执行

The `connector.motherduck.flight` node SHALL trigger a MotherDuck Flight run via the MCP tool `run_flight`, poll `get_flight_run` until a terminal status, and emit exactly one output row with 8 columns.

MUST emit: `run_id (Utf8), run_number (UInt64), flight_name (Utf8), flight_version (UInt32), status (Utf8), exit_code (Int32), duration_ms (Int64), logs_tail (Utf8)`.

#### Scenario: 成功触发并轮询到 SUCCEEDED

- **GIVEN** 配置了有效 `flight_id` 且 mock MCP server 脚本化返回 PENDING→RUNNING→SUCCEEDED
- **WHEN** DAG 执行到 Flight 节点
- **THEN** 输出恰一行，`status="SUCCEEDED"`，`exit_code=0`，`logs_tail` 非空

#### Scenario: fire-and-forget 模式

- **GIVEN** `wait=false`
- **WHEN** `run_flight` 返回 run 行
- **THEN** 立即 emit 该行（`status="RUNNING"`，`exit_code=NULL`）并结束流，不轮询

#### Scenario: Flight 以非零退出码失败

- **GIVEN** `fail_on_nonzero_exit=true` 且 Flight run 终态 `FAILED`，`exit_code=3`
- **WHEN** 轮询到终态
- **THEN** 节点执行返回 Err，错误信息含 run_id 与 exit_code

#### Scenario: 超时取消

- **GIVEN** `timeout_secs=5`、`cancel_on_timeout=true`、Flight 长时间 RUNNING
- **WHEN** 轮询超过 timeout
- **THEN** 调用 `cancel_flight_run` 后节点失败，错误信息标明 "cancelled on timeout"

### Requirement: Run Ledger 幂等保护

The Flight node SHALL consult the run ledger before triggering: if a terminal (SUCCEEDED) row already exists for `(dag_run_id, node_id)`, it MUST NOT trigger a new run and SHALL re-emit the ledger row instead.

#### Scenario: resume 不二次触发

- **GIVEN** ledger 中已有 `(dag_run_id, node_id)` 的 SUCCEEDED 行
- **WHEN** 同一 dag_run 恢复执行且整层重跑到该节点
- **THEN** 不调用 `run_flight`，直接 emit ledger 中的旧行

#### Scenario: RUN_KEY 幂等键传递

- **WHEN** 节点触发 run
- **THEN** 传给 `run_flight` 的 config MUST 包含 `RUN_KEY = sha256(dag_run_id + node_id) 截取前 16 位`，且该键记入 ledger

### Requirement: 配置校验

The node config validation SHALL reject: both `flight_id` and `flight_name` absent; `poll_interval_secs < 5`; `timeout_secs > 28800`; config values that are not strings.

#### Scenario: 缺少 flight 标识

- **GIVEN** `flight_id` 与 `flight_name` 均未提供
- **WHEN** 校验配置
- **THEN** 返回语义错误，指明二者必居其一

#### Scenario: 轮询间隔过短

- **GIVEN** `poll_interval_secs=1`
- **WHEN** 校验配置
- **THEN** 校验失败——Pulse 按查询计费，最小间隔 5 秒
