# dag-flight-compiler Specification

## ADDED Requirements

### Requirement: 白名单节点编译

The compiler SHALL compile only nodes on the whitelist (`core.io.csv_read`, `core.io.parquet_read`, `core.io.http_fetch` (query form), `core.sql`, `core.transform.*` with static SQL equivalents, `core.io.duckdb_write`, `connector.motherduck.flight`) into standalone Python/duckdb statements, and MUST fail compilation with an aggregated error listing every non-compilable node when any node is outside the whitelist.

MUST NOT silently skip or stub a node: the output covers all nodes or compilation fails.

#### Scenario: 全白名单 DAG 编译成功

- **GIVEN** 一个仅含 csv_read → sql → duckdb_write 的 DAG
- **WHEN** 以 inline 模式编译
- **THEN** 产出完整 Python 脚本，三节点各对应一段语句，含 `SWANFLOW_DEPLOY_OK` 尾标记

#### Scenario: 白名单外节点硬失败并列全清单

- **GIVEN** DAG 含一个 `core.ai.llm` 节点和一个 `core.script.python` 节点
- **WHEN** 编译
- **THEN** 编译失败，错误同时列出两个节点的 type_id 与 node_id（聚合错误，非首错即停）

#### Scenario: 模板变量缺失编译失败

- **GIVEN** SQL 节点引用 `{{undefined_var}}` 且 workflow 无该变量声明
- **WHEN** 编译
- **THEN** 编译失败，错误指明未解析的模板变量——不留运行期悬念

### Requirement: inline 模式产物

The inline compiler SHALL emit a single Flight program: header comment (dag id, timestamp, mode + three red-letter warnings), sequential per-node statements in topological order, and the `SWANFLOW_DEPLOY_OK <dag_id>` success marker.

#### Scenario: 拓扑序保证

- **GIVEN** DAG 有 a→b→c 链与孤立的 d（无边）
- **WHEN** 编译
- **THEN** 生成语句顺序满足 a 先于 b 先于 c；d 出现在链之后（孤立节点按 id 序排在末尾）

#### Scenario: 嵌套 Flight 触发编译

- **GIVEN** DAG 含 `connector.motherduck.flight` 节点
- **WHEN** 编译
- **THEN** 该节点编译为 `MD_RUN_FLIGHT` 调用语句，flight_id 与 config（含 RUN_KEY）原样传入

### Requirement: per-node 模式与调度 Flight

The per-node compiler SHALL emit one Flight per whitelisted node plus a scheduler Flight that persists per-node status in a `deploy_<dag>_state` table on `md:`, triggers nodes in topological order via `MD_RUN_FLIGHT`, polls with the connector's backoff parameters, stops on first failure, and skips nodes already SUCCEEDED in the state table on re-run.

#### Scenario: 调度 Flight 跳过已完成节点

- **GIVEN** 状态表中节点 a 已为 SUCCEEDED、b 未开始
- **WHEN** 调度 Flight 重跑
- **THEN** a 不被再次触发，b 正常执行（at-least-once 缓解）

#### Scenario: 失败即停

- **GIVEN** 节点 b 的 Flight run 终态 FAILED
- **WHEN** 调度 Flight 轮询到 FAILED
- **THEN** 后续节点不被触发，状态表记录 b=FAILED，调度 Flight 以非零退出
