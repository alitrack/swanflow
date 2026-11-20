# flight-deployer Specification

## ADDED Requirements

### Requirement: 部署与清单

The deployer SHALL create/update Flights via the MCP tools `create_flight`/`update_flight` over the streamable HTTP transport (reusing the connector's `HttpMcpClient`), persist compiled sources to a local `flights/` directory (git-diffable plain text), and write a `swanflow-deploy-<dag_id>.json` manifest recording flight_id, flight_name, version, source sha256, and a rollback pointer to the previous manifest.

#### Scenario: 首次部署

- **GIVEN** 编译产物（inline 单 Flight）与有效 token（或 mock endpoint）
- **WHEN** `swanflow deploy --target motherduck`
- **THEN** mock/服务端收到 `create_flight` 调用（Bearer 认证、flight name、source），本地 `flights/` 出现源码副本，清单写入且 `rollback` 为 null

#### Scenario: 二次部署链式回滚锚

- **GIVEN** 同 DAG 已有一份清单
- **WHEN** 再次部署（内容有变）
- **THEN** 新清单的 `rollback` 指向旧清单，旧 flight 源码仍可在 `flights/` 找到

### Requirement: 回滚

`swanflow deploy --rollback <dag_id>` SHALL restore the previous manifest's flight sources via `update_flight`, and MUST refuse rollback when the current manifest has no rollback pointer (first deployment).

#### Scenario: 回滚成功

- **GIVEN** 当前清单 rollback 指向 v1，v1 源码在 `flights/`
- **WHEN** 执行回滚
- **THEN** `update_flight` 以 v1 源码被调用，清单指针上移到 v1

#### Scenario: 无可回滚时拒绝

- **GIVEN** 首次部署的清单（rollback=null）
- **WHEN** 执行回滚
- **THEN** 操作被拒绝并说明原因，零 MCP 调用发出
