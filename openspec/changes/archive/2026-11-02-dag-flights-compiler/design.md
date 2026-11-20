# Design: DAG → Flights Compiler

## 0. 分层

```
swanflow deploy --target motherduck [--mode inline|per-node] <workflow.yaml>
    │
    ▼
Compile (纯函数, 全量单测)                      Deploy (IO, mock E2E)
Workflow ──► FlightProgram {                   ──► HttpMcpClient (复用 M1)
  main_py: String          inline 脚本            create_flight × N
  per_node: Vec<FlightProgram>  per-node          update_flight (回滚)
  manifest: DeployManifest                       swanflow-deploy-<dag>.json
}                                                flights/ 本地副本
```

编译器零 IO（任何环境可测）；部署器只依赖第一阶段的 HTTP 传输。

## 1. inline 编译（MVP 主力）

### 1.1 节点降级表（白名单）

| swanflow type_id | 编译产物（Python） |
|---|---|
| `core.io.csv_read` | `_con.execute("CREATE TABLE node_{id} AS SELECT * FROM read_csv_auto('{path}')")` |
| `core.io.parquet_read` | 同上，`read_parquet('{path}')` |
| `core.io.http_fetch`（仅 query 形态） | `read_json_auto('{url}')` 嵌入其 query |
| `core.sql` | 原样嵌入 `config.query`（`{{input_0}}`/`{{node_x}}` 模板先在编译期解析为表名） |
| `core.transform.project/filter/aggregate` | 编译期改写为其等价 SQL（已有 SQL 表示的节点才收） |
| `core.io.duckdb_write` | `ATTACH 'md:' AS md; CREATE TABLE md.{table} AS SELECT * FROM node_{id}` |
| `connector.motherduck.flight` | `SELECT * FROM MD_RUN_FLIGHT(flight_id := '{id}', config := {map})`（嵌套触发） |
| 其他一切 | ❌ 编译期硬失败，错误聚合列出全部不可编译节点（type_id × node_id 清单） |

**规则：宁拒勿骗。** 编译产物必须覆盖 DAG 的**全部节点**，或者干脆不产出。

### 1.2 生成脚手架

```python
# Compiled from swanflow DAG <dag_id> at <ts> — mode: inline
# ⚠️ runs as owner | at-least-once | version-locks at run time
import duckdb
_con = duckdb.connect()
# ── node <id> (<type_id>) ──
<per-node compiled statement>
...
print("SWANFLOW_DEPLOY_OK <dag_id>")
```

- 成功尾标记 `SWANFLOW_DEPLOY_OK`：run 日志可 grep 部署健康。
- 不做 requirements 安装——inline 只依赖 `duckdb`（MotherDuck Flight 镜像自带），requirements 固定为 `duckdb==1.5.*`。

### 1.3 模板解析顺序（编译期）

引擎在运行期做的 `{{input_N}}`/`{{node_x}}`/`{{var}}` 渲染，编译器在**编译期**做同样的事：边表推出 `input_N` → 源节点表名；`{{var}}` 找 workflow 级变量声明，找不到即编译失败（不留运行期悬念）。

## 2. per-node 编译

- 每个白名单节点一个 Flight，flight name = `swanflow_<dag_id8>_<node_id8>`；
- 调度 Flight：启动时在 `md:` 建状态表 `deploy_<dag_id8>_state(run_key, node_id, status, updated_at)`，按 petgraph 拓扑序逐节点 `MD_RUN_FLIGHT` + 轮询（复用第一阶段参数：5s 起、15s 顶、指数退避）+ 状态表更新 + 失败即停；
- 恢复语义：调度 Flight 重跑时先查状态表，SUCCEEDED 的节点跳过（at-least-once 缓解，同第一阶段 ledger 思路，只是 ledger 落在 MotherDuck 表里）。

## 3. DeployManifest

```jsonc
{
  "dag_id": "…", "mode": "inline", "deployed_at": "…",
  "owner_hint": "<token 对应账号的掩码标识>",
  "flights": [
    { "flight_id": "…", "flight_name": "…", "version": 1,
      "source_sha256": "…" }   // 本地 flights/ 副本的哈希，回滚校验用
  ],
  "rollback": null              // 上一份清单的指针，链式
}
```

`--rollback`：读当前清单 → 逐 flight `update_flight` 回 `flights/` 里的上一版源码 → 清单指针上移。没有上一版 → 拒绝回滚（第一次部署无可回）。

## 4. 测试策略

- **编译器（纯函数，全量单测）**：每个白名单节点类型一条「config → 生成语句」golden 测试；模板解析（input_N/var 缺失两种失败）；白名单外节点硬失败并列全清单；inline/per-node 双模式 manifest 断言。
- **部署器（mock MCP server E2E，复用第一阶段 harness）**：create_flight 调用形状（Bearer/工具名/参数 JSON）；回滚走 update_flight；无上一版时回滚拒绝。
- **不做 live**（token 指令，同第一阶段）。

## 5. 里程碑

| M | 内容 | 验收 |
|---|---|---|
| M1 | 编译器核心：白名单降级表 + inline 生成 + golden 测试 | 纯函数单测全绿；examples/ 里 2 个纯 SQL DAG 编译产物与 golden 一致 |
| M2 | per-node 编译 + 调度 Flight 生成 + manifest | 同上，双模式 |
| M3 | 部署器：create/update + CLI 接线 + mock E2E | mock server 断言调用形状；回滚路径过测 |
| M4 | 文档（部署/回滚/三条红字继承）+ tasks 收口 | 文档审阅过 |

## 6. 开放问题（实现期解决）

1. `core.transform.*` 里哪些节点有可静态改写的 SQL 表示——M1 第一天 grep 节点实现定白名单边界（宁窄勿宽）。
2. per-node 模式跨 Flight 传数据靠 `md:` 表——表命名冲突（两次部署同 DAG）用 `deploy_<dag_id8>_<run_key>` 前缀规避，M2 定稿。
3. Flight name 长度上限与合法字符集——mock 里按 MotherDuck 文档约束（≤200、`[a-zA-Z0-9_-]`），live 后校准。
