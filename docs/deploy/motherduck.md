# Deploy to MotherDuck Flights（`swanflow deploy`）

把 swanflow DAG **编译并部署**成 MotherDuck Flights——部署后按 cron 自跑，
不依赖本地进程。swanflow 在这条链路里是**编译器/部署器**，不是运行时。

```bash
swanflow deploy workflow.yaml                    # inline：整图 → 1 个 Flight
swanflow deploy workflow.yaml --mode per-node    # 每节点 1 Flight + 1 调度 Flight
swanflow deploy workflow.yaml --rollback         # 回滚到上一版部署
```

## ⚠️ 三条红字（继承自 Flight 节点，编译进每个产物头部注释）

1. **runs as owner** —— Flight 用其 owner 的 token 与密钥执行；部署前对
   每个 Flight 配置 `GRANT RUN`。
2. **at-least-once** —— 重跑可能重复副作用。per-node 调度 Flight 的状态表
   只跳过已 SUCCEEDED 的节点（缓解，非保证）；Flight 侧须按 `RUN_KEY`
   幂等。
3. **version-locks at run time** —— run 锁定触发时的 current 版本。部署
   清单记录 source sha256，跨版本对账以清单为准。

## 编译白名单（宁拒勿骗）

可编译：`csv_read` / `parquet_read` / `http_fetch`（query 形态）/ `sql` /
带静态 SQL 的 `transform.*` / `duckdb_write`（目标重定向为 `md:`）/
`connector.motherduck.flight`（嵌套触发）。

白名单外（LLM / script / luajit / 运行时计算的 transform）→ **编译期硬失败**，
错误聚合列出全部不可编译节点。绝不静默跳过。

## 产物

| 位置 | 内容 |
|---|---|
| MotherDuck 侧 | N 个 Flight（inline=1；per-node=节点数+1 调度器） |
| `./flights/*.py` | 源码副本——纯文本，**入 git**，可 diff |
| `./flights/swanflow-deploy-<dag>.json` | 部署清单（flight_id/version/sha256/回滚指针），**入 git** |
| `./flights/history-*.json` | 回滚时归档的旧清单 |

回滚 = 读清单 → 校验 `flights/` 副本 sha256 与清单一致 → `update_flight`
恢复上一版。**首次部署（rollback=null）拒绝回滚**；本地副本缺失或哈希漂移
也拒绝（源码必须 git 跟踪）。

## 调度说明

我们**只部署、不托管 cron**：Flight 的 schedule 在 MotherDuck 控制台自行
配置——避免替你决定计费节奏。调度 Flight 的轮询固定 5s（Pulse 按查询计费）。

## token

`MOTHERDUCK_TOKEN` 环境变量（或 `--token`）。清单里的 `owner_hint` 只记
token 哈希前 8 位，绝不落 token 本身。

## 诚实边界

协议层（create/update_flight 调用形状、清单链、回滚路径）已由本地 mock
server E2E 全覆盖（`tests/motherduck_flight_deploy.rs`）。**未对真实
api.motherduck.com 实测**（需 token，按指令放弃）——首次 live 使用时先跑
一次 `swanflow deploy` 小 DAG 验证握手。
