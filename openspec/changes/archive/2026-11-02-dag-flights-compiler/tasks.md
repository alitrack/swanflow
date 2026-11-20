# Tasks: DAG → Flights Compiler

## M1: 编译器核心（inline）
- [x] 1.1 `src/deploy/mod.rs`：FlightProgram/DeployManifest 类型 + 稳定 topo 排序（孤立节点 id 序排末尾）
- [x] 1.2 `src/deploy/motherduck.rs`：白名单降级表 + 编译期模板解析（input_N/node_x/var，缺失即失败）
- [x] 1.3 inline 生成器：拓扑序 + 脚手架（头注释三条红字 + SWANFLOW_DEPLOY_OK 尾标记）
- [x] 1.4 golden 单测：8 条编译器测试 + examples/ 真实 YAML 对拍

## M2: per-node 编译
- [x] 2.1 逐节点 Flight + 调度 Flight（md: 状态表 _skip/_mark + 拓扑扇出 + 5s 轮询 + 失败即停 + SUCCEEDED 跳过）
- [x] 2.2 manifest 双模式断言单测

## M3: 部署器 + CLI
- [x] 3.1 create_flight/update_flight 写侧客户端（复用 mcp/http.rs；owner_hint 只记 token 哈希前 8 位）
- [x] 3.2 `swanflow deploy` 子命令（--mode/--endpoint/--token/--flights-dir/--manifest-dir/--rollback）+ flights/ 本地副本 + 清单链 + history 归档
- [x] 3.3 mock MCP server E2E（4 测试）：首次部署（create 形状+Bearer 断言+rollback=null+本地副本）/ 二次部署链式回滚锚 / 回滚（update_flight 携 v1 源码+sha256 校验）/ 首次部署拒绝回滚（零 MCP 调用）/ 真实 example per-node 全链路（N 次 create+调度器在场）

## M4: 文档
- [x] 4.1 `docs/deploy/motherduck.md`：部署/回滚流程 + 三条红字继承 + 调度自配说明 + 诚实边界（mock 全覆盖、live 未测）

## 验证记录（2026-11-02，M1–M4 全部完成）

- `cargo test -p swanflow --lib deploy::` → **11 passed / 0 failed**
- `cargo test -p swanflow --test dag_flight_compile_examples` → **2 passed / 0 failed**
- `cargo test -p swanflow --test motherduck_flight_deploy` → **4 passed / 0 failed**
- 合计 **17/17 全绿**；全部验证基于本地 mock，零 token（符合用户指令）。

## 实现备注

- `Workflow.graph` 是 petgraph StableGraph——topo 用 Kahn 稳定版（ready 集 id 最小优先），确定性 byte-for-byte。
- 回滚安全：清单 sha256 与本地副本不符即拒绝；`history-` 前缀归档旧清单。
- `duckdb_write` 编译为 `md:` 目标；transform 无静态 SQL 视同白名单外。
