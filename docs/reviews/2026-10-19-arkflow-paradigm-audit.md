# SwanFlow 就绪度深审（CODE_REVIEW）— ArkFlow 范式

- **对象**: `/tmp/swanflow-gov`（swanflow，origin/main 快照 `fab4a07`，5 crates ≈ 87K LOC）
- **审查日期**: 2026-09-30
- **方法**: 纯静态审查（grep/read 统计 + 逐文件精读关键路径）；未构建、未运行测试、未联网
- **审查范围**: `crates/swanflow`（49.7K 行，主 crate）、`crates/swanflow-core`、`swanflow-ai`、`swanflow-knime`、`swanflow-plugin-sdk`、顶层 `tests/`（42 个集成测试文件）。顶层 `src/` 树（66 文件）经核实**不是 workspace 成员**（根 Cargo.toml 无 `[package]`，无任何 Cargo.toml 指向它），为死代码，未计入质量评估（见 P3-1）。

## 1. 审查范围与方法

按优先级精读了以下路径，其余按风险面抽样：

| 区域 | 文件 | 关注点 |
|---|---|---|
| 编译器 | `crates/swanflow/src/core/compiler.rs`（456 行） | preflight/migration/placeholder 诊断 |
| late-binding SQL | `core/engine/planner.rs`、`nodes/sql.rs` | 参数内联、共享 SessionContext 竞态 |
| 执行器 | `core/engine/mod.rs`（2619 行） | 三套执行路径的错误传播、checkpoint、fan-out |
| 数据源 | `core/datasource/mod.rs`、`excel.rs` | DuckDB 读写桥、Excel 写入 |
| 连接器 | `nodes/io/nodes.rs`、`core/connection_config.rs` | DBX 兼容、凭据处理 |
| KNIME 导入 | `crates/swanflow-knime/src/{parser,mapper}.rs`（mapper 3811 行） | 未受信 .knm 解析、映射正确性 |
| AI 层 | `crates/swanflow-ai/*`、`crates/swanflow/src/agent/mod.rs` | agent 循环、ModelRouter |
| 错误契约 | 全仓 `unwrap/expect/panic` 统计（非测试：swanflow 192 处、core 57 处 [几乎全在 mock]、ai/knime/sdk ≈ 0-1 处）；`let _ =`/`.ok()` 吞错统计（engine/mod.rs 53、excel.rs 23、datasource/mod.rs 23、daemon.rs 13） |

## 2. 质量分层排序

**一段话**：内核类型层（swanflow-core 的 graph/repair/validation）与 staged DataFusion 执行器骨架是全仓最强资产——preflight 迁移链、可恢复性分级、观测探针（Materializer/StateProbe）、write-sink 的 per-db 锁纪律（codex-review r3 痕迹清晰）都显示出"最近几轮对抗性审查"的沉淀；DuckSqlExec 的 FAIL-LOUD schema guard、CSV/Parquet 的 `validate_local_file` fail-fast 也都在正确一侧。**但质量断层恰好落在用户真实接触面上**：KNIME 导入映射层是全仓最弱层（把 CSV Writer 映射成 CSV Reader、生成 `UPDATE ... SET ...` 这种必然非法的 SQL、XML 解析出错静默截断），连接器/DBX 配置层次之（Mongo 凭据静默丢弃、`read_only` 只打印不生效、URL 不做 percent-encoding）。与 ArkFlow 的教训一致：**插件/导入层是质量断层**。更关键的是，主执行路径（staged executor）的"失败→运行状态"契约是断的（P1-2/P1-3），使得上游所有正确性缺陷都会被"运行成功"的表象掩盖。

| 层（强→弱） | 代表模块 | 判断依据 |
|---|---|---|
| ① 内核类型/契约层 | swanflow-core（graph、repair、validation、error 分类） | 非测试 panic 族仅 57 处且集中在 mock；类型化 RepairOperator 有边界常量（MAX_REPAIR_NODES=3） |
| ② Staged 执行器 + 编译器 | engine/mod.rs 主路径、compiler.rs、observer.rs | 结构完整（levels/checkpoint/probes/skip-fresh），但错误传播契约破损（P1-2/3） |
| ③ 持久化桥 | DuckDBWriteSinkExec（写）、DuckSqlExec（读） | write-sink 有锁/事务/重试纪律；DuckSql 有 fail-loud schema guard |
| ④ AI 层 | swanflow-ai（router/hook/prompt）、agent/mod.rs | 体量小、无生产 unwrap；agent 有路径消毒 + canonicalize 双保险 |
| ⑤ 连接器/配置层 | connection_config.rs（DBX）、DbConnectorNode、HttpFetch | 凭据处理多处缺陷（P2-2/3/4/5），fail-open 加载 |
| ⑥ Excel I/O | datasource/excel.rs（1771 行） | 单元格写失败 warn-only 静默丢（P2-14），downcast unwrap 密集 |
| ⑦ KNIME 导入层（最弱） | swanflow-knime mapper/parser | 语义级映射错误（P1-7）+ 解析 fail-open（P2-6）+ 验收门槛仅 >50% 转换率 |

修复资源应按 ②（错误契约）→ ③（late-binding SQL，P1-1）→ ⑦（导入层）→ ⑤（连接器）顺序投放。

## 3. P1 清单（数据正确性/可用性/安全硬阻塞）

### P1-1 late-binding SQL 参数内联：多位占位符被毁 + 非 ASCII 必崩 + 双重替换
- **位置**: `crates/swanflow/src/nodes/sql.rs:155-161`（第一遍升序替换）；`core/engine/planner.rs:394, 631-675`（第二遍 `inline_placeholders`/`replace_placeholder_desc`）
- **触发条件**: 任何使用 `core.sql` 节点 + `params` 的工作流。
  (a) `propose_plan` 用升序 `query.replace("$1", v1)` 做第一遍替换：`$1` 会先命中 `$10` 的前缀，`... b = $10` 变成 `b = <v1>0` —— 第 2..10 个参数被静默改写为错误字面量（**静默错数据**）。
  (b) 第二遍 `replace_placeholder_desc` 按字节推进（`i += 1` 后对 `haystack[i..]` 切片）：替换后的 query 中只要存在**任意非 ASCII 字符**（中文注释、emoji、非 ASCII 过滤值——参数值已被第一遍内联进 query），下一个字节边界切片即 panic；release profile `panic="abort"`（根 Cargo.toml）→ 整个进程（含 daemon）中止。**对所有非 ASCII SQL 是必崩路径**。
  (c) 双重替换叠加：第一遍的链式 `String::replace` 会重新扫描已替换进去的参数值，值中含 `$N` 文本（如 `"$100"`）会被再次替换。planner 单测只覆盖了 ASCII 三个 case。
- **影响**: 静默错数据（a）、进程必崩（b）、参数值损坏（c）。
- **修复**: 删除 `propose_plan` 里的第一遍替换（proxy 保留原始 query + params），只保留 planner 的降序内联；`replace_placeholder_desc` 改为按 `char_indices` 遍历（或先 `is_ascii()` 判断、非 ASCII 走 DataFusion 原生参数绑定）；补测试：`$1`/`$10`/`$11` 混用、非 ASCII query/参数值、值含 `$N`。

### P1-2 Staged 主执行器从不因节点失败而中止运行（违反 ADR-013 的 Fail 语义）
- **位置**: `crates/swanflow/src/core/engine/mod.rs:1222-1258`（执行错误分支）、`1358-1388`（规划错误二次分支）、`1398, 1416`（checkpoint `completed_levels: (0..=level)` 与 `completed_at: Some(...)`）；daemon 侧 `core/engine/daemon.rs:844-872`
- **触发条件**: 主路径 `execute_workflow`（CLI `run`、daemon 交互/自动化均走它，main.rs:467/665、daemon.rs:844/1639）中任一节点在执行或规划阶段失败——无论其 `error_mode` 是默认 `Fail` 还是 `Retry(n)`（staged 路径根本没有重试循环，`Retry` 分支只打印 "retry exhausted"，见 mod.rs:1241-1247）。
- **影响**: 错误分支只发 `NodeEvent::Error` 然后**继续执行下一个 stage/level**；任务结束后照常写"completed" checkpoint（`completed_at: Some`），失败 level 被记为 completed → 断点续跑会**跳过失败的 level**。下游节点随后读失败节点没有新产出的表：若有上一轮缓存则**静默读到旧数据**，否则 table-not-found 级联。CLI 表面用 exit(1) 掩盖了问题（main.rs:505-513），但 daemon 自动化把带错误的运行仅记 `tracing::warn!`（daemon.rs:859），"completed" 对调度器/下游消费者成立 —— 定时 ETL 静默"完成"。
- **修复**: 维护 per-run 硬错误标志；Fail 策略失败即 `break` 出 level 循环、不发 completed checkpoint、发终端 run-failed 事件；`Retry(n)` 落实真实重试（复用 `core/retry.rs`）；daemon 自动化把 errs>0 视为失败并告警/重排。

### P1-3 物化（materialization）失败被吞 + 重跑时表重注册无 deregister → 静默复用上一轮数据
- **位置**: `crates/swanflow/src/core/observer.rs:443-467`（`File::create` 失败整体跳过、`ArrowWriter/write/close` 三连 `unwrap()`、metadata 写入 `.ok()`）；`core/engine/mod.rs:976`（`let _ = ctx_shared.register_table(...)` 无 deregister）对比 `mod.rs:1045` 附近 skip-fresh 分支有 `deregister_table`、`planner.rs:388-392` 的 alias 路径有 deregister+retry —— **同文件两处，一处新一处旧**；`mod.rs:171-197`（`sync_metadata_to_context` 的 `register_parquet(...).ok()` 静默跳过坏/缺文件）
- **触发条件**: 重跑（同一 cache_dir）任一 workflow + 本轮物化写入失败（磁盘满/权限/IO 错误）：level 开始时 `sync_metadata_to_context` 已把上一轮的 parquet 视图注册为 `node_<id>` 表 → 本轮 `register_table` 因 "already exists" 失败被 `let _ =` 吞掉 → 表仍指向上一轮文件，节点照常发 `Executed`+`Materialized` 事件；且 observer 的 `unwrap()` 在 IO 错误下 panic → panic=abort 直接杀掉长驻 daemon。
- **影响**: 下游节点静默消费上一轮数据而 UI 显示全绿；IO 错误直接进程死亡。
- **修复**: 物化失败必须升级为 `NodeEvent::Error`（或流内错误）；主分支注册前 `deregister_table`（对齐 skip-fresh 分支）；observer 内 4 个 `unwrap()` 改为向流推送 `Err`；`sync_metadata_to_context` 对注册失败至少 emit 诊断事件。

### P1-4 DuckDB 引擎 `compile_to_sql` 把所有单输入自定义节点当透明 no-op → 静默空转
- **位置**: `crates/swanflow/src/core/engine/duckdb_backend.rs:464-475`（`LP::Extension` 单输入 → 直接编译其子计划，注释自称 "extension nodes are no-ops"）
- **触发条件**: 任何含自定义节点（`core.sql.duckdb`、excel sink、`DuckDBWriteSink`、`DuckDBScanLogical`/http_fetch 等）的工作流走 DuckDB 后端——`swanflow knime run --engine duckdb`（main.rs:1256-1268 设置 `with_engine` 后调用的 `execute_workflow` 虽忽略 mode，但 `execute_workflow_duckdb` 一旦接回即触发）、legacy yaml 路径里 `engine_mode: Auto` 的子引擎（mod.rs:1551，Auto 对纯 SQL/IO 管道默认落 DuckDB）。
- **影响**: 用户 SQL/写库/抓数被**静默跳过**：整条流水线退化为上游原样数据，每个节点报 Executed/Materialized，sink 从未写盘。典型"静默丢数据"。
- **修复**: `compile_to_sql` 对无法表示的自定义节点 fail-closed（返回 `NotImplemented` 并让路由层切回 DataFusion 后端）；同时修复 `resolve_mode` 的判定（`core.sql.duckdb`、excel、write-sink 都应强制 DataFusion 或实现对应 SQL 发射）。另：`execute_workflow_duckdb`（mod.rs:417，400+ 行）**零调用方**，是携带旧"always-continue"语义的死执行器，应删除或接线测试。

### P1-5 未受信工作流文件 → 经 DuckDB 扩展通道任意代码执行
- **位置**: `crates/swanflow/src/nodes/sql_duckdb.rs:303-320`（`load_extensions`：非 `.duckdb_extension` 条目直接 `INSTALL {ext}`，路径条目 `LOAD '<任意本地路径>'`；连接用 `allow_unsigned_extensions()`）；`core/datasource/mod.rs:481-490`（DuckDBScanExec 同样 `INSTALL {ext}` + `LOAD`）；对照 `nodes/sql_duckdb.rs:546-582` 的 `validate()` 只封禁 **query 字符串**里的 `install/load/attach`，对 `extensions` 配置项无任何限制
- **触发条件**: 执行/导入一个节点配置里带 `extensions: ["<名字>"]` 的工作流文件（恶意 KNIME 导入、团队共享模板、`agent analyze` 生成的 YAML 都属此输入域）；`INSTALL` 会**联网下载并执行** DuckDB 扩展仓库的预编译二进制，路径条目则加载本地任意 unsigned 二进制。
- **影响**: 引擎进程（daemon/MCP server/Python 桥）RCE/本地二进制执行；validate 的"封禁"被 `extensions` 配置一行绕过。
- **修复**: 扩展名白名单（可配置，默认仅官方 core 集合）；默认禁网（`INSTALL` 需显式 capability/flag 打开）；`.duckdb_extension` 路径加载需显式允许；把"工作流文件=未受信输入"写进 SECURITY.md 威胁模型并加 E2E 测试。

### P1-6（存疑）共享 SessionContext 的 `input_0` 别名竞态 → 跨节点串数据
- **位置**: `crates/swanflow/src/core/engine/planner.rs:377-405`（SqlProxy 把所有输入注册进**共享** `self.ctx` 的 `input_{i}`）
- **触发条件**: 多个 `core.sql` 节点在同级/并发规划（fan-out 场景）。代码注释自认："Concurrent fan_out children share this context and may race on the same alias"（planner.rs:381-386）；deregister+retry 只处理**注册冲突**，不处理**别名被覆盖**：若节点 B 先重注册 `input_0`，节点 A 的 `ctx.sql()` 名称解析会绑到 B 的表提供者。同文件的 pivot 节点已因同样问题改走独立 ctx（planner.rs 中 pivot 注释："an earlier node in the same stage had re-registered input_0 ... was flaky"）——证明该竞态真实存在，但 SqlProxy 路径未跟进。
- **影响**: 间歇性跨节点数据串读（静默错数据）；是否表现为错误取决于调度交织，故标**存疑**（机制确凿，危害待复现）。
- **修复**: 每个 SqlProxy 在独立临时 `SessionContext` 中注册 `input_i`（照抄 pivot 的成熟做法），不碰共享 ctx。

### P1-7 KNIME 导入层语义级映射错误（用户接触面最弱层）
- **位置**: `crates/swanflow-knime/src/mapper.rs:92`（`CSVWriterNodeFactory` → **`core.io.csv_read`**）；`mapper.rs:415-417`（`DBUpdateNodeFactory` → `UPDATE "{table}" SET ... FROM input_0`，字面 `SET ...` 必然非法 SQL）；`mapper.rs:400-404`（DateTimeRange 缺配置时硬编码 `2020-01-01/2020-12-31` 静默生效）；`mapper.rs:405-417, 480-500`（RowFilter/HTTP 请求把 KNIME 配置值**不转义**拼进 SQL）
- **触发条件**: `swanflow knime import/run` 导入含 CSV Writer / Update / Date Range / 含引号配置值的 KNIME 工作流。
- **影响**: 导入"成功"但语义反转/必坏：写节点变成读节点（**输出静默丢失**）、Update 节点运行必失败、日期范围静默错值、SQL 注入进生成的工作流。验收门槛只是 "≥50% 节点被转换"（`crates/swanflow-knime/tests/e2e_all.rs:69`），上述缺陷全部漏网。
- **修复**: 逐映射正确性断言（writer 必须映射到写节点）；SQL 拼接统一走转义/参数化；缺失配置 fail-closed（映射进 `core.missing.placeholder` 并在报告中列明）；把 e2e 门槛从"转换率"升级为"转换后可编译 + 关键 sink 语义断言"。

## 4. P2 清单（应修缺陷，从简）

1. **DbConnector 凭据/SQL 拼接**: `nodes/io/nodes.rs:991-1000` `ATTACH '{url}'` 不转义单引号（密码含 `'` → 断 SQL/注入本地 DuckDB，`attach_sql` 无 banned-statement 检查）；`SELECT * FROM {table}` 表名裸拼；`nodes/io/nodes.rs:1115` 错误信息回显**展开后的 URL**（含 `${ENV}` 展开出的密码）→ 凭据泄进日志/事件。修复：percent-encode userinfo、`''` 转义、错误消息脱敏。
2. **`read_only` 契约谎言**: `connection_config.rs:74` 声明 "prevents writes"，全仓仅 `main.rs:1593` 打印它，任何驱动都不执行 → 移除或落实。
3. **MongoDB 凭据静默丢弃**: `connection_config.rs:117-122` URL 模板不含 user/password → 带认证的 DBX Mongo profile 导入后匿名连接。
4. **URL 无 percent-encoding**: `connection_config.rs:96-145` 密码含 `@`/`/` 时 URL 被驱动误解析（`p@ss` → host=`ss`）。
5. **连接配置 fail-open**: `connection_config.rs:205-213` `load_dir` 对坏 YAML 只 eprintln 跳过 → 用户以为连接已加载。
6. **KNIME 解析 fail-open**: `swanflow-knime/src/parser.rs:371` `Err(_) => break`（XML 出错静默截断，拿到半份配置继续"成功"导入）；`parser.rs:95` settings 文件读不到时静默置空配置。
7. **fan-out 父节点结果被吞**: `engine/mod.rs:1845` `let _ = compile_and_execute_node(...)` —— 父节点按自身 error_mode 应失败时也被无条件降级为"继续"，子节点对着缺失/陈旧的父输出执行。
8. **Conditional 双实现语义分叉**: legacy 路径只取上游 **LIMIT 1** 行做子串判断（`engine/mod.rs:1718,1742`），staged 路径全量扫描（mod.rs:880 附近）→ 分支结果依赖走哪条执行路径。
9. **Loop `until` 双实现分叉**: `execute_yaml_workflow` 用 Materialized 事件拼 "done" 串匹配（mod.rs:1575），`execute_node_with_mode` 用文本输出 `contains(until)`（mod.rs:1795 附近）→ 同一 YAML 两种行为。
10. **Self-healing 假修复**: `engine/mod.rs:1211-1233` 注释宣称 "apply and re-run"，实际既不调 `apply_repair` 也不重跑，hook 返回 `Some(repair)` 时反而**吞掉错误事件**（`handled=true`）→ 把响亮失败变成静默继续。
11. **模板渲染非确定**: `core/templating.rs:40-70` 按 `HashMap` 迭代序逐 key `replace`，上下文值本身含占位符时结果随 key 序变化 → 配置渲染/ADR-014 摘要链（skip-if-fresh 依赖渲染字节稳定）非确定。应单遍、顺序无关地解析。
12. **HttpFetch SSRF 面**: `nodes/io/nodes.rs:335-390` 无 scheme/内网 host 限制，daemon 进程代抓 `http://169.254.169.254/...` 之类。至少加可配置的 host 黑名单。
13. **Excel 导出单元格静默丢**: `datasource/excel.rs:861-863, 1495-1551` 单元格写失败 `warn!`/`.ok()` 后继续，工作簿照常保存、节点报成功。
14. **`--engine duckdb` 空操作**: `main.rs:1256-1268` 设置了 `with_engine` 却调用忽略 `engine_mode` 的 `execute_workflow` → 用户以为换了引擎。要么接 `execute_workflow_duckdb`，要么删掉该选项。
15. **`Retry(n)` 在 staged 路径不存在**: `engine/mod.rs:1241-1247` 直接打 "retry exhausted" 发事件，无任何重试（违反 ADR-013 第 4 条）。

## 5. P3 结构债

1. **顶层 `src/` 死树**（66 文件，含 pyo3 `lib.rs` 与 `core/` 分叉拷贝：compiler 383 行 vs 456 行、adapter/protocol 146 vs 212 行）：不在 workspace 内、不参与构建，但对人和 AI 审查者都是强误导。删除或并入 crate。
2. **`execute_workflow_duckdb` 零调用方**（mod.rs:417，400+ 行）：与 staged 路径大面积重复（level 循环/checkpoint/事件），语义却是旧版 always-continue。删除或接线。
3. **三执行器并存、纪律分叉**：staged / legacy yaml（`execute_node_with_mode`+`compile_and_execute_node`）/ dead duckdb。legacy 路径还有：事件通道被换成断头 channel（mod.rs:2177 `event_tx: mpsc::unbounded_channel().0` → 该路径所有物理层事件静默丢失）、只执行 `compiled_plans[0]`（mod.rs:2175，多端口节点的 error_output 等端口在此路径被丢弃）。应把 legacy 路径收敛为 staged 的特例或移除 `--legacy-yaml`。
4. **panic 族密度**：swanflow 主 crate 非测试 192 处（hot：kernel/mock 57 [可接受]、mcp/server 20、excel 17、sql_collapse_probe 10）；叠加 `panic="abort"` → 长驻进程中任何 IO/解析边界错误都是进程级故障。建议给 datasource/observer 关键路径做 unwrap 清零专项。
5. **吞错文化**：engine+datasource 关键文件 ~114 处 `let _ =`/`.ok()`（P1-3、P2-3/5 都由此滋生）。建立"事件通道可达处禁止裸吞"的 lint/审查规则。
6. **`println!/eprintln!` 生产噪音**：staged 循环每节点 `eprintln!("DEBUG: Executing Stage Level ...")`、materializer 每节点 DEBUG eprintln —— daemon 长期运行下是日志卫生问题，应换 tracing 分级。
7. **Workflow 反序列化不校验重复 node id**：`core/graph.rs` 的 `add_node`/serde 路径均不查重；未受信 YAML 里两个节点同 UUID 时，编译器 `compiled_outputs: HashMap<Uuid,_>` 与 `node_table_name(id)` 表名碰撞 → 后者静默覆盖前者的输出表（串数据）。解析期应 fail-closed。
8. **KNIME mapper 3811 行生成式映射表无单测护栏**（见 P1-7 的验收门槛问题）。

## 6. 测试面缺口（对照 ADR-013 / ADR-014）

- **ADR-013 弹性扇出：集成层零覆盖**。`tests/` 42 个文件中 grep 不到任何 `fan_out`/`FanOut`；crate 内仅对 `classify_child_outcome`/`aggregate_child_outcome` 做孤立单测（mod.rs:2431-2530），不覆盖：父节点失败被 `let _ =` 吞（mod.rs:1845）、`Retry(n)` 子进程重试循环、join-error 必硬失败、以及 staged 路径**根本没有** fan-out 编排（该策略只活在 legacy 路径）这一事实本身。
- **ADR-014（资产语义 + Stage 4 webhook 触发）**：webhook 队列/路径解析有 crate 内单测（webhook_server.rs:180-230），但**没有** "webhook 到达 → automation sensor 触发 → 执行 → consume-after-claim" 的 E2E；skip-if-fresh 的 record/skip 字节对称性（mod.rs:790-830 大段注释承诺的契约）也看不到对应的"重跑必须命中/必须不命中"集成断言。
- **late-binding SQL 参数**：只有 3 个 ASCII 单测（planner.rs:679-720）。缺：`$1/$10/$11` 升序替换破坏、非 ASCII 崩溃（P1-1a/b）、值含 `$N`、propose_plan 与 planner 双重替换的交互。
- **失败语义**：没有"默认 Fail 策略下节点失败必须使运行失败"的回归测试（P1-2 因此长期存活）；materializer 写失败、重跑表注册冲突（P1-3）零覆盖；`execute_workflow_duckdb`/`compile_to_sql` 全死无测试（P1-4）。
- **KNIME 导入**：门槛 = 转换率 >50%（e2e_all.rs:69），无语义映射断言（P1-7）；`swanflow-knime` 无坏 XML/缺 settings 的 fail-closed 测试（P2-6）。
- **连接器层**：DBX profile 坏文件静默跳过、Mongo 凭据丢弃、`read_only` 不生效（P2-2/3/4/5）均无测试。

## 7. 总体判词

SwanFlow 的内核骨架（swanflow-core 契约层 + staged 执行器 + 经过多轮对抗审查的写桥/读桥）已达到"可以承载生产 ETL"的结构水平，近半年的 codex-review/worker 加固痕迹清晰可辨；但它目前**不能**作为可信的执行底座——主执行路径把"失败"降级为"带警告的成功"（P1-2/3），late-binding SQL 参数通道能静默改写查询（P1-1）并在非 ASCII 输入上必崩（P1-1b），DuckDB 后端会把自定义节点整段静默跳过（P1-4），而用户真正接触的 KNIME/DBX 连接器层恰是全仓质量断层（P1-7/P2-2~6），未受信工作流还留有经扩展通道 RCE 的安全面（P1-5）。按"执行器错误契约 → SQL 参数通道 → 导入/连接器层 → 安全白名单"的顺序投入 2-3 个迭代，并补齐 ADR-013/014 的集成测试护栏后，方可从"演示级数据引擎"晋级为"运营级引擎"。

---
*审查者: ArkFlow 治理 Lane C（只读静态审查，未执行任何构建/测试命令）*
