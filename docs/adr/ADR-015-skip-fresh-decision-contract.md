# ADR-015: skip-fresh 决策契约（Decision Contract）

**Status**: Accepted
**Date**: 2026-10-16
**Deciders**: SwanFlow maintainers

> 本文件是 skip-if-fresh（`--skip-fresh` / `SWANFLOW_SKIP_FRESH=1`）语义的
> **单一权威页**。代码注释引用本 ADR 的规则编号；代码与本文件冲突时，以本
> 文件的 fail-closed 原则为准并修代码。移植自 Understand-Anything
> (Egonex-AI) 增量正确性战役 PR #683 的两个资产：① 决策契约文档（skip 语义
> 的单一权威页）；② fail-closed 门禁（校验不过 → 重算，绝不静默复用）。

## Context

`try_skip_fresh`（`crates/swanflow/src/core/engine/assets.rs:844`）在
digest 链（v2 定界 + 规范化，commit `d774c25`；v3 rendered config，
commit `cfc92e1`）、interval env 边界（commit `0ef9d85`）与 ledger 锁
协调（`LedgerGuard` 双层：进程内 `LEDGER_MUTEX` + 跨进程
`<cache>/assets.lock` flock，`assets.rs:30` 起）之外，此前**不校验物化
产物的 schema**：digest 匹配 + parquet 文件存在即复用
（旧代码在 `assets.rs` 文件存在检查后直接 `Some(parquet_path)`）。引擎
升级 / 字符串家族漂移（Utf8 ↔ Utf8View）/ 产物被异族引擎重写等场景下，
会静默复用错列批次。本 ADR 把 skip 决策提升为 8 条显式规则，并把 schema
指纹纳入前置条件链（R6）。

全部代码位置以本批次落地后的行号为准（HEAD `56c0a18` + 批次 1/3 改动，
未提交）。

## 规则

### R1. skip 的完整前置条件链——任一失败，fail-safe 执行

`try_skip_fresh` 返回 `Some(parquet_path)` 当且仅当以下 **8 步全部通过**；
任何一步失败（含 I/O 错误）都落到 `None` = 正常执行节点（执行本身不受
skip 机制故障影响，"best-effort like the write side"，
`assets.rs:844` 文档注释）：

| # | 前置条件 | 代码位置 |
|---|---|---|
| 1 | `<cache_dir>/assets.duckdb` 存在（ledger 曾建立） | `assets.rs:854` |
| 2 | `LedgerGuard::acquire` 取得跨进程 flock 且 `exclusive()` 为真（P0：无协调的读不得支撑 skip 决策） | `assets.rs:859–864` |
| 3 | `open_ledger_connection` 成功——结构性损坏先备份（`.corrupt-*`）再重建，瞬态错误 → 执行 | `assets.rs:865–871`；恢复逻辑 `assets.rs:307`（签名识别）/ `327`（备份重建） |
| 4 | **上游 digest 集完整**：`upstream.len() == upstream_digests.len()`——部分链绝不对着陈旧记录匹配（codex-review P1） | `assets.rs:877` |
| 5 | 记录 digest == 重算 digest（v3 算法，`assets.rs:247` 版本标签） | `assets.rs:884–887` |
| 6 | `metadata.duckdb` 可开，且 `flow_metadata` 有 `node_id` 命中、`parquet_path != ''` 的行（表 DDL：`engine/mod.rs:296–302`） | `assets.rs:890–898` |
| 7 | parquet 文件在盘上存在 | `assets.rs:900–902` |
| 8 | **schema 指纹匹配**（本批次新增，R6）：记录侧 `schema_json` 非空且非 `'{}'`，且与 parquet **footer** schema 指纹字节相等（footer 经内嵌 `ARROW:schema` hint 还原 arrow schema，不读行组数据） | `assets.rs:903–925`；指纹助手 `schema_fingerprint` `assets.rs:803`、`parquet_footer_fingerprint` `assets.rs:818`；记录侧写入 `observer.rs:463–467` |

引擎侧调用点（仅 `--skip-fresh` 打开时进入）：`engine/mod.rs:1000`（门
`df_skip_fresh`，`engine/mod.rs:764`），`try_skip_fresh` 调用
`engine/mod.rs:1033`；CLI 开关 `main.rs:43`（字段）、`main.rs:430`
（`Commands::Run`）、`main.rs:445`（`with_skip_fresh`）。未开 flag 时
整段契约不生效，每节点照常执行。

### R2. unknown 的语义：unknown → 执行，且决策阶段零状态变更

guard 拿不到（flock 30s 超时 / 被他进程持有，`assets.rs:860`）、ledger
连接失败（`assets.rs:865–871` 的 `Err(_) => return None`）、
`flow_metadata` 表/行缺失、parquet 文件缺失（`assets.rs:900`）、footer
不可读（`parquet_footer_fingerprint` 返回 `None`，`assets.rs:818–831`）、
`schema_json` 为 NULL / `''` / `'{}'`（`assets.rs:917–920`）——全部归入
**unknown**：

- 动作 = **执行节点**（fail-closed），绝不 skip；
- skip 路径是**纯读**：不推进 ledger、不覆盖任何记录。状态只能由随后的
  执行 + 物化改变——`MaterializingStream` 在 parquet 写完后
  `INSERT OR REPLACE` 一行 `flow_metadata`（`observer.rs:449–467`）。
  "读不懂" 不得被解释为 "没变过"。

### R3. digest 版本失效语义：升版全量失效一次，绝不静默跨版 skip

- v2（commit `d774c25`）：长度前缀定界 + JSON 规范化，取代裸拼接
  （防 ("ab","c") 与 ("a","bc") 碰撞）——旧 v1 记录一次性全部失效；
- v3（commit `cfc92e1`）：新增 rendered-config 字段
  （`assets.rs:266`），`DIGEST_VERSION = 3`（`assets.rs:34`），版本标签
  `b"swanflow-digest-v3"` 直接进哈希（`assets.rs:247`）——v2 记录再次
  一次性全部失效。
- 语义固定为：**升版 = 全体受影响节点重算恰好一次**，之后恢复可 skip。
  任何代码路径不得让新旧版本 digest 互相"凑合匹配"。
- 本批次的 schema 指纹与之一致：无指纹的旧行 fail-closed **恰好一次**，
  重算后记录真实指纹（`observer.rs:463–467`），下次即恢复可 skip
  （一次性迁移成本，不是错误；测试锚点
  `try_skip_fresh_empty_schema_json_recomputes`，`assets.rs:1619`）。

### R4. interval 哈希边界（commit `0ef9d85`）

`SWANFLOW_INTERVAL_START` / `SWANFLOW_INTERVAL_END` 环境变量进入 digest
（`compute_digest` 内 `assets.rs:272–274`）：backfill 按天绑定
interval，raw config 跨天相同——没有这一项，skip-if-fresh 会把 day-1
的 parquet 复用到所有天。边界约定：**env 未设（普通运行）= 空串
update**，空串在长度前缀定界下是确定的字节，因此**旧 ledger 记录 bit 级
兼容**（普通运行不会因"引擎现在会读 env"而整体失效）；env 已设则不同
interval 必得不同 digest（day-1 ≠ day-2）。测试锚点：
`digest_changes_with_interval_env`（`assets.rs:1366`）。

### R5. rendered config 语义（commit `cfc92e1`）

digest 哈希**渲染后**的 config（模板解析后的 JSON），与 raw config 一样
先做 JSON 规范化（键序无关，`assets.rs` `canonicalize_json_str`）。
正确性前提是**双端同一 render**：记录侧（引擎执行后
`record_asset_run_for`，`assets.rs:686`；入口 `record_asset_run`
`assets.rs:724`）与 skip 侧（`engine/mod.rs:1006–1025` 的
`df_rendered_configs`，注释明示 render 是 workflow 定义的纯函数、
该路径永不需要 `""`）必须使用**同一渲染函数 + 同一上下文**
（compile-time 的 `workflow.context`），保证两侧字节一致；
`status_for_workflow`（`assets.rs:979`）对 `assets status` 走同一
render，保证 CLI 判定与引擎判定对称。调用方无法渲染时必须传 `""` 并
注释原因；**空渲染 ≠ 等价渲染**（测试锚点：
`digest_depends_on_rendered_config` `assets.rs:1176`、
`try_skip_fresh_rendered_context_mismatch` `assets.rs:1201`）。

### R6. schema 指纹不匹配 = 产物不可复用（本批次新增，fail-closed）

**swanflow 的对偶律：schema 漂移绝不证明可复用**（UA 原命题："空抽取
绝不证明删除"）。

- **记录侧**：物化落盘时把真实 arrow 输出 schema 序列化为
  `name:type,name:type,...`（**逻辑** `DataType` 的 Display，
  `schema_fingerprint`，`assets.rs:793–811`）写入
  `flow_metadata.schema_json`（`observer.rs:463–467`；列 DDL
  `engine/mod.rs:296–302`；既有 lineage 读取路径
  `engine/mod.rs:62–93` 原样受益）。
- **skip 侧**：只读 parquet **footer**（`parquet_footer_fingerprint`，
  `assets.rs:812–831`；经 ArrowWriter 恒写入的内嵌 `ARROW:schema`
  hint 还原 arrow schema，使 `Utf8View` 等视图家族在往返后不失真），
  与记录值比对（`assets.rs:903–925`）。
- **不匹配或记录为空/NULL/`'{}'` → `None` → 重算**。引擎升级、
  Utf8→Utf8View 家族漂移、异族引擎重写产物，全部落在此处被拦截。
- 家族区分是硬要求：指纹必须能区分 `Utf8` 与 `Utf8View`（逻辑类型
  Display 天然区分；测试锚点 `schema_fingerprint_distinguishes_utf8_from_utf8view`
  `assets.rs:1549`、`schema_fingerprint_roundtrip_via_parquet_footer`
  `assets.rs:1569`）——物理类型级指纹（"VARCHAR"）会让 Utf8View 产物
  冒充 Utf8，禁止。失配场景的 skip 行为锚点：
  `try_skip_fresh_schema_mismatch_recomputes` `assets.rs:1590`。

### R7. 唯一重试语义：重算（天然幂等），无定向重试

本设计**没有定向重试**。skip 被拒绝后的唯一"重试"原语是**整体重算**：
节点重算对钉死的上游 digest 是确定性的、幂等的，且物化完成时原子地
同时重写 parquet 与 `flow_metadata` 行——parquet `close()`
（`observer.rs:449`）成功后才 `INSERT OR REPLACE`
（`observer.rs:465–467`），不存在"半更新"的中间态。

与 UA 的差异：UA 的 symbol-loss 校验失败后对**单个符号/表**做一次性
定向重试（部分抽取可局部重做）。swanflow 不做定向重试的理由：节点输出
是**整表**物化，不存在"部分可复用"的粒度；引入定向重试需要额外的
"哪个产物被拒、为何被拒"状态机，而整体重算已经幂等且成本可控。若未来
出现按表/按列的部分失效需求，应重开 ADR 引入定向重试。

### R8. 决策矩阵

输入状态按 `AssetVerdict`（`assets.rs:949–977`，`assets status` 与执行
路径共用同一 digest 判定）× 物化证据分类；动作列是 `try_skip_fresh`
的行为：

| 输入状态 | 判定方式（代码位置） | 物化证据 | 动作 |
|---|---|---|---|
| **fresh** | 有记录行（`recorded_digest`，`assets.rs:471`）且 digest == 重算（R1#5） | parquet 在盘（R1#7）且 schema 指纹匹配（R1#8） | **skip**：复用 parquet 登记表视图（`engine/mod.rs:1042–1052`），发 `Materialized`/`Executed` 事件，并把重算 digest 写入 `df_digest_map` 供下游链续接（`engine/mod.rs:1069–1080`） |
| **stale-config** | digest 不等且上游全 fresh（`status_for_workflow` 的近似判定，`assets.rs:1025–1046`） | 不参与决策 | **执行**（重算 + 重记账 `observer.rs:449–467`） |
| **stale-upstream** | digest 不等且存在非 fresh 上游（`assets.rs:1025–1046`） | 不参与决策 | **执行** |
| **never-run** | 无 ledger 行（`recorded_digest` = None，`assets.rs:471–480`） | 不参与决策 | **执行** |
| **unknown** | R2 列举：guard 未协调 / 连接失败 / 表行缺失 / 文件缺失 / footer 不可读 / 空指纹 | 不可证明 | **执行**（fail-closed；决策阶段零状态变更） |

其中 **schema 指纹失配** 与 **空指纹旧行** 是 unknown 的两个子情形
（R6/R3）：都执行；空指纹情形在执行后自动完成迁移（新行带真实指纹），
下一轮即可 skip。

## Consequences

1. 引擎升级 / 家族漂移下的错列批次复用被结构性堵死；代价是无指纹旧行
   各 fail-closed 一次（与 digest 升版同形，R3）。
2. skip 路径保持纯读 + 一次 footer 解析（不读行组数据），边际成本可忽略。
3. 本 ADR 是契约页：后续任何改动 skip 语义的 PR 必须同时更新本文件与
   `try_skip_fresh` 的文档注释（`assets.rs:834–843`），二者不得漂移。

## 参考

- Understand-Anything (Egonex-AI) PR #683：symbol-loss-validation
  8 条规则页 + fail-closed 门禁（本 ADR 的对岸原型）。
- ADR-014（asset semantics 分阶段采用）：本机制所属的 Stage 1 ledger
  语义。
- 本批次（1/3）改动：`crates/swanflow/src/core/engine/assets.rs`
  （`schema_fingerprint` / `parquet_footer_fingerprint` / `try_skip_fresh`
  第 8 步 / 6 个单测）、`crates/swanflow/src/core/observer.rs`
  （记录侧写真实指纹）、本文件。
