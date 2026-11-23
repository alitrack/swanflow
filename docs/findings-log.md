# Findings Log — 缺陷逃逸分层台账

> 每个逃逸到 E2E/CI/线上的缺陷，按「哪层抓到 / 本该哪层抓到」记账。
> 层从便宜到贵：`unit · offline · live/e2e · gate(CI) · field`。
> 规则：引擎 bug 修复的 commit 必须同步在本文件加一条；每 10 条对
> "should have been" 做 tally，最高的层 = 下一个要加固的测试层。
>
> 格式与方法来源：skill `evidence-matrix-engineering`（移植自 rivet-lab/rivet
> 的 `docs/findings-log.md`，见 claw wiki `raw/articles/research/rivet-lab-rivet-db-to-parquet-cdc-20261123.md`）。

---

### FL-1: 对字符串列发 AVG 导致规划失败
- wf157 / profiler 节点 E2E（战役 Bug #13）
- caught by: live/e2e —— wf157 首跑 RUN_EXIT=1，DataFusion 无 Utf8→Float64 隐式强转
- should have been: unit —— 节点 `validate()`/`propose_plan()` 契约测试本该按列类型提前拒绝
- guard now: 数值聚合节点按列类型过滤 + 回归单测（290 基线内）

### FL-2: 新逻辑节点注册后 extension planner 无 downcast 分支
- wf163 / `core.ai.mcp_tool`（McpToolLogical）
- caught by: live/e2e —— 运行时 `No physical plan found for user defined logical node`
- should have been: gate —— 「注册节点 × planner lowering」应是派生矩阵：注册了
  Extension logical 而 `core/engine/planner.rs` 无对应分支，CI 就该红，而不是等 E2E
- guard now: 修法 = 镜仿相邻分支一行 lowering；矩阵规则待挂 CI（见 skill
  `evidence-matrix-engineering` 规则 1）

### FL-3: 0 行批次静默不物化
- wf161（设计稿预判错）/ wf168（空流首跑踩坑）
- caught by: live/e2e —— RUN_EXIT=0 但零产物 parquet，`MaterializingStream` 的
  `batches.is_empty()` guard 直接跳过写文件
- should have been: unit —— 物化语义（0 行写不写文件）应有契约测试钉死，而不是
  靠设计稿猜
- guard now: 断言走下游 `COUNT(*)` 间接验证；设计稿以第一次真跑校准

### FL-4: retry 四处分叉，YAML 路径零延迟紧循环
- retry 统一重构战役
- caught by: offline（代码审计）—— 四个分叉实现行为不一致
- should have been: unit —— 各路径的退避时序本该有单测锁定差异；layering 门
  本该拦住重复实现
- guard now: 单一真相源 `crates/swanflow/src/core/retry.rs`（base 100ms，
  2^min(n,4) 封顶），基线 290；新增手写退避循环 = 违规

### FL-5: excel_report 图表 range 裸写导致 insert_chart 静默吞错
- wf169
- caught by: live/e2e —— 裸 `B2:B4` 让 rust_xlsxwriter 构系列失败，
  `insert_chart(...).ok()` 把错误吞掉，产物无图表但 exit 0
- should have been: unit —— 图表生成错误不允许 `.ok()` 吞掉；range 合法性
  （须 `Sheet!$A$1:$A$4` 全格式）可在节点内静态校验
- guard now: 全格式 range + openpyxl 读不回图表的已知限制改用 zipfile 查
  `xl/charts/chart1.xml`

### FL-6: luajit 扩展 install 失败返回 ok=false 不抛错，输出全 NULL 看似透传
- wf79（离线缓存 sha256 不一致：本地构建 vs registry v0.32.3）
- caught by: live/e2e —— `luajit_s('base64',…)` 返回 NULL 而非报错
- should have been: unit —— install 失败必须是硬错误（fail-loud），静默降级到
  NULL 输出是最危险的一类失败
- guard now: `luajit_module(mode:='upgrade', sql_name:=…)` 重取缓存即愈；
  探针法 = registry-loaded ext vs `allow_unsigned_extensions` 本地 LOAD 双路对比
- 备注 `env:` 缓存哈希不一致属环境级，但「失败不抛错」是产品缺陷，两条分开记

---

## Tally（每 10 条更新）

| should have been | 条数 |
|---|---|
| unit（节点契约/错误处理） | 4（FL-1/3/5/6） |
| gate（派生矩阵挂 CI） | 1（FL-2） |
| unit + layering 门 | 1（FL-4） |

**结论（6 条样本）**：系统性失守层是 **unit 的节点契约面**——E2E 在抓本该
单测拦的问题。下一个加固方向：给每个注册节点的 `validate()`/`propose_plan()`
补契约测试，并把「注册节点 × planner lowering」做成 CI 派生矩阵（FL-2 的
guard 升级为规则）。
