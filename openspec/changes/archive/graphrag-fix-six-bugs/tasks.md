# GraphRAG 六缺陷修复 — 任务

## 执行前置

- 不触碰他人未提交区（`git status` 先查，只 add 自己改的文件）
- 真实可跑的测试命令：
  - `bash /tmp/graphrag/run_ingest.sh`（ingestion E2E，LLM=8014）
  - `bash /tmp/graphrag/run_retrieval.sh`（检索 E2E）

## 已完成任务

- [x] T1 GraphSchemas::node_schema 加 `name` 列（id/name/label/properties），`__graph_nodes` 建表同步 + 旧库 ALTER 兼容
- [x] T2 EntityExtractor 保留 name、LLM/JSON 错误 `?` 显式传播（带行号）
- [x] T3 GraphIndexWriter 写 name 列、Connection/INSERT 错误 `.map_err` 传播
- [x] T4 GraphRetriever 递归 CTE 真多跳（depth 生效、含反向边）+ `entity_name` 锚点配置 + 输出带实体名 + 错误传播
- [x] T5 单测：schema 列序锚定 ×2；分区行数回归 `tests/partition_row_loss_regression.rs`
- [x] T6 ~~全量重编~~ 增量重编 + 264 单测（--skip luajit，见 watch-list）+ ingestion/retrieval E2E 全绿

## E2E 验证结果（2026-09-23，qwen3.8-27b@8014）

- ingestion：3 句 → 6 实体（Alice/Bob/ACME Corp/San Francisco/Senior Software Engineer/workflow automation）+ 6 边（WORKS_FOR/HAS_ROLE/HIRED/CTO_OF/based_in/specializes_in），name 全非空，幂等重跑一致
- retrieval：entity_name='Alice' depth=2 → 上下文含全部 6 实体（含 2 跳邻居）

## 追加修复（E2E 迭代中发现，Bug 7-11）

- [x] Bug 7 引擎级分区丢失：StateProbeExec/MaterializerExec 只拉输入分区 0 → 构造时包 CoalescePartitionsExec（影响所有 UNION ALL 类多分区数据源）
- [x] Bug 8 EntityExtractorExec 同病 → coalesce
- [x] Bug 9 GraphRetrieverExec/GraphIndexWriterExec 同病 → coalesce（防御性）
- [x] Bug 10 检索 SQL 裸表名（表在 meta. schema 下）→ 3 处限定名
- [x] Bug 11 递归 CTE 锚点列数不匹配（缺 hop 初值）+ 结果列 Utf8View 裸 unwrap panic → `SELECT id, 0` + 显式 cast

## Watch-list（不在本变更内修）

- luajit 单测偶发死锁（40 线程 futex 全等待，每次挂的测试不同，与本次改动无关）→ 建议单独立项
- ext_functions.duckdb 版本号 999 打开失败噪音（DuckDB 新旧版本混用）→ 建议重建该 catalog
