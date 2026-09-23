# Why

GraphRAG ingestion E2E（2026-09-23 真跑，examples/graph_rag_flow.yaml + 8014 qwen3.8-27b）链路 EXIT=0 但产物严重残缺：3 句话只落 3 实体/2 边（应 6+ 实体/6 边），且实体名丢弃。逐层读码定位 6 个真 bug，全部属"静默成功陷阱"族（与本仓已修的 CSV 缺文件/duckdb_read 静默空库同族）。

模型侧已排除：节点同款 prompt 直打 8014 三句全返回合法 JSON、实体全对。

# What

| # | Bug | 位置 | 修法 |
|---|---|---|---|
| 1 | 实体名丢弃：name hash 成 id 后不落任何列，图里只剩类型 | `nodes/ai/graph.rs` EntityExtractionStream + `nodes/mod.rs` GraphSchemas | node_schema 加 `name` 列（id, name, label, properties）；extractor 写 name |
| 2 | LLM 失败/JSON 解析失败双层 `if let Ok` 静默吞掉，行直接消失 | graph.rs L408-443 | 失败→`Err(DataFusionError::Execution)` 传播 |
| 3 | GraphIndexWriter 的 open/INSERT 全 `.ok()` 丢弃错误 | `nodes/io/nodes.rs` GraphIndexingStream | 错误传播；连接失败=节点失败 |
| 4 | retriever `depth` 配置收了但 SQL 恒 1 跳 | graph.rs GraphRetrievalStream | 递归 CTE 实现真 depth |
| 5 | retriever 上下文输出 "Entity: Person"（类型非名字）——Bug1 下游放大 | graph.rs L734 | 输出 "Entity: Alice (Person)" |
| 6 | retriever 执行显示 Executed 但零产物（查询/写盘静默失败的组合面） | graph.rs L716-740 | query_arrow 失败改显式 Err；配合 2/3 修复后复测 |

顺带修：retriever 查询输入是 int64 hash id——用户不可能知道 hash。检索锚点改 name（config 传 `entity_name`，SQL 用 name 匹配 id）。schema 变更后 `__graph_nodes` 表加 name 列（CREATE TABLE IF NOT EXISTS 加列 + INSERT 含 name）。

# Out of Scope

- 向量检索层（vss/turbovec）——纯图检索先跑通
- context_fuser 的 LLM 融合质量调优
- extractor batch 单次 LLM 调用省 token 优化
