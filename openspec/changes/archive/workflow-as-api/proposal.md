# 工作流当 API / MCP 工具 — 提案

## Why

用户要把 swanflow 工作流（含 GraphRAG 检索流）当 API 函数与 MCP 工具消费：挂一次、之后像调函数一样参数化调用并拿回真实数据。现状三个缺口：

1. MCP `run_workflow` 只返回事件计数，不返回节点物化数据（调用方拿不到结果）
2. 无法从调用侧传参——参数写死在 YAML 里（没有 params → `workflow.context` 的注入通路）
3. 没有"注册"概念——工具是引擎级的 `run_workflow`，不是"我的检索流"这个具名工具

## What Changes

- MCP `run_workflow` 新增可选 `params`（object）：合并进 `workflow.context`（params 优先级高于 YAML 内同名值），配合现有 `${context.x}`/`{{x}}` 模板即参数化
- MCP `run_workflow` 新增可选 `output_nodes`（string[]）：执行后从 `flow_metadata` 读回指定节点的 parquet，结果以 JSON 行数组放进 text 返回；不指定时维持原摘要行为
- 新增 MCP 工具 `register_workflow(name, workflow_yaml, description?, param_schema?)`：注册为具名工具，`tools/list` 动态合并注册项；调用具名工具 = 以参数执行该工作流并返回 output_nodes 数据
- MCP server 增加注册表持久化（`~/.swanflow/registered_workflows.json`），重启不丢
- daemon `workflow.submit` 新增可选 `params`，语义同上
- 注册工具的输出节点集合由 YAML 根级 `outputs: [node_name...]` 声明，注册时读入

## 端到端验收

GraphRAG 检索流注册为 `graph_query` 工具后，MCP 客户端 `tools/call graph_query {"entity": "Alice", "depth": 2}` 返回命中实体的 context 文本数组（与直接跑 retrieval_v2.yaml 的 parquet 内容一致）。

## 影响面

- `crates/swanflow/src/mcp/server.rs`（主要改动）
- `crates/swanflow/src/core/engine/daemon.rs`（params 字段）
- `crates/swanflow/src/core/graph.rs`（Workflow 根级 `outputs` 字段，serde default）
- 不改执行内核；只动 MCP/daemon 服务层
