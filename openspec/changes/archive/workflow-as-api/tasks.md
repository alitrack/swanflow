# 工作流当 API — 任务

- [x] T1 `graph.rs`：Workflow 根级 `outputs: Vec<String>`（serde default）
- [x] T2 MCP `run_workflow`：`params` 合并进 context（params 覆盖 YAML 同名）
- [x] T3 MCP 结果读回：`output_nodes` / YAML `outputs` / 注册项 outputs 三级生效；duckdb::arrow 转 JSON
- [x] T4 `register_workflow`/`unregister_workflow`/`list_registered`；持久化 `~/.swanflow/registered_workflows.json`
- [x] T5 `tools/list` 合并注册项；具名调用 = params 执行 + outputs 返回；保留名拒绝
- [x] T6 daemon `workflow.submit` 加 `params`
- [x] T7 单测 4 个（YAML/JSON 解析、outputs、保留名、to_tool）
- [x] T8 E2E：stdio JSON-RPC 真协议——注册 graph_query → tools/list 含 9 工具 → Alice depth=2 命中 ACME/Bob → Bob depth=1 参数覆盖生效 → MCP_E2E=PASS
- [x] T9 示例 `examples/graph_query_registered.yaml`

## 验证轮中额外修复（原计划外，E2E 逼出）

- [x] stdout 污染：MCP banner + registry [DEBUG]×5 全部改 stderr（stdout 是帧通道）
- [x] transport 空行 bug：Content-Length 行后未消费 `\r\n`，泄入 body 截断 JSON 尾
- [x] 返回信封：content 数组补 `{type:"text", text}` 标准格式
- [x] 双 arrow 版本冲突：转换器切 duckdb::arrow（query_arrow 产物）
