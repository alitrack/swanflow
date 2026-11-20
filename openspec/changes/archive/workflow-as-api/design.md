# 工作流当 API — 设计

## 现有机制（已核实）

- `Workflow.context: HashMap<String, JsonValue>`（graph.rs L103）——全局变量，节点配置经 `TemplateEngine::render_json` 解析 `${context.x}`（精确替换，可换类型）与 `{{x}}`（mustache 字符串内嵌）（engine/mod.rs L973-974 → L1497）
- `flow_metadata` 表（node_id, table_name, parquet_path, schema_json）——物化结果可读回（engine/mod.rs L61-72, L255）
- MCP server：JSON-RPC over stdio，`tools/list` 静态返回 5 工具，`tools/call` 路由到 `call_tool`（server.rs）

## 关键设计

### params 注入语义

`run_workflow(workflow_yaml, params?)`：
```
merged_context = workflow.context（YAML 声明） + params（调用侧，后者覆盖前者）
```
反序列化 YAML → 合并 → 执行。不引入新模板语法，复用现有两级占位符。

### 结果读回

`output_nodes: ["retriever"]`（或注册时 YAML 根级 `outputs:`）：
- 执行成功后按 node name 找 node_id，查 `flow_metadata` 最新 parquet_path
- 用 duckdb `read_parquet` 读为 JSON 行数组（上限 max_rows_per_node 防爆）
- 返回 `text` = `{"outputs": {"<node>": [ {...}, ... ]}, "events": N}`
- 指定节点无物化 → 该键置 `null` 并附 warning 字符串（显式不静默）

### 注册表

- `~/.swanflow/registered_workflows.json`：`{name: {yaml, description, param_schema, outputs}}`
- `register_workflow` / `list_registered` / `unregister_workflow` 三工具
- `tools/list` = 静态 5 工具 + 注册项（具名，input_schema=param_schema 或从 YAML 中 `${context.x}` 引用自动推导）
- `tools/call <name>`（非保留名）= load yaml → merge params（= 整个 arguments）→ 执行 → 读回 outputs
- 保留名冲突：注册名撞保留工具名 → 显式报错

### daemon

`workflow.submit` params 字段同语义（一行 merge，执行通路不变）。

## 风险与边界

- parquet 读回走独立 duckdb 连接（`:memory:` + read_parquet），不与会话争锁
- params 只进 context，不碰 node config 结构——无 schema 污染
- 大结果集：截断到 max_rows_per_node，附 `truncated: true`
