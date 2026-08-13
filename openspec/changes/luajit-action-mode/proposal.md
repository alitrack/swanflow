## Why

duckdb-luajit-libs 的 `incremental` / `scd2` 是**动作型库**：执行 DML/DDL（`_duckdb_call`/`_duckdb_query`）并返回 JSON 字符串，既不是表生成器（table）也不是标量变换（scalar）。SwanFlow 的 luajit 壳节点只有 table/scalar 两种模式，动作型库无法在画布上表达——用户必须退回手写 core.sql 节点。DuckDB 生态没有原生增量/SCD2（301 社区扩展真空位），这两个库是 ETL 流程层的核心价值，画布上必须有对应节点。

## What Changes

- `LuaJitNode` 增加第三种模式 **`action`**：`SELECT luajit_s('<source>', <opts struct>) AS result`——opts 从 config `args`（JSON 对象）渲染为 DuckDB struct 字面量
- 新增 `struct_literal()`：JSON value → DuckDB 字面量（struct/list/string/number/bool/null，单引号转义，嵌套递归），语法已在 DuckDB 1.5.5 实测
- `LuaJitLibNode` 增加 `action_args` 字段：INDEX 扫描时识别动作型库（incremental/scd2），自动预填 mode=action + args 模板（用户填 target/source/ts_col 等即可）
- validate 允许 action 模式（args 必填）

## Capabilities

### New Capabilities
- `luajit-action-nodes`: 动作型 Lua 库在画布上成为一等节点（拖拽 → 填 args → 执行 → 输出 JSON 行）

### Modified Capabilities
- `luajit-shell-node`: table/scalar 之外新增 action 形态；config_schema 的 args 由 array 改为 object/array

## Impact

- **luajit.rs**: propose_plan 新增分支 + struct_literal 辅助函数 + LuaJitLibNode 字段扩展（INDEX 扫描处同步）
- **画布**: 重启后自动出现 `luajit.incremental` / `luajit.scd2` 节点（INDEX 自动注册机制，零手动注册）
- **兼容**: table/scalar 行为不变；args 字段语义从"多参数组"变为"action opts"，旧用法（table/scalar 带 args）不受影响（只读 args 的只有新 action 分支）
- **测试**: 5 个新单测（struct_literal / action query / prefill / action libs 识别 / validate）
