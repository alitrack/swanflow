# Spec: luajit-self-evolution

**Change**: luajit-self-evolution
**Date**: 2026-08-12

## 节点规格

### `core.luajit` — LuaJIT Script（通用壳节点）

```json
{
  "type_id": "core.luajit",
  "config_schema": {
    "type": "object",
    "properties": {
      "mode": { "type": "string", "enum": ["table", "scalar"], "default": "table" },
      "source": { "type": "string", "description": "Lua 源码传给 luajit_table/luajit_s" },
      "sql_name": { "type": "string", "description": "已注册 UDF 名（luajit_module install/quick_compile）" },
      "args": { "type": "array", "items": { "type": "any" } },
      "columns": { "type": "array", "items": { "type": "string" }, "description": "scalar 模式作用列（默认全部）" }
    },
    "required": ["source"]
  },
  "ports": { "input": 1（scalar 模式用）, "output": 1 }
}
```

- **table 模式**：`SELECT * FROM luajit_table('<source>')`（无输入，生成器）
- **scalar 模式**：`SELECT luajit_s('<source>', "col") ... FROM input_0`（变换输入表）

### `luajit.<name>` — libs 自动注册节点

- type_id = `luajit.<lib_name>`，source 预填 lib 名，config 仅暴露 mode/args
- 来源：duckdb-luajit-libs INDEX 扫描（启动时，缺失静默跳过）

## Agent 工具规格

### `create_lua_lib`

```json
{
  "name": "create_lua_lib",
  "arguments": {
    "lib_name": "string（清洗为 ascii alnum + 下划线）",
    "source": "string（Lua 源码，非空）",
    "category": "string?（默认 udf）"
  }
}
```

行为：写 `libs/<cat>/<name>.lua` → INDEX append（`name|` 前缀去重，无 INDEX 则新建）
→ 打印路径 + 使用方式。落盘根：`SWANFLOW_LUAJIT_LIBS_DIR` >
`/mnt/d/wsl2/duckdb-luajit-libs` > `~/.duckdb/luajit-libs`。

## 环境变量

| 变量 | 用途 |
|---|---|
| `SWANFLOW_DUCKDB_EXTENSIONS` | 逗号分隔扩展路径，open 后自动 LOAD |
| `SWANFLOW_LUAJIT_EXT` | 显式 luajit 扩展路径 |
| `SWANFLOW_LUAJIT_LIBS_DIR` | libs 仓库根（create_lua_lib 写盘） |
| `SWANFLOW_LUAJIT_LIBS_INDEX` | INDEX 路径（自动注册扫描） |

## 安全模型

- Lua 运行在 luajit 扩展 trusted sandbox（P4，借鉴 pllua）
- `_duckdb_call`/`_duckdb_query` 需 normal mode + 审批门
- system prompt 已注明：io/os/ffi 需明确审批
