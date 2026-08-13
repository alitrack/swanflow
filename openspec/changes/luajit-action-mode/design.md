# Luajit Action Mode — Design

## 关键前置验证（已实测）

1. **DuckDB struct 字面量语法**（1.5.5）：`{'op':'run','target':'tgt'}`——键单引号、字符串单引号+转义、数字/bool 裸写、null→NULL。经 `luajit_s('incremental', {'op':'run',...})` 实测可正确转 Lua table（`luajit_module` 的 struct→Lua 转换接受此语法）。`{target:=...}` 是语法错误（踩过）。
2. **luajit_s 调用形态**：`luajit_s(name, struct)` 直接传 struct → Lua table ✓（MACRO 包装 `luajit_vs(name, cast(x1 AS VARCHAR))` 会把 struct 转字符串丢类型——**必须用 luajit_s 而非 MACRO**）。
3. **install 不可用**：`luajit_module(mode:='install')` 读远程/缓存 INDEX（本地改 INDEX 无效，实测报 "lib not found in INDEX"）。E2E 路径 = quick_compile + regexp 包装（`return incremental` → `return function(opts) return incremental.run(opts) end`，注意 `[\r\n]*$` 匹配行尾换行）。SwanFlow 场景依赖 luajit 扩展的注册名解析（`source` = lib 名），luajit_s 按名从扩展缓存加载。

## 设计决策

### 1. action 模式 SQL 形态
```
SELECT luajit_s('<source>', {'op':'run','target':'tgt',...}) AS result
```
- 无输入端口（同 table 模式）；返回单行单列 `result`（VARCHAR JSON）
- `args` 缺失时渲染 `NULL`（validate 已拦）

### 2. struct_literal 渲染规则
| JSON | DuckDB 字面量 |
|---|---|
| object | `{'k':v, ...}`（键单引号） |
| array | `[v, ...]` |
| string | `'...'`（`'`→`''`） |
| number/bool | 裸写 |
| null | `NULL` |

### 3. LuaJitLibNode action 识别
- `luajit_libs_from_index()` 内硬编码映射 `{incremental, scd2} → args 模板`（与 duckdb-luajit-libs INDEX 同步维护）
- propose_plan 预填：用户未显式设 mode/args 时注入 `mode=action` + 模板
- 新动作型库只需加一行映射 + 重启

### 4. 为什么不走 install/compile
- install 读远程 INDEX（本地仓库改动不可见）
- compile 注册的 MACRO 参数 cast 成 VARCHAR 丢 struct 类型
- quick_compile 需要源码直传 + return-function 形态；SwanFlow 节点以 lib 名为 source，走 luajit_s 按名加载最简

## 陷阱（勿重复踩）

- **struct 键必须引号**：`{target:=...}` / `{target:...}` 均语法错误
- **MACRO 参数 cast**：`luajit_vs('x', CAST(x1 AS VARCHAR))` 把 struct 转字符串 → Lua 侧 `p.target` 为 nil（静默失败，返回 NULL）
- **INDEX 行尾换行**：regexp 替换 lib 尾部 `return <name>` 时用 `[\r\n]*$`
- **action 模式返回**：luajit_s 返回 Lua table 显示 `table: 0x...` 指针不可读 → lib 必须返回 JSON 字符串（incremental/scd2 已实现 `{"loaded":N,...}`）
