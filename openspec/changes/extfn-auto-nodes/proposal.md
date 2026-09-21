# Proposal: extfn-auto-nodes — 把「扩展函数」接成画布算子

> 状态：**待评审**（按用户铁律「先研究蓝本源码 → 画图证明理解 → 获认可后才开发」）
> 锚点：`#2026092103` ｜ 前置调研页：wiki `duckdb-extension-to-skill-to-operator-20260921`

## Why

SwanFlow 已经证明**「节点可以不写 Rust、从清单里长出来」**：`LuaJitLibNode`
（`crates/swanflow/src/nodes/luajit.rs:230`）扫 `duckdb-luajit-libs/INDEX`，把每个 lib
注册成 `luajit.<lib>` 画布节点（`crates/swanflow/src/nodes/registry.rs:104-117`）。

但这个机制**只接在一个源上** —— 47 个 Lua lib（`INDEX` 实测 47 行）。

DuckDB 侧真正的算子源是**扩展函数**（`duckdb_functions()` 自省；本机 v2.0 实测
**2,225 个函数 / 12 个扩展**）。DuckFlow 里有一对构件把这件事做了一半，SwanFlow 侧：

| 构件 | DuckFlow | SwanFlow |
|---|---|---|
| `ext_list_funcs` | ✅ `DuckDbCompiler.cs:1756-1765` | ❌ **不存在** |
| `extension_call` | ✅ `DuckDbCompiler.cs:1724-1754` | ❌ **不存在** |

> ⚠️ **最容易误判的一点（已逐行核实）**：SwanFlow 里唯一像 `extension_call` 的候选
> `DuckDBExtensionNode`（`nodes/boundary.rs:94-162`）**不是它** —— 那是**纯 LOADER**：
> `input_ports: vec![]` / `output_ports: vec![]`（0 端口）、`propose_plan` 里
> `Connection::open_in_memory()` + `INSTALL`/`LOAD` 后 **`return Ok(vec![])`**，
> **不调用任何扩展函数、不产出任何数据**；`validate` 还带 **12 项硬编码白名单**
> （`boundary.rs:122-135`）。⇒ 不能当蓝本用。

⇒ **移植这对构件是一个真新增，不是改造存量。**

## What

1. 新增 `crates/swanflow/src/nodes/extfn.rs`：`ExtFnNode`，含
   `spec()`（按 `function_type` / `parameter_types` 推端口与 config_schema）、
   `validate()`、`propose_plan()`（产出 `DuckDBScanLogical`，见 design.md §4）。
2. 新增扫描函数 `extfn_nodes_from_table()`：从**物化的 `ext_functions` 表**读签名 → 节点集合。
3. `crates/swanflow/src/nodes/registry.rs` 挂**第二个**自动注册源（与 luajit 并列，带白名单）。
4. 新增建表脚本 `scripts/build_ext_functions.py`（或 `.sql`）—— 复现 PoC 的那一条 SQL。
5. 单测 + e2e（e2e 必须走 `DuckDBScanExec` 直构造，见 design.md §5）。

## Out of Scope（本次不做）

- ⛔ 不重写 DuckFlow 的 **294 个唯一节点类型**（`NodeSchema.cs` 实测），范式不同。
- ⛔ 不搬 `ExtensionMarketplace.cs:28` 的硬编码策展目录。
- ⛔ 不做 DuckFlow 的 `{{input}}` **字符串模板分支**（`DuckDbCompiler.cs:1727-1736`）——
  SwanFlow 有 typed ports + `config_schema`，模板字符串是它的历史包袱，不需要。
- ⛔ 不改 `core.flow.extension` 的 12 项白名单 —— 那是**独立改动**，见 design.md 决策 4。
- ⛔ 不做 skill 侧（Hermes skills 出口）—— 那是**并行的另一条出口**，共享同一上游
  `ext_functions` 表，不在本 change 内。

## 验收标准（草案）

| # | 标准 | 验证方式 |
|---|---|---|
| A1 | `cargo check -p swanflow` 通过 | 构建 |
| A2 | 从 `ext_functions` 表构造出 N 个 `ExtFnNode`，`type_id` 唯一且形如 `ext.<ext>.<fn>` | 单测 |
| A3 | `spec()` 对 table 函数给出「输入表 + 输出表」端口，对 scalar 给出「输入表 → 输出表（列变换）」 | 单测 |
| A4 | `propose_plan` 产出 `DuckDBScanLogical`（**不是** `SqlProxyLogical`） | 单测断言 variant |
| A5 | 白名单外扩展**不被注册**（防节点爆炸） | 单测 |
| A6 | 真跑一个扩展函数（如 `spatial` 的某函数）端到端出数据 | e2e（需扩展可载，见 design.md §5 风险） |
