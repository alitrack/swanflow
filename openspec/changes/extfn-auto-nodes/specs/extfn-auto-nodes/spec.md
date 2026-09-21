# spec: extfn auto nodes

## 节点命名

| 形态 | 值 |
|------|-----|
| `type_id` | `ext.<extension>.<function>`，如 `ext.ducklake.ducklake_list_files` |
| `display_name` | `<function> (<extension>)` |

扩展名必须进 `type_id`：`NodeRegistry::register_node` 是 `HashMap::insert`
（`nodes/registry.rs:21-24`），同名即**静默覆盖**；`duckdb_functions()` 里已存在同名
overload 行（如 `ai_summarize_agg` 出现两次），跨扩展同名完全可能。
副产品：同 `(extension, function)` 的 overload 自动合并为一个节点，重载解析交给 DuckDB 调用期。

## `ExtFnNode` config

| 字段 | 类型 | 默认 | 语义 |
|------|------|------|------|
| `mode` | string | 按 `function_type` 推：`table`→table，其余→scalar | `table` = 生成器/扫描器（无输入）；`scalar` = 逐列变换 |
| `args` | object\|array\|string | — | object → DuckDB struct 字面量（单个位置 STRUCT 参数）；array → 位置参数；string → **原样拼入**（`format='geojson'` 这类具名参数的逃生舱） |
| `columns` | string[] | — | **仅 scalar 模式，且必填**：作为函数第一个参数的输入列 |

`extension` / `function` **不进 config**（已烧进 `type_id`），与 `LuaJitLibNode` 同构。

## 节点 SQL 形态

```
table :
  args 为空        SELECT * FROM <fn>()
  args 非空        SELECT * FROM <fn>(<args>)

scalar:
  SELECT * FROM (
    SELECT <全部输入列>, <fn>("col", <args>) AS "col", ...
    FROM input_0
  ) AS _extfn_transform
```

- scalar 模式**保留全部输入列**并原地覆盖被指定的列（追加而非替换整表）
- `columns` 缺失 → `propose_plan`/`validate` 双双报错，**不猜列**。扩展函数几乎都只对
  特定类型有意义（`st_area(geom)`），对全列硬套必然运行期报错 ⇒ 显式优于静默
- 扩展函数不在 DataFusion 函数目录里 ⇒ 必须产 `DuckDBScanLogical`（DuckDB 执行 →
  COPY parquet → DataFusion 读回）。走 `SqlProxyLogical` 在 planning 期即失败

## 扩展引用解析（`extension_ref`）

| 优先级 | 来源 | 说明 |
|--------|------|------|
| 1 | `SWANFLOW_EXTENSIONS_DIR` + `/<name>.duckdb_extension` | 显式目录 |
| 2 | `~/.duckdb/extensions/<version>/<platform>/<name>.duckdb_extension` | 版本目录降序，取最新命中 |
| 3 | 裸 `<name>` | 发布版走 registry `INSTALL`+`LOAD` |

优先磁盘绝对路径的原因：dev/alpha 构建不发布扩展二进制，`INSTALL` 必失败，只有
`LOAD '<绝对路径>'` 进得去。

## `ext_functions` 目录表（`scripts/build_ext_functions.py` 产出）

列：`extension_name, function_name, function_type, parameters, parameter_types,
return_type, description, examples, categories, has_side_effects, stability, internal,
enabled`

`enabled` = 人工策展开关，构建时按下式预置：

```sql
enabled = (function_type = 'table' AND extension_name <> 'core_functions')
```

档位语义：

| 档 | 条件 | 默认 | 本机实测 |
|----|------|------|----------|
| 档 1 自动注册 | `function_type='table'` 且非 `core_functions` | `enabled=TRUE` | 93 行 → **66 节点** |
| 档 2 需策展 | `function_type IN ('scalar','aggregate')` 且非 `core_functions` | `enabled=FALSE` | 405 候选 |
| 硬排除 | `core_functions`（1,709）、`pragma`（5）、`macro`（11）、`table_macro`（2） | — | 已由 `core.sql` 覆盖 |

`function_type='table'` 是**零误伤**过滤器：core 的 table 函数是 **0 个**（实测），
故该条件天然等价于「扩展提供的真算子」。而 `aggregate` 1,165 个**全在 core**。

## 注册期过滤（`extfn_nodes_from_table`）

```sql
WHERE coalesce(enabled, false)
  AND extension_name <> 'core_functions'
  AND function_type NOT IN ('pragma', 'macro', 'table_macro')
```

代码侧**重复**施加过滤，故即使目录表由更宽松的脚本产出也撑不爆画布。

`internal` **刻意不作为过滤条件**：DuckDB 里它表示「C++ 实现而非 SQL 宏」，对
`duckdb_functions()` 全部 3,253 行均为 TRUE（实测）。写进谓词会**静默启用 0 个节点**。

## 路径与环境变量

| 变量 | 默认 | 用途 |
|------|------|------|
| `SWANFLOW_EXT_FUNCTIONS_DB` | `target/swanflow_cache/ext_functions.duckdb` | 目录表路径（与引擎的 `metadata.duckdb` 同目录） |
| `SWANFLOW_EXTENSIONS_DIR` | — | 显式扩展二进制目录 |
| `SWANFLOW_DUCKDB_CLI` | 自动探测 | 构建脚本用哪个 duckdb CLI |

目录表**不并进 `metadata.duckdb`**：该库有已知并发写风险（并行测试共享 → DuckDB C++
abort），且生命周期由 engine 接管；独立文件解耦且安全。

**表不存在 → 静默不注册**，与 luajit「Missing INDEX is fine — just the generic shell node
remains」（`registry.rs:106`）同语义。

## 描述字段的诚实性要求

`parameter_types` 填充率 98.1%（2,183/2,225），但 `description` **分裂**：填了的
只有 `core_functions` 99.8% / `spatial` 95.8% / `ai` 100%，其余受检扩展**全为 0%**
（icu / json / parquet / ducklake / quack / postgres_scanner / fts / excel /
sqlite_scanner / shell…）。填了的恰是用 DuckDB **新元数据机制**注册的少数；用旧 C API
注册的一律为空。

⇒ 节点描述**必须显式声明缺失**（"No upstream description available — arguments are
inferred from the signature only."），**不得**让用户以为自动生成的节点自带文档。
