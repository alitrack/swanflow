# extfn 自动节点 —「扩展函数长成画布算子」使用指南

> 对应 openspec change: `extfn-auto-nodes` ｜ 锚点 `#2026092103`
> 设计动机与双 lane 验证记录见 wiki: `duckdb-extension-to-skill-to-operator-2026-09-21`

## 一句话

DuckDB 的扩展函数（`duckdb_functions()` 自省到的）会被物化成一张 `ext_functions`
目录表，SwanFlow 启动时读它、自动注册成画布节点 —— **不写 Rust、不改代码，画布自动长出算子**。

## 机制（三层）

```
duckdb_functions()                       ① 采集：scripts/build_ext_functions.py
        │                                   把本机所有可 LOAD 扩展的函数目录
        ▼                                   物化成 ext_functions 表
<cache_dir>/ext_functions.duckdb        ② 目录：一个 DuckDB 文件
        │                                   （或 $SWANFLOW_EXT_FUNCTIONS_DB 指定的路径）
        ▼
NodeRegistry::default()                 ③ 注册：extfn.rs 扫表，
                                        每个启用行 → 一个 ext.<扩展>.<函数> 节点
```

节点 `type_id` 格式：`ext.<extension>.<function>`（如 `ext.spatial.st_area`）。
扩展名和函数名烧进 `type_id` 而不是 config —— 两个扩展导出同名函数时不会互相
静默覆盖（registry 是 HashMap insert）。

## 快速开始

### 1. 生成目录

```bash
# 用本机 duckdb CLI 采集（默认输出到缓存目录）
python3 scripts/build_ext_functions.py

# 或指定位置 / 查看
python3 scripts/build_ext_functions.py --db /path/to/ext_functions.duckdb --list-only
```

采集范围：本机所有可 LOAD 的扩展。函数启用规则（tier-1 自动注册闸）：

- `table` 型函数（生成器/扫描器）且非 `core_functions` 且非 internal → **启用**
- 其余（scalar/aggregate）默认不启用 —— 否则画布会一次性长出 2,000+ 节点

### 2. 让 SwanFlow 读到目录

按顺序解析：

1. `$SWANFLOW_EXT_FUNCTIONS_DB`（显式指定）
2. `<cache_dir>/ext_functions.duckdb`（默认；与引擎缓存同目录）

### 3. 画布 / YAML 里用

```yaml
graph:
  nodes:
    - id: "..."
      type_id: "ext.sqlite_scanner.sqlite_scan"   # ext.<扩展>.<函数>
      config:
        path: "/data/my.db"
        args: [...]          # 函数实参（按目录表 parameters 顺序）
        columns: [...]       # scalar 模式必填：哪些输入列喂给函数
```

两种模式（由函数类型自动决定）：

| 函数型 | 模式 | 生成的 SQL | 输入 |
|---|---|---|---|
| `table` | 生成器 | `SELECT * FROM <fn>(args)` | 无（自产数据） |
| `scalar` / `aggregate` | 列变换 | `SELECT *, <fn>(col, args) AS col__extfn FROM input_0` | 上游端口 |

`__extfn` 后缀别名消歧：输出列与透传输入列同名时，下游按名消费不歧义。

### 4. 验证接线（可选）

```bash
SWANFLOW_EXT_FUNCTIONS_DB=<catalog> cargo run --example e2e_extfn
# 期望输出：E2E_EXTFN_OK <n>，且 probe get_node() 往返成功
```

## 常用操作

**开一批 scalar 函数（如 spatial 全家）**：

```sql
-- 直接改目录表，重启 SwanFlow 生效
UPDATE ext_functions SET enabled = TRUE WHERE extension_name = 'spatial';
```

**加新机器上的扩展**：重跑 `build_ext_functions.py` 即可（幂等重建）。

**查当前哪些函数可用**：

```bash
python3 scripts/build_ext_functions.py --list-only
```

## 已知边界（如实告知）

1. **`INSTALL` 上限（上游未知数）**：本机是 dev/alpha 构建（hash `5319116087`），
   官方不为 dev 构建发布二进制 ⇒ `INSTALL <ext>` 100% 失败。目录采集范围的天花板
   = 本机已有可 LOAD 的扩展。想扩目录，先在某个发布版 DuckDB 里装好扩展再采集。
2. **1.5.5 `sqlite_scan` 第二参数只认表名**，不认 SQL（与上游文档表述不符；
   tasks.md 留观）。
3. **无真画布 UI 验证**：链路每一环都有 CLI/e2e 证据（见下），但尚未在图形画布
   上人工点过。
4. 社区扩展的版本兼容矩阵在独立仓库
   [duckatlas](https://github.com/alitrack/duckatlas)（`extension_versions` 已有
   292 行，其中 85 行为五版本真测）。

## 验证证据链（一手）

| 环节 | 证据 |
|---|---|
| 采集脚本 | `build_ext_functions.py` 实跑，2,225 函数 / 66 启用（v2.0 / 12 扩展） |
| 注册接线 | `cargo run --example e2e_extfn` → `E2E_EXTFN_OK`，get_node 往返成功 |
| 真跑数据流 | `ext.sqlite_scanner.sqlite_scan` 工作流 → sink 落盘 → 读回逐值一致（pi lane + 独立复现） |
| 安全加固 | sigil 独立 review 18 条指控分诊：采纳 5（标识符白名单、quote_ident、struct key 转义、`__extfn` 别名、tie-breaker），修复后 257/257 全绿（`13ed83c`） |
| 引擎桥修复 | VARCHAR/BLOB 列 panic（Utf8View vs Utf8）逐列 cast 修复（`05b6e4f`），影响全部 DuckDBScan 消费者 |

## 源码索引

| 文件 | 角色 |
|---|---|
| `scripts/build_ext_functions.py` | ① 采集物化 |
| `crates/swanflow/src/nodes/extfn.rs` | ② 目录解析 + ③ 节点定义/SQL 生成 |
| `crates/swanflow/src/nodes/registry.rs` | 注册接线（`ext.*` 前缀分发） |
| `crates/swanflow/examples/e2e_extfn.rs` | 接线 e2e 验证 |
| `openspec/changes/extfn-auto-nodes/` | proposal / design / specs / tasks 全档案 |
