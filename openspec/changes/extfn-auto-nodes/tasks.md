# extfn-auto-nodes

## 执行前置

- 不触碰他人未提交区（本任务只动 `nodes/{extfn.rs(新), mod.rs, luajit.rs, registry.rs}`
  + `scripts/build_ext_functions.py`(新) + 本 openspec 目录；工作树里属他会话的
  `Cargo.toml` / `nodes/transform.rs` / `core/engine/planner.rs` / `core/datasource/excel.rs`
  **一律不碰**）
- 本机 cargo TLS 全坏 ⇒ 编译必须带 `--offline`（见 skill `cargo-offline-deps`）
- 单测命令（WSL，需 LD_LIBRARY_PATH 指 libpython3.11）：
  ```bash
  cd /mnt/d/wsl2/swanflow && env LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib cargo test -p swanflow --lib extfn --offline
  ```
- 目录构建命令（纯 Python + DuckDB，不需要编译，可独立验证）：
  ```bash
  cd /mnt/d/wsl2/swanflow && python3 scripts/build_ext_functions.py
  ```
- 提交按语义拆分：`feat(nodes)` + `scripts(...)` + `docs(openspec)`，直接落 main

## 铁律

- **三档过滤不得放宽**：任一放宽都会让画布从 66 个节点涨到 2,225 个。放宽必须走
  `UPDATE ext_functions SET enabled = TRUE`（人工策展），不得改代码里的默认谓词
- **`internal` 不是过滤器**：DuckDB 里它表示「C++ 实现而非 SQL 宏」，对
  `duckdb_functions()` **全部 3,253 行都是 TRUE**（实测）。把它写进谓词 = 静默启用 0 个节点。
  区分系统函数与扩展函数的是 `extension_name`
- **缺目录必须静默**：`ext_functions.duckdb` 不存在 → 不注册任何 ext 节点、不 panic、
  不影响其余注册表（与 luajit「Missing INDEX is fine」同语义）
- **扩展与函数名进 `type_id`，不进 config**：`register_node` 是 `HashMap::insert`，
  同名即静默覆盖
- **重载必须显式折叠**：`SELECT DISTINCT` 对**全部选中列**去重，而重载行的
  `return_type`/`parameters` 不同 ⇒ **折不掉**（实测 93 行只对应 66 个
  `(extension, function)`，23 组重复）。必须用 `QUALIFY row_number() OVER (PARTITION BY
  extension_name, function_name ORDER BY …) = 1` 取确定性代表
- **不走 `SqlProxyLogical`**：扩展函数不在 DataFusion 函数目录里，planning 期必报
  「函数不存在」。必须产 `DuckDBScanLogical`
- **`core.flow.extension` 的 12 项白名单本次不动**（安全相关改动，独立 change）

## 已完成（均经实跑/源码核实）

- [x] 上游事实核实：`duckdb_functions()` 的 `extension_name` 列**只有 v2.x 有**，v1.5.x 无
      （v1.5.4 基线 2,951 函数，列为
      `function_name/database_name/function_type/schema_name/internal`）
- [x] 蓝本对照：DuckFlow `ExtListFuncs`（`DuckDbCompiler.cs:1756-1765`）**按函数名 pattern
      过滤、根本没用 `extension_name`** ⇒ 移植的是语义、实现须升级
- [x] 蓝本对照：SwanFlow 的 `DuckDBExtensionNode`（`nodes/boundary.rs:94-162`）是**纯
      LOADER**（0 端口、`propose_plan` 返回 `vec![]`、12 项硬编码白名单），**不是**
      `extension_call` ⇒ 两构件是真空白
- [x] 挂载点确认：`nodes/registry.rs:104-117` 自动注册块（luajit 源）
- [x] `TypedNodeBehavior` 端口 const 疑点澄清：`LuaJitLibNode` 自己就声明 1/1
      （`luajit.rs:321-324`）而其 table 模式实际 0 输入 ⇒ const 是宽松声明非强制校验
- [x] `scripts/build_ext_functions.py` 建成并**实跑通过**：`catalogued 2225 /
      nodes_enabled 66 / rows_enabled 93 / exts 12`
- [x] 逐扩展 enabled 节点数**与独立 SQL 预测逐项一致**：ducklake 20 / json 10 / quack 10 /
      parquet 8 / postgres_scanner 8 / sqlite_scanner 3 / autocomplete 3 / icu 2 /
      excel 1 / shell 1 = 66
- [x] `nodes/extfn.rs` 建成（`ExtFnNode` + `ExtFnDef` + `extfn_nodes_from_table` +
      `extension_ref` + `default_ext_functions_db` + 11 个单测）
- [x] 接线：`nodes/mod.rs` 加 `pub mod extfn;`；`luajit.rs` 的 `struct_literal` 提
      `pub(crate)`、`discover_schema` 改签名收 `extensions: &[String]`（行为等价）；`registry.rs`
      加第二个自动注册源
- [x] 顺手修掉 `registry.rs` 既存日志 bug：luajit 块原来打的是
      `self.nodes.len().saturating_sub(1)`（整个注册表大小）冒充 lib 数
- [x] **注册期 SQL 用真 DuckDB 验过**（不需要编译）：`node_count 66 /
      distinct_type_ids 66 / 重复 type_id 0 行`；逐扩展分布与前次独立预测逐项一致
- [x] **编译 + 单测全绿**（`TEST_EXIT=0`）：
      ```bash
      cargo test -p swanflow --lib extfn --offline
      # running 12 tests
      # test result: ok. 12 passed; 0 failed; 0 ignored; 237 filtered out
      ```
- [x] 修掉 1 个测试断言错误 + 1 个文档注释错误：`struct_literal` 实际是
      `parts?.join(", ")`（逗号**+空格**），而它的文档注释写的是无空格形态
      `{'op':'run','n':1}`。我的断言照抄了注释 ⇒ 失败（**实现是对的**）。
      两处均已修：`extfn.rs` 断言改为实际输出、`luajit.rs` 注释改为 `{'op':'run', 'n':1}`
- [x] 顺带确认 `serde_json` 在本仓**保序**（`preserve_order` 开启）⇒ struct 键序 = 书写顺序

## 遗留待办

- [ ] **端到端未验**：真起 SwanFlow 看 `Auto-registered 66 DuckDB extension function nodes`
      日志 + `list_nodes` 里出现 `ext.<ext>.<fn>`
- [ ] **未验 tier-1 节点真能跑数据**：需挑一个确实可载的扩展（ducklake / quack /
      postgres_scanner / sqlite_scanner / excel / fts）做一次真实 `propose_plan` → 执行
- [ ] 未测 `mode=scalar` 在真实扩展函数（如 spatial `st_area`）上的行为——本机 v2.0 目录无
      spatial（v1.5.4 目录才有），需换 CLI 或换机器
- [ ] **`INSTALL` 能力未测**：本机全是 dev/alpha 构建（hash `5319116087`），官方不发布二进制，
      `INSTALL` 100% 失败 ⇒ 覆盖率天花板 = 有没有一个可 INSTALL 的发布版运行时。这是本方案
      唯一未解的上游未知数
- [ ] `duckatlas.extension_versions` 表 0 行（兼容性矩阵数据缺口，独立任务）
- [ ] 未把「怎么用」写进用户文档 / wiki
