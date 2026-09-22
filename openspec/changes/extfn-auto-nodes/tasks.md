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

- [x] **集成后 tip 已编译 + 全套 lib 测试通过**（rebase 到 origin/main 的
      duckdb 1.5.5 升级 + 两个 audit fix 之上之后，`5a6fa5e`）：
      ```bash
      cargo test -p swanflow --lib --offline   # 冷编译 + 全套
      # test result: ok. 255 passed; 0 failed; 0 filtered out; finished in 8.79s
      ```
      注意冷启动坑：本仓 `.gitignore:16` 忽略 `Cargo.lock` ⇒ 干净克隆无锁 ⇒
      离线解析拒绝 yanked `spin 0.10.0`（`crc-fast ← aws-smithy-checksums ←
      aws-sdk-s3` 链）。解法 = 拷共享树的 `Cargo.lock` 进克隆（yanked 版本
      「已锁定即可用」）。
- [x] **端到端已验**（`examples/e2e_extfn.rs`，真起引擎）：
      ```text
      SWANFLOW_EXT_FUNCTIONS_DB=<catalog> cargo run --example e2e_extfn
      catalog: ext_functions_154.duckdb (v1.5.4-built, mode B)
      total node types : 288
      ext.* node types : 173        ← 与目录构建预测 173 精确一致
      probe get_node(ext.motherduck.MD_ACCESS_TOKENS) -> round-trip ok
      E2E_EXTFN_OK 173
      ```
      同时实证：**引擎（bundled DuckDB 1.5.5）可直接打开 v1.5.4 CLI 建的目录文件**
      （存储格式跨 1.5.x 兼容），注册期 SQL/谓词在真引擎连接上全部成立。
- [ ] 未验 tier-1 节点真能跑数据：需挑一个确实可载的扩展（ducklake / quack /
      postgres_scanner / sqlite_scanner / excel / fts）做一次真实 `propose_plan` → 执行
- [ ] 未测 `mode=scalar` 在真实扩展函数（如 spatial `st_area`）上的行为——本机 v2.0 目录无
      spatial（v1.5.4 目录才有），需换 CLI 或换机器
- [ ] **`INSTALL` 能力未测**：本机全是 dev/alpha 构建（hash `5319116087`），官方不发布二进制，
      `INSTALL` 100% 失败 ⇒ 覆盖率天花板 = 有没有一个可 INSTALL 的发布版运行时。这是本方案
      唯一未解的上游未知数
- [ ] `duckatlas.extension_versions` 表 0 行（兼容性矩阵数据缺口，独立任务）
- [ ] 未把「怎么用」写进用户文档 / wiki

## 2026-09-22 遗留清理

- [x] **`swanflow run` 节点级错误吞掉后仍 EXIT=0 —— 已修**（`5ae513d`）：
      复现（CSV 源 → `SELECT no_such_column`，规划期 `NodeEvent::Error` 打到 stderr
      后照样 "✅ Execution Finished" + EXIT=0）。修法：run 的事件循环追踪
      `NodeEvent::Error` 与 `NodeState::Error(_)`，任一命中则
      "❌ Execution Finished with errors." + `exit(1)`。加载期错误（坏 YAML/UUID）
      原本就非零退出。新增 `tests/cli_exit_code.rs`（起真二进制，双用例钉住契约）；
      lib 测试 257/257 无回归。
      测试坑两条：(a) 二进制动态链接 `libpython3.11.so.1.0`（pyo3），跑测试须带
      `.cargo/config.toml [env]` 里同款 `LD_LIBRARY_PATH`；(b) 引擎缓存目录
      `target/swanflow_cache`（含独占锁的 `metadata.duckdb`）相对 cwd 硬编码 ⇒
      每个测试 spawn 用独立 cwd，否则两测试并发互撞 DuckDB 文件锁。
- [ ] **新发现（更深的一档）：CSV 源文件不存在时节点照样 `Executed`**（复现
      `/tmp/fail_repro.yaml`：`core.io.csv` 指向不存在路径，两个节点全部
      Executed、EXIT=0、零报错）——缺文件错误被吞在比事件循环更早的层（大概率是
      注册表/规划期的 schema fallback）。待定位修复。
