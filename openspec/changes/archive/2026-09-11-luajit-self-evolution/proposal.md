# Change Proposal: LuaJIT Self-Evolution (luajit-self-evolution)

**Status**: Implemented ✅（2026-08-12 全部完成，待用户确认推送合并）
**Date**: 2026-08-12
**Change ID**: luajit-self-evolution
**Source**: sigil 的 LuaJIT 自进化闭环（research #2026081204）移植到 SwanFlow

## 1. Why

SwanFlow 的 agent loop 只能编排**已注册**的节点类型；模型遇到现有 653 个节点
表达不了的需求时没有逃生口。sigil（用户另一项目）已实现「模型写 Lua → 落盘 →
下次可用」的进化闭环，底座是 duckdb-luajit 扩展。SwanFlow 已内置 DuckDB 引擎
（duckdb crate 1.10503.1 = DuckDB v1.5.3），扩展机制完整，具备同款能力。

## 2. What

让 SwanFlow 具备 sigil 式自进化：

1. **扩展加载**：`DuckDBEngine::open` 支持 `allow_unsigned_extensions` +
   `SWANFLOW_DUCKDB_EXTENSIONS` 环境变量（逗号分隔扩展路径，open 后逐个 LOAD）；
   未设置时行为完全不变（向后兼容）。
2. **壳节点**：`core.luajit`（mode=table 无输入生成器 / mode=scalar 变换输入表），
   执行路径 = DuckDBScanLogical/DuckDBScanExec（LOAD luajit 扩展 → SQL →
   COPY parquet → DataFusion 读回），绕开 duckdb arrow-58 vs datafusion arrow-57
   类型冲突。
3. **自动注册**：启动时扫描 duckdb-luajit-libs 仓库 INDEX（`name|path` 每行），
   每个 lib 注册为 `luajit.<name>` 节点——「画布上长出节点」。
4. **Agent 工具**：`create_lua_lib {lib_name, source, category}`——模型写 Lua →
   清洗名字 → 落盘 `libs/<cat>/<name>.lua` → 更新 INDEX（去重）→ 立即经
   core.luajit 可用，重启后自动注册为专用节点。

## 3. Explicitly Out of Scope

- 不升级 duckdb 到 1.5.5（1.5.3 实测可加载 1.5.5 编译的扩展，扩展 ABI 小版本
  向后兼容；升级引入全量重编译 + 行为差异风险）
- 不做 FFI 直连重方案（产品形态：Option A luajit 壳节点）
- 不做社区注册表发布（自用走本地未签名扩展；分发后续单独走）
- 不触碰 `crates/swanflow-core/src/nodes.rs`、`registry.rs`（M 状态遗留，铁律）
