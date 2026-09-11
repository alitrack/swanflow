# Tasks: Declarative Middleware Stack

**Change**: middleware-stack-compiler
**Date**: 2026-08-11
**Status**: Implemented ✅（2026-08-11 全部完成，待用户确认推送合并）

## 执行前置

- 分支：`feat/middleware-stack-compiler` ✅
- 铁律：未触碰未提交区 `crates/swanflow-core/src/nodes.rs`、`crates/swanflow-core/src/registry.rs`（M 状态）、`openspec/changes/graph-repair-operators/` ✅
- 测试环境：`LD_LIBRARY_PATH` + `CARGO_BUILD_JOBS=4` ✅

## Milestone 1: middleware.rs 核心 ✅

- [x] 1.1 `middleware.rs`：`MiddlewareDecl`、`PlanMiddleware` trait、`MiddlewareBuilder`、`mw_builders()/mw_schemas()/mw_categories()` 三注册表
- [x] 1.2 `compile_middleware_stack()`：顺序保留 + enabled=false 跳过 + 未知类型硬错误 + schema 校验（复用 swanflow_core::validation::SchemaValidator）
- [x] 1.3 单测 7 个：声明→编译栈顺序、enabled=false、未知类型、bad declaration（带索引）、非数组、bad config（带指针）、无 middleware 字段

## Milestone 2: 内置 middleware ✅

- [x] 2.1 `column_select`（projection 重写）+ schema
- [x] 2.2 `row_limit`（limit fetch 重写）+ schema
- [x] 2.3 `column_rename`（全列投影 + Alias 重写）+ schema —— **替代原计划 sample**（datafusion 52 移除 Sample 节点且 expr_fn 无 random()，纯 plan 层无法实现采样；见 ADR-012 Alternatives）
- [x] 2.4 单测 7 个：每个 middleware 行为 + 错误 config

## Milestone 3: 引擎接入 ✅

- [x] 3.1 `mod.rs` 导出 middleware 模块
- [x] 3.2 引擎节点执行路径（`engine/mod.rs` 3a 段，propose_plan 前）应用 compiled stack：`compile_middleware_stack` + `apply_stack_all`（anyhow → DataFusionError::Plan 转换）
- [x] 3.3 端到端测试：`apply_stack_all_runs_in_declared_order`（row_limit → column_rename 顺序生效）

## Milestone 4: 文档与收尾 ✅

- [x] 4.1 ADR-012-declarative-middleware-stack.md（Forge middleware_compiler 出处 + sample 弃用记录）
- [x] 4.2 CONTRIBUTING.md（新建，含 middleware 三处注册扩展指南 + 测试环境）
- [x] 4.3 `cargo check --workspace` 零错误 + `cargo test -p swanflow-core` 84/84 不变
- [x] 4.4 middleware 测试 15/15 绿
- [x] 4.5 提交 → 用户确认推送合并（已 merge 进 main：d9828df；2026-09-11 记账补勾）

## 基线记录（2026-08-11 预规划门实测）

- cargo check --workspace 零错误 ✅
- swanflow-core 84/84 绿（含 P0 新增 8 测试）✅
- middleware 15/15 绿（本 change 新增）
- 主 crate --lib：129 绿 + 2 存量失败（core::templating mustache，commit 0bf6a8f 引入）+ 1 存量失败（duckdb_backend in-memory table，duckdb crate 内部 panic）——均与本 change 无关
- 环境注意：LD_LIBRARY_PATH 必需；全量 workspace 测试 OOM → CARGO_BUILD_JOBS=4；libduckdb-sys 每 profile 变体重编译 10-15 分钟
