# Tasks: Elastic Fan-Out Error Policy

**Change**: elastic-fanout-error-policy
**Date**: 2026-08-11
**Status**: Implemented ✅（2026-08-11 全部完成，待用户确认推送合并）

## 执行前置

- 分支：`feat/elastic-fanout-error-policy` ✅
- 铁律：未触碰未提交区 `crates/swanflow-core/src/nodes.rs`、`crates/swanflow-core/src/registry.rs`（M 状态）、`openspec/changes/graph-repair-operators/` ✅
- 测试环境：`LD_LIBRARY_PATH` + `CARGO_BUILD_JOBS=4` ✅

## Milestone 1: 分类逻辑（纯函数）✅

- [x] 1.1 `ChildOutcome` enum（Ok / ContinueWithError / HardError）+ `classify_child_outcome(error_mode, result, is_join_error)` 纯函数
- [x] 1.2 单测 5 个：4 policy × 成功矩阵、fail→HardError、skip→ContinueWithError、retry 耗尽→HardError、join 错误永远 Hard

## Milestone 2: retry helper 提取（修订）✅

- [x] 2.1 ~~闭包式共享 helper~~ **否决**：Rust `FnMut` 借用规则（捕获 `&mut step_context` 的 future 不能逃逸闭包体）
- [x] 2.2 **单节点路径完全恢复原样**（零行为改变，R6 最安全）
- [x] 2.3 fan-out 路径内联 retry 循环（与单节点语义一致：retry 耗尽 → 按 Fail 分类）

## Milestone 3: fan-out 路径接入 ✅

- [x] 3.1 spawn 内按子节点 error_mode 分类执行：Retry → 内联重试；其余单次执行；`classify_child_outcome` 分类
- [x] 3.2 `aggregate_child_outcome` 纯函数：Ok→outputs / Skip→outputs+"Error:"+NodeEvent::Error / Fail→hard_errors+NodeEvent::Error
- [x] 3.3 聚合：hard_errors 非空 → 返回 Err（聚合消息）+ `NodeEvent::StateChanged(Error)`；join 错误 → 硬失败
- [x] 3.4 单测 2 个：skip 聚合（含事件断言）、hard 聚合（含事件断言）

## Milestone 4: 文档与收尾 ✅

- [x] 4.1 ADR-013-elastic-fanout-error-policy.md（Forge §3.3 弹性 fan-out 出处 + 默认值变更说明 + retry helper 否决记录）
- [x] 4.2 `cargo check --workspace` 零错误 + `cargo test -p swanflow-core` 84/84 不变
- [x] 4.3 engine::tests 7/7 绿（分类 5 + 聚合 2）
- [ ] 4.4 提交 → 用户确认推送合并（待办）

## 行为变更说明（默认值变更）

- **之前**：fan-out 子节点失败 → 无条件 `"Error: ..."` 字符串，静默继续
- **现在**：子节点 `error_mode: fail`（默认）→ fan-out 步骤整体失败（Err + 事件）；`skip` → 继续但发 NodeEvent::Error（可见）；`retry(n)` → 重试后按 fail
- **迁移**：依赖旧「总是继续」行为的工作流需在子节点显式声明 `error_mode: skip`（一行）

## 基线记录（2026-08-11 预规划门实测）

- cargo check --workspace 零错误
- swanflow-core 84/84 绿（P0 新增后基线）
- middleware 15/15 绿（P1 新增后基线）
- engine::tests 7/7 绿（本 change 新增）
- 主 crate --lib：129 绿 + 3 存量失败（templating ×2 + duckdb in-memory）——均与本 change 无关
- 环境：LD_LIBRARY_PATH 必需；CARGO_BUILD_JOBS=4 防 OOM；libduckdb-sys 每 profile 变体重编译 10-15 分钟
