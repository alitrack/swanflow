---
id: ADR-002
title: "Nika借鉴与工程纪律采纳 — clippy强制、#[non_exhaustile]策略、unwrap管控"
status: accepted
date: "2026-06-16"
deciders: ["@lhy"]
tags: ["工程纪律", "clippy", "non_exhaustive", "unwrap"]
affected_crates: ["swanflow"]
---

# ADR-002: Nika借鉴与工程纪律采纳

## Context

对 Nika (supernovae-st/nika) 的深度源码研究表明，该项目的工程纪律是其核心竞争力：38 crate 零 unwrap、强制 `#[non_exhaustive]`、`clippy.toml` 编译时禁令。SwanFlow 作为同类项目（Rust 数据科学 DAG 引擎 + AI Agent），可以从其纪律中受益。

决策关键：SwanFlow 的工程纪律应该从哪些方面开始？

## Decision

分阶段采纳 Nika 的工程实践：

**Phase 1（本次提交）**：
1. `clippy.toml` 禁止 `std::dbg`
2. 所有 `pub enum` 加 `#[non_exhaustive]`（15 个枚举）
3. `scripts/ci/check-no-unwrap.sh` — CI 中 warning-level 监控 src/ 中 unwrap/expect

**Phase 2（未来）**：
- 逐步消除 src/ 中 unwrap/expect（当前 88 处）
- 扩展 clippy.toml 禁止 `std::env::var`、`std::process::Command` 等
- 引入 LOC 预算上限

## Consequences

**正向**：
- 公共 API 演进更安全（添加枚举变体不破坏外部代码）
- `dbg!` 不会误提交
- unwrap 数量可追踪、可消减

**负向**：
- 单 crate 内 `#[non_exhaustive]` 无实际效果（跨 crate 才生效），真正的收益在后续 split 后
- unwrap 脚本只是 warning，不阻止 CI 通过
