---
id: ADR-006
title: 模块拆分与文件大小纪律
status: accepted
deciders: lhy
date: 2026-06-16
supersedes: ADR-005
tags: [architecture, module-organization, refactoring]
---

# 模块拆分与文件大小纪律

## Context

Nika 项目（supernovae-st/nika）严格执行每个 crate ≤15K LOC、每个文件 ≤1.5K LOC 的纪律。这对 AI 辅助开发至关重要——小文件让 AI 能在上下文窗口内准确推理。

SwanFlow 增长到 31 个源文件、57+ 节点类型后，单片文件成为瓶颈：

| 文件 | 原始行数 | 现状 |
|---|---|---|
| `src/nodes/io.rs` | 1,784 ✅ → `io/` 子目录 | `io/excel.rs` (461) + `io/nodes.rs` (1,283) + `io/mod.rs` |
| `src/core/engine.rs` | 1,263 ✅ → `engine/` 子目录 | `engine/mod.rs` (915) + `engine/planner.rs` (362) + `engine/metadata.rs` (7) |
| `src/core/datasource.rs` | 1,933 ⏳ → 待拆分 | 含 Excel/DB/types/Connector 混合 |

## Decision

1. **文件上限 1,500 行**。超限文件必须拆分到子目录。
2. **子目录模式**：`parent.rs` → `parent/mod.rs` + `parent/sub1.rs`。
3. **`mod.rs` 仅做 re-export**，不包含业务逻辑。
4. **每个子文件一个职责**（excel 节点 vs 其他节点，引擎核心 vs planner）。
5. **datasource.rs 待拆分**：Excel 类型 → `excel.rs`，Sqlx/DuckDB 扫描 → `scan.rs`，ConnConfig + Connector trait 留 `mod.rs`。

## Consequences

- ✅ `io.rs` 从 1,784 行降至子文件 <1,300 行
- ✅ `engine.rs` 从 1,263 行降至 `mod.rs` 915 行 + 职责分离
- ⏳ `datasource.rs` 1,933 行等待拆分，不影响编译
- ✅ 外部 `use crate::core::engine::SwanFlowEngine` 等导入路径不变
