---
id: ADR-005
title: "模块拆分策略 — LOC 预算与文件拆分警戒线"
status: proposed
date: "2026-06-16"
deciders: ["@lhy"]
tags: ["architecture", "modularity", "loc-budget"]
affected_crates: ["swanflow"]
related: ["ADR-002"]
---

# ADR-005: 模块拆分策略

## Context

Nika 规定每个文件 ≤1,500 LOC，每个 crate ≤15K LOC。SwanFlow 当前 49 个源文件共 19,200 LOC，但有 3 个文件超过 1,500 LOC 警戒线：

| 文件 | LOC | 问题 |
|---|---|---|
| `src/core/datasource.rs` | 1,931 | 🔴 超 1,500 线 |
| `src/nodes/io.rs` | 1,773 | 🔴 超 1,500 线 |
| `src/nodes/ai/graph.rs` | 1,533 | 🔴 超 1,500 线 |
| `src/core/engine.rs` | 1,247 | 🟡 接近 |
| `src/nodes/ai/retrieval.rs` | 1,217 | 🟡 接近 |

AI 助手在 1,500+ LOC 的文件中推理准确率显著下降（模式匹配式猜测增多）。

## Decision

采用宽松版 Nika LOC 策略：2,000 LOC 硬上限（而非 1,500）。

**优先拆分目标**（3 个文件）：

### 1. `datasource.rs` (1,931 LOC)
拆分方向：
- `datasource/scan.rs` — 各类 scan 逻辑（DuckDBScanLogical, ExcelScan 等）
- `datasource/arrow_helpers.rs` — Arrow 数组构建/转换
- `datasource/schema.rs` — 架构推断
- `datasource/profiler.rs` — 数据剖析

### 2. `io.rs` (1,773 LOC)
拆分方向：
- `io/read.rs` — CSV, Parquet, Universal, DuckDB 读取节点
- `io/write.rs` — DuckDB, Excel 写入节点
- `io/excel.rs` — Excel 相关节点（read/write/template/report/sheet_names）
- `io/helpers.rs` — DuckDBHelper

### 3. `ai/graph.rs` (1,533 LOC)
拆分方向：
- `ai/graph/nodes.rs` — RAG 节点定义
- `ai/graph/schema.rs` — 图 schema
- `ai/graph/ops.rs` — 图操作

## Consequences

**正向**：
- 每个文件更聚焦，AI 推理准确率提升
- 新节点加入时更清晰归属

**负向**：
- 拆分后需要更新模块声明和 pub use 导出
- 需要确保导入路径不破坏外部引用
