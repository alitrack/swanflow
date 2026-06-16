---
id: ADR-004
title: "DuckDB 作为必选依赖 — 嵌入式 SQL 引擎"
status: accepted
date: "2026-06-16"
deciders: ["@lhy"]
tags: ["duckdb", "sql", "dependency", "embedded-engine"]
affected_crates: ["swanflow"]
related: ["ADR-001"]
---

# ADR-004: DuckDB 作为必选依赖

## Context

SwanFlow 使用 DuckDB 作为嵌入式 SQL 引擎。问题是：DuckDB 应该是必选还是可选？

考虑依据：
- DuckDB 用于 I/O 节点（DuckDBReadNode、DuckDBWriteNode、DuckDBExtensionNode）
- DuckDB 也用于 SQL 变换节点的内部执行引擎（SqlNode、FilterNode、AggregateNode 等通过 DuckDB `:memory:` 执行 SQL）
- 选择 `DUCKDB_DOWNLOAD_LIB=1` 自动下载官方 libduckdb.so（首次 28s，缓存后 8s）
- 总编译缓存命中时间约 8s

## Decision

**DuckDB 是必选依赖。** 不设为 feature gate。

理由：
1. DuckDB 是 SwanFlow 的嵌入式 SQL 引擎，不是可选的 I/O 格式。去掉 DuckDB = 去掉 SQL 计算。
2. `DUCKDB_DOWNLOAD_LIB=1` 已零手动步骤。首次 28s 自动下载官方 libduckdb.so。
3. 运行时支持所有 DuckDB 扩展（`LOAD json;` / `LOAD parquet;` / `LOAD icu;` 等）。
4. 数据科学 DAG 引擎没有 SQL = 残废。

## Consequences

**正向**：
- 构建流程简化（无需 feature gate）
- 所有节点在任何构建中都可用
- SQL 扩展随时可加载（无需重新编译）

**负向**：
- 构建环境需要网络（下载 libduckdb.so）
- 二进制大小增加（含 DuckDB C 库 ~20MB）
