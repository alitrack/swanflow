# Spec: Core Node Stability

## Purpose

确保所有核心节点通过三级测试（单节点正确性 / 两节点链路 / 三节点 DAG），消除非预期边界行为。

## Contract

- 每个核心节点必须有 T1 测试覆盖
- 每个链路场景必须有 T2 测试覆盖
- 分支/合并场景必须有 T3 测试覆盖
- Late-Binding 引用 `SELECT * FROM node_uuid` 在所有数据节点上工作

## Node Catalog

| 节点 | T1 | T2 | T3 | Late-Binding | Notes |
|------|----|----|----|--------------|-------|
| DatabaseSource | ✅ | ✅ | ✅ | ✅ | 含 Postgres/MySQL/SQLite/ClickHouse |
| SQLNode | ✅ | ✅ | ✅ | ✅ | 参数化查询 |
| FilterNode | ✅ | ✅ | ✅ | N/A | AND/OR/NOT/NULL/in |
| GroupByNode | ✅ | ✅ | ✅ | N/A | HAVING 支持 |
| JoinNode | ✅ | ✅ | ✅ | N/A | LEFT/FULL/ANTI/SEMI/CROSS |
| SortNode | ✅ | ✅ | N/A | N/A | 多字段 + NULL FIRST/LAST |
| FormulaNode | ✅ | ✅ | N/A | N/A | 线性 + 条件逻辑 |
| MissingNode | ✅ | N/A | N/A | N/A | 占位报错 + 候选提示 |
| PythonUDFNode | ✅ | ✅ | ✅ | N/A | 错误传递 |

## Boundary Cases

- 空表（0 行）
- 超宽表（100+ 列）
- 特殊列名（含空格/引号/Unicode）
- NULL 值在聚合/排序中的行为
- 重复列名在 Join 中的行为
