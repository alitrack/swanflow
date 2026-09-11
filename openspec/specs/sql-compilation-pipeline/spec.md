# Spec: SQL Compilation Pipeline

## Purpose

将 DataFusion `LogicalPlan` 编译为 DuckDB SQL，使 SwanFlow 的 pipeline 可以完全在 DuckDB 中执行。编译产物是可读的 SQL 文本，支持调试、审计和跨引擎移植。

## Contract

### 1. 基础表扫描

```
GIVEN LogicalPlan::TableScan { table_name: "users" }
WHEN compile_to_sql(plan)
THEN 返回 "SELECT * FROM \"users\""
```

### 2. Projection（列选择）

```
GIVEN TableScan("users") → Projection([col("id"), col("name")])
WHEN compile_to_sql(plan)
THEN 返回 "SELECT users.id, users.name FROM (SELECT * FROM \"users\") AS _sub"
```

### 3. Filter（行过滤）

```
GIVEN TableScan("users") → Filter(col("id") > 1)
WHEN compile_to_sql(plan)
THEN 返回 "SELECT * FROM (SELECT * FROM \"users\") AS _sub WHERE users.id > Int64(1)"
```

### 4. Aggregate（聚合）

```
GIVEN TableScan("sales") → Aggregate(group=[col("region")], aggr=[COUNT(col("amount"))])
WHEN compile_to_sql(plan)
THEN 返回包含 "SELECT region, COUNT(amount) FROM (...) AS _sub GROUP BY region" 的 SQL
```

### 5. Sort + Limit

```
GIVEN TableScan("users") → Sort([col("id") DESC]) → Limit(skip=0, fetch=10)
WHEN compile_to_sql(plan)
THEN 返回包含 "ORDER BY ... LIMIT 10" 的 SQL
```

### 6. Join

```
GIVEN TableScan("orders") JOIN TableScan("users") ON orders.user_id = users.id
WHEN compile_to_sql(plan)
THEN 返回包含 "INNER JOIN" 和 "ON orders.user_id = users.id" 的 SQL
```

### 7. Union

```
GIVEN TableScan("t1") UNION ALL TableScan("t2")
WHEN compile_to_sql(plan)
THEN 返回 "(SELECT * FROM \"t1\") UNION ALL (SELECT * FROM \"t2\")"
```

### 8. Extension Node Pass-through

```
GIVEN SwanFlow MaterializerLogical wrapping a TableScan("users")
WHEN compile_to_sql(materializer_plan)
THEN 剥离 MaterializerLogical，编译内层 plan 为 "SELECT * FROM \"users\""
```

### 9. 多 Extension 节点嵌套

```
GIVEN MaterializerLogical(StateProbeLogical(TableScan("data")))
WHEN compile_to_sql(plan)
THEN 递归剥离所有 Extension 节点，编译内层 TableScan
```

### 10. 嵌套子查询别名保留

```
GIVEN SubqueryAlias(TableScan("raw"), alias="cleaned")
WHEN compile_to_sql(plan)
THEN SQL 中包含 "(SELECT * FROM \"raw\") AS \"cleaned\""
```

### 11. NotImplemented 安全回退

```
GIVEN 一个 SwanFlow 尚未支持编译的 LogicalPlan variant (如 Window)
WHEN compile_to_sql(plan)
THEN 返回 Err(NotImplemented("SQL compilation not implemented for LogicalPlan variant: ...")), 不 panic
```

## Boundary Cases

| 场景 | 行为 |
|---|---|
| EmptyRelation (produce_one_row=true) | `SELECT 1` |
| EmptyRelation (produce_one_row=false) | `SELECT * FROM (VALUES (NULL)) AS _empty WHERE FALSE` |
| 无 group key 的聚合 | `SELECT aggs FROM (...) AS _sub`（无 GROUP BY） |
| Limit with skip > 0 | `LIMIT n OFFSET m` |
| Join 无 ON 条件 | `CROSS JOIN`（而非 INNER JOIN） |
| Extension 节点有 0 个 input | 返回 NotImplemented |
| Extension 节点有 2+ 个 input | 返回 NotImplemented |
| Expression 包含 CAST/literal | 使用 DataFusion 的 Display 格式输出 |
| 极深的嵌套子查询 | 递归编译无深度限制（由 DuckDB 的 SQL parser 限制承担） |
| Distinct 在多层嵌套内 | `SELECT DISTINCT * FROM (...)` 递归编译内层 |

## Implementation Notes

- 使用 `LogicalPlan` 的 `Display` trait 格式化 Expression——这已经生成 SQL 兼容的语法
- DuckDB 标识符加双引号 `"` 以处理大小写和特殊字符
- 不要尝试优化生成的 SQL（如谓词下推、子查询消除）——让 DuckDB 的优化器处理
- Extension 节点的 `inputs()` 方法返回 `Vec<&LogicalPlan>`，递归编译第一个 input 即可
