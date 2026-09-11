# Spec: QueryEngine Trait

## Purpose

定义一个与具体查询引擎无关的统一执行接口。解耦 workflow 编译器/执行器与底层引擎实现，使 SwanFlow 可以在 DataFusion 和 DuckDB 之间切换（未来可扩展到更多引擎）。

## Contract

### 1. 引擎标识

```
GIVEN 一个 DataFusionEngine 实例
WHEN 调用 name()
THEN 返回 "DataFusion"

GIVEN 一个 DuckDBEngine 实例
WHEN 调用 name()
THEN 返回 "DuckDB"
```

### 2. 能力声明

```
GIVEN DataFusionEngine
WHEN 读取 capabilities()
THEN logical_plan_execution = true, sql_compilation = false, external_attach = false

GIVEN DuckDBEngine
WHEN 读取 capabilities()
THEN logical_plan_execution = false, sql_compilation = true, external_attach = true
```

### 3. 表注册 (register_table)

```
GIVEN 任意 QueryEngine 实现
AND 一个 RegisteredTable { name: "users", source: InMemory { batches } }
WHEN 调用 register_table(table)
THEN table_exists("users") 返回 true
AND execute_sql("SELECT * FROM users") 返回对应的 RecordBatches
```

```
GIVEN 任意 QueryEngine 实现
AND 一个 RegisteredTable { name: "logs", source: Parquet { path } }
WHEN 调用 register_table(table)
THEN execute_sql("SELECT COUNT(*) FROM logs") 成功执行
```

### 4. SQL 执行 (execute_sql)

```
GIVEN DuckDBEngine with table "users" (id INT, name VARCHAR)
WHEN execute_sql("SELECT * FROM users WHERE id > 1")
THEN 返回过滤后的 RecordBatches
AND 返回的 Arrow Schema 正确映射 DuckDB 类型 (INT→Int64, VARCHAR→Utf8)
```

```
GIVEN DataFusionEngine with table "users"
WHEN execute_sql("SELECT * FROM users")
THEN 返回完整的 RecordBatches
```

### 5. LogicalPlan 执行 (execute_logical_plan)

```
GIVEN DataFusionEngine
WHEN execute_logical_plan(scan_plan)
THEN 返回对应的 RecordBatches（通过 DataFusion PhysicalPlan 执行）

GIVEN DuckDBEngine
WHEN execute_logical_plan(scan_plan)
THEN 内部调用 compile_to_sql → execute_sql（两步委托）
```

### 6. 表注销 (deregister_table)

```
GIVEN 已注册的表 "users"
WHEN deregister_table("users")
THEN table_exists("users") 返回 false
AND execute_sql("SELECT * FROM users") 返回错误
```

### 7. 表列表 (list_tables)

```
GIVEN DuckDBEngine with tables ["users", "orders"]
WHEN list_tables()
THEN 返回包含 "users" 和 "orders" 的 Vec<String>
```

### 8. 表 Schema (get_table_schema)

```
GIVEN DuckDBEngine with table "users" (id BIGINT, name VARCHAR)
WHEN get_table_schema("users")
THEN 返回 Schema { fields: [Field("id", Int64), Field("name", Utf8)] }
```

### 9. arc_clone

```
GIVEN 任意 QueryEngine 实现
WHEN arc_clone()
THEN 返回一个新的 Arc<dyn QueryEngine>，共享底层连接
```

## Boundary Cases

| 场景 | 行为 |
|---|---|
| 重复注册同名表 | 第二次注册覆盖第一次（符合当前 SwanFlow 行为） |
| 执行空结果 SQL | 返回空的 `Vec<RecordBatch>`，不报错 |
| deregister 不存在的表 | DuckDB: DROP VIEW IF EXISTS 静默成功；DataFusion: 返回 error |
| execute_sql 传入非 SELECT | DuckDB: 返回空 Vec（DDL）；DataFusion: 返回空 Vec |
| Native TableSource on DataFusion | 返回 NotImplemented error |
| DuckDB Connection 并发访问 | Mutex 保护，spawn_blocking 避免阻塞 async runtime |
| DuckDB 返回的列类型不在映射表中 | 降级为 Utf8，不 panic |
