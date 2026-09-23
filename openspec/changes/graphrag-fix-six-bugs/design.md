# GraphRAG 六缺陷修复 — 设计

## 前置验证（已做）

1. 模型侧排除：8014 直打同款 prompt，3 句全对（/tmp/graphrag/probe_all3.sh）
2. 图索引现状：`__graph_nodes` 3 行（label=Person/Org/Role，properties 全 {}）、`__graph_edges` 2 行
3. retriever SQL 手工验证可查到数据
4. `__graph_nodes` 建表处：grep `__graph_nodes` 在 engine 或 io 层（实现时定位 CREATE TABLE 语句同步加 name 列）

## 关键设计

### Bug 7（E2E 复验时新发现，引擎级，比 1-6 更严重）

单节点 `core.sql` 3 行 UNION ALL 只物化 1 行。探针测试实证 DataFusion 52 的 UnionExec 每分支一个输出分区（3 个单行分区）；而 `StateProbeExec`/`MaterializerExec` 声明 1 输出分区、`execute(0)` 只拉 `input.execute(0)`——分区 1/2 整体静默丢弃。

**修法**：两个 Exec 构造时把输入包一层 `CoalescePartitionsExec`（合并为单分区），行为不变、数据完整。回归测试 `tests/partition_row_loss_regression.rs` 锚定。

### Schema 变更（GraphSchemas::node_schema）

```
id: Int64 (hash of name，保留——边表外键仍用它)
name: Utf8 (新增，实体名)
label: Utf8 (类型)
properties: Utf8 (JSON)
```

`__graph_nodes` DuckDB 表同步加 `name` 列。向后兼容：旧表无 name 列时 `ALTER TABLE ... ADD COLUMN`（try+忽略已存在错误）。

### Extractor 错误传播

EntityExtractionStream 的 async 块：`provider.complete` 失败或 JSON parse 失败 → 返回 `Err(Execution("Entity extraction failed for row {i}: {e}"))`。注意批内多行——一行的失败即整批失败（fail-fast，与 ErrorMode::Fail 默认一致）。

### Retriever 真多跳

递归 CTE：
```sql
WITH RECURSIVE walk(entity_id, depth) AS (
  SELECT id, 0 FROM __graph_nodes WHERE name = $name
  UNION ALL
  SELECT CASE WHEN e.source_id = w.entity_id THEN e.target_id ELSE e.source_id END, w.depth+1
  FROM walk w JOIN __graph_edges e ON e.source_id = w.entity_id OR e.target_id = w.entity_id
  WHERE w.depth < $depth
)
SELECT DISTINCT n.name, n.label, n.properties
FROM walk w JOIN __graph_nodes n ON n.id = w.entity_id
```

输入仍兼容 int64 id 列；新增 config `entity_name: str`（二选一：entity_name 或上游 entity_id）。

### IndexWriter 错误传播

`Connection::open` Err → Execution error；每条 INSERT 的 `.ok()` → `?`。 DuckDB rust crate 的 execute 返回 Result，直接传播。注意保持 `INSERT OR REPLACE`。

## 陷阱预判

- **hash 冲突**：name hash 成 id 理论碰撞——保持现状（COLLISION 时 INSERT OR REPLACE 覆盖，可接受），不改键策略
- **递归 CTE 的 OR join**：DuckDB 支持递归 CTE + 不等值 join 条件，但性能一般——depth 通常 1-2，可接受
- **旧 parquet 缓存**：2222..._0.parquet 是旧 schema（无 name 列）——重跑前清 target/swanflow_cache 对应文件，或 materialize 键含 schema 版本（out of scope，手动清）
- **Utf8View 分叉**：已修的 cast 逻辑（datasource/mod.rs）覆盖 DuckDBScanExec 路径，不走该路径的 GraphRetrievalStream 直接 StringBuilder 构建，无此问题

## 验证配方

1. 单测：GraphSchemas::node_schema 含 4 列；递归 CTE SQL 模板生成（depth 参数注入）
2. E2E：清 cache → 重跑 graph_rag_flow.yaml → 断言 `__graph_nodes` ≥6 行且 name 列非空、`__graph_edges` ≥6 行
3. 检索 E2E：entity_name='Alice' depth=2 → context 包含 'ACME Corp' 与 'Bob'
4. 回归：`cargo test -p swanflow --lib`（既有 262 全过）
