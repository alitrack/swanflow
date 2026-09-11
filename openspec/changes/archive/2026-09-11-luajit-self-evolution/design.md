# Design: LuaJIT Self-Evolution

**Change**: luajit-self-evolution
**Date**: 2026-08-12

## 1. 关键前置验证（S1）

- duckdb crate `=1.10503.1`（DuckDB v1.5.3）可 LOAD 对齐 1.5.5 编译的
  luajit v0.30 扩展，`luajit_table` 输出 a/b/c（/tmp/luajit-verify，exit 0）。
- 未签名扩展必须**启动级** `allow_unsigned_extensions`（Config 传入；
  `SET` 在 execute_batch 不生效）。
- 版本映射：`1.10503.1`=1.5.3、`1.10505.0`=1.5.5；宽松版本号会被解析到
  1.10505.0，须 `=` 精确锁定。

## 2. duckdb crate 1.10503 Arrow statement API 陷阱（S2 修复）

- `column_count()`/`column_type()`/`schema()` 走 `result_unwrap()`，查询未执行
  即 panic（`raw_statement.rs:91: The statement was not executed yet`，源码
  `FIXME(mlafeldt)`）。流式表函数（luajit_table）必须执行后读。
- **修复**：`execute_sql` 改为先 `query([])` 执行，再经 `rows.as_ref()`
  （`Rows` 持有 `&mut Statement`）读 `schema()`。
- **类型映射**：`schema()` 返回 duckdb crate 的 arrow-58 Schema，字段 Debug
  格式是 `Int64`/`Utf8`（非 SQL 名 `BIGINT`/`VARCHAR`），原 `duckdb_type_to_str`
  映射表不命中 → 整数列 fallback 成 Utf8。补齐 INT8/16/32/64/UINT 等映射。

## 3. 壳节点执行路径（S3）

```
core.luajit (config: {mode, source, args, columns})
  └─ propose_plan: 生成 SQL
       table:  SELECT * FROM luajit_table('<source>')
       scalar: SELECT * FROM (SELECT luajit_s('<source>', "col") ... FROM input_0)
  └─ DuckDBScanLogical (connection_path=":memory:", extensions=[luajit 路径])
       └─ DuckDBScanExec::execute
            ├─ duckdb::Connection::open (allow_unsigned_extensions)
            ├─ LOAD '<luajit 扩展路径>'（绝对路径直接 LOAD，跳过 INSTALL）
            ├─ query → COPY (format parquet) → 临时文件
            └─ DataFusion read parquet → RecordBatchStream（绕开 arrow 版本冲突）
```

扩展路径解析优先级：
1. `SWANFLOW_LUAJIT_EXT`（显式）
2. `SWANFLOW_DUCKDB_EXTENSIONS` 中含 luajit 的条目
3. 默认 `/mnt/d/wsl2/luajit/build/release/luajit.duckdb_extension`，兜底 `luajit`

## 4. libs 自动注册（S3）

- INDEX 格式：`name|path` 每行（`#` 注释、空行跳过）。
- `luajit_libs_from_index(index_path)` → `Vec<LuaJitLibNode>`，每个节点
  type_id = `luajit.<name>`，config schema 只暴露 mode/args，source 预填 lib 名。
- 注册点：`NodeRegistry::register_core_nodes` 内，INDEX 不存在时静默跳过
  （仅保留通用 `core.luajit` 壳节点）。
- 本机实测：12 个 lib 自动注册（export/etl/html_escape/base64/sudoku/id3/
  dicom/dirscan/uuid/crc32/json/zip_list）。

## 5. create_lua_lib 工具（S4）

- 参数：`lib_name`（清洗为 ascii alnum+下划线）、`source`、`category`（默认 udf）。
- 落盘：libs 根（`SWANFLOW_LUAJIT_LIBS_DIR` > `/mnt/d/wsl2/duckdb-luajit-libs` >
  `~/.duckdb/luajit-libs`）→ `libs/<cat>/<name>.lua`。
- INDEX：存在则 append（`name|` 前缀去重）；不存在则新建。
- 反馈：打印路径 + 两种使用方式（立即 core.luajit / 重启自动注册）。
- 安全：库运行在 luajit 扩展 trusted sandbox（P4，借鉴 pllua）；
  `_duckdb_call`/`_duckdb_query` 走审批门（提示写在 system prompt）。

## 6. 环境变量汇总

| 变量 | 作用 |
|---|---|
| `SWANFLOW_DUCKDB_EXTENSIONS` | 逗号分隔扩展路径，open 后自动 LOAD（S2） |
| `SWANFLOW_LUAJIT_EXT` | 显式指定 luajit 扩展路径（壳节点/发现 schema） |
| `SWANFLOW_LUAJIT_LIBS_DIR` | libs 仓库根（create_lua_lib 写盘位置） |
| `SWANFLOW_LUAJIT_LIBS_INDEX` | INDEX 路径（自动注册扫描） |
