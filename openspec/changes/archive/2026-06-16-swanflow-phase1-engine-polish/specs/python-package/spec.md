# Spec: Python Package

## Purpose

让 Python 用户可以通过 `pip install swanflow` 直接使用 Rust 引擎，实现 `import swanflow` 即可运行 YAML 工作流和 Python DAG。

## Contract

- `pip install swanflow` → 下载预编译 wheel（无需本地 Rust 工具链）
- `swanflow.run("workflow.yaml")` → 运行 YAML 工作流
- `swanflow.DAG()` → Python 构建 DAG 并执行
- `.to_pandas()` → zero-copy 通过 Arrow C Data Interface
- 支持 Linux x86_64 + macOS ARM

## API Surface

```python
import swanflow

# 运行 YAML 工作流
result = swanflow.run("workflows/analysis.yaml")

# 运行 YAML 字符串
yaml_str = """
tasks:
  - id: src
    kind: databaseSource
    params: { url: "postgres://..." }
  - id: q
    kind: sql
    params: { query: "SELECT * FROM src" }
"""
result = swanflow.run_yaml(yaml_str)

# Python DAG API
dag = swanflow.DAG()
src = dag.add_source(url="postgres://...")
q = dag.add_sql("SELECT * FROM src")
dag.connect(src, q)
result = dag.run()

# 结果提取
df = result.to_pandas()     # zero-copy
table = result.to_arrow()   # PyArrow
json = result.to_json()
```

## Build Matrix

| Platform | Rust Target | Status |
|----------|-------------|--------|
| Linux x86_64 | x86_64-unknown-linux-gnu | ✅ CI |
| macOS ARM | aarch64-apple-darwin | ✅ Manual |
| macOS x86_64 | x86_64-apple-darwin | ⏳ |
| Windows | x86_64-pc-windows-msvc | ⏳ Phase 2 |
