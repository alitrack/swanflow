# SwanFlow Tutorials & Documentation

Welcome to SwanFlow! This guide will help you understand how to use the various nodes and features of the engine through declarative YAML workflows.

## Getting Started

Before running the tutorials, generate the sample data files and setup dependencies:
```bash
# Setup DuckDB
bash setup_duckdb.sh

# Python dependencies
pip install pyarrow arro3-core pandas

# Generate sample data
cargo run --example generate_data_xlsx
cargo run --example generate_parquet
python3 generate_data.py
```

## Tutorial List

### 01: Basic Data Flow
**File:** `tutorials/01_basic_data_flow.yaml`  
**Concepts:** CSV Reading, Filtering, SQL Transformations.

### 02: Excel Processing
**File:** `tutorials/02_excel_processing.yaml`  
**Concepts:** High-performance Excel reading (`calamine`) and writing (`rust_xlsxwriter`).

### 03: Logic Encapsulation (MetaNodes)
**File:** `tutorials/03_metanode_logic.yaml`  
**Concepts:** Sub-workflows, `core.input` and `core.output` boundary nodes, recursive static inlining.

### 06: Python Integration (Zero-Copy)
**File:** `tutorials/06_python_integration.yaml`  
**Concepts:** PyO3 integration, Arrow C Data Interface (zero-copy). Performance optimized.

### 08: Advanced Analytics (Aggregate & Window)
**File:** `tutorials/08_analytical_analytics.yaml`  
**Concepts:** GroupBy, Aggregations, Window Functions (Ranking).

### 09: Professional Excel Reports
**File:** `tutorials/09_excel_reports.yaml`  
**Concepts:** Multi-sheet reporting, professional styling, and automatic charts.

### 10: Dynamic Parameterization
**File:** `tutorials/10_dynamic_parameterization.yaml`  
**Concepts:** Global Context, `${context.key}` template interpolation.

### 11: Table to Variable
**File:** `tutorials/11_table_to_variable.yaml`  
**Concepts:** Extracting variables from data during compilation (Control Flow promotion).

---

## Node Reference

| Node Type ID | Name | Description |
| :--- | :--- | :--- |
| `core.io.csv` | CSV Reader | Reads standard CSV files. |
| `core.io.parquet` | Parquet Reader | Columnar reading via DataFusion. |
| `core.io.excel_read` | Excel Reader | Reads .xlsx via calamine. |
| `core.io.excel_write` | Excel Writer | Professional dashboards with styling/charts. |
| `core.io.duckdb_read` | DuckDB Reader | SQL queries on DuckDB or cloud datasets. |
| `core.io.http_fetch` | HTTP Fetch | Fetch JSON data from APIs. |
| `core.transform.filter` | Filter | SQL-expression based row filtering. |
| `core.transform.aggregate` | Aggregate | GroupBy and multi-column aggregation. |
| `core.transform.window` | Window | Analytical window functions. |
| `core.transform.profiler` | Data Profiler | Statistical metrics (min, max, nulls, unique). |
| `core.transform.pivot` | Pivot Table | Group by rows, spread a column into one column per distinct value, aggregate (sum/avg/min/max/count). |
| `core.flow.condition` | Condition (IF) | Logical branching based on SQL expressions. |
| `core.flow.foreach` | ForEach Loop | Iterate over a list of values. |
| `core.flow.variable_creator` | Var Creator | Manually define flow variables. |
| `core.flow.table_to_variable` | Table to Var | Promote data rows to control variables. |
| `core.flow.extension` | DB Plugin | Load DuckDB extensions (httpfs, spatial). |
| `core.script.python` | Python Script | Custom Python logic (High-performance Zero-copy). |
| `core.input` / `core.output` | Ports | Entrance/Exit for MetaNode subgraphs. |

---

## How to Run a Tutorial
```bash
cargo run -- run tutorials/<filename>.yaml
```
Use the `--help` flag for more options.
