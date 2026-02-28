# SwanFlow Tutorials & Documentation

Welcome to SwanFlow! This guide will help you understand how to use the various nodes and features of the engine through declarative YAML workflows.

## Getting Started

Before running the tutorials, generate the sample data files:
```bash
cargo run --example generate_data_xlsx
cargo run --example generate_parquet
python3 generate_data.py
```

## Tutorial List

### 01: Basic Data Flow
**File:** `tutorials/01_basic_data_flow.yaml`  
**Concepts:** CSV Reading, Filtering, SQL Transformations.  
**How to run:**
```bash
cargo run -- run tutorials/01_basic_data_flow.yaml
```

### 02: Excel Processing
**File:** `tutorials/02_excel_processing.yaml`  
**Concepts:** High-performance Excel reading (`calamine`) and writing (`rust_xlsxwriter`).  
**How to run:**
```bash
cargo run -- run tutorials/02_excel_processing.yaml
```

### 03: Logic Encapsulation (MetaNodes)
**File:** `tutorials/03_metanode_logic.yaml`  
**Concepts:** Sub-workflows, `core.input` and `core.output` boundary nodes, recursive static inlining.  
**How to run:**
```bash
cargo run -- run tutorials/03_metanode_logic.yaml
```

### 04: Branching and Parallelism
**File:** `tutorials/04_branching_flow.yaml`  
**Concepts:** DAG branching, multi-node output from a single source, DataFusion parallel execution.  
**How to run:**
```bash
cargo run -- run tutorials/04_branching_flow.yaml
```

### 06: Python Integration
**File:** `tutorials/06_python_integration.yaml`  
**Concepts:** PyO3 integration, Arrow C Data Interface (zero-copy foundation).  
**How to run:**
```bash
cargo run -- run tutorials/06_python_integration.yaml
```

### 07: MCP Tool Bridging
**File:** `tutorials/07_mcp_bridging.yaml`  
**Concepts:** Model Context Protocol integration, calling external AI tools as workflow nodes.  
**How to run:**
```bash
cargo run -- run tutorials/07_mcp_bridging.yaml
```

### 08: Advanced Analytics (Aggregate & Window)
**File:** `tutorials/08_analytical_analytics.yaml`  
**Concepts:** GroupBy, Aggregations, Window Functions (Ranking).  
**How to run:**
```bash
cargo run -- run tutorials/08_analytical_analytics.yaml
```

### 09: Advanced Excel Reports
**File:** `tutorials/09_excel_reports.yaml`  
**Concepts:** Professional Excel output with charts and sheet management.  
**How to run:**
```bash
cargo run -- run tutorials/09_excel_reports.yaml
```

---

## Node Reference

| Node Type ID | Name | Description |
| :--- | :--- | :--- |
| `core.io.csv` | CSV Reader | Reads standard CSV files. |
| `core.io.parquet` | Parquet Reader | High-performance columnar reading. |
| `core.io.excel_read` | Excel Reader | Reads .xlsx/.xls via calamine. |
| `core.io.excel_write` | Excel Writer | Creates .xlsx via rust_xlsxwriter. |
| `core.transform.filter` | Filter | SQL-expression based row filtering. |
| `core.transform.project` | Project | Column selection and renaming. |
| `core.transform.aggregate` | Aggregate | GroupBy and multi-column aggregation. |
| `core.transform.window` | Window | Analytical window functions. |
| `core.sql` | SQL Transform | Full SQL support on input tables. |
| `core.metanode` | MetaNode | Container for a nested sub-workflow. |
| `core.input` | Sub-flow Input | Entrance for data in a MetaNode. |
| `core.output` | Sub-flow Output | Exit for data in a MetaNode. |
| `core.script.python` | Python Script | Custom Python logic (Zero-copy). |
| `core.ai.mcp.*` | MCP Bridge | Call any tool from an MCP server. |
