# SwanFlow (Rust-Native Data Science Engine)

SwanFlow is a high-performance, modular data science engine designed to surpass the limitations of Pandas and KNIME. Built entirely in Rust using **Apache DataFusion 52.1.0** and **Apache Arrow 57.3.0**, it delivers industrial-grade performance with a "Late-Binding" architecture and a "White-box Autopilot" AI experience.

## 🚀 Key Differentiators

- **Late-Binding SQL Recompiler**: Defer SQL parsing until physical planning time. This allows true "Decoupled SQL" where nodes can reference each other via `SELECT * FROM node_uuid` without explicit graph edges.
- **Universal Connector (Dual-Driver)**: High-performance vectorized ingress via **DuckDB Scanners** (Postgres, SQLite) and broad database connectivity via **SQLx Any** (MySQL, MSSQL).
- **Universal Read (qv-style)**: A single intelligent node that auto-detects formats (Parquet, CSV, JSON) and handles directory-based data lakes with recursive scanning and Hive-partition discovery.
- **Zero-Copy Python (Bridge 2.0)**: Use `pyo3-arrow` and Arrow's C Data Interface (FFI) to pass data between Rust and Python libraries (Pandas, Scikit-learn) with **zero serialization overhead**.
- **Pro Excel Dashboards**: Generate multi-sheet, boardroom-quality Excel reports with professional styling, automatic charts, and native Excel Tables.
- **Intelligent Analyst Agent**: A built-in reasoning loop that autonomously plans, validates, and self-corrects data workflows based on natural language intent.

## 🛠 Core Architecture

- **Compute**: Apache DataFusion 52 (LogicalPlan/PhysicalPlan optimization).
- **Memory**: Apache Arrow 57 (Memory-safe, zero-copy streaming).
- **Lakehouse Index**: Automatic materialization into content-addressable Parquet files with metadata indexed in a local DuckDB catalog.
- **Observability**: Asynchronous "Traffic Light" status (Red/Yellow/Green) via non-blocking Physical Plan Probes.

## 🚦 Quick Start

### 1. Prerequisites
Ensure you have the Rust toolchain and Python dependencies installed:
```bash
# Install DuckDB shared library
bash setup_duckdb.sh

# Install Python zero-copy dependencies
pip install pyarrow pandas
```

### 2. Run a Tutorial
Execute one of our 19 curated tutorials:
```bash
# High-Performance Postgres/SQLite Scanner
cargo run -- run tutorials/17_database_connector.yaml

# Lakehouse SQL (Decoupled referencing)
cargo run -- run tutorials/13_lakehouse_sql.yaml

# Universal JSON & Directory Globbing
cargo run -- run tutorials/18_universal_json_glob.yaml

# Conditional Formatting & Excel Tables
cargo run -- run tutorials/14_conditional_formatting.yaml
```

### 3. AI Autopilot
Start the Analyst Agent to build workflows via natural language:
```bash
# Requires LLM_API_KEY in .env
# Example Prompt: "Connect to postgres, join users with sales.parquet, and highlight outliers in a red Excel table"
```

## 📚 Documentation
- **[TUTORIALS.md](TUTORIALS.md)**: Deep dive into all 30+ node types and advanced flow control.
- **[ARCHITECTURE.md](docs/plans/2026-02-28-swanflow-architecture-design.md)**: Design philosophy and technical specification.

---

## Technical Stack
- **Engine**: Apache DataFusion 52.1.0
- **FFI Bridge**: PyO3 0.26 / pyo3-arrow 0.14
- **Reporting**: rust_xlsxwriter / calamine
- **Database**: DuckDB 1.4.4 / SQLx 0.8
- **Graph Logic**: `petgraph` / `tokio`
