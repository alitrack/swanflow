# SwanFlow (Rust-Native Data Science Engine)

SwanFlow is a high-performance, modular data science engine designed to surpass the limitations of Pandas and KNIME. Built entirely in Rust using **Apache DataFusion** and **Apache Arrow**, it delivers industrial-grade performance with a "White-box Autopilot" AI experience.

## 🚀 Key Differentiators

- **Zero-Copy Python (Bridge 2.0)**: Use `pyo3-arrow` and Arrow's C Data Interface (FFI) to pass data between Rust and Python libraries (Pandas, Scikit-learn) with **zero serialization overhead**.
- **Intelligent Analyst Agent**: A built-in Re-Act reasoning loop that can autonomously plan, validate, and self-correct data workflows based on natural language intent.
- **Pro Excel Dashboards**: Generate multi-sheet, boardroom-quality Excel reports with professional styling, automatic charts, and native Pivot Tables.
- **Transparent MetaNodes**: High-level subgraphs that expand during compilation, allowing DataFusion's optimizer to push predicates and projections across boundaries.
- **Variable Management**: A clean, template-based parameter system (`${context.key}`) that eliminates the visual "red-line" clutter found in legacy tools.
- **DuckDB Powered**: Native high-performance SQL analysis and cloud data fetching via DuckDB's extension ecosystem.

## 🛠 Core Architecture

- **Compute**: Apache DataFusion (LogicalPlan/PhysicalPlan optimization).
- **Memory**: Apache Arrow (Memory-safe, zero-copy streaming).
- **Observability**: Asynchronous "Traffic Light" status (Red/Yellow/Green) via non-blocking Physical Plan Probes.
- **Caching**: Smart materialization using SHA256 content-addressable Parquet checkpoints.

## 🚦 Quick Start

### 1. Prerequisites
Ensure you have the Rust toolchain and Python dependencies installed:
```bash
# Install DuckDB shared library
bash setup_duckdb.sh

# Install Python zero-copy dependencies
pip install pyarrow arro3-core pandas
```

### 2. Run a Tutorial
Execute one of our 11 curated tutorials:
```bash
# Basic ETL
cargo run -- run tutorials/01_basic_data_flow.yaml

# Zero-Copy Python
cargo run -- run tutorials/06_python_integration.yaml

# Dynamic Parameterization
cargo run -- run tutorials/10_dynamic_parameterization.yaml
```

### 3. AI Autopilot
Start the Analyst Agent to build workflows via natural language:
```bash
# Requires LLM_API_KEY in .env
# Example Prompt: "Analyze sales.csv, calculate monthly growth, and export a Pro Excel dashboard"
```

## 📚 Documentation
- **[TUTORIALS.md](TUTORIALS.md)**: Deep dive into all 20+ node types and advanced flow control.
- **[ARCHITECTURE.md](docs/plans/2026-02-28-swanflow-architecture-design.md)**: Design philosophy and technical specification.

---

## Technical Stack
- **Engine**: Apache DataFusion 45.0
- **FFI Bridge**: PyO3 0.23 / pyo3-arrow 0.7
- **Reporting**: rust_xlsxwriter / calamine
- **Database**: DuckDB 1.4.4 (System Linked)
- **Graph Logic**: `petgraph` / `tokio`
