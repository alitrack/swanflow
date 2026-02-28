# SwanFlow (Rust-Native Data Science Engine)

SwanFlow is a high-performance, modular data science workflow engine inspired by KNIME, but built from the ground up in Rust using **Apache DataFusion** and **Apache Arrow**.

## Core Features
- **Performance**: Transparent MetaNode expansion and global plan optimization.
- **Asynchronous**: Non-blocking node status monitoring (Traffic Lights).
- **Industrial Grade**: Native support for Parquet and Excel (via `calamine` and `rust_xlsxwriter`).
- **Smart**: AI-assisted node configuration and MCP tool bridging.
- **Declarative**: Entire pipelines defined in human-readable YAML/JSON.

## Quick Start

1. **Install Rust Toolchain**:
   ```bash
   curl https://sh.rustup.rs -sSf | sh
   ```

2. **Generate Sample Data**:
   ```bash
   cargo run --example generate_data_xlsx
   cargo run --example generate_parquet
   python3 generate_data.py
   ```

3. **Run a Workflow**:
   ```bash
   cargo run -- run tutorials/01_basic_data_flow.yaml
   ```

## Documentation & Learning
For detailed tutorials covering each node type, please see [TUTORIALS.md](TUTORIALS.md).

---

## Technical Stack
- **Engine**: Apache DataFusion
- **Memory**: Apache Arrow
- **Graph**: `petgraph`
- **Async**: `tokio`
- **Serialization**: `serde` / `serde_yaml`
