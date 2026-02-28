## 1. Directory Structure
```text
src/
├── core/
│   ├── graph.rs      # Petgraph-based DAG implementation
│   └── mod.rs
├── nodes/
│   ├── mod.rs        # Core NodeBehavior trait, specs, and state definitions
│   ├── registry.rs   # Thread-safe node registry
│   └── common.rs     # Common data types shared across nodes
└── main.rs           # Entry point for the core engine
```

## 2. Key Components

### 2.1 `NodeBehavior` Trait (The Contract)
- **`spec()`**: Returns the `NodeSpec` (OpenSpec schema) for the node.
- **`validate()`**: Validates input configuration against the node's JSON schema.
- **`propose_plan()`**: Build a DataFusion `LogicalPlan` given input plans and config.

### 2.2 `Workflow` Graph (Logical L1)
- Uses `petgraph`'s `StableGraph` to store `NodeInstance` and `EdgeInstance`.
- Supports recursive MetaNode definition by allowing a `NodeInstance` to point to a sub-`Workflow`.

## 3. Data Schema
- **NodeSpec**: JSON/YAML definition for node metadata (ports, config schema).
- **NodeEvent**: Async message enum for Red/Yellow/Green state transitions.

## 4. Dependencies
- `datafusion`: Core query and plan engine.
- `petgraph`: DAG and graph algorithm support.
- `tokio`: Async runtime and channels.
- `serde`/`serde_json`: Configuration and spec serialization.
- `uuid`: Reliable identification for nodes and flows.
- `async-trait`: Enabling asynchronous behavior in traits.
