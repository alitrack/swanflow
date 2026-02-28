# SwanFlow Architecture Design

## 1. Overview
SwanFlow is a high-performance, Rust-native data science engine inspired by KNIME but powered by **Apache DataFusion** and **Apache Arrow**. It aims to provide a modular, AI-friendly environment for data processing with near-native performance.

### Core Principles
- **Performance First:** Leverage DataFusion's query optimizer and Arrow's zero-copy memory.
- **Lazy Evaluation:** Pass `LogicalPlan` (via `DataFrame`) between nodes to maximize cross-node optimization.
- **Transparent Subgraphs:** MetaNodes are expanded during compilation, allowing global optimization across boundaries.
- **Reactive State:** Execution state (Traffic Lights) is decoupled from the calculation using an asynchronous observer pattern.

## 2. Technical Stack
- **Compute:** Apache DataFusion (SQL, DataFrame API).
- **Memory:** Apache Arrow (RecordBatches).
- **Data Persistence:** Parquet, DuckLake.
- **Graph Management:** `petgraph` (Directed Acyclic Graph).
- **Asynchrony:** `tokio` (for scheduling and state updates).

## 3. Component Architecture

### 3.1 Node Representation
Every node implements the `NodeBehavior` trait. Nodes do not perform the final execution but rather build upon the `LogicalPlan`.

```rust
#[async_trait]
pub trait NodeBehavior: Send + Sync {
    fn type_id(&self) -> &str;
    fn validate(&self, config: &NodeConfig) -> Result<()>;
    async fn execute(
        &self,
        ctx: &SessionContext,
        inputs: Vec<Arc<DataFrame>>,
        config: &NodeConfig,
    ) -> Result<Vec<Arc<DataFrame>>>;
}
```

- **SqlNode:** Directly maps a SQL query to `ctx.sql()`.
- **PyO3Node:** Uses Python to transform data via Arrow-native buffers.
- **MetaNode:** A recursive container holding a sub-workflow.

### 3.2 Workflow Compiler
The compiler transforms the DAG into a final DataFusion `LogicalPlan`.

1.  **Topological Sort:** Determines execution order.
2.  **Recursive Expansion:** When a `MetaNode` is encountered, its internal graph is compiled and its output linked to the parent graph.
3.  **Optimization:** DataFusion applies global optimizations (Predicate Pushdown, Projection Pruning) across the entire expanded plan.

### 3.3 Observation System (Traffic Lights)
Instead of blocking execution at every node (like KNIME), SwanFlow uses a **Reactive Observer**:
- **Execution Context:** Carries an `mpsc::Sender` for events.
- **Observation Hooks:** Special "Identity" operators are inserted at key points in the `LogicalPlan` to trigger "Executed" state updates when data flows through them.
- **States:** `Unconfigured` (Grey), `Configured` (Red), `Running` (Yellow), `Executed` (Green).

## 4. Workflow Schema (JSON/YAML)
Designed for both human readability and AI generation.

```yaml
workflow:
  id: "uuid-123"
  nodes:
    - id: "read_1"
      type: "io.parquet"
      config: { path: "data.parquet" }
    - id: "filter_1"
      type: "transform.sql"
      config: { query: "SELECT * FROM input_0 WHERE val > 10" }
      metadata:
        ai_prompt: "Filter for values greater than 10"
  edges:
    - { source: "read_1", target: "filter_1", port: 0 }
```

## 5. Next Steps
1.  **Scaffolding:** Initialize a Rust workspace with `datafusion`, `petgraph`, and `tokio`.
2.  **Core Traits:** Finalize the `NodeBehavior` and `Workflow` types.
3.  **MVP Compiler:** Build a compiler that can flatten a MetaNode into a single DataFusion plan.
4.  **UI/API:** Develop a JSON-RPC or REST interface for graph manipulation and state monitoring.
