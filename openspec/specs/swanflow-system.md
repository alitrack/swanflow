# SwanFlow System Specification

## 1. Overview
SwanFlow is a modular, high-performance, Rust-native workflow engine for data science. It provides a visual-node-based interface for complex data pipelines, leveraging Apache DataFusion for lazy, global-optimized execution and Apache Arrow for zero-copy memory management.

## 2. Core Modules

### 2.1 Node Protocol (`nodes`)
Defines the `NodeBehavior` trait and `NodeSpec` for self-describing, pluggable nodes (SQL, Python, MetaNode, etc.).

### 2.2 Workflow Graph (`core/graph`)
Manages the DAG of `NodeInstance` and `EdgeInstance` using `petgraph`. Supports recursive `MetaNode` nesting.

### 2.3 Workflow Compiler (`core/compiler`)
Recursively inlines/flattens the logical DAG into a single DataFusion `LogicalPlan`. Optimizes cross-node boundaries.

### 2.4 Execution Observer (`core/observer`)
Uses a reactive, non-blocking `StateProbe` (Custom physical operator) to broadcast node execution states (Traffic Lights) via `tokio::sync::mpsc`.

## 3. Tech Stack
- **Language:** Rust
- **Engine:** Apache DataFusion (LogicalPlan/PhysicalPlan)
- **Memory:** Apache Arrow (RecordBatch/Zero-copy)
- **Graph:** `petgraph`
- **Concurrency:** `tokio` (Async/Await, Channels)
- **Serialization:** `serde`, `serde_json` (OpenSpec compatibility)

## 4. Implementation Plan

### Phase 1: Core Foundation & Spec Definitions
1.  Initialize Rust workspace and `Cargo.toml`.
2.  Define `NodeBehavior` trait and `NodeSpec` (OpenSpec format).
3.  Implement `petgraph`-based logical graph with `MetaNode` support.

### Phase 2: Compiler & Execution
1.  Implement `WorkflowCompiler` with recursive inlining (Static Inlining).
2.  Integrate DataFusion `SessionContext` for plan generation.
3.  Develop `StateProbe` custom operator and `ExecutionObserver`.

### Phase 3: Essential Nodes & Validation
1.  Implement `SqlNode` (proposing `LogicalPlan` from queries).
2.  Implement `MetaNode` (transparent subgraph logic).
3.  Develop a CLI runner to verify DAG execution and observer state updates.

### Phase 4: AI & Extended Capability
1.  Implement "Smart Node" generation via LLM-assisted configuration.
2.  Add Python support via PyO3.
3.  Introduce MCP (Model Context Protocol) bridging for external tool nodes.

## 5. Success Criteria
- **Performance:** Outperform KNIME in large-dataset IO and CPU-bound tasks.
- **Optimization:** Confirm Predicate Pushdown propagates through MetaNode boundaries.
- **Reliability:** 100% accurate traffic-light state updates without calculation stalls.

## 6. Functional Requirements

### Requirement: Efficient Cycle Detection
The `Workflow` implementation SHALL provide an efficient mechanism for detecting cycles when adding edges, with a complexity not exceeding $O(N + E)$, and without cloning the entire graph for each addition.

#### Scenario: Cycle detection on edge addition
- **WHEN** an edge is added that would create a directed cycle in the workflow graph
- **THEN** the addition SHALL fail with an appropriate error message and the graph MUST remain unchanged.

### Requirement: Optimized Workflow Compilation
The `WorkflowCompiler` SHALL perform compilation of a logical workflow into a DataFusion plan with efficient topological sorting and variable-dependency detection ($O(N+E)$ complexity).

#### Scenario: Variable dependency ordering
- **WHEN** a node `B` depends on a variable `${context.x}` produced by node `A`
- **THEN** the compiler SHALL ensure node `A` is compiled and its variables updated before node `B`.

### Requirement: Node Event Error Reporting
The `NodeEvent` stream SHALL accurately report errors encountered during the physical execution of each node's plan, including specific error messages from the DataFusion or Python engines.

#### Scenario: Execution error broadcast
- **WHEN** a `SqlNode` fails due to an invalid query during physical execution
- **THEN** a `NodeEvent::Error` SHALL be broadcast with the specific error message and node ID.
## ADDED Requirements

### Requirement: Shared Engine State
The system SHALL maintain a shared, thread-safe execution state using `Arc<SessionContext>` to ensure all components (Compiler, Engine, and Custom Operators) have a consistent view of the DataFusion catalog and configuration.

#### Scenario: Catalog consistency across tasks
- **WHEN** a custom operator registers a new table in the `SessionContext` within a background task
- **THEN** that table SHALL be immediately visible to the main engine thread and subsequent workflow stages without manual synchronization loops.
