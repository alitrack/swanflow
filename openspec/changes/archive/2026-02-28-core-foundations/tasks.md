# Tasks: Core Foundations

## 1. Project Initialization

- [x] **Task 1.1: `cargo init` and `Cargo.toml` Setup**
  - Initialize the Rust project.
  - Add `datafusion`, `tokio`, `petgraph`, `serde`, `serde_json`, `async-trait`, `uuid`.
  - Set up a clean `src/` layout with `core` and `nodes` subdirectories.

## 2. Core Traits and Metadata

- [x] **Task 1.2: Define Node Trait and Specifications**
  - Implement `NodeSpec`, `PortSpec`, `NodeState`, and `NodeEvent` in `src/nodes/mod.rs`.
  - Implement the `NodeBehavior` trait.

- [x] **Task 1.3: Implement Node Registry**
  - Create a thread-safe `NodeRegistry` in `src/nodes/registry.rs`.
  - Support node registration and type-based retrieval.

## 3. Workflow Graph (Logical L1)

- [x] **Task 1.4: Graph Structure and Management**
  - Implement `Workflow`, `NodeInstance`, and `EdgeInstance` in `src/core/graph.rs`.
  - Use `petgraph::StableGraph` for DAG management.
  - Implement node/edge addition/deletion logic.

- [x] **Task 1.5: MetaNode Metadata Support**
  - Add support for a `sub_workflow` field within `NodeInstance`.
  - Verify node connectivity and basic cycles (DAG validation).

## 4. Verification

- [x] **Task 1.6: Unit Tests for Core Foundation**
  - Test node registration/retrieval.
  - Test building a 3-node graph and verify connectivity.
  - Test MetaNode nesting structure.
