## Why
SwanFlow needs a solid, high-performance foundation in Rust to manage data pipelines. We must establish the core traits and graph structures early to ensure future modules (Compiler, Observer, Python support) have a unified interface to build upon.

## What Changes
- Initialize the Rust project with high-performance dependencies (DataFusion, Petgraph, Tokio).
- Define the `NodeBehavior` trait as the universal contract for all nodes.
- Implement a thread-safe `NodeRegistry` for dynamic discovery of node types.
- Create a `petgraph`-based `Workflow` graph for logical pipeline management.

## Capabilities

### New Capabilities
- `node-protocol`: The standard interface and specification format for all SwanFlow nodes.
- `workflow-management`: Core logic for managing a Directed Acyclic Graph (DAG) of nodes and edges.

### Modified Capabilities
- (None - Initial system)

## Impact
This is the foundational change. It will affect all future code as it defines the primary traits and data structures for the entire engine.
