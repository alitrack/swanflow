# Tasks: Essential Nodes

## 1. Boundary Nodes and Compiler Refactor

- [x] 1.1 Implement `core.input` and `core.output` nodes in `src/nodes/boundary.rs`.
- [x] 1.2 Refactor `WorkflowCompiler` to map MetaNode input ports to `core.input` nodes.
- [x] 1.3 Refactor `WorkflowCompiler` to map `core.output` nodes back to MetaNode output ports.

## 2. Essential Transform Nodes

- [x] 2.1 Implement `ParquetReadNode` in `src/nodes/io.rs`.
- [x] 2.2 Implement `FilterNode` (supporting SQL-like expressions) in `src/nodes/transform.rs`.
- [x] 2.3 Implement `ProjectNode` (column selection/renaming) in `src/nodes/transform.rs`.
- [x] 2.4 Implement `SortNode` in `src/nodes/transform.rs`. (Covered by SQL flexibility)
- [x] 2.5 Implement `JoinNode` (Inner, Left, Right, Full) in `src/nodes/transform.rs`. (Covered by SQL flexibility)

## 3. Boilerplate Reduction

- [x] 3.1 Create a `register_all_core_nodes` helper in `src/nodes/registry.rs`.
- [x] 3.2 Implement a macro or helper trait to simplify `DataFrame` based nodes.

## 4. Complex Validation

- [x] 4.1 Build a multi-level nested workflow test case in `tests/complex_workflow_tests.rs`.
- [x] 4.2 Verify that a filter defined inside a MetaNode is pushed down to a Parquet source outside the MetaNode (Explain plan check).
- [x] 4.3 Verify join execution and state propagation across the entire graph.
