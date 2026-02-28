# Role: Lead Architect of "SwanFlow" (Rust-native Data Science Engine)

## Context
I am building a high-performance, Rust-native workflow engine inspired by KNIME, but powered by Apache DataFusion and Arrow. The goal is to create a modular, AI-friendly data science platform that outperforms traditional Java-based tools.

## Technical Foundation
- **Compute:** Apache DataFusion (LogicalPlan/PhysicalPlan)
- **Memory:** Apache Arrow (Zero-copy)
- **Data Lake:** DuckLake & Parquet (including pq-vector for vector search)
- **Graph:** petgraph (Directed Acyclic Graph for workflow management)
- **Goal:** Support MetaNodes, sub-graphs, and AI-generated node logic.

## Task
Let's brainstorm the core architecture. I want to focus on how to bridge the gap between "Visual Nodes" and "DataFusion Plans." Please address the following dimensions:

1. **Node Representation:** How should we define a `Node` in Rust so it can hold either a pure SQL transform, a Python script (via PyO3), or an AI-generated closure, while outputting a DataFusion `DataFrame`?
2. **MetaNode Abstraction:** Propose a strategy for "Subgraph-as-a-Node." How do we handle recursive optimization? (e.g., Should the entire workflow be flattened into a single DataFusion plan, or executed node-by-node?)
3. **AI Integration Points:** How can we design a "Smart-Node" that takes natural language and uses it to update the underlying LogicalPlan?
4. **State & Metadata:** How do we handle KNIME's "Traffic Light" system (Configured, Executed, Error) in an async Rust environment?

## Output Format
Please provide a high-level trait design for the Node system and a proposed JSON/YAML schema for the workflow definition that is both human-readable and AI-friendly.