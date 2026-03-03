## Context
Current RAG implementations in `rustflow` are primarily vector-based. To support complex multi-hop reasoning, we need a knowledge graph infrastructure. This design introduces atomic nodes for graph extraction, indexing, and retrieval, leveraging DuckDB for storage and Arrow for data exchange.

## Goals / Non-Goals
**Goals:**
- Implement `ai.entity_extractor` for LLM-driven graph discovery.
- Implement `io.graph_index_writer` for persistent DuckDB storage.
- Implement `ai.graph_retriever` for relationship-aware context generation.
- Establish a "Table-as-Graph" data protocol using Arrow RecordBatch.
- Ensure 100% test coverage for graph construction and retrieval workflows.

**Non-Goals:**
- Complex graph visualization (Sigma.js integration is out of scope for the engine).
- Advanced graph algorithms (e.g., PageRank, Louvain) in the initial release.

## Decisions

### 1. The "Table-as-Graph" Protocol
Nodes will output and accept graph data as two distinct RecordBatch ports:
- **Nodes Port**: `[id: Int64, label: Utf8, properties: Utf8(JSON)]`
- **Edges Port**: `[source_id: Int64, target_id: Int64, type: Utf8, weight: Float64]`
- **Rationale**: This allows users to use existing relational nodes (SQL, Transform) to filter or join graph data, while keeping the engine core simple.

### 2. DuckDB System Tables
We will reserve two tables in the materialization database: `__graph_nodes` and `__graph_edges`.
- **Rationale**: DuckDB is already integrated and provides transactional integrity. Using standard tables simplifies backup, recovery, and inspection.

### 3. Entity Extractor Implementation
The `ai.entity_extractor` will use a structured output prompt (JSON mode) to force the LLM to return a list of nodes and edges. It will handle the mapping from text chunk IDs to graph metadata.
- **Rationale**: Standardizes extraction across different LLM providers (OpenAI, Claude, Ollama).

### 4. Graph Retriever with CSR Fallback
The `ai.graph_retriever` will query DuckDB for relevant graph fragments. For deep traversals (multi-hop), it will load these fragments into an in-memory CSR (Compressed Sparse Row) structure using the `petgraph` crate for maximum speed.

## Risks / Trade-offs
- **[Risk]**: Duplicate Entities (Entity Resolution). → **Mitigation**: Initial implementation will use exact string hashing for IDs. We will add a `transform.graph_merger` node later for more advanced deduplication.
- **[Risk]**: DuckDB write contention. → **Mitigation**: The engine's staged execution model ensures that materializers at the same level run concurrently but the final commit to system tables is handled through a serialized sync phase.
