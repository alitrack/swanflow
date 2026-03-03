# Design Document: Graph-RAG Metanode and Atomic Graph Nodes

## Context
Standard Vector RAG often fails to answer high-level thematic questions or perform multi-hop reasoning across documents. GraphRAG, as implemented in systems like EdgeQuake, addresses this by building a knowledge graph of entities and relationships. `rustflow` aims to provide this capability using its existing node-based architecture and DuckDB materialization layer.

## Goals / Non-Goals
**Goals:**
- Provide atomic nodes for document ingestion, entity extraction, graph indexing, and multi-hop retrieval.
- Use DuckDB as the primary persistent storage for graph data (nodes and edges).
- Define a "Table-as-Graph" protocol using Arrow RecordBatch for inter-node communication.
- Support OpenAI-compatible LLMs (with customizable `base_url`) for entity extraction.
- Implement robust automated workflow tests to verify graph construction and retrieval quality.

**Non-Goals:**
- Implementing a standalone graph database engine.
- Supporting non-columnar graph formats between nodes.

## Decisions

### 1. Storage Layer: Embedded DuckDB
We will use DuckDB to store two system-level tables: `__graph_nodes` and `__graph_edges`.
- **Rationale**: `rustflow` already uses DuckDB for materialization. This adds zero new dependencies and provides ACID compliance for graph updates.

### 2. Data Protocol: Table-Centric (Arrow RecordBatch)
Graphs will be passed between nodes as pairs of RecordBatches.
- **Node Schema**: `id: Int64, label: Utf8, properties: Utf8 (JSON)`
- **Edge Schema**: `source_id: Int64, target_id: Int64, type: Utf8, weight: Float64`
- **Rationale**: Aligns with Apache GraphAr standards and allows existing `SqlNode` to process graph data without modification.

### 3. Entity Extraction: OpenAI-Compatible Provider
The `ai.entity_extractor` node will utilize the existing provider abstraction, supporting `base_url` for local LLM integration (Ollama, vLLM).
- **Rationale**: Standardizes LLM interactions while supporting the growing ecosystem of local inference servers.

### 4. Retrieval Strategy: Multi-Mode Graph Search
The `ai.graph_retriever` node will support:
- **Local Search**: Retrieval of the immediate neighborhood of entities found in the query.
- **Global Search**: (Future) Community-based retrieval for thematic questions.

## Data Flow
1. **Ingestion**: `io.universal_read` -> `transform.text_splitter` -> Text Chunks.
2. **Extraction**: Text Chunks -> `ai.entity_extractor` -> Node/Edge Tables.
3. **Indexing**: Node/Edge Tables -> `io.graph_index_writer` -> DuckDB.
4. **Query**: User Query -> `ai.entity_extractor` (for keywords) -> `ai.graph_retriever` -> Context.
5. **Fusion**: (Vector Context + Graph Context) -> `ai.context_fuser` -> Final Context -> LLM.

## Risks / Trade-offs
- **[Risk]**: Entity Resolution (Merging similar entities). → **Mitigation**: Implement a `transform.graph_merger` node that uses simple fuzzy matching or LLM-based deduplication.
- **[Risk]**: Performance of deep graph traversals in DuckDB. → **Mitigation**: `graph_retriever` will load relevant graph fragments into a memory-efficient CSR structure for fast multi-hop search.

## Verification Plan
- **Building Test**: `tests/rag_build_test.rs` - Verify full chain from raw text to DuckDB graph tables.
- **Retrieval Test**: `tests/rag_query_test.rs` - Verify that multi-hop relationships are correctly retrieved as context.
- **Concurrency Test**: `tests/rag_concurrency_test.rs` - Verify multiple materializers can update the graph index safely.
