## Why
Traditional Vector RAG struggles with multi-hop reasoning and thematic queries. By implementing atomic graph nodes, `rustflow` will enable users to build sophisticated GraphRAG workflows that extract and index entities and relationships, providing a more comprehensive knowledge context for AI-native data analysis.

## What Changes
- **Entity Extraction**: New `ai.entity_extractor` node utilizing LLMs to discover nodes and edges from text chunks.
- **Graph Indexing**: New `io.graph_index_writer` node to persist extracted graph data into system-level DuckDB tables.
- **Multi-Hop Retrieval**: New `ai.graph_retriever` node for navigating relationships within the indexed knowledge graph.
- **Context Fusion**: New `ai.context_fuser` node to merge and re-rank results from vector and graph retrieval paths.
- **Workflow Verification**: Add comprehensive integration tests (`tests/rag_build_test.rs`, `tests/rag_query_test.rs`) to ensure RAG quality and stability.

## Capabilities

### New Capabilities
- `entity-extraction`: Requirements for discovering and normalizing nodes and edges from unstructured text.
- `graph-indexing`: Requirements for persistent storage and idempotent updates of property graphs in DuckDB.
- `graph-retrieval`: Requirements for multi-hop neighbor search and relationship-aware context generation.
- `context-fusion`: Requirements for merging heterogeneous context sources (vector/graph) into a final LLM prompt.

### Modified Capabilities
- `swanflow-system`: Update system requirements to support "Table-as-Graph" protocols across node boundaries.

## Impact
- **Affected Code**: `src/nodes/mod.rs`, `src/nodes/registry.rs`, `src/core/datasource.rs`.
- **New Files**: `src/nodes/ai/graph.rs`, `src/nodes/io/graph.rs`, `tests/rag_build_test.rs`.
- **APIs**: New RecordBatch schemas for Node and Edge ports.
