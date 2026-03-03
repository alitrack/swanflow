## 1. Internal Infrastructure & Protocols

- [x] 1.1 Define standard RecordBatch schemas for Node and Edge ports in `src/nodes/mod.rs`.
- [x] 1.2 Implement utility functions for string-to-Int64 hashing (entity resolution) in `src/nodes/common.rs`.
- [x] 1.3 Update DuckDB initialization in `src/core/engine.rs` to ensure `__graph_nodes` and `__graph_edges` tables exist.

## 2. Graph Construction Nodes

- [x] 2.1 Implement `ai.entity_extractor` in `src/nodes/ai/mod.rs` with JSON-mode LLM prompt.
- [x] 2.2 Implement `io.graph_index_writer` in `src/nodes/io.rs` for idempotent DuckDB updates.
- [x] 2.3 Implement `transform.graph_merger` for basic entity normalization and deduplication.

## 3. Graph Retrieval & Fusion

- [x] 3.1 Implement `ai.graph_retriever` in `src/nodes/ai/mod.rs` supporting 1-hop neighbor traversal.
- [x] 3.2 Implement `ai.context_fuser` to merge vector and graph retrieval outputs.
- [x] 3.3 Register all new graph nodes in `NodeRegistry`.

## 4. Verification & Hardening

- [x] 4.1 Create `tests/rag_build_test.rs` to verify full ingestion-to-index workflow.
- [x] 4.2 Create `tests/rag_query_test.rs` to verify retrieval accuracy and context generation.
- [x] 4.3 Fix async deadlocks in `poll_next` for EntityExtractor and GraphIndexWriter.
- [x] 4.4 Benchmark multi-hop retrieval performance with varying graph sizes.
