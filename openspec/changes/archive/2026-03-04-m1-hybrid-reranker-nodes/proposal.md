## Why

M0 has baseline retrieval chain, but lacks M1 retrieval quality boosters. We need first-class hybrid retrieval and reranking operators in engine contracts.

## What Changes

- Add `core.ai.hybrid_retriever` node.
- Add `core.ai.reranker` node.
- Register both nodes in core registry.
- Add dedicated tests for fusion/ranking/registration/validation.

## Impact

- `src/nodes/ai/retrieval.rs`
- `src/nodes/registry.rs`
- `tests/m1_hybrid_rerank_tests.rs`
