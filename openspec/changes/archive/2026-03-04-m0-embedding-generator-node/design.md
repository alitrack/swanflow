## Context

M0 vector workflow currently starts at writer/retriever contracts, but misses an explicit embedding generation operator.

## Design

- Introduce `EmbeddingGeneratorNode` in `retrieval.rs`.
- Use deterministic hash-based vectorization for local, offline-safe behavior.
- Validate dimensions and config boundaries strictly.

## Rollout

1. Add failing tests for node behavior and registration.
2. Implement minimal embedding generator and register it.
3. Run targeted regression tests and finalize change artifacts.
