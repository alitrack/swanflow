## Context

RAG and tool-augmented pipelines need a deterministic baseline verifier before adopting heavier model-driven verification.

## Design

- Add verification node in `src/nodes/ai/verification.rs`.
- Keep NodeBehavior plan pass-through, with deterministic helper method for claim verification.
- Enforce strict config validation.

## Rollout

1. Add failing tests for verification behavior and registration.
2. Implement verifier node with deterministic overlap scoring.
3. Run targeted regressions and finalize artifacts.
