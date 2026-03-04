## Context

The 40-item AI operator blueprint is used as an auxiliary reference, not a mandatory all-at-once implementation list. This change prioritizes M0 gaps that can quickly improve practical engine capability.

## Design

- Keep nodes small and contract-first.
- Reuse existing `core.ai.mcp_invoke`, Graph nodes, and universal IO where possible.
- Add deterministic testable contracts for citation and structured output.

## Rollout

1. Add shared contracts and change scaffolding.
2. Implement first-wave nodes with TDD.
3. Validate with integration tests and archive change.
