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

## Verification Evidence (2026-03-04)

- `cargo test --test m0_structured_io_tests -- --nocapture` -> pass (2/2)
- `cargo test --test m0_citation_tracker_tests -- --nocapture` -> pass (2/2)
- `cargo test --test m0_retrieval_nodes_tests -- --nocapture` -> pass (3/3)
- `cargo test --test m0_vector_contract_tests -- --nocapture` -> pass (3/3)
- `cargo test --test m0_tool_executor_tests -- --nocapture` -> pass (3/3)
- `cargo test --test m0_universal_resolver_tests -- --nocapture` -> pass (1/1)
- `cargo test --test m0_rag_basic_integration --test m0_rag_tool_integration -- --nocapture` -> pass (2/2)
- `cargo test --test compiler_tests --test engine_tests --test stability_hardening_tests -- --nocapture` -> pass (11/11)

## Known Non-Goals

- No full MCP runtime orchestration changes beyond output normalization contract.
- No new UI/operator-panel work; this change targets Rust engine node contracts only.
- No optimization on retrieval quality ranking; query planning/chunking stay minimal and deterministic.
