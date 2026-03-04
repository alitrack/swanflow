## Context

PII detection alone is insufficient for production safety because outbound content still needs masking.

## Design

- Add `PiiMaskerNode` in `src/nodes/ai/privacy.rs`.
- Generate deterministic placeholder tokens for detected spans.
- Provide reversible de-mask helper based on emitted replacements.

## Rollout

1. Add failing tests for mask/de-mask behavior and registration.
2. Implement masking and restoration helpers with config guardrails.
3. Run targeted regressions and finalize artifacts.
