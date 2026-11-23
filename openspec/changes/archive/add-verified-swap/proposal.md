# Add core/verified_swap — marked-block swap gated by a source hash

## Why

Research spike from the fast-anything deep dive (claw wiki #2026112311,
2026-11-23). fast-anything's enable/disable cycle encodes three invariants
every "generated artifact goes live only after it passed a mechanical gate"
flow needs:

1. append-only swap, removable byte-for-byte (its CI asserts this with
   `git diff --exit-code` after disable);
2. an FNV-1a source-tree hash seals "verified" so enabling refuses drift
   (its issues #26/#67 were exactly this bypass);
3. an unbalanced marker strip is an error, never silent success (#69).

swanflow has no equivalent facility today (grep: no FNV/source-hash/marked-
block code in `crates/`). The upcoming plugin/approval work (design.md M2)
and any future hot-path replacement need exactly this shape.

## What Changes

- New leaf module `crates/swanflow/src/core/verified_swap.rs` (no engine
  wiring, no root `src/` edits):
  - `markers` / `has_block` / `append_block` (upsert) / `strip_block`
    (line-anchored, CRLF-aware, `StripOutcome::{Restored, NotPresent}`,
    `Err` on unbalanced blocks) — reference: fast-anything
    `src/backend.rs:350-390`, improved with line-anchored marker matching.
  - `source_hash` (FNV-1a 64 over domain + sorted (path, content) pairs) and
    `hash_tree` IO wrapper — reference: fast-anything `src/backend.rs:324`.
  - `SwapGate::seal` / `check` — refuses when current hash != sealed hash.
- 13 unit tests in-file, including the byte-for-byte round-trip over
  LF/CRLF/empty/no-trailing-newline texts, the #69 corruption refusal, and
  hash order-independence.

## Non-goals

- No `insert_marked` (insert at offset) until a call site needs it.
- No marker escaping: bodies must not contain their own marker lines at
  line start.
- No call-site adoption (nodes/plugin host) — that lands via its own change
  when M2 wiring starts.
- FNV-1a is implemented inline (6 lines, domain-separated) rather than
  pulling the `fnv` crate: the ordering/domain semantics are the value here,
  not the primitive.
