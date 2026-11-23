# Tasks

- [x] Grep crates/ for existing FNV / source-hash / marked-block facilities (none found)
- [x] Read reference impl (fast-anything src/backend.rs:324-390) and pin its invariants
- [x] Implement `crates/swanflow/src/core/verified_swap.rs` (markers, strip/append, source_hash, hash_tree, SwapGate)
- [x] Declare `pub mod verified_swap;` in `crates/swanflow/src/core/mod.rs`
- [x] 13 unit tests: byte-for-byte round-trip (LF/CRLF/empty/no-trailing-newline), upsert, NotPresent, unbalanced=Err, sibling blocks, mid-line marker decoys, hash order-independence/content+path drift/mtime-independence, gate drift refusal
- [x] `cargo test -p swanflow --lib verified_swap -- --test-threads=1` green
- [x] clippy: no new lint mentioning verified_swap
- [x] rustfmt idempotent (two passes + --check exit 0)
- [x] Full serial lib regression `cargo test -p swanflow --lib -- --test-threads=1` green
- [x] Archive this change; single-concern commit (module + mod.rs + archived spec), push
