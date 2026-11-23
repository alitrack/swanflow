# Tasks

- [x] Evidence: dangling rollback pointer located (deployer.rs:117 sets pointer, only rollback() writes history-)
- [x] Evidence: E2E hand-writes the history file today ("Simulate the previous manifest snapshot")
- [x] `verify_local_sources` extracted; deploy() gates before MCP connect; rollback() gates before first update_flight
- [x] `archive_outgoing` byte-exact archive; deploy() calls it before writing the new manifest
- [x] `programs_hash` sealed into DeployManifest (serde default → old manifests load)
- [x] `deployer::check` via SwapGate; missing seal = explicit error; CLI `deploy --check`
- [x] E2E rewritten: automatic-archive assertions replace the hand-written simulation
- [x] New E2E: drift refusal with zero MCP calls; check() refusal matrix
- [x] Gates: deploy E2E serial green, clippy 0 new lints, rustfmt idempotent, full serial lib regression green
- [x] Archive this change; single-concern commit; push
