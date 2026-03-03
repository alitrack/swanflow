## 1. Asynchronous Materialization & Telemetry

- [x] 1.1 Refactor `MaterializingStream::poll_next` in `src/core/observer.rs` to use `tokio::task::spawn_blocking` for Parquet writing and DuckDB metadata recording.
- [x] 1.2 Implement a throttled event emission mechanism in `StateProbeStream` to prevent receiver overwhelm.
- [x] 1.3 Ensure `register_parquet` calls in `MaterializingStream` are correctly awaited or tracked within the engine's execution task.

## 2. Secure SQL & Data Access

- [x] 2.1 Update `SqlNode` config and `SqlProxyLogical` in `src/nodes/sql.rs` to support optional query parameters (`params`).
- [x] 2.2 Refactor `DbConnectorNode::propose_plan` in `src/nodes/io.rs` to expand `${ENV_VAR}` placeholders in connection URLs.
- [x] 2.3 **BREAKING**: Implement parameter binding logic in `SqlxScanExec` within `src/core/datasource.rs` to support safe SQL execution.

## 3. Core Engine & Shared State Hardening

- [x] 3.1 Audit `SwanFlowEngine::execute_workflow` in `src/core/engine.rs` to ensure all `SessionContext` modifications are thread-safe and consistently visible.
- [x] 3.2 Implement a more robust `Retry Sync` mechanism that doesn't rely on fixed `tokio::time::sleep` durations.
- [x] 3.3 Replace all remaining `futures::executor::block_on` calls in `src/nodes/io.rs` with proper `async` patterns.

## 4. Verification

- [x] 4.1 Create a security-focused integration test verifying SQL injection prevention in `SqlNode`.
- [x] 4.2 Create a performance benchmark test to verify non-blocking behavior of `MaterializingStream` under heavy load.
- [x] 4.3 Verify secure credential expansion using environment variables in a multi-node workflow.

