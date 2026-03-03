## Context

The current SwanFlow implementation has several architectural bottlenecks and security risks:
1. `MaterializingStream` performs blocking IO in `poll_next`, potentially stalling the async executor.
2. `SqlNode` and `DbConnectorNode` are vulnerable to SQL injection due to lack of parameterization.
3. Catalog synchronization relies on manual loops and explicit deregistration, which is error-prone.
4. Database credentials are often exposed in plain text in connection URLs.

## Goals / Non-Goals

**Goals:**
- Eliminate all blocking IO from the primary async execution path.
- Hardening SQL execution against injection attacks via parameterization.
- Simplify catalog management using shared `Arc<SessionContext>` and atomic registrations.
- Provide a secure mechanism for handling database credentials.
- Throttling telemetry events to maintain receiver stability.

**Non-Goals:**
- Implementing a full-blown secret store (KMS).
- Refactoring the entire DataFusion physical planning logic.
- Changing the logical graph representation (`petgraph`).

## Decisions

### 1. Asynchronous Parquet Materialization
We will use `tokio::task::spawn_blocking` within the `MaterializingStream`'s finalization logic to offload Parquet writing and DuckDB metadata updates. This ensures that the `poll_next` method remains non-blocking.
- **Alternative**: Using `tokio::fs` directly. However, the `parquet` crate and `duckdb` are primarily synchronous, so `spawn_blocking` is the standard approach for these libraries in a tokio environment.

### 2. Parameterized SQL Node
The `SqlNode` config will be updated to support a `params` field. We will use a placeholder syntax (e.g., `$1`, `$2`) in the query and bind values at execution time.
- **Alternative**: Sanitization via regex. This is extremely insecure and not recommended for production systems.

### 3. Shared State via `Arc<SessionContext>`
We will strictly enforce the use of a single `Arc<SessionContext>` passed from `SwanFlowEngine` to `WorkflowCompiler` and down to custom operators like `MaterializerExec`.
- **Alternative**: Cloning `SessionContext`. As discovered during implementation, cloning creates isolated catalogs, leading to "table not found" errors.

### 4. Secret Masking and Environment Expansion
`DbConnectorNode` will support `${ENV_VAR}` expansion in its connection URLs. The engine will expand these variables only at the physical planning stage, keeping the logical configuration clean of plain-text passwords.

## Risks / Trade-offs

- **[Risk]**: `spawn_blocking` overhead. → **Mitigation**: Materialization usually happens at the end of a node's execution (on `Poll::Ready(None)`), so the overhead is negligible compared to the execution time.
- **[Risk]**: Breaking change for `SqlNode`. → **Mitigation**: Provide clear documentation and migration path for existing workflows using string concatenation.
- **[Risk]**: Deadlocks in nested `block_on`. → **Mitigation**: Replace all `futures::executor::block_on` with proper `async` methods or `tokio::task::spawn_blocking` if necessary.
