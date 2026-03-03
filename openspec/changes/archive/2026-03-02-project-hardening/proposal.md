## Why

A comprehensive review of the SwanFlow engine reveals significant opportunities for hardening across five key dimensions: logical correctness, performance, security, maintainability, and testability. Currently, synchronous blocking IO in async contexts, potential SQL injection risks, and tight coupling in materialization logic present risks to production readiness and scalability. Implementing these hardening measures now will ensure a robust foundation for future AI-native data capabilities.

## What Changes

- **Asynchronous Observer IO**: Refactor `MaterializingStream` to use `tokio::fs` and asynchronous database interactions, eliminating blocking calls in `poll_next`.
- **Parameterized SQL Execution**: **BREAKING** Update `SqlNode` and `DbConnectorNode` to support parameterized queries, mitigating SQL injection risks.
- **Unified Catalog Management**: Refactor `SwanFlowEngine` to provide a truly idempotent and thread-safe catalog synchronization mechanism, removing manual deregistration loops.
- **Secure Configuration Management**: Implement a mechanism to handle sensitive connection strings (e.g., database passwords) securely, avoiding plain-text exposure in configuration.
- **Enhanced Execution Traceability**: Optimize `StateProbeStream` to use buffered or throttled events to prevent receiver overwhelm in high-throughput scenarios.

## Capabilities

### New Capabilities
- `async-staged-execution`: Formalizes the level-by-level staged execution logic with proper async IO and materialization synchronization.
- `secure-data-connectors`: Defines requirements for secure credential handling and parameterized query support across all database connectors.
- `high-performance-telemetry`: Specifies requirements for efficient, non-blocking workflow observation and state reporting.

### Modified Capabilities
- `swanflow-system`: Update system-wide requirements for shared state management and catalog consistency using `Arc<SessionContext>`.

## Impact

- **Affected Code**: `src/core/engine.rs`, `src/core/compiler.rs`, `src/core/observer.rs`, `src/nodes/sql.rs`, `src/nodes/io.rs`.
- **APIs**: BREAKING change to SQL node configuration to support parameters.
- **Dependencies**: Potential addition of `tokio-postgres` or enhanced `sqlx` usage for full async database interaction.
