## Context

SwanFlow aims to outsource complex cognition to external agents via MCP. Currently, we only have a spec-level definition of MCP nodes. To make them functional, we need a runtime client that handles process spawning, line-delimited JSON-RPC messaging, and result extraction.

## Goals / Non-Goals

**Goals:**
- Implement a `McpClient` that manages a child process (stdio transport).
- Implement basic MCP lifecycle: `initialize` -> `call_tool`.
- Update `McpInvokeNode` to perform real tool calls during execution.
- Map MCP results (usually a list of content blocks) into a standard Arrow RecordBatch.

**Non-Goals:**
- Implementing MCP Server capabilities (SwanFlow is only a client).
- Supporting SSE (Server-Sent Events) or WebSocket transports in this phase (focus on local stdio).
- Complex resource/prompt management (focus only on `call_tool`).

## Decisions

### 1. Stdio Transport via Tokio Framed
- **Decision**: Use `tokio::process` to spawn servers and `tokio_util::codec` (LinesCodec) for line-based JSON-RPC messaging.
- **Rationale**: MCP stdio transport is inherently line-delimited. Using `Framed` simplifies bidirectional asynchronous communication.
- **Alternatives**: Raw `Read`/`Write` loops. *Rejected* due to complexity in handling partial lines.

### 2. Ephemeral vs Persistent Clients
- **Decision**: For the MVP, `McpInvokeNode` will spawn a new server process for each execution and terminate it after completion.
- **Rationale**: Simplifies state management and avoids dangling processes in this initial phase.
- **Alternatives**: Global connection pool. *Deferred* to future performance optimization.

### 3. Result Mapping Logic
- **Decision**: The `McpInvokeNode` will flatten MCP `CallToolResult` (text/image/json) into a Utf8 column named `content`.
- **Rationale**: Provides maximum flexibility for downstream AI nodes (like `EntityExtractor`) to parse the results.

## Risks / Trade-offs

- **[Risk]** Hanging subprocesses → **[Mitigation]** Use `tokio::spawn` with a timeout and ensure the `Child` process is killed on drop or error.
- **[Risk]** Slow startup time for Node.js/Python servers → **[Mitigation]** This is a known trade-off for the ephemeral process model; persistent connections will be Phase 2.
- **[Risk]** Version mismatch in JSON-RPC → **[Mitigation]** Stick strictly to the MCP 1.0 specification.
