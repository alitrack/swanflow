## Why

The current `McpInvokeNode` is a schema-only placeholder that does not perform actual tool calls. To enable effective Deep Research and agentic workflows, SwanFlow needs a robust runtime implementation that can communicate with Model Context Protocol (MCP) servers via stdio and JSON-RPC. This will allow the engine to leverage real-world tools like Google Search, Brave Search, and custom local tools.

## What Changes

- **MCP Client Core**: Implement a JSON-RPC client capable of managing lifecycle and messaging with MCP servers.
- **Stdio Transport**: Add support for spawning and communicating with local MCP server processes via stdin/stdout.
- **Runtime McpInvokeNode**: Fully implement the execution logic for `McpInvokeNode`, converting tool results into standardized Arrow RecordBatches.
- **Tool Discovery & Registration**: A mechanism to define MCP server configurations (path, args, env) in the workflow.

## Capabilities

### New Capabilities
- `mcp-stdio-transport`: Low-level protocol for process management and bidirectional communication with local servers.
- `mcp-json-rpc-protocol`: Implementation of MCP-compliant JSON-RPC messaging (requests, responses, notifications).
- `mcp-result-standardization`: Automated conversion of diverse JSON tool outputs into the standard Graph Data Protocol.

### Modified Capabilities
- `standardized-graph-protocol`: Extend validator logic to specifically handle raw JSON strings from MCP outputs.

## Impact

- **Nodes**: `src/nodes/ai/mcp.rs` will be significantly expanded with runtime logic.
- **Core Engine**: Integration with process management for MCP server lifecycles.
- **Dependencies**: Add `tokio-util` (for Framed read/write) and `serde_json` enhancements.
