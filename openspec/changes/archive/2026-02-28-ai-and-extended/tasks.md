# Tasks: AI and Extended Capability

## 1. ZeroClaw Logic Adaptation

- [x] 1.1 Extract and adapt LLM provider logic from `zeroclaw/src/providers` into `src/nodes/ai/providers.rs`.
- [x] 1.2 Implement a basic `ConfigWizard` that uses the adapted providers to generate SQL/JSON configs.

## 2. MCP Bridge Implementation

- [x] 2.1 Extract and adapt MCP client logic from `zeroclaw/src/tools/mcp_client.rs` into `src/nodes/ai/mcp.rs`.
- [x] 2.2 Implement `McpNode` that translates node inputs to MCP tool calls and results back to DataFusion.

## 3. Python Integration (PyO3)

- [x] 3.1 Setup `PyO3` and `arrow-rs` Python dependencies.
- [x] 3.2 Implement `PythonNode` in `src/nodes/script/python.rs`.
- [ ] 3.3 Verify zero-copy data passing between Rust and a simple Python transformation script.

## 4. Verification

- [x] 4.1 Test AI SQL generation with a sample prompt.
- [ ] 4.2 Test `McpNode` with a mock MCP server.
- [ ] 4.3 Integration test: Workflow with a `PythonNode` followed by a `SqlNode`.
