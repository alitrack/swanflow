# Design: AI and Extended Capability

## 1. AI Provider Adaptation (from ZeroClaw)
SwanFlow will integrate a simplified version of ZeroClaw's provider system. 
- **Location:** `src/nodes/ai/providers.rs`
- **Goal:** Support Anthropic, OpenAI, and Gemini for node configuration tasks.
- **Workflow:** UI triggers an LLM request → LLM returns JSON config → SwanFlow validates against `NodeSpec::config_schema`.

## 2. McpNode (Model Context Protocol)
We will implement an `McpNode` that uses a shared `McpRegistry` (borrowed from ZeroClaw).
- **Function:** A special node type that maps an MCP tool to a SwanFlow node.
- **Mapping:** MCP Tool Arguments ↔ Node Configuration; MCP Tool Result ↔ Output Dataframe (JSON-to-Arrow conversion).

## 3. Python Integration (PyO3)
- **Engine:** Use `pyo3` to embed a Python interpreter.
- **Data Exchange:** Use `arrow` crate's FFI capabilities to pass `RecordBatch` pointers to Python's `pyarrow` without copying.
- **Node:** `PythonNode` will execute a user-provided script snippet.

## 4. Modified Node Protocol
Update `NodeSpec` to include an `ai_metadata` field, allowing AI to understand the context of a node when generating suggestions.
