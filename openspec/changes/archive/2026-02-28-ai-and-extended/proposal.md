## Why
To empower SwanFlow with AI-driven flexibility and extensibility. We need to support natural language configuration of nodes, integration with external AI tools via MCP (Model Context Protocol), and advanced scripting capabilities via Python.

## What Changes
- Implement a **Smart Configurator** that uses LLMs to generate node configurations (e.g., SQL queries) from natural language.
- Implement `McpNode` to bridge SwanFlow with external MCP servers, allowing nodes to call AI-powered tools.
- Implement `PythonNode` using PyO3 to allow custom data transformations in Python.
- Integrate ZeroClaw's provider logic for multi-LLM support.

## Capabilities

### New Capabilities
- `ai-assisted-configuration`: Natural language interface for node setup.
- `mcp-integration`: Ability to use MCP servers as workflow nodes.
- `python-scripting`: Native Python transformation support via Arrow zero-copy.

### Modified Capabilities
- `node-protocol`: Updated to support AI metadata and configuration triggers.

## Impact
This change makes SwanFlow a "Smart" engine. It introduces dependencies on PyO3 and potentially ZeroClaw's internal modules or logic patterns.
