# swanflow docs MCP sidecar

Bruin-style documentation MCP server (`bruin mcp` pattern): exposes the SwanFlow
repo's documentation to coding agents over stdio JSON-RPC (MCP 2024-11-05).

**Read-only.** Three tools, no execution capability:

| Tool | What it returns |
|---|---|
| `swanflow_get_overview` | README.md |
| `swanflow_get_docs_tree` | all doc files (docs/, tutorials/, root md/yaml) |
| `swanflow_get_doc_content` | one doc file by repo-relative path |

## Run

```bash
python3 sidecars/docs_mcp/server.py --repo /path/to/swanflow
```

## Verify (zero-dependency probe, 8 checks)

```bash
python3 sidecars/docs_mcp/probe.py
```

## Wire into a client

Claude Code / Codex / Cursor / opencode MCP config:

```json
{
  "mcpServers": {
    "swanflow-docs": {
      "command": "python3",
      "args": ["/abs/path/to/swanflow/sidecars/docs_mcp/server.py", "--repo", "/abs/path/to/swanflow"]
    }
  }
}
```
