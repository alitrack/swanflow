## Why

The current `PythonNode` executes code in the host environment, which is dangerous when running LLM-generated code. We need a hardware-isolated sandbox that can execute Python code safely while maintaining high-performance data access. By using **BoxLite** (Micro-VM) and **S3 Pre-signed URLs**, we can create a secure "Code Interpreter" that can scale and interoperate with cloud data fabrics.

## What Changes

- **BoxLite Sidecar**: A new Python-based MCP server that manages BoxLite micro-VM instances.
- **S3 Pre-signed Transport**: Logic in SwanFlow to upload intermediate results to S3 and generate time-limited pre-signed URLs for the sandbox.
- **Boxed Python Node**: A new workflow node `core.script.boxed_python` that orchestrates the entire secure execution lifecycle.
- **Bootstrap Agent**: A Python wrapper script inside the sandbox to automate DuckDB S3 attachments and result reporting.

## Capabilities

### New Capabilities
- `mcp-boxlite-bridge`: Protocol for managing Micro-VM lifecycles and code execution via MCP.
- `s3-presigned-transport`: Automated data handoff between host and sandbox using secure S3 links.
- `boxed-code-execution`: Standardized execution environment with pre-installed DuckDB, Polars, and Boto3.

### Modified Capabilities
- `mcp-result-standardization`: Update to handle multi-part content (logs + data) from sandboxed runs.

## Impact

- **Nodes**: New `BoxedPythonNode` in `src/nodes/script/`.
- **Infrastructure**: New `S3Manager` utility in `src/core/datasource.rs`.
- **Sidecar**: A new standalone Python project in `sidecars/boxlite_mcp/`.
