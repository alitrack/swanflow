## Context

Running untrusted AI-generated code requires strict isolation. Previous attempts using standard Docker containers shared the host kernel, posing a security risk. BoxLite provides hardware-level Micro-VM isolation. SwanFlow needs to bridge its Rust-based data fabric with this isolated Python environment.

## Goals / Non-Goals

**Goals:**
- Provide a `BoxedPythonNode` that runs code in a Micro-VM.
- Use S3 as the exclusive data exchange layer between SwanFlow and the Micro-VM.
- Automate the generation of S3 pre-signed URLs for both input and output.
- Ensure the sandbox is ephemeral and destroyed after each workflow step.

**Non-Goals:**
- Supporting interactive shells inside the VM.
- Managing long-term persistence within the VM (everything must go to S3).
- Supporting GPU passthrough in this phase.

## Decisions

### 1. Python Sidecar for VM Management
- **Decision**: Implement a standalone Python service using the BoxLite SDK, exposed as an MCP server.
- **Rationale**: BoxLite's primary stable API is in Python. Wrapping it in MCP allows SwanFlow (Rust) to leverage its existing `McpInvokeNode` infrastructure to control VMs without maintaining complex Rust-to-Python-C bindings.

### 2. S3 as the Secure Air-gap
- **Decision**: All data transfers (Input Parquet -> Output Parquet) must go through S3.
- **Rationale**: Hardware VMs cannot easily share memory or local filesystems with the host. S3 provides a clean, secure, and cloud-native air-gap. Using Pre-signed URLs ensures the sandbox never sees long-term credentials.

### 3. DuckDB HTTPFS for Performance
- **Decision**: Pre-install DuckDB with the `httpfs` extension in the sandbox image.
- **Rationale**: This allows the Python code to query S3 Parquet files directly as if they were local tables, minimizing startup latency and VM memory footprint.

## Risks / Trade-offs

- **[Risk]** Network latency for S3 transfers → **[Mitigation]** In local development, use MinIO on the same machine to simulate S3 at disk speeds.
- **[Risk]** Large result sets exhausting VM memory → **[Mitigation]** Enforce strict output limits and encourage the use of DuckDB for aggregations inside the VM.
- **[Risk]** Zombie VMs on crash → **[Mitigation]** Sidecar will implement a heartbeat and "Reaper" logic to kill VMs whose parent process has died.
