## ADDED Requirements

### Requirement: Micro-VM Lifecycle Control via MCP
The system SHALL provide an MCP server (Sidecar) that exposes tools to create, execute, and destroy BoxLite micro-VM instances.

#### Scenario: Running code in a new VM
- **WHEN** the `run_code` tool is called with `python` code and a base image name
- **THEN** the Sidecar SHALL spawn a new BoxLite VM
- **AND** it SHALL execute the code inside the VM
- **AND** it SHALL return the stdout/stderr as an MCP tool result.

### Requirement: Automatic Resource Reclamation
The Sidecar SHALL automatically terminate any Micro-VM instance after the execution completes or if a specified timeout is reached.

#### Scenario: Timeout enforcement
- **WHEN** a VM task exceeds the `max_execution_time` (e.g., 60 seconds)
- **THEN** the Sidecar SHALL force-kill the VM and return a timeout error result.
