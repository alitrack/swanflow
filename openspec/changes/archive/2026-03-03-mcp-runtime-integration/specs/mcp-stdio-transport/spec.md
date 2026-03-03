## ADDED Requirements

### Requirement: Local Process Lifecycle Management
The system SHALL be able to spawn local executable files or script interpreters (e.g., `node`, `python`) as background processes to act as MCP servers.

#### Scenario: Spawning a node-based MCP server
- **WHEN** provided with a command `node` and argument `./server.js`
- **THEN** the system SHALL successfully spawn the process
- **AND** it SHALL capture the stdin/stdout for communication.

### Requirement: Stdio Bidirectional Communication
The system SHALL implement a non-blocking bidirectional transport layer using stdio pipes, where each message is a single line of text (newline-delimited).

#### Scenario: Sending and receiving a message
- **WHEN** a JSON string is sent to the server's stdin
- **THEN** the system SHALL wait for and read a corresponding JSON response from the server's stdout.
