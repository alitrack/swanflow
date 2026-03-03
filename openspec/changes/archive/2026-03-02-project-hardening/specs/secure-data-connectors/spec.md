## ADDED Requirements

### Requirement: Parameterized SQL Support
The system SHALL support parameterized queries for both `SqlNode` and `DbConnectorNode` to prevent SQL injection vulnerabilities.

#### Scenario: SQL injection prevention
- **WHEN** a user provides a dynamic query filter via a variable
- **THEN** the system SHALL execute the query using bind parameters rather than string concatenation.

### Requirement: Secure Credential Handling
The system SHALL ensure that sensitive information, such as database passwords in connection URLs, is not stored or transmitted in plain text within the logical workflow configuration.

#### Scenario: Sensitive URL protection
- **WHEN** a `DbConnectorNode` is configured with a URL containing a password
- **THEN** the system SHALL provide a mechanism (e.g., environment variable expansion or a secret store) to resolve the full URL at runtime without exposing it in the `Workflow` JSON.
