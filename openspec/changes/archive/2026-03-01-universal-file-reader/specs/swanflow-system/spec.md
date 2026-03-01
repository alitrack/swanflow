## MODIFIED Requirements

### Requirement: Node Registration
The `rustflow` registry SHALL include the `UniversalReadNode` under the `core.io` namespace.

#### Scenario: Verify universal reader presence
- **WHEN** the engine initializes
- **THEN** the `core.io.universal_read` node SHALL be available for selection.
