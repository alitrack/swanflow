## MODIFIED Requirements

### Requirement: Execution Plan Node Interface
The `ExecutionPlan` trait SHALL be implemented by custom nodes using the latest DataFusion 52.x signatures.

#### Scenario: children() returns owned arcs
- **WHEN** the engine calls `children()` on a custom `ExecutionPlan`
- **THEN** it SHALL return a `Vec<Arc<dyn ExecutionPlan>>` (owned) instead of a `Vec<&Arc<...>>`.

### Requirement: Scalar and Expression Literals
The system SHALL support the updated `ScalarValue` and `Expr::Literal` API requirements.

#### Scenario: Literal creation with FieldMetadata
- **WHEN** creating an `Expr::Literal` for a constant value
- **THEN** the call SHALL provide an `Option<FieldMetadata>` as required by the DataFusion 52.x API.
