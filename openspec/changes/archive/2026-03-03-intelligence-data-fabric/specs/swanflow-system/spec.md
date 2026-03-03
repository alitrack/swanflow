## ADDED Requirements

### Requirement: Global Intelligence Hooks
The `SwanFlowEngine` SHALL support a system of "Intelligence Hooks" that allow external AI logic to be injected into the execution flow.

#### Scenario: Registering an Intelligence Hook
- **WHEN** the engine is initialized with an `ErrorHook`
- **THEN** any node failure SHALL be passed to that hook before the engine terminates.

### Requirement: Context-Aware Node Reconfiguration
The engine SHALL allow hooks to modify node configurations and update global context variables (`${context.*}`) during execution.

#### Scenario: Dynamic context update
- **WHEN** an AI node updates `${context.model_choice}` to "gpt-4o"
- **THEN** all downstream nodes SHALL have their `${context.model_choice}` references resolved to "gpt-4o" immediately.
