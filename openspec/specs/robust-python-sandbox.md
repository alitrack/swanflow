# Robust Python Sandbox Specification

## 1. Overview
SwanFlow provides a zero-copy Python integration for advanced data transformations. To ensure security, the execution environment must be restricted to prevent unauthorized system access or network communication.

## 2. Requirements

### Requirement: Restricted Python Imports
The system SHALL provide a mechanism to restrict Python imports in the `core.script.python` node. By default, it MUST block access to dangerous modules such as `os`, `sys`, `subprocess`, `socket`, `requests`, `urllib`, `http`, `pickle`, `marshal`, and `code`.

#### Scenario: Blocked import attempt
- **WHEN** a user provides a script containing `import os`
- **THEN** the system SHALL raise an execution error and prevent the script from running.

### Requirement: Python Script Execution Sandbox
The system SHALL wrap the user-provided Python script in a controlled environment that ensures the `transform(batch)` function is correctly called and its result is returned as an Apache Arrow RecordBatch.

#### Scenario: Valid script execution
- **WHEN** a user provides a script that performs a calculation and returns a `PyRecordBatch`
- **THEN** the system SHALL successfully execute the script and produce the transformed output.

#### Scenario: Script without return value
- **WHEN** a user provides a script that does not return a value
- **THEN** the system SHALL raise an error indicating that the `transform` function must return a result.
