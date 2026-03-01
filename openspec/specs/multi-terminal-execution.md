# Multi-Terminal Execution Specification

## 1. Overview
Complex workflows often require writing data to multiple destinations (e.g., CSV, SQL, Excel) simultaneously. The engine must support concurrent execution of all terminal nodes in the DAG.

## 2. Requirements

### Requirement: Concurrent Terminal Execution
The system SHALL identify all terminal nodes (nodes with no outgoing edges or `core.output` nodes) in a workflow and execute them concurrently using DataFusion's execution engine.

#### Scenario: Workflow with multiple write nodes
- **WHEN** a workflow contains two `core.io.csv_write` nodes that are not connected to any subsequent nodes
- **THEN** both nodes SHALL be executed and their respective CSV files generated.

### Requirement: Unified Error Broadcasting
The system SHALL capture errors from any of the terminal execution paths and broadcast them through the `NodeEvent` stream to notify the user.

#### Scenario: Single terminal failure
- **WHEN** one of two terminal write nodes fails due to a permission error
- **THEN** the system SHALL broadcast an error event for the failing node while allowing other nodes to complete if possible.
