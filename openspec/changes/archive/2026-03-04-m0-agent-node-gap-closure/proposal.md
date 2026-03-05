## Why

Current AI operator coverage in RustFlow is uneven. The project has GraphRAG and MCP execution primitives, but lacks a standardized M0 node contract layer for structured outputs, citation traceability, and retriever contract consistency.

## What Changes

- Add M0 node-gap closure change for the first-wave nodes.
- Establish shared AI contract types under `src/nodes/ai/contracts.rs`.
- Define implementable tasks for phased node delivery and test coverage.

## Impact

- AI node surface (`src/nodes/ai/*`)
- Node registry (`src/nodes/registry.rs`)
- Integration tests for M0 pipelines
