## Why

Currently, SwanFlow is a high-performance execution engine, but it lacks "intelligence" to handle the inherent messiness of real-world data science: broken SQL queries, inconsistent semantic data, and the need for deep, iterative research. By introducing an "Intelligence Data Fabric," we transform SwanFlow from a passive executor into a self-healing, research-capable production line that outsources complex cognition to external Agents (via MCP) while maintaining high-performance data sovereignty in Rust.

## What Changes

- **Intelligence Interceptor**: A middleware layer in the engine to catch errors and trigger AI-based self-healing.
- **Table Schema Validator**: A new core node to normalize messy Agent outputs into standardized Arrow RecordBatches.
- **Deep Research MetaNode**: A pre-configured subgraph that orchestrates MCP search, parallel fetching, and graph indexing.
- **Semantic Cleaning Engine**: A sampling-based AI utility to generate high-speed Rust/Regex cleaning rules.
- **Dynamic Routing**: Logic to allow LLMs to modify workflow paths based on real-time data profiling.

## Capabilities

### New Capabilities
- `self-healing-execution`: Protocols for automatic error capture, LLM-based repair, and single-attempt retry logic.
- `semantic-data-refining`: Sampling-based strategy for generating deterministic cleaning rules from AI insights.
- `dynamic-workflow-routing`: Mechanisms for LLMs to inspect data profiles and redirect execution paths.
- `standardized-graph-protocol`: A unified schema (Node/Edge) and validation node for all graph-related operations.
- `deep-research-orchestration`: A MetaNode pattern for recursive, agentic web research integrated with Graph RAG.

### Modified Capabilities
- `swanflow-system`: Update core engine requirements to support Intelligence Hooks (Error/Quality/Routing).

## Impact

- **Core Engine**: `src/core/engine.rs` will be modified to include the `IntelligenceHook` system.
- **Node Registry**: `src/nodes/registry.rs` will include new validator, profiler, and router nodes.
- **Graph Logic**: `src/nodes/ai/graph.rs` will be extended to support the standardized protocol and new research nodes.
- **Dependencies**: Integration with `dotenvy` and enhanced MCP tool invocation.
