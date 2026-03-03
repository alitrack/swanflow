## 1. Standardized Graph Foundation

- [x] 1.1 Refactor `src/nodes/ai/graph.rs` to use `GraphSchemas::node_schema()` and `edge_schema()` consistently.
- [x] 1.2 Implement `TableSchemaValidatorNode` in `src/nodes/transform.rs` with AI-assisted mapping logic.
- [x] 1.3 Add unit tests for `TableSchemaValidator` to ensure 100% compliance with Golden Schema.

## 2. Intelligence Hook System (Self-Healing)

- [x] 2.1 Define `IntelligenceHook` trait in `src/core/engine.rs`.
- [x] 2.2 Implement `ErrorHook` within `execute_workflow` loop to capture `DataFusionError`.
- [x] 2.3 Integrate `ConfigWizard` with `ErrorHook` to propose and apply one-time SQL/Config fixes.
- [x] 2.4 Add integration tests for "SQL Syntax Self-Healing" scenario.

## 3. Semantic Cleaning & Dynamic Routing

- [x] 3.1 Implement `ProfilerNode` to calculate null ratios and data distributions.
- [x] 3.2 Implement `SemanticCleanerNode` using the "Sample-Analyze-Apply" pattern.
- [x] 3.3 Implement `AiRouterNode` to support dynamic branching based on profiler metrics.
- [x] 3.4 Add tests for "Sampling-Based Semantic Mapping".

## 4. Deep Research MetaNode

- [x] 4.1 Implement `McpInvokeNode` to handle tool calls via MCP (Google Search, etc.).
- [x] 4.2 Create `ResearchLoopMetaNode` in `src/nodes/ai/graph.rs` to orchestrate recursive search and extraction.
- [x] 4.3 Implement parallel fetch logic with hard caps (`max_pages`, `timeout`).
- [x] 4.4 Create a complex example workflow `examples/deep_research_demo.yaml` combining all intelligence features.
