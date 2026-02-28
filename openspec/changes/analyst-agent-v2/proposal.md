## Why
To create a truly industrial-grade AI data analyst that outperforms existing tools like PandasAI and ChatExcel. By implementing a "Hybrid Agent Loop," SwanFlow will enable autonomous workflow planning, structural self-correction, and high-performance execution on large datasets.

## What Changes
- Implement the **Hybrid Reasoning Loop**: Support for macro planning (full YAML generation) and atomic correction (node-by-node patching).
- Implement **Structural Self-Correction**: The agent will receive structured error feedback from the compiler's `infer_metadata` pass and automatically fix its plan.
- Integrate **DuckDB Extensions and DataFusion UDFs**: Empower the agent to load plugins (e.g., `httpfs`, `spatial`) or write custom Python logic when standard nodes aren't enough.
- Add **Statistical Insight Tools**: Give the agent the ability to "see" data distributions before generating analysis logic.

## Capabilities

### New Capabilities
- `hybrid-agent-orchestration`: A multi-tier reasoning loop for planning and fixing workflows.
- `statistical-profiling`: Automated data distribution and quality assessment for AI context.
- `dynamic-extensibility-control`: Agent-driven loading of DuckDB extensions and registration of Python UDFs.

### Modified Capabilities
- `streaming-preview`: Updated to include statistical summaries.
- `data-materialization`: Integrated with the agent's "Draft-Verify-Execute" cycle.

## Impact
- **Agent System**: Major upgrade to `src/agent/mod.rs` based on ZeroClaw patterns.
- **Compiler**: Added support for incremental graph updates during the agent loop.
- **Token Efficiency**: Dramatic reduction in context usage by passing schemas/stats instead of raw rows.
