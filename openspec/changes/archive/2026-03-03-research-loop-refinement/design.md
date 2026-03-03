## Context

SwanFlow now has the capability to call MCP tools and extract entities. However, these operations are currently linear. True deep research requires an iterative approach where the output of one extraction phase (e.g., "Company X uses Technology Y") triggers a new search phase (e.g., "What is Technology Y?"). We need to implement this feedback loop within the `ResearchLoopMetaNode`.

## Goals / Non-Goals

**Goals:**
- Implement an iterative execution model for `ResearchLoopMetaNode`.
- Use an LLM to generate "next-step" queries based on current Graph Index state.
- Deduplicate queries and URLs to ensure research efficiency.
- Limit recursion depth and total resource usage.

**Non-Goals:**
- Real-time visualization of the loop (CLI logs only for now).
- Implementing a persistent database outside of DuckDB.
- Handling web search without MCP (MCP is the strictly enforced transport).

## Decisions

### 1. BFS-style Iteration (Breadth-First)
- **Decision**: Research will proceed in stages. Level 0: User Query -> URL List -> Initial Entities. Level 1: New Clues -> More URLs -> More Entities.
- **Rationale**: BFS is easier to cap and monitor for resource usage compared to DFS (Depth-First). It ensures a broad initial understanding before diving too deep into any single niche.

### 2. The "Gap Analysis" Prompt
- **Decision**: After each ingestion step, the MetaNode will call an LLM with a summary of the current graph and ask: "Based on this graph, what 3 keywords are missing to fully understand the topic?"
- **Rationale**: This leverages the LLM's reasoning to drive the research direction dynamically rather than following a hard-coded script.

### 3. Integrated State (Visited Set)
- **Decision**: Maintain an `HashSet<String>` of processed URLs and Queries in the MetaNode's execution state.
- **Rationale**: Prevents infinite loops where two topics point to each other and redundant API calls.

## Risks / Trade-offs

- **[Risk]** Context Window Overflow → **[Mitigation]** Instead of passing full webpage text, only pass the extracted Graph Summary (Nodes/Edges) back to the LLM for gap analysis.
- **[Risk]** High API Costs → **[Mitigation]** Impose a strict `max_depth` (default 2) and `max_total_pages` (default 15).
- **[Risk]** Hallucinated Search Queries → **[Mitigation]** Log all generated queries to the console so the user can see if the agent is deviating from the original topic.
