## Why

The current `ResearchLoopMetaNode` is a static placeholder. To achieve true Deep Research, the system must be able to recursively explore a topic by extracting new "seed keywords" from initial search results and automatically triggering subsequent search/extraction cycles. This change transforms SwanFlow into an autonomous research engine capable of building high-density knowledge graphs from minimal user input.

## What Changes

- **Recursive MetaNode Execution**: Fully implement the execution logic for `ResearchLoopMetaNode` using a multi-step iterative loop.
- **Dynamic Clue Extraction**: Add logic to the `EntityExtractor` or a new `ClueExtractor` node to identify missing information and generate new search queries.
- **Stateful Research Context**: Maintain a "visited" list of URLs and queries within the MetaNode to prevent infinite loops and redundant fetching.
- **Graph Accumulation**: Ensure each iteration merges its findings into the global Graph Index in real-time.

## Capabilities

### New Capabilities
- `recursive-agentic-loop`: Support for MetaNodes that can dynamically re-invoke their own sub-graphs based on intermediate results.
- `information-gap-analysis`: LLM-driven identification of missing entities or relationships to generate follow-up research tasks.

### Modified Capabilities
- `deep-research-orchestration`: Update the requirement to explicitly include recursive depth and feedback mechanisms.

## Impact

- **Nodes**: `src/nodes/ai/graph.rs` will be modified to implement the recursive loop.
- **Core Engine**: Minor updates to handle nested execution contexts if necessary.
- **Specs**: Update `deep-research-orchestration` to include state management requirements.
