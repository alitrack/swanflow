## ADDED Requirements

### Requirement: Recursive Deep Research MetaNode
The system SHALL provide a `ResearchLoopMetaNode` that implements an agentic research loop by orchestrating multiple child nodes (McpInvoke, HttpFetch, EntityExtractor, GraphIndexWriter).

#### Scenario: Recursive research depth
- **WHEN** the `ResearchLoopMetaNode` is given a query and a `max_depth` of 2
- **THEN** it SHALL execute an initial search and, based on findings, automatically trigger a second level of more granular searching.

### Requirement: Concurrent Fetching with Hard Caps
The system SHALL execute web fetch tasks in parallel but MUST adhere to a hard `max_pages_per_step` and `timeout` per page to prevent resource exhaustion.

#### Scenario: Parallel fetch cap
- **WHEN** a search returns 50 URLs
- **AND** the `max_pages_per_step` is 10
- **THEN** only the top 10 URLs SHALL be fetched and processed concurrently.
