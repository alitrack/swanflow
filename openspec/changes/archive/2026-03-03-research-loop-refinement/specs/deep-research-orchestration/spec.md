## MODIFIED Requirements

### Requirement: Recursive Deep Research MetaNode
The system SHALL provide a `ResearchLoopMetaNode` that implements an agentic research loop by orchestrating multiple child nodes (McpInvoke, HttpFetch, EntityExtractor, GraphIndexWriter) and supports recursive execution based on intermediate findings.

#### Scenario: Recursive research depth
- **WHEN** the `ResearchLoopMetaNode` is given a query and a `max_depth` of 2
- **THEN** it SHALL execute an initial search and, based on findings, automatically trigger a second level of more granular searching.
- **AND** it SHALL maintain a "visited" set to avoid infinite loops.
