## ADDED Requirements

### Requirement: LLM-Driven Information Gap Discovery
The system SHALL use an LLM to analyze the current state of the extracted Graph Index and identify "information gaps" or missing links required to satisfy the research goal.

#### Scenario: Generating follow-up queries
- **WHEN** the current graph contains the node "Node.js" and the edge "USES" pointing to an unknown entity
- **THEN** the LLM SHALL generate a targeted search query like "Which runtime does Node.js use for its core event loop?" to fill the gap.

### Requirement: Weighted Relevance for Loop Triggering
The system SHALL only trigger a new research iteration if the generated clues exceed a configurable relevance threshold, ensuring the loop focuses on high-value information.

#### Scenario: Terminating low-value research
- **WHEN** the LLM identifies only trivial or already known facts as the next clues
- **THEN** the system SHALL stop the recursion and return the final graph result.
