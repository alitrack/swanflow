## ADDED Requirements

### Requirement: Hybrid Retrieval Contract
The system SHALL support a hybrid retriever node that fuses sparse and semantic candidate scores using configurable weighting.

#### Scenario: Weighted score fusion
- **WHEN** sparse and semantic candidate lists are provided with `alpha`
- **THEN** the node SHALL compute deterministic fused scores
- **AND** return results sorted by fused relevance.

### Requirement: Reranker Contract
The system SHALL support a reranker node that reorders candidate contexts for a query.

#### Scenario: Query-aware reranking
- **WHEN** candidate contexts and a query are provided
- **THEN** the node SHALL compute deterministic rerank scores
- **AND** return top-k ordered candidates.
