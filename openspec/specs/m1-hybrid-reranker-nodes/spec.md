# m1-hybrid-reranker-nodes Specification

## Purpose
TBD - created by archiving change m1-hybrid-reranker-nodes. Update Purpose after archive.
## Requirements
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

#### Scenario: Backend selection contract
- **WHEN** reranker config sets `backend` to `rule`, `model`, or `llm`
- **THEN** the node SHALL accept the value and execute backend-specific ranking logic
- **AND** unknown backend values SHALL be rejected by validation.

#### Scenario: Model backend HTTP call with resilience
- **WHEN** reranker config sets `backend=model` and provides `model_endpoint`
- **THEN** the node SHALL call the endpoint with timeout and retry controls
- **AND** fallback to deterministic ranking only when `fail_open=true`.

