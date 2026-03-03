## Context

Currently, SwanFlow operates as a linear, deterministic execution engine. While high-performing, it cannot autonomously recover from execution errors or handle semantic inconsistencies in raw data. The current RAG implementation is also siloed. We need a way to inject "intelligence" at critical junctions—errors, data quality checks, and strategic routing—without sacrificing Rust's performance.

## Goals / Non-Goals

**Goals:**
- Implement a **Self-Healing Hook** in `execute_workflow` to catch and AI-repair SQL/Config errors.
- Create a **TableSchemaValidator** node that uses AI for mapping but Rust for bulk execution.
- Standardize the **Node/Edge Protocol** for graph storage to enable interoperability between all research nodes.
- Implement a **Deep Research MetaNode** that orchestrates MCP-based searching and parallel ingestion.

**Non-Goals:**
- Building a custom LLM model (all intelligence is via external APIs/MCP).
- Implementing a full-blown autonomous agent that can modify the file system directly (only workflow internal logic is modified).
- Real-time streaming cleaning for every row (we use sampling + rule generation).

## Decisions

### 1. The Intelligence Hook System (Observer Pattern)
- **Decision**: Introduce a `trait IntelligenceHook` that is called by the `SwanFlowEngine` during the `execute_workflow` loop.
- **Rationale**: Decouples the "Repair/Route" logic from the core execution state machine. It allows us to add new AI capabilities without modifying the topological sorter.
- **Alternatives**: Embedding AI logic directly in nodes. *Rejected* because it's hard to manage retries and cross-node context.

### 2. Semantic Cleaning via Rule Synthesis
- **Decision**: Use a "Sample-Analyze-Apply" pattern.
- **Rationale**: AI is slow/expensive for 1M rows. Generating a deterministic rule (Regex/Map) from a 100-row sample allows Rust to process the remaining 999,900 rows at native speed.
- **Alternatives**: Streaming row-by-row cleaning. *Rejected* due to latency and cost.

### 3. Standardized Graph Protocol (The Golden Schema)
- **Decision**: Enforce a strict internal Arrow Schema for all Graph-related nodes (`id: Int64, label: Utf8, properties: Utf8`).
- **Rationale**: Ensures the `GraphRetriever`, `GraphIndexWriter`, and any SQL node can join and query research data without schema mismatches.
- **Alternatives**: Dynamic schema (late binding). *Rejected* because it breaks DataFusion's compile-time optimization.

### 4. Outsourcing Search to MCP
- **Decision**: SwanFlow will NOT implement search nodes (e.g., Tavily, Serper) directly. Instead, it will provide a robust `McpInvokeNode`.
- **Rationale**: Search APIs change frequently. Using MCP (Model Context Protocol) allows us to leverage external tools that are maintained independently.

## Risks / Trade-offs

- **[Risk]** AI Hallucination in SQL Repair → **[Mitigation]** Limit auto-retry to 1 attempt. If it fails, report the original error and the AI's suggested fix to the user.
- **[Risk]** Data leakage via LLM API → **[Mitigation]** The `SemanticCleaner` only sends a small sample (top N rows) to the LLM, reducing exposure of the full dataset.
- **[Risk]** Recursive Loop in Deep Research → **[Mitigation]** MetaNode will have a hard `max_depth` and `max_pages` constraint.
