# M0 Node Gap Closure Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在现有 Rust 引擎中补齐首批 8 个 M0 缺口节点，形成可测试的“计划-检索-结构化-引用追踪”闭环。

**Architecture:** 在 `src/nodes/ai` 下新增“检索与契约”节点模块，复用现有 `core.ai.mcp_invoke`、`core.io.universal_read` 与稳定性错误分类框架。节点能力以“最小可用契约”优先，先确保输入输出语义统一与端到端可回归，再迭代实现深度能力。

**Tech Stack:** Rust, Tokio, DataFusion, serde_json, anyhow, OpenSpec(opsx), cargo test

---

### Task 1: OpenSpec Change + Shared Contracts Scaffold

**Files:**
- Create: `openspec/changes/m0-agent-node-gap-closure/proposal.md`
- Create: `openspec/changes/m0-agent-node-gap-closure/design.md`
- Create: `openspec/changes/m0-agent-node-gap-closure/tasks.md`
- Create: `openspec/changes/m0-agent-node-gap-closure/specs/m0-agent-node-gap-closure/spec.md`
- Create: `src/nodes/ai/contracts.rs`
- Modify: `src/nodes/ai/mod.rs`

**Step 1: Write the failing test**

```rust
#[test]
fn test_contract_types_are_serializable() {
    let v = rustflow::nodes::ai::contracts::CitationItem {
        source_id: "s1".into(),
        snippet: "x".into(),
        score: 0.9,
        start: 0,
        end: 1,
    };
    let s = serde_json::to_string(&v).unwrap();
    assert!(s.contains("source_id"));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_contracts_tests test_contract_types_are_serializable -- --nocapture`  
Expected: FAIL with unresolved module/type.

**Step 3: Write minimal implementation**

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CitationItem {
    pub source_id: String,
    pub snippet: String,
    pub score: f32,
    pub start: usize,
    pub end: usize,
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_contracts_tests test_contract_types_are_serializable -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add openspec/changes/m0-agent-node-gap-closure src/nodes/ai/contracts.rs src/nodes/ai/mod.rs tests/m0_contracts_tests.rs
git commit -m "feat: scaffold m0 node-gap contracts and opsx change"
```

### Task 2: Structured IO Contract Node

**Files:**
- Create: `tests/m0_structured_io_tests.rs`
- Modify: `src/nodes/ai/contracts.rs`
- Modify: `src/nodes/registry.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_structured_io_contract_rejects_missing_required_field() {
    // config schema requires `answer`
    // input payload missing `answer`
    // expect validation-class error
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_structured_io_tests test_structured_io_contract_rejects_missing_required_field -- --nocapture`  
Expected: FAIL with missing node type or behavior mismatch.

**Step 3: Write minimal implementation**

```rust
pub struct StructuredIoContractNode;
// validate: require `schema` in config
// propose_plan: pass-through plan but annotate enforcement metadata
// runtime validator: check required keys in JSON payload column
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_structured_io_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/ai/contracts.rs src/nodes/registry.rs tests/m0_structured_io_tests.rs
git commit -m "feat: add structured io contract node with validation"
```

### Task 3: Citation Tracker Node

**Files:**
- Create: `tests/m0_citation_tracker_tests.rs`
- Modify: `src/nodes/ai/contracts.rs`
- Modify: `src/nodes/registry.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_citation_tracker_emits_source_mapping() {
    // input contains answer + source chunks
    // expect output includes citations array with source_id/snippet/span
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_citation_tracker_tests test_citation_tracker_emits_source_mapping -- --nocapture`  
Expected: FAIL (node not implemented).

**Step 3: Write minimal implementation**

```rust
pub struct CitationTrackerNode;
// deterministic citation extraction from source chunks
// enforce max citations + stable ordering by score desc
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_citation_tracker_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/ai/contracts.rs src/nodes/registry.rs tests/m0_citation_tracker_tests.rs
git commit -m "feat: add citation tracker node for traceable outputs"
```

### Task 4: Query Planning + Semantic Chunker Nodes

**Files:**
- Create: `src/nodes/ai/retrieval.rs`
- Create: `tests/m0_retrieval_nodes_tests.rs`
- Modify: `src/nodes/ai/mod.rs`
- Modify: `src/nodes/registry.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_query_planning_splits_goal_into_ranked_subqueries() {}

#[tokio::test]
async fn test_semantic_chunker_respects_chunk_and_overlap_limits() {}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_retrieval_nodes_tests -- --nocapture`  
Expected: FAIL with unresolved `QueryPlanningNode` / `SemanticChunkerNode`.

**Step 3: Write minimal implementation**

```rust
pub struct QueryPlanningNode;
pub struct SemanticChunkerNode;
// QueryPlanning: heuristic split by punctuation + intent keywords
// Chunker: sentence window chunking with overlap and deterministic ids
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_retrieval_nodes_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/ai/retrieval.rs src/nodes/ai/mod.rs src/nodes/registry.rs tests/m0_retrieval_nodes_tests.rs
git commit -m "feat: add query planning and semantic chunker nodes"
```

### Task 5: Vector Index Writer/Retriever Contract Unification

**Files:**
- Modify: `src/nodes/ai/retrieval.rs`
- Create: `tests/m0_vector_contract_tests.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_vector_writer_outputs_index_metadata_contract() {}

#[tokio::test]
async fn test_vector_retriever_outputs_ranked_context_contract() {}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_vector_contract_tests -- --nocapture`  
Expected: FAIL due to missing standardized fields (`index_id`, `embedding_dim`, `score`).

**Step 3: Write minimal implementation**

```rust
pub struct VectorIndexWriterNode;
pub struct VectorRetrieverNode;
// writer/retriever return unified JSON envelope:
// { index_id, source_id, chunk_id, content, embedding_dim, score }
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_vector_contract_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/ai/retrieval.rs tests/m0_vector_contract_tests.rs
git commit -m "feat: unify vector writer and retriever contracts"
```

### Task 6: Tool Executor Wrapper on MCP Invoke

**Files:**
- Create: `src/nodes/ai/tool_executor.rs`
- Create: `tests/m0_tool_executor_tests.rs`
- Modify: `src/nodes/ai/mod.rs`
- Modify: `src/nodes/registry.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_tool_executor_normalizes_success_and_error_payloads() {}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_tool_executor_tests -- --nocapture`  
Expected: FAIL (node not found / payload mismatch).

**Step 3: Write minimal implementation**

```rust
pub struct ToolExecutorNode;
// compose existing core.ai.mcp_invoke behavior
// normalize output: {status, tool_name, data, error, latency_ms}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_tool_executor_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/ai/tool_executor.rs src/nodes/ai/mod.rs src/nodes/registry.rs tests/m0_tool_executor_tests.rs
git commit -m "feat: add tool executor node with normalized mcp result contract"
```

### Task 7: Universal Resolver Extension

**Files:**
- Modify: `src/nodes/io.rs`
- Create: `tests/m0_universal_resolver_tests.rs`

**Step 1: Write the failing test**

```rust
#[tokio::test]
async fn test_universal_resolver_routes_url_file_table_sources() {}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test m0_universal_resolver_tests -- --nocapture`  
Expected: FAIL (no route metadata / unsupported source type).

**Step 3: Write minimal implementation**

```rust
// extend core.io.universal_read config:
// { source_type: auto|url|file|table, source: "...", format: ... }
// produce route diagnostics in metadata payload
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test m0_universal_resolver_tests -- --nocapture`  
Expected: PASS

**Step 5: Commit**

```bash
git add src/nodes/io.rs tests/m0_universal_resolver_tests.rs
git commit -m "feat: extend universal resolver routing contract"
```

### Task 8: End-to-End Workflows + Regression Gate

**Files:**
- Create: `tests/m0_rag_basic_integration.rs`
- Create: `tests/m0_rag_tool_integration.rs`
- Modify: `openspec/changes/m0-agent-node-gap-closure/tasks.md`

**Step 1: Write the failing integration tests**

```rust
#[tokio::test]
async fn test_m0_rag_basic_pipeline() {}

#[tokio::test]
async fn test_m0_rag_with_tool_executor_pipeline() {}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test m0_rag_basic_integration --test m0_rag_tool_integration -- --nocapture`  
Expected: FAIL due to missing wiring/contracts.

**Step 3: Wire minimal pipeline configs and registry entries**

```rust
// build workflow:
// QueryPlanning -> UniversalResolver -> SemanticChunker
// -> VectorIndexWriter -> VectorRetriever -> StructuredIO -> CitationTracker
// optional branch: ToolExecutor
```

**Step 4: Run full verification**

Run:
- `cargo test --test m0_rag_basic_integration --test m0_rag_tool_integration -- --nocapture`
- `cargo test --test compiler_tests --test engine_tests --test stability_hardening_tests -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add tests/m0_rag_basic_integration.rs tests/m0_rag_tool_integration.rs openspec/changes/m0-agent-node-gap-closure/tasks.md
git commit -m "test: add m0 end-to-end rag and tool integration coverage"
```

### Task 9: Docs + Archive Prep

**Files:**
- Modify: `openspec/changes/m0-agent-node-gap-closure/tasks.md`
- Modify: `openspec/changes/m0-agent-node-gap-closure/design.md`
- Modify: `openspec/changes/m0-agent-node-gap-closure/specs/m0-agent-node-gap-closure/spec.md`

**Step 1: Write failing check (manual)**

```text
Checklist fails if any task remains unchecked or any acceptance criterion lacks evidence.
```

**Step 2: Run opsx status check**

Run: `openspec instructions apply --change m0-agent-node-gap-closure --json`  
Expected: all tasks done, state `all_done`.

**Step 3: Finalize change notes**

```text
Document verification commands, outputs, and known non-goals.
```

**Step 4: Re-run smoke verification**

Run:
- `cargo test --test m0_rag_basic_integration -- --nocapture`
- `cargo test --test m0_rag_tool_integration -- --nocapture`

Expected: PASS

**Step 5: Commit**

```bash
git add openspec/changes/m0-agent-node-gap-closure
git commit -m "docs: finalize m0 node gap closure opsx artifacts"
```
