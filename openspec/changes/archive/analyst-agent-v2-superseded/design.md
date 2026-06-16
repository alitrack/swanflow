## Context
SwanFlow has a functional core but its agent is currently a basic linear planner. To compete with state-of-the-art analysts, we need to implement a hybrid reasoning loop where the AI can macro-plan, validate its own logic using our zero-copy inference engine, and atomicity fix errors.

## Goals / Non-Goals

**Goals:**
- Implement a **3-Tier Toolset** (Macro, Atomic, Inspection).
- Implement an autonomous loop that handles **Structural Validation** (fixing SQL/YAML schema errors before asking the human).
- Support dynamic capability expansion via **DuckDB Extensions** and **Python UDFs**.
- Implement a **Statistical Profiler** to provide data distributions to the LLM.

**Non-Goals:**
- Real-time training of ML models (Focus is on analysis and reporting).
- General-purpose coding agent (Focus is strictly on Data Science and Excel).

## Decisions

### 1. Hybrid Orchestration (The "Plan-Verify-Patch" Loop)
**Decision**: The agent will first output a full YAML proposal. The compiler then runs an `infer_metadata` pass. If it fails, the compiler returns a JSON error containing the mismatched fields. The agent then enters "Atomic Mode" to patch the specific configuration.
**Rationale**: This mimics how human experts work—draw a big picture, then fix details. It's much faster than building a 10-node DAG node-by-node.

### 2. Statistical Profiling as LLM Context
**Decision**: Add an `AggregateNode` based profiling pass during inference.
**Rationale**: Instead of sending raw data (expensive/slow), we send `min`, `max`, `null_count`, and `top_k` values. This gives the AI enough context to write correct `WHERE` clauses and Join keys.

### 3. DuckDB Plugin Management
**Decision**: Introduce `core.flow.extension` node.
**Rationale**: By making extension loading a node in the DAG, we ensure reproducibility. The agent can "equip" itself with powers like `httpfs` or `fts` dynamically.

## Risks / Trade-offs
- **[Risk] Infinite Loop** → Mitigation: Strict iteration limit (max 5 self-corrections) before escalating to the human.
- **[Risk] Privacy** → Mitigation: Use ZeroClaw's pattern of scrubbing sensitive values from statistical profiles if required.
- **[Risk] Complexity** → Mitigation: Reuse our existing `WorkflowCompiler` for all validation logic.
