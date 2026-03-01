## Context

SwanFlow is a high-performance workflow engine, but its current core implementation has several bottlenecks and safety gaps. The engine's inability to handle multiple terminal nodes limits its utility for complex ETL tasks. Suboptimal graph algorithms ($O(N^2)$ and unnecessary cloning) will cause performance issues as workflows scale. The Python integration is powerful but lacks a proper security sandbox, making it risky to execute user-provided code.

## Goals / Non-Goals

**Goals:**
- Fix critical syntax errors to enable compilation and testing.
- Enable concurrent execution of all terminal nodes in a workflow.
- Optimize graph and compiler algorithms for better scalability.
- Implement a more robust (though not necessarily perfect) security sandbox for Python scripts.
- Improve error reporting and propagation across the engine.

**Non-Goals:**
- Implementing a full Linux-level container sandbox for Python (out of scope for now).
- Complete rewrite of the compiler logic (incremental improvement only).
- Adding new node types beyond what's necessary for fixing issues.

## Decisions

### 1. Concurrent Multi-Terminal Execution
- **Decision**: Use `tokio::task::JoinSet` to manage multiple concurrent `datafusion::physical_plan::collect` calls.
- **Rationale**: This allows the engine to fully utilize DataFusion's parallelism across different output branches. Each task in the `JoinSet` will be responsible for reporting its own progress/errors back to the `NodeEvent` stream.
- **Alternatives**: Sequential execution (current state, but limited); Manual thread management (too complex, `tokio` is already used).

### 2. Efficient Cycle Detection
- **Decision**: Use `petgraph::algo::is_cyclic_directed` directly on the `StableGraph` without cloning.
- **Rationale**: `petgraph`'s algorithm is efficient ($O(N+E)$) and can run on the existing graph. Cloning the graph is $O(N+E)$ anyway, so doing it every time is redundant and slow.
- **Alternatives**: Incremental cycle detection (faster for very frequent updates, but more complex to implement).

### 3. Optimized Variable Dependency Detection
- **Decision**: Pre-index variable producers.
- **Rationale**: Instead of a nested loop checking every node against every other node ($O(N^2)$), we scan all nodes once to find "variable producers" and then scan once more to find "consumers" using a HashMap of variable names to producer node indices. This reduces the check to $O(N + V)$ where $V$ is the number of variables.
- **Alternatives**: Continue with $O(N^2)$ (unacceptable for large graphs).

### 4. Restricted Python Environment
- **Decision**: Use an import hook or a restricted `locals` dictionary in `pyo3`.
- **Rationale**: While a full sandbox is hard, we can intercept `__import__` in the Python `locals` to block specific modules. This is more robust than a simple string search.
- **Alternatives**: Separate process execution (too slow, breaks zero-copy); Full containerization (too complex).

## Risks / Trade-offs

- **[Risk]** Intercepting `__import__` can still be bypassed by advanced Python users. 
    - **Mitigation**: Combine with a basic AST check or a more restrictive `__builtins__` list.
- **[Risk]** Concurrent terminal execution might cause resource contention if many large plans run at once.
    - **Mitigation**: Rely on DataFusion's internal task scheduling and memory management, but monitor for OOM.
- **[Risk]** Changing compilation order or side effects in `table_to_variable` might break existing (undocumented) workflow assumptions.
    - **Mitigation**: Add comprehensive tests for variable-dependent workflows.
