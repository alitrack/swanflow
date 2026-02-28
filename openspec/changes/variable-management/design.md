## Context
Current SwanFlow nodes have static configurations. To support real-world automation, we need parameters that can change at runtime (e.g., date ranges, S3 prefixes). KNIME uses "red connections," but we want to avoid visual clutter by using a "Shadow Context" that is accessible to all nodes via string templating.

## Goals / Non-Goals

**Goals:**
- Implement a global `context` dictionary in the `Workflow` object.
- Support `${var}` syntax in any node configuration field.
- Support **Dynamic Scalar Outputs**: some nodes can output single values (variables) instead of tables.
- Implement `TableToVariableNode` to bridge the gap between data and control flow.

**Non-Goals:**
- Complex conditional looping based on variables (Phase 7's LoopNode handles this differently).
- Persistence of variable history across different workflow versions.

## Decisions

### 1. Templating vs. Visual Connections
**Decision**: Use string interpolation (`${context.foo}`) instead of explicit variable ports.
**Rationale**: This keeps the DAG clean. AI Agents find it much easier to write templates than to manage extra graph edges.

### 2. Double-Pass Compilation
**Decision**: The compiler will perform a first pass to resolve all static variables (from the `context` dictionary) and a late-binding pass for dynamic flow variables.
**Rationale**: Some variables are known at compile-time (Config), while others are only known during execution (Result of a query). Late-binding ensures we can use calculated results as parameters.

### 3. Type Coercion
**Decision**: Variables will be stored as `serde_json::Value`.
**Rationale**: This provides maximum flexibility for AI to inject strings, numbers, or even small JSON arrays as parameters.

## Risks / Trade-offs
- **[Risk] Missing Variable** → Mitigation: The compiler will throw a structured error if a placeholder cannot be resolved, which the Analyst Agent can then fix.
- **[Risk] Execution Order** → Mitigation: If node B depends on a variable from node A, the compiler must ensure A is executed first (even if there is no data edge).
