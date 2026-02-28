# Tasks: Enterprise Intelligence Pack

## 1. Advanced Connectivity (DuckDB & HTTP)

- [x] 1.1 Integrate `duckdb` crate and implement `DuckDBReadNode`.
- [x] 1.2 Implement `DuckDBWriteNode` for fast analytical storage.
- [ ] 1.3 Implement `HttpFetchNode` with automatic JSON-to-Arrow conversion.

## 2. Logic and Flow Control

- [ ] 2.1 Implement `ConditionNode`: Branching based on SQL boolean expressions.
- [ ] 2.2 Implement `LoopNode`: Supporting `ForEach` iterator over input lists.
- [ ] 2.3 Update `WorkflowCompiler` to handle non-linear branching and sub-graph iteration.

## 3. Excel Pro: Pivot and Charts

- [x] 3.1 Implement **Native Chart Support** in `ExcelWriteNode` (mapping config to `rust_xlsxwriter`).
- [x] 3.2 Implement **Native Pivot Table** support (Definition injection into `.xlsx`).
- [ ] 3.3 Create an **Excel Styling DSL** for conditional formatting and headers.

## 4. Analyst Agent (ZeroClaw Migration)

- [ ] 4.1 Create `src/agent/loop_.rs` by adapting ZeroClaw's Reasoning Loop.
- [ ] 4.2 Define **SwanFlow Tools** for the Agent: `ListNodes`, `DraftWorkflow`, `ValidateSchema`.
- [ ] 4.3 Implement the **HOTL (Human-on-the-Loop)** CLI interface for workflow approval.

## 5. End-to-End Validation

- [ ] 5.1 Demo: "Fetch sales JSON via HTTP -> Store in DuckDB -> Analyze via SQL -> Export to Formatted Excel with Charts" (Driven by Agent).
