# Tasks: Intelligent Analyst Agent v2

## 1. Agent Core and Reasoning Loop

- [ ] 1.1 Refactor `AnalystAgent` to support the **Reasoning Loop** (Plan -> Infer -> Verify -> Fix).
- [ ] 1.2 Implement the **Macro-Planner Tool**: `generate_workflow_yaml`.
- [ ] 1.3 Implement **Atomic-Correction Tools**: `patch_node_config`, `add_node`, `connect`.
- [ ] 1.4 Integrate the loop with `SwanFlowEngine::infer_metadata` for real-time validation.

## 2. Statistical Profiling & Insights

- [ ] 2.1 Implement `ProfilerNode` (specialized AggregateNode) to compute min/max/nulls/top-k.
- [ ] 2.2 Update `WorkflowMetadata` to include statistical profiles for each output port.
- [ ] 2.3 Implement **Context Injection**: ensure the Agent sees these statistics when planning.

## 3. Dynamic Power-Ups (Extensions & UDFs)

- [ ] 3.1 Implement `core.flow.extension` node to handle DuckDB `INSTALL/LOAD` commands.
- [ ] 3.2 Implement `core.flow.python_udf` node to register custom logic via `pyo3`.
- [ ] 3.3 Add Agent tools to "search" for available DuckDB extensions.

## 4. Professional Excel Visualization (Pro Mode)

- [ ] 4.1 Update `ExcelSinkExec` to support **Native Chart injection** from configuration.
- [ ] 4.2 Implement **Native Pivot Table** metadata injection.
- [ ] 4.3 Add a demo tutorial showing an Agent-generated report with charts.

## 5. Verification

- [ ] 5.1 Test the "Self-Correction" loop by giving the Agent an incorrect initial schema.
- [ ] 5.2 Benchmark the Agent's token usage with vs. without Statistical Profiling.
- [ ] 5.3 End-to-End: "Analyze Q3 sales from S3, find outliers, and export Excel dashboard" fully autonomously.
