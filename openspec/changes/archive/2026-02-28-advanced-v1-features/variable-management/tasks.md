# Tasks: Variable Management

## 1. Data Model and Infrastructure

- [ ] 1.1 Update `Workflow` struct in `src/core/graph.rs` to include a `context: HashMap<String, Value>`.
- [ ] 1.2 Implement a `TemplateEngine` helper to perform `${var}` string substitution.
- [ ] 1.3 Update `WorkflowCompiler` to resolve static variables from the global context during compilation.

## 2. Dynamic Flow Variables

- [ ] 2.1 Implement `core.flow.variable_creator` node to define variables manually.
- [ ] 2.2 Implement `core.flow.table_to_variable` node to capture data from Table results.
- [ ] 2.3 Implement **Variable Output Support**: allow `propose_plan` to return named scalars.

## 3. Order and Dependency

- [ ] 3.1 Refactor the topological sorter to consider "implicit" variable dependencies (if node B uses a variable from node A).
- [ ] 3.2 Update `NodeRegistry` to include the new flow control nodes.

## 4. AI and Validation

- [ ] 4.1 Update `AnalystAgent` system prompt to explain how to use the `${context.key}` syntax.
- [ ] 4.2 Create a demo tutorial: `Tutorial 10: Dynamic Parameterization`.
- [ ] 4.3 End-to-End: Agent uses a global variable to control a SQL filter.
