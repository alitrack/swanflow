# Tasks: Variable Management

> **记账修正 (2026-09-11)**：原勾选全部为 0%，但代码已落地大部分。
> 本次按代码实证回填；3.1 与 4.1 仍为真缺口，单独标注。
> 验证：`cargo test --lib -- --test-threads=1`（flow_vars/templating 全绿）。

## 1. Data Model and Infrastructure

- [x] 1.1 Update `Workflow` struct in `src/core/graph.rs` to include a `context: HashMap<String, Value>`.
  - 实证：`graph.rs:105  pub context: HashMap<String, JsonValue>`
- [x] 1.2 Implement a `TemplateEngine` helper to perform `${var}` string substitution.
  - 实证：`crates/swanflow/src/core/templating.rs` TemplateEngine（render_string/render_json/resolve_value，支持 ${context.key} + {{mustache}}）
- [x] 1.3 Update `WorkflowCompiler` to resolve static variables from the global context during compilation.
  - 实证：`compiler.rs:297  TemplateEngine::render_json(raw_config, &workflow.context)`

## 2. Dynamic Flow Variables

- [x] 2.1 Implement `core.flow.variable_creator` node to define variables manually.
  - 实证：`transform.rs:469 VariableCreatorNode`，已注册
- [x] 2.2 Implement `core.flow.table_to_variable` node to capture data from Table results.
  - 实证：`transform.rs:41 TableToVariableNode`，已注册
- [x] 2.3 Implement **Variable Output Support**: allow `propose_plan` to return named scalars.
  - 实证：VariableCreatorNode::propose_plan 产出变量定义并写入 context

## 3. Order and Dependency

- [ ] 3.1 Refactor the topological sorter to consider "implicit" variable dependencies (if node B uses a variable from node A).
  - **真缺口**：sorter 无隐式变量依赖处理。当前靠编译期 render_json 静态解析，不解析 DAG 级变量依赖。
- [x] 3.2 Update `NodeRegistry` to include the new flow control nodes.
  - 实证：`registry.rs:141-142` 注册两个 flow 变量节点

## 4. AI and Validation

- [ ] 4.1 Update `AnalystAgent` system prompt to explain how to use the `${context.key}` syntax.
  - **真缺口**：agent/mod.rs system prompt 未提及变量占位符语法。
- [x] 4.2 Create a demo tutorial: `Tutorial 10: Dynamic Parameterization`.
  - 实证：`TUTORIALS.md:47` + `tutorials/10_dynamic_parameterization.yaml`
- [x] 4.3 End-to-End: Agent uses a global variable to control a SQL filter.
  - 实证：`tests/agent_variable_tests.rs`（E2E：agent 经全局变量控制 SQL filter）

## 收尾
- 待做（可选，未阻塞）：3.1 隐式依赖、4.1 agent prompt 变量语法文档
