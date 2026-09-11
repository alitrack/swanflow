# Tasks: Variable Management

> **记账修正 (2026-09-11)**：原勾选全部为 0%，但代码已落地大部分。
> 本次按代码实证回填；**4.1 已于同日落地**（agent prompt 变量段），唯一剩余 3.1。
> 验证：`cargo test --lib -- --test-threads=1`（flow_vars/templating/agent 全绿）。

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

- [x] 3.1 Refactor the topological sorter to consider "implicit" variable dependencies (if node B uses a variable from node A).
  - 已落地 (2026-09-11)：`engine/mod.rs` 新增 `reorder_for_variable_deps()`：
    YAML 执行路径 toposort 后做稳定重排（Kahn + 原拓扑序 tie-break），
    消费 `{{var}}`/`${context.var}` 的节点保证排在 `output_var` 同名产出
    节点之后。**不加数据边**（避免改变下游输入端口数），只调执行顺序；
    隐式环时降级回基础拓扑序（不死锁）。提取器：`templating.rs`
    `TemplateEngine::extract_variable_refs()`。测试 6 个（engine 4 +
    templating 2）：mustache/context 两种语法、稳定性（无隐式依赖顺序不变）、
    隐式环降级、提取器正确性/垃圾输入忽略。
- [x] 3.2 Update `NodeRegistry` to include the new flow control nodes.
  - 实证：`registry.rs:141-142` 注册两个 flow 变量节点

## 4. AI and Validation

- [x] 4.1 Update `AnalystAgent` system prompt to explain how to use the `${context.key}` syntax.
  - **已落地 (2026-09-11)**：`agent/mod.rs` `build_agent_system_prompt` 新增
    "VARIABLES & CONFIG INTERPOLATION" 段，教 `${context.key}` / `{{var:default}}` /
    `%%var:default%%` 三种运行时插值语法（均经实测：compiler.rs:297 + engine/mod.rs:1457
    都调 render_json，flow_vars 处理 `{{var}}`/`%%var%%`）+ 渲染测试 `test_system_prompt_renders_variable_and_perf_guidance` 守门。
- [x] 4.2 Create a demo tutorial: `Tutorial 10: Dynamic Parameterization`.
  - 实证：`TUTORIALS.md:47` + `tutorials/10_dynamic_parameterization.yaml`
- [x] 4.3 End-to-End: Agent uses a global variable to control a SQL filter.
  - 实证：`tests/agent_variable_tests.rs`（E2E：agent 经全局变量控制 SQL filter）

## 收尾
- 唯一剩余：**3.1** 拓扑隐式变量依赖（重排依赖分析，非阻塞）。
- ~~4.1 agent prompt 变量语法~~ 已于 2026-09-11 落地。
