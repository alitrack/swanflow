# Proposal: Graph Repair Operators (5-Operator Local Repair)

## Why

现有 `OpenAiHook::try_fix` 的修复能力只有一个自由度：**修改失败节点的 config 后重跑**（对应 GraSP 论文的 REBIND 算子）。调研 GraSP（arXiv 2604.17870）后确认，单算子修复有三个结构性缺口：

1. **前置条件缺失无法修复**。节点失败常因"上游没产出它需要的东西"（如 SQL 节点引用了不存在的列/表）。改 config 没用——需要在**拓扑上插入一个前置节点**（INSERT_PREREQ）。
2. **节点类型选错无法修复**。当前节点类型根本不适合该任务（如用 filter 做聚合）。需要**替换节点类型**且保持下游接口兼容（SUBSTITUTE）。
3. **失败传播无界**。修复只针对单个失败节点，失败若影响下游（重跑后产出 schema 变了），下游会连环失败——没有图级视角（REWIRE / BYPASS）。

GraSP 的五个类型化修复算子（REBIND / INSERT_PREREQ / SUBSTITUTE / REWIRE / BYPASS）把修复从"改参数"升级为"图变换"，失败传播从 O(N) 降到 O(dʰ)。SwanFlow 已具备全部前置条件（拓扑感知 PromptBuilder、5 维 ModelRouter、图内核），只差让 LLM 输出结构化修复指令并在执行器应用它。

## What Changes

### New: `crates/swanflow-ai/src/repair.rs`

```rust
/// 修复算子类型 —— 对应 GraSP 五算子
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "operator", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RepairOperator {
    /// 修改失败节点参数（原行为）
    Rebind { fixed_config: Value },
    /// 在失败节点前插入一个建立前置条件的子图
    InsertPrereq {
        insert_node: NodeSpec,
        insert_before: String, // 失败节点 id
    },
    /// 替换失败节点类型，保持下游接口兼容
    Substitute {
        new_type: String,
        new_config: Value,
    },
    /// 局部重连边（增/删/改类型），作用于失败节点邻域
    Rewire {
        add_edges: Vec<EdgeSpec>,
        remove_edges: Vec<EdgeRef>,
    },
    /// 跳过失败节点（当前状态已满足下游需求时）
    Bypass,
}

/// 图变换应用结果
pub struct RepairOutcome {
    pub applied: RepairOperator,
    pub affected_nodes: Vec<String>, // 受影响节点 id（用于失效范围）
    pub message: String,
}
```

### Modified: `crates/swanflow-ai/src/hook.rs`

- `FixResponse` 替换为 `RepairOperator`（serde tag 兼容旧响应：`{"operator":"REBIND","fixed_config":...}`，向后兼容）
- `try_fix` 返回 `Option<RepairOperator>` 而非 `Option<Value>`
- `fix_instructions` 提示词更新：列出五个算子及选择规则（按失败类型：前置失败→INSERT_PREREQ，执行失败→REBIND/SUBSTITUTE，接口不兼容→REWIRE，已满足→BYPASS）
- `PromptContext` 补充：`graph_topology: Vec<EdgeSpec>`（完整图拓扑，当前只传空 vec）

### Modified: `crates/swanflow-core/src/kernel/`（图变换应用）

- 新增 `apply_repair(workflow: &mut Workflow, op: RepairOperator) -> Result<RepairOutcome>`
  - REBIND：`update_config`（现有路径）
  - INSERT_PREREQ：在失败节点前插入新节点 + 边，重排拓扑
  - SUBSTITUTE：改 type_id + config，校验下游边 schema 兼容（warning 级，不硬失败）
  - REWIRE：增删边（走 `add_edge`/`remove_edge` 现有 API）
  - BYPASS：标记节点跳过，下游按"已满足"继续
- 修复边界：`h=2` 跳邻域（默认），`|ΔV|≤3`，`|ΔE|≤5`（GraSP 默认参数）
- 修复失败后仍走现有 HITL 升级路径（self-healing-execution spec 已定义）

### Non-Goals

- ❌ 不做执行时 DAG 编译（从自然语言任务生成图）——那是 GraSP 论文的编译层，属于未来 AnalystAgent 路线
- ❌ 不改 AiRouter（dynamic-workflow-routing spec 已覆盖运行时条件分流）
- ❌ 不引入循环/undo 支持——DAG 假设保持

## Capabilities

### New Capabilities

- **五算子图修复**：失败节点可通过插入前置、替换类型、重连边、跳过四种新方式修复（+ 原有改参）
- **有界修复**：修复影响限定在 h 跳邻域，不触发整图重跑

### Modified Capabilities

- **LLM 修复**：`try_fix` 从"改 config"升级为"输出图变换指令"，PromptBuilder 传入完整拓扑

## Reference

- GraSP: Graph-Structured Skill Compositions for LLM Agents — https://arxiv.org/abs/2604.17870（§2.4 五算子、§3.3 消融：局部修复 +1.3/+1.4，全局重规划 -3.2/-3.1）
