# Proposal: KNIME Full Compatibility Pipeline

## Why

SwanFlow 目前对 KNIME 工作流的兼容性为零——代码库中没有 KNIME parser、mapper 或 execution 层。之前探索性的 68 个工厂映射仅覆盖 KNIME 节点池的 ~15%，且大量依赖 SQL 降级和直通，不是真正的兼容。

**用户明确要求："真正能用"**——这意味着：

1. 覆盖 KNIME 社区版常用节点类别的 80%+
2. 变量流（flow variables）正确建模
3. 循环节点（Loop）正确执行
4. 可视化节点在 Shaper 中正确渲染
5. 错误信息可理解、可溯源到 KNIME 原始节点

当前 SwanFlow 已有的基础设施可以直接复用：
- DataFusion + DuckDB 双引擎（`EngineMode::Auto`）
- `Workflow`/`NodeInstance` 图模型
- `NodeRegistry` 可扩展注册
- `SwanFlowEngine::execute_workflow` 执行循环
- Shaper SQLite 写入模式（DuckDB `ATTACH`）

KNIME 工作流的 XML 格式稳定，社区版 5.4+ 节点池约 500+ 工厂类型。一期目标覆盖核心 120 个节点（IO/Transform/分析/可视化）。

## What Changes

### 新增 crate: `swanflow-knime`
独立的 KNIME 互操作 crate，遵循 SwanFlow workspace 约定：
- `crates/swanflow-knime/` — pure Rust, 仅依赖 `swanflow-core`
- CLI 子命令集成到 `swanflow` binary (`swanflow knime *`)

### 四大模块

| 模块 | 职责 | 输入 | 输出 |
|---|---|---|---|
| **Parser** | 解析 KNIME XML (.knwf) | KNIME workflow XML | `SwanWorkflow` (内部 IR) |
| **Mapper** | 节点类型映射 + 配置翻译 | KNIME node XML | `NodeInstance` + `config` |
| **Execution** | 工作流执行 + 变量流 | `SwanWorkflow` | Arrow RecordBatches + 事件流 |
| **Shaper Push** | 仪表板生成 | 执行结果 | Shaper SQLite `apps` 表 |

### 执行模型

```
KNIME XML ──[Parser]──▶ SwanWorkflow (IR)
                              │
                              ▼
                     ┌─[Mapper]──▶ NodeInstance[]
                     │
                     ▼
               SwanFlowEngine.execute_workflow()
                     │
                     ├──▶ DataFusion (transform/aggregate)
                     ├──▶ DuckDB    (SQL/join/window)
                     └──▶ Passthrough (viz nodes → Shaper SQL)
                     │
                     ▼
               Shaper Push ──▶ 仪表板
```

### 变量流模型

KNIME 的 flow variables 是 DAG 执行中的关键概念——它们不是数据表的列，而是标量/字符串/JSON 配置在节点间传递。

在 SwanFlow 中建模为 `WorkflowContext`：
- `HashMap<String, JsonValue>` — 全局变量池
- 每个节点可以 `read_var(name)` / `write_var(name, value)`
- `VariableCreatorNode`、`TableToVariableNode` 已存在于 registry 中

### 循环支持

KNIME 的循环节点（Loop Start/End、Chunk Loop、Counting Loop、Recursive Loop）是真正使工作流"能用"的关键——真实场景中大量使用循环做分页爬取、批量处理、收敛计算。

在 SwanFlow 中：`NodeInstance.mode = StepMode::Loop` + `sub_workflow` 子图，由引擎的 `execute_loop()` 处理。

## Capabilities

### New Capabilities

- **C1: KNIME Parser** — 完整解析 `.knwf` XML，保留节点拓扑、配置参数、端口连接、flow variables
- **C2: KNIME Mapper** — 120 个工厂映射（v1 目标），覆盖 IO/Transform/Analytics/Visualization 四大类
- **C3: Variable Flow** — `WorkflowContext` 读/写/传播 flow variables
- **C4: Loop Execution** — Loop Start/End、Chunk Loop、Counting Loop 的 SwanFlow 执行
- **C5: Shaper Push** — DuckDB `ATTACH` 写入 Shaper SQLite，支持 10 种图表类型
- **C6: CLI 命令** — `swanflow knime {parse,run,push} <file.knwf> [--print] [--engine]`

### Modified Capabilities

- **M1: NodeInstance 扩展** — 增加 `knime_meta` 字段保存 KNIME 原始节点信息（用于错误溯源）
- **M2: SwanFlowEngine 扩展** — `execute_loop()` 方法处理子图循环

### Non-goals (v1)

- 不覆盖 KNIME 的 Java Snippet / Python Script 节点（超出 Rust 沙箱能力）
- 不覆盖交互式 Widget 节点（Slider、File Upload 等）
- 不覆盖 KNIME Server / WebPortal 节点
- 不覆盖 KNIME 的 PMML/WEKA 机器学习节点（使用 SwanFlow 自有 ML 替代）
- 不做 KNIME → SwanFlow 图形化编辑器——仅 CLI

## Impact

| 系统 | 影响 |
|---|---|
| `crates/swanflow-knime/` | **新增** crate，包含 parser/mapper/exec/shaper 四模块 |
| `crates/swanflow-core/` | **轻微** — NodeInstance 增加 `knime_meta` 可选字段 |
| `crates/swanflow/` | **新增** — CLI `knime` 子命令组 |
| `crates/swanflow/src/core/engine/` | **新增** — `execute_loop()` 方法 |
| Workflow 模型 | **兼容** — 不破坏现有 Workflow 格式 |
| 现有节点 | **不变** — 所有已有节点类型继续正常工作 |
| Shaper SQLite | **只读写入** — 不修改 Shaper 自身 schema |

## Acceptance Criteria

1. `swanflow knime parse example.knwf` — 解析成功率 100%，错误信息引用原文件行号
2. `swanflow knime run example.knwf --print` — 端到端执行，显示前 5 行
3. `swanflow knime run loop_test.knwf` — 循环节点正确迭代
4. `swanflow knime shaper --push` — 图表出现在 Shaper UI
5. 120 个工厂映射全部在 mapper 单元测试中覆盖
