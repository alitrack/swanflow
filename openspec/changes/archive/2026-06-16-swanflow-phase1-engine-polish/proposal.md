## Why

SwanFlow 的核心引擎已经验证了 Late-Binding SQL 和零拷贝 Python 桥两大约束性创新。19 个端到端教程可运行，证明架构设计正确。但距离"可用的产品"仍有差距：

1. **核心节点不稳定**——部分节点（如 MissingNode、条件分支）尚未补齐，边界情况容错不够
2. **AI Analyst Agent 半成品**——IntelligenceHook 和 AnalystAgent 有框架但缺少完整自愈循环
3. **无 Python 包分发**——当前只能 `cargo run`，用户门槛极高
4. **连接器薄弱**——只有 DuckDB Scanner + SQLx，缺少 50+ 数据库覆盖
5. **工作流模式单一**——只有 sequential DAG，缺少 fan-out/loop/conditional/trigger 等模式

与此同时，外部环境提供了明确的机会窗口：

- **DataFusion 生态正在爆发**（v53+，社区活跃），SwanFlow 是其中唯一直接对标 KNIME/dbt 的项目
- **Nika / OpenFang** 证明了"Rust 单二进制 + YAML 声明式"模式的市场需求
- 新发布的 00-research-report 指出 **DAG + Ontology + Agent 三级融合** 是当前研究最前沿，SwanFlow 天然具备前两级

## What Changes

本 Change 是 SwanFlow 的 Phase 1 引擎打磨阶段，包含 5 个子 Spec：

| 子 Spec | 描述 | 预估周期 |
|---------|------|----------|
| core-node-stability | 核心节点稳定性补齐 | 2-3 周 |
| ai-analyst-agent | AI Analyst Agent 完善 | 2-3 周 |
| python-package | Python 包发布 | 1 周 |
| connector-layer | 50+ 数据库连接器集成 | 2 周 |
| openfang-patterns | 工作流模式扩展 | 2 周 |

共计约 9-13 周（可并行推进）。

### 不做的事情（非目标）

- ❌ 不做 UI/前端（留到 Phase 3）
- ❌ 不做分布式执行（留到 Phase 3）
- ❌ 不做开放规范发布（留到 Phase 3）
- ❌ 不直接 fork OpenFang 代码（只吸收设计模式）

## Capabilities

### New Capabilities

- `core-nodes-complete`: 所有核心节点通过边界测试验证
- `hybrid-agent-complete`: AnalystAgent v2 完成"规划→推断→验证→修复"闭环
- `python-package`: `pip install swanflow` 可用
- `connector-50plus`: 50+ 数据库连接器（基于 DBX 驱动模式）
- `workflow-step-modes`: 支持 fan-out / collect / conditional / loop 四种模式
- `event-triggers`: 支持简单事件触发工作流执行

### Modified Capabilities

- `late-binding-sql`: 在更多节点类型上验证 Late-Binding 引用
- `intelligence-hook`: 扩展到所有核心节点，不只限于数据节点
- `yaml-workflow-definition`: 加入 step_mode、trigger 等新字段
- `cli-interface`: 支持 `swanflow run` 替代 `cargo run`

## Impact

- **Affected systems**:
  - `src/engine/` — 核心执行引擎（新增 step mode 支持）
  - `src/agent/` — AI Agent 重写 + 自愈闭环
  - `src/nodes/` — 所有核心节点稳定性
  - `src/connectors/` — 全新连接器层
  - `src/cli/` — CLI 入口（swanflow 独立二进制）
  - `Cargo.toml` — 新增依赖（各数据库驱动）
- **Affected APIs**:
  - YAML workflow schema（新增 `mode` / `trigger` / `error_mode` 字段）
  - Analyst Agent tool interface
  - Python `import swanflow` API
- **Dependencies**: Apache DataFusion, DuckDB drivers, SQLx, pyo3, tokio
- **Research references**: 00-research-report §2.1 (GraSP typed DAG), §2.2 (DYNO DAG+Ontology), §3.2 (OpenFang step modes)
