# M0 Node Gap Design (Blueprint-Assisted)

## Context

- `docs/ai-operators-40-blueprint.md` 提供了 40 项能力蓝图，用于长期能力参照。
- 当前 `rustflow` 已具备部分 AI 与检索基座（MCP、GraphRAG、Research Loop、Boxed Python）。
- 本设计把 40 项视为“辅助导航”，不是一次性实现清单。

## Goals

- 以最小实现成本补齐 M0 的关键缺口，形成可复用的智能体执行闭环。
- 统一节点契约（输入输出、错误分类、可追溯性），减少后续重构成本。
- 所有新增能力均通过 OpenSpec（opsx）管理，具备规范化验收路径。

## Non-Goals

- 不实现 UI/前端可视化配置。
- 不在本阶段完成 M1/M2 全量能力（治理平台、团队协作、全量评测体系）。
- 不重写已稳定节点，仅在契约层做兼容增强。

## Layered Design

### L1 Access Layer

- Credential Vault Bridge
- Provider Authenticator
- Model Catalog Selector

> 本阶段仅定义接口和最小落点，暂不深挖企业级密钥托管能力。

### L2 Execution Layer

- Agent Prompter
- MCP Client Connector
- MCP Tool Discovery
- Tool Executor

> 复用现有 `core.ai.mcp_invoke`，补统一请求/响应与错误语义。

### L3 Retrieval Layer

- Query Planning
- Universal Resolver
- Semantic Chunker
- Vector Index Writer
- Vector Retriever

> 重点增强检索链路的标准化与可组合性。

### L4 Trust & Output Layer

- Structured IO Contract
- Citation Tracker

> 保证“可验证输出”和“可追溯证据”。

## Phase-1 Node Scope (Recommended 8)

1. Structured IO Contract
2. Citation Tracker
3. Query Planning
4. Semantic Chunker
5. Vector Index Writer (contract unification)
6. Vector Retriever (contract unification)
7. Tool Executor (wrapper on `core.ai.mcp_invoke`)
8. Universal Resolver (extend `core.io.universal_read`)

## Runtime Data Flow

1. Query Planning 生成子问题与优先级
2. Universal Resolver 路由 URL/文件/表资源
3. Semantic Chunker 生成语义分块
4. Vector Index Writer 建索引并记录元信息
5. Vector Retriever 召回候选上下文
6. Tool Executor（可选）补充外部工具结果
7. Structured IO Contract 校验/修复输出结构
8. Citation Tracker 生成结论-证据映射

## Error & Stability Contract

- 统一错误分类：`validation` / `migration` / `planning` / `execution`
- 禁止 panic 作为节点失败路径
- 仅 `planning` / `execution` 进入自愈策略
- `validation` / `migration` 必须输出确定性 remediation 信息

## Testing & Acceptance

### Node Tests

- 每个新增节点至少覆盖：
  - valid path
  - invalid config
  - boundary input

### Integration Tests

- `RAG-basic`：检索 + 结构化输出 + 引用追踪
- `RAG+Tool`：引入 Tool Executor

### Regression Gates

- `tests/compiler_tests.rs`
- `tests/engine_tests.rs`
- `tests/stability_hardening_tests.rs`

## Opsx Guidance

- 建议使用一个主变更承载本轮工作：`m0-agent-node-gap-closure`
- 规范流程：
  - `openspec propose`：写入目标、范围、验收
  - `openspec apply`：按任务小步实现
  - `openspec archive`：完成后归档
- 任务拆分原则：
  - 每个节点独立任务
  - 每个任务包含测试、实现、回归验证、提交

## Risks & Mitigations

- 风险：节点数量多导致耦合扩大  
  - 缓解：按层次与契约分治，先做 8 个节点
- 风险：检索链路正确但输出不可验证  
  - 缓解：将 Structured IO + Citation 设为首批最高优先
- 风险：旧工作流兼容性退化  
  - 缓解：保留 type alias 与 placeholder 降级能力

## Decision

- 采用“M0 缺口优先、蓝图辅助约束”的实施策略。
- 先交付首批 8 节点与端到端可追溯链路，再进入 M1 增强。
