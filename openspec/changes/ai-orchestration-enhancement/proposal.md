# Proposal: AI Orchestration Enhancement

## Why

SwanFlow 当前的 MCP server（7 tools）已经能让外部 AI Agent 编排工作流——这是正确的架构方向。但 **L2 运行时智能层** 缺失生产级实现：

1. `IntelligenceHook` trait 已定义但无实现——错误发生时引擎无法自主修复
2. 无 `ModelRouter`——若需要 LLM 辅助修复错误，无法根据错误复杂度选择合适模型（全量调最贵模型 = 浪费）
3. 无结构化 `PromptBuilder`——若需要组装修复提示词，只能手拼字符串
4. `run_query` 的 SELECT-only 守卫和 `check_capability_gate()` 已设计（ADR-009）但需要完整实现和测试覆盖

**对标分析**：
- OpenFang 的 `ModelRouter`（375 行）和 `PromptBuilder`（943 行）是经过验证的模块化设计
- NPP 的 2115 行内嵌提示词是反面教材——SwanFlow 必须走模块化路径
- OpenFusion 的多模型 fusion 不适合嵌入引擎——保留为独立评测服务

## What Changes

### New: `crates/swanflow-ai/` — 可选 AI 增强 crate

```
crates/swanflow-ai/
├── Cargo.toml        ← 依赖 reqwest, serde_json (已有 workspace deps)
├── src/
│   ├── lib.rs        ← feature gate: swanflow-ai
│   ├── router.rs     ← ModelRouter: heuristic → budget
│   ├── hook.rs       ← OpenAiHook: IntelligenceHook 实现
│   ├── prompt.rs     ← PromptBuilder: 模块化上下文组装
│   └── client.rs     ← 通用 HTTP LLM client (OpenAI 兼容)
```

### Modified

- `crates/swanflow/Cargo.toml`: 添加 optional `swanflow-ai` feature
- `crates/swanflow/src/core/engine/mod.rs`: `SwanFlowEngine` 新增 `ai_hook: Option<Arc<dyn AiHook>>`
- `crates/swanflow/src/mcp/server.rs`: `run_query` 实现 SELECT-only 守卫

### Non-Goals

- ❌ 不在引擎内嵌长提示词（NPP 式反模式）
- ❌ 不实现多模型 fusion/Judge（留给 OpenFusion）
- ❌ 不实现 Agent Loop/ReAct 模式（SwanFlow 是管道引擎）
- ❌ 不添加新的外部依赖（重用已有 reqwest/serde_json）
- ❌ 不修改 `swanflow-core` kernel traits

## Capabilities

### New Capabilities

1. **ModelRouter** — 启发式复杂度评分，按预算选模型
2. **OpenAiHook** — LLM 驱动的错误修复（当 `IntelligenceHook` 触发时）
3. **PromptBuilder** — 从模块化上下文组装 AI 提示词
4. **SELECT-only 守卫** — `run_query` MCP 工具拒绝写操作
5. **安全门文档** — README + ADR 更新，明确结构级 AI 安全设计

### Modified Capabilities

- `IntelligenceHook` 从空 trait → 有参考实现的 trait
- `run_query` 从无守卫 → SELECT-only

## Impact

| Affected | Impact |
|----------|--------|
| `swanflow` crate | 新增 optional dep，`SwanFlowEngine` 新增字段 |
| `swanflow-ai` crate | **新 crate**，≤500 行代码 |
| `swanflow-core` crate | **零改动** |
| MCP server | `run_query` 工具新增 SQL 关键词拒绝 |
| 外部 Agent | 零影响——MCP 工具 schema 不变 |
| 包体积 | feature-gated，不启用则零开销 |
