# AI Operators Implementation Status & Roadmap

> Date: 2026-03-03  
> Project: rustflow  
> Base Checklist: `ai-operators-60-checklist.md`

## 1. 状态概览表格

| # | 算子名称 (Operator) | 状态 (Status) | 对应实现/替代方案 | 建议 (Recommendation) |
|---|---|---|---|---|
| **M0** | **核心必选 (Core)** | | | |
| 1 | Credential Vault Bridge | 待实现 | 建议集成到 `.env` 或 `Vault` | 生产级安全需要 |
| 2 | Provider Authenticator | **已实现** | `OpenAiProvider` / `providers.rs` | 已支持多模型统一接入 |
| 4 | Chat Completion | **已实现** | `ai.chat_completion` (部分) | 核心原语 |
| 5 | Embedding Generator | 待实现 | 计划中 | RAG 检索必备 |
| 9 | Structured Output Enforcer | **已实现** | `ConfigWizard::extract_json_from_response` | 提高 AI 节点稳定性 |
| 10 | JSON Auto-Repair Parser | **已实现** | 同上 | 关键自愈能力 |
| 13 | Agent Prompter | **已实现** | `ResearchLoopMetaNode` (初步) | 驱动递归研究的核心 |
| 15 | MCP Client Connector | **已实现** | `McpInvokeNode` / `mcp_client.rs` | 插件化生态基石 |
| 18 | Tool Executor | **已实现** | `McpInvokeNode` 运行时 | 执行外部工具调用 |
| 19 | Query Planner | **已实现** | `InformationGapNode` (初步) | 任务拆解与信息差分析 |
| 26 | Web Scraper | **无必要** | 外包给 MCP | 由 Google Search/Tavily MCP 承担 |
| 27 | Site Crawler | **无必要** | 外包给 MCP | 由 Firecrawl/Jina MCP 承担 |
| **M1** | **生产增强 (Production)** | | | |
| 32 | Vector Index Writer | 待实现 | 计划中 | 闭环离线 RAG 链路 |
| 33 | Vector Retriever | 待实现 | 计划中 | RAG 检索链路 |
| 36 | Entity Extractor | **已实现** | `ai.entity_extractor` | 图谱构建核心 |
| 37 | Relation Extractor | **已实现** | `ai.entity_extractor` (双输出) | 关系提取 |
| 40 | Report Outline Planner | **无必要** | 提示词工程 | 使用 `ai.chat_completion` + 模板 |
| 45 | Human Approval Gate | 待实现 | 计划中 | 关键风险节点拦截 |
| **M2** | **战略差异 (Strategic)** | | | |
| 49 | GraphRAG Local Search | **已实现** | `ai.graph_retriever` | 邻居节点多跳检索 |
| 52 | Recursive Loop Controller | **已实现** | `ResearchState` in `graph.rs` | 递归终止与状态追踪 |
| 53 | Reflection / Critic | **已实现** | `IntelligenceHook` / `ErrorHook` | 实现自愈与结果优化 |
| 59 | Code Interpreter Sandbox | **已实现** | `core.script.boxed_python` | 基于 BoxLite (Micro-VM) + S3 |

## 2. 详细分类建议

### A. 建议移除/外包 (Outsource to MCP)
以下算子不建议在 SwanFlow 核心库中实现，应通过 `McpInvokeNode` 调用成熟的第三方 MCP 服务：
- **全网搜索与抓取**：Web Scraper, Site Crawler, Multi-Search Aggregator, Universal Resolver。
- **特定垂直领域 AI**：PII Detector, Content Moderation (建议调用云端服务如 Azure Content Safety)。

### B. 建议合并/模板化 (Merge into General AI Nodes)
以下算子不应作为独立节点存在，而应作为通用 AI 节点的 Prompt 预设：
- **内容处理**：Claim Extractor, Fact Checker, Metadata Enricher。统一使用 `EntityExtractor` 配置不同的 Extraction Schema。
- **报告生成**：Report Outline Planner, Report Composer。统一使用 `ChatCompletion` 配套相应的变量模板。

### C. 缺失的关键短板 (Critical Gaps to Implement)
为了完成 2026 年的 Deep Research 愿景，接下来的工作重点：
1.  **Embedding & Vector Search**：目前的 RAG 只有 Graph 侧，缺失了传统的 Vector 侧，无法实现 Hybrid RAG。
2.  **Code Interpreter Hardening**：目前的 `PythonNode` 权限过大，需要实现基于 WASM 或轻量虚拟机的安全沙箱。
3.  **Human-in-the-loop (HITL)**：引擎需要支持 `Suspend/Resume` 状态，以便在 `Human Approval Gate` 处停下来等待用户输入。

## 3. 结论
目前的 60 个算子中，我们已经高效实现了约 **15-20 个核心算子**（主要集中在最有技术壁垒的递归、图谱和协议层）。通过 **MCP 策略**，我们可以立刻“间接拥有” checklist 中约 **15 个抓取和搜索算子**。
