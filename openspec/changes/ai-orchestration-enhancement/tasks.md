# Tasks: AI Orchestration Enhancement

## 1. Infrastructure: swanflow-ai Crate Scaffolding

- [ ] 1.1 创建 `crates/swanflow-ai/` 目录 + `Cargo.toml` + `src/lib.rs`
- [ ] 1.2 添加 workspace member + `swanflow` optional dep
- [ ] 1.3 `cargo check --workspace` 确认骨架编译通过

## 2. LlmClient — 通用 HTTP LLM 调用层

- [ ] 2.1 实现 `LlmClient` 结构体（OpenAI 兼容 `/v1/chat/completions`）
- [ ] 2.2 支持 `KeyStore` trait 解析 API key
- [ ] 2.3 单元测试：mock HTTP server

## 3. ModelRouter — 启发式预算路由

- [ ] 3.1 实现 `ModelRouter` + `FixComplexity` 枚举
- [ ] 3.2 实现 `RouterConfig::from_env()` 环境变量配置
- [ ] 3.3 单元测试：Simple/Medium/Complex 准确性

## 4. PromptBuilder — 模块化提示词组装

- [ ] 4.1 实现 `PromptContext` + `PromptBuilder::build_fix_prompt()`
- [ ] 4.2 单元测试：截断行为、JSON 转义
- [ ] 4.3 验证总长度 ≤ 4096 chars

## 5. OpenAiHook — IntelligenceHook 实现

- [ ] 5.1 实现 `OpenAiHook` 结构体 + `IntelligenceHook` trait impl
- [ ] 5.2 实现 `on_error()` 完整流程（router → prompt → client → parse）
- [ ] 5.3 `on_data_quality()` stub 返回 None
- [ ] 5.4 集成测试：mock LLM 返回 → 验证 config 修复

## 6. SELECT-only Guard — MCP run_query 安全门

- [ ] 6.1 在 `run_query` handler 添加 SQL 关键词拒绝
- [ ] 6.2 禁止列表：INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/TRUNCATE/REPLACE/MERGE
- [ ] 6.3 错误码 `-32602 Invalid params` + 描述性消息
- [ ] 6.4 测试：SELECT 通过 / INSERT 被拒 / DELETE 被拒

## 7. Integration & Feature Gate

- [ ] 7.1 `SwanFlowEngine` 新增 `ai_hook` 字段（Optional）
- [ ] 7.2 feature gate: `swanflow = { features = ["ai"] }` → 激活
- [ ] 7.3 不启用 feature 时零编译开销验证
- [ ] 7.4 `cargo build --workspace` + `cargo test --workspace` 全绿

## 8. Documentation

- [ ] 8.1 ADR-010: AI Orchestration Enhancement
- [ ] 8.2 README 更新：AI 增强功能说明
- [ ] 8.3 安全门文档：`docs/security/ai-safety-gates.md`
