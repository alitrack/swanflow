# Tasks: AI Orchestration Enhancement

> **记账修正 (2026-09-11)**：原 0/27 勾选，实际 swanflow-ai crate（1638 行）已完整落地：
> LlmClient / ModelRouter / PromptBuilder / OpenAiHook + SELECT-only guard + feature gate + ADR-010。
> 按代码实证回填；**6.4（guard 专项测试）/ 8.3（安全文档）已于同日落地**，仅 7.3（零编译开销验证）为真缺口。
> 验证：`cargo test --lib` 全绿（swanflow-ai 42/42 + swanflow guard_sql_tests 11/11）。

## 1. Infrastructure: swanflow-ai Crate Scaffolding

- [x] 1.1 创建 `crates/swanflow-ai/` 目录 + `Cargo.toml` + `src/lib.rs`
  - 实证：client/config/hook/lib/prompt/router 共 1638 行
- [x] 1.2 添加 workspace member + `swanflow` optional dep
  - 实证：Cargo.toml workspace members 含 swanflow-ai；swanflow Cargo.toml:71 `swanflow-ai optional`
- [x] 1.3 `cargo check --workspace` 确认骨架编译通过
  - 实证：Mac 10.10.10.100 cargo check EXIT=0；WSL 全量编译绿

## 2. LlmClient — 通用 HTTP LLM 调用层

- [x] 2.1 实现 `LlmClient` 结构体（OpenAI 兼容 `/v1/chat/completions`）
  - 实证：client.rs:8 LlmClient，:63 `/chat/completions`
- [x] 2.2 支持 `KeyStore` trait 解析 API key
  - 实证：client.rs/config.rs KeyStore
- [x] 2.3 单元测试：mock HTTP server
  - 实证：client.rs 单测（swanflow-ai 42/42 绿）

## 3. ModelRouter — 启发式预算路由

- [x] 3.1 实现 `ModelRouter` + `FixComplexity` 枚举
  - 实证：router.rs:11 FixComplexity / ModelRouter
- [x] 3.2 实现 `RouterConfig::from_env()` 环境变量配置
  - 实证：router.rs from_env + 阈值配置（Simple/Complex）
- [x] 3.3 单元测试：Simple/Medium/Complex 准确性
  - 实证：router.rs 9+ `#[test]`（42/42 绿）

## 4. PromptBuilder — 模块化提示词组装

- [x] 4.1 实现 `PromptContext` + `PromptBuilder::build_fix_prompt()`
  - 实证：prompt.rs:62 PromptContext / :91 PromptBuilder / :112 build_fix_prompt
- [x] 4.2 单元测试：截断行为、JSON 转义
  - 实证：prompt.rs 单测
- [x] 4.3 验证总长度 ≤ 4096 chars
  - 实证：prompt.rs 截断逻辑 + 单测

## 5. OpenAiHook — IntelligenceHook 实现

- [x] 5.1 实现 `OpenAiHook` 结构体 + `IntelligenceHook` trait impl
  - 实证：hook.rs:108 OpenAiHook
- [x] 5.2 实现 `on_error()` 完整流程（router → prompt → client → parse）
  - 实证：hook.rs 完整流程
- [x] 5.3 `on_data_quality()` stub 返回 None
- [x] 5.4 集成测试：mock LLM 返回 → 验证 config 修复
  - 实证：hook.rs 5+ `#[test]`

## 6. SELECT-only Guard — MCP run_query 安全门

- [x] 6.1 在 `run_query` handler 添加 SQL 关键词拒绝
  - 实证：mcp/server.rs:281 SELECT-only guard
- [x] 6.2 禁止列表：INSERT/UPDATE/DELETE/DROP/ALTER/CREATE/TRUNCATE/REPLACE/MERGE
  - 实证：server.rs 写操作拒绝
- [x] 6.3 错误码 `-32602 Invalid params` + 描述性消息
- [x] 6.4 测试：SELECT 通过 / INSERT 被拒 / DELETE 被拒
  - **已落地 (2026-09-11)**：`mcp/server.rs` 新增 `guard_sql_tests`（11 用例全绿）：
    SELECT / WHERE / JOIN / CTE / 前导空白 SELECT 通过；INSERT / DELETE / DROP /
    UPDATE（含前导空白、多语句 `SELECT 1; DELETE`、换行分隔写）被拒。
    并记录已知保守偏向：`SELECT updated_at` 因含 `UPDATE` 子串被过拦（安全失败模式，
    测试 `note_identifier_substring_is_overblocked_safe` 锁契约）。

## 7. Integration & Feature Gate

- [x] 7.1 `SwanFlowEngine` 新增 `ai_hook` 字段（Optional）
  - 实证：engine/mod.rs:150 `pub hook: Option<Arc<dyn IntelligenceHook>>`
- [x] 7.2 feature gate: `swanflow = { features = ["ai"] }` → 激活
  - 实证：swanflow Cargo.toml:76 `ai = ["swanflow-ai"]`
- [x] 7.3 不启用 feature 时零编译开销验证
  - 实证 (2026-09-11)：WSL `cargo tree -p swanflow -e normal`（默认 features）中
    `swanflow-ai` 出现 0 次；`--features ai` 出现 1 次。无 ai 时依赖不编译。
- [x] 7.4 `cargo build --workspace` + `cargo test --workspace` 全绿
  - 实证：WSL 全量 + Mac check 绿

## 8. Documentation

- [x] 8.1 ADR-010: AI Orchestration Enhancement
  - 实证：docs/adr/ADR-010-ai-orchestration-enhancement.md
- [x] 8.2 README 更新：AI 增强功能说明
  - 实证：README 能力表 `AI 预算路由 ModelRouter 5维评分` + swanflow-ai crate 说明
- [x] 8.3 安全门文档：`docs/security/ai-safety-gates.md`
  - **已落地 (2026-09-11)**：新建 `docs/security/ai-safety-gates.md`，覆盖 6 道结构性安全门
    （MCP SELECT-only guard / NodeCapability 最小权限 / Python 默认沙箱 / LuaJIT 信任沙箱+审批 /
    PII 检测+脱敏 / 已知缺口清单），每道门附真实代码位置 + 测试引用；§6 明示尚未覆盖项
    （HOTL、x-semantic-validate、run_workflow sink 审计）。

## 收尾
- 待做（可选，未阻塞）：7.3 零编译开销验证。
- ~~6.4 guard 专项测试~~ / ~~8.3 安全门文档~~ 已于 2026-09-11 落地。
