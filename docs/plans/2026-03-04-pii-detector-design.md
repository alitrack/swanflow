# M1-08 PII Detector Node Design

## Context

- Blueprint `docs/ai-operators-40-blueprint.md` 将 `PII Detector` 定义为 M1 生产增强关键节点。
- 当前 Rust 引擎缺少独立、可注册、可测试的 PII 检测节点。
- UI 仍未开发，优先提供稳定后端契约能力，供后续流程节点复用。

## Goals

- 新增 `core.ai.pii_detector` 节点并注册到默认 `NodeRegistry`。
- 提供确定性 PII 检测（邮箱、电话、SSN、银行卡号）能力，避免依赖外部模型。
- 提供最小配置守卫（`min_confidence`、`categories`）并覆盖测试。

## Non-Goals

- 不实现脱敏回写与复原（该能力归属 `PII Masker / De-masker`）。
- 不接入云端内容安全服务。
- 不改动 UI 和前端配置器。

## Design

- 新增 `src/nodes/ai/privacy.rs`：
  - `PiiMatch`：单条匹配结果（类型、值、位置、置信度）。
  - `PiiDetectionResult`：一次文本检测输出。
  - `PiiDetectorNode`：提供 `detect_pii`（确定性正则检测）和 `NodeBehavior`。
- 配置项：
  - `min_confidence`：`[0,1]`，默认 `0.5`。
  - `categories`：可选字符串数组，允许 `email|phone|ssn|credit_card`。
- `propose_plan` 采用单输入 pass-through，先保证节点契约与注册闭环。

## Testing Strategy

- 先新增失败测试：
  - 检测常见 PII（邮箱/电话/SSN）
  - 分类过滤与阈值生效
  - 非法配置校验
  - 节点注册校验
- 再实现最小代码使测试转绿。
- 运行目标回归：`m1_pii_detector_tests` + 既有 M1/M0 增量回归。

## Opsx Guidance

- Change: `m1-pii-detector-node`
- 按任务顺序推进：测试 -> 实现 -> 注册 -> 回归 -> 归档。
