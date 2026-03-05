# M1-09 PII Masker / De-masker Node Design

## Context

- `M1-08 PII Detector` 已具备确定性敏感信息识别能力。
- 生产链路仍缺少“出网前脱敏、回流后复原”的基础算子能力。
- 当前阶段目标是 Rust 引擎闭环，UI 暂不涉及。

## Goals

- 新增 `core.ai.pii_masker` 节点并注册到默认 `NodeRegistry`。
- 提供可逆脱敏：`mask_pii` 生成 token + 映射，`demask_pii` 按映射恢复原文。
- 与 `PII Detector` 协同，支持 `categories` 与 `min_confidence` 过滤。

## Non-Goals

- 不引入外部密钥托管或加密存储。
- 不实现策略审批流（留给 `Policy & Privacy Gate` 与 `Human Approval Gate`）。

## Design

- 在 `src/nodes/ai/privacy.rs` 扩展：
  - `PiiMaskReplacement`
  - `PiiMaskResult`
  - `PiiMaskerNode`
- `mask_pii`：
  - 复用 `PiiDetectorNode::detect_pii` 获取命中项。
  - 按出现顺序生成 token（默认前缀 `PII`）。
  - 逆序替换文本，避免偏移错位。
- `demask_pii`：
  - 按 token 映射恢复原文。
- `NodeBehavior` 当前保持 pass-through，优先契约可复用性。

## Testing Strategy

- TDD 测试覆盖：
  - 脱敏与复原正确性
  - 分类过滤下的脱敏行为
  - 非法配置校验
  - 节点注册
- 回归测试包含现有 `m1_pii_detector_tests` 与关键 M1/M0 测试。

## Opsx Guidance

- Change: `m1-pii-masker-node`
- 路径：tests -> implementation -> registry -> regression -> archive
