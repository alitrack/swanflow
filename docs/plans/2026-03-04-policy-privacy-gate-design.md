# M1-10 Policy & Privacy Gate Node Design

## Context

- 已完成 `PII Detector` 与 `PII Masker / De-masker`。
- 仍缺少统一策略节点，用于判定动作是否允许执行、是否必须审批。

## Goals

- 新增 `core.ai.policy_privacy_gate` 节点并注册。
- 提供确定性决策：`allow/deny` + `requires_approval` + `reason`。
- 支持配置化阻断规则与审批动作规则。

## Non-Goals

- 不接入外部策略引擎（OPA 等）。
- 不实现真实人工审批等待状态机。

## Design

- 在 `src/nodes/ai/privacy.rs` 扩展：
  - `PolicyDecision`
  - `PolicyPrivacyGateNode`
- 决策优先级：
  1. `block_when_pii=true` 且 `has_pii=true` => deny
  2. `action` 命中 `blocked_actions` => deny
  3. 其他允许
  4. `action` 命中 `approval_actions` 时 `requires_approval=true`

## Testing Strategy

- 覆盖：PII 阻断、审批标记、配置校验、节点注册。
- 回归：叠加 PII 与既有 M0/M1 关键测试。
