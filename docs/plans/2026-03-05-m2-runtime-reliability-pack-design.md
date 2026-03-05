# M2 Runtime Reliability Pack Design

## Context

- M0/M1 已补齐检索、验证、隐私治理基础节点。
- M2 需要先补“可靠性底座”，再扩展复杂编排。
- 当前优先 Rust 引擎后端契约，不涉及 UI。

## Scope (Phase-1 M2)

1. Retry/Backoff + Idempotency Guard
2. Checkpoint/Resume Controller
3. Run-State Store
4. Observability & Audit Ledger
5. Agent Loop Orchestrator (minimal)

## Goals

- 为 M2 节点提供确定性、可测试、可注册的最小能力闭环。
- 每个节点先提供 helper 方法和 `NodeBehavior` 契约，`propose_plan` 保持 pass-through。
- 对关键配置项做严格校验，保证行为可预测。

## Non-Goals

- 不实现分布式持久化存储。
- 不实现跨进程恢复和真正调度器。
- 不实现完整多智能体通信协议。

## Design

- 新增 `src/nodes/ai/runtime.rs`，集中承载 M2 runtime 节点：
  - `RetryBackoffGuardNode`
  - `CheckpointResumeControllerNode`
  - `RunStateStoreNode`
  - `ObservabilityAuditLedgerNode`
  - `AgentLoopOrchestratorNode`
- 统一风格：
  - helper 函数用于确定性行为（重试序列、checkpoint token、state 操作、审计事件、循环编排）
  - NodeBehavior 提供 config schema + validate + pass-through plan
- 在 `src/nodes/registry.rs` 注册 5 个新节点：
  - `core.ai.retry_backoff_idempotency_guard`
  - `core.ai.checkpoint_resume_controller`
  - `core.ai.run_state_store`
  - `core.ai.observability_audit_ledger`
  - `core.ai.agent_loop_orchestrator`

## Testing Strategy

- 新增 `tests/m2_runtime_reliability_tests.rs`，覆盖：
  - 重试退避与幂等 key 去重
  - checkpoint save/resume
  - run-state put/get/delete
  - 审计事件记录与过滤
  - loop orchestrator 终止条件
  - 5 节点注册校验与配置错误校验
- 回归测试：叠加已有 M0/M1 核心增量套件。

## Opsx Flow

- Change: `m2-runtime-reliability-pack`
- 路径：tests(red) -> implementation(green) -> regression -> archive
