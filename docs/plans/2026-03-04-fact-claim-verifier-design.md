# Fact & Claim Verifier Node Design (2026-03-04)

## Goal

落地 `M1-07 Fact & Claim Verifier` 节点：
- `core.ai.fact_claim_verifier`

用于在检索后链路中对 claim 做证据一致性评分和判定，提供可测试、可复用的验证契约。

## Contract

Type ID:
- `core.ai.fact_claim_verifier`

Config:
- `support_threshold` (0.0..1.0, default 0.6)
- `require_evidence` (bool, default true)

Output fields (algorithm helper):
- `claim`
- `score`
- `verdict` (`supported|not_supported|insufficient_evidence`)
- `evidence_excerpt` (optional)

## Algorithm (MVP)

- 对 claim 和 evidence 做规范化分词
- 计算 token overlap 比例作为支持分数
- 使用 `support_threshold` 给出 verdict
- 缺失 evidence 且 `require_evidence=true` 时给出 `insufficient_evidence`

## Non-goals

- 本轮不接入外部 fact-check API
- 不做跨文档推理链置信度融合
