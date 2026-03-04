# Embedding Generator Node Design (2026-03-04)

## Goal

补齐 `M0-05 Embedding Generator`，提供稳定、可测试、可注册的引擎节点：
- `core.ai.embedding_generator`

## Contract

Type ID:
- `core.ai.embedding_generator`

Config:
- `embedding_dim` (integer, >0, default 384)
- `normalize` (boolean, default true)
- `features_per_token` (integer, 1..8, default 2)

Behavior:
- 使用确定性哈希向量化策略生成 embedding
- 相同输入在相同配置下输出完全一致
- 可选 L2 归一化

## Integration

- 实现位置：`src/nodes/ai/retrieval.rs`
- 注册位置：`src/nodes/registry.rs`
- 测试：
  - `tests/m0_embedding_generator_tests.rs`
  - `tests/m0_contract_validation_tests.rs`（embedding_dim 边界）

## Non-goals

- 本轮不接入外部 embedding provider API
- 本轮不做 ANN 索引联动优化
