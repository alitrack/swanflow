# M1 Hybrid Retriever + Reranker Design (2026-03-04)

## Goal

在现有 M0 检索闭环上补齐 M1 检索增强核心：
- `core.ai.hybrid_retriever`
- `core.ai.reranker`

目标是先提供可测试、可注册、可编排的最小契约实现，再逐步接入外部 rerank 模型或 LLM rerank 后端。

## Design Choice

采用双层结构：
1. 节点层（NodeBehavior）提供统一配置与 workflow 集成。
2. 算法层（纯函数/方法）提供可单测的排序与融合逻辑。

这样可以把“节点编排能力”和“具体 rerank 后端能力”解耦，后续可无缝切换：
- 专用 rerank 模型（cross-encoder）
- LLM listwise rerank

## Contracts

### Hybrid Retriever

Type ID: `core.ai.hybrid_retriever`

Config:
- `alpha` (0.0~1.0): embedding 分数权重
- `top_k` (>0): 返回上限

Fusion:
- 按 `source_id` 合并 sparse/semantic 两路候选
- 分数：`final = (1-alpha)*sparse + alpha*semantic`
- 稳定排序：`score desc` + `source_id asc`

### Reranker

Type ID: `core.ai.reranker`

Config:
- `top_k` (>0)
- `query_term_boost` (>=0)

Ranking:
- 对候选文本做 query token overlap 加权
- `final = base_score + overlap_count * query_term_boost`
- 稳定排序并截断 `top_k`

## Testing

新增测试覆盖：
- Hybrid 融合权重与排序行为
- Reranker 排序与 top_k
- 两节点注册到 NodeRegistry
- 配置边界校验

## Non-goals

- 本轮不接入真实外部模型 provider 调用
- 不做跨节点 runtime 批处理优化
- 不引入额外存储或新执行引擎算子
