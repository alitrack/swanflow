# Spec: AI Analyst Agent

## Purpose

完成 AnalystAgent v2 的"规划→推断→验证→修复"闭环，使其能在自然语言描述下自主构建、验证并修复数据科学工作流。

> 本 spec 是对已有 `openspec/changes/analyst-agent-v2/` 的完善和扩展，不是替代。

## Contract

- Agent 接收自然语言描述 → 输出可执行的 YAML 工作流
- Agent 在编译错误后自动修复（不要求用户干预）
- Agent 在运行错误后自动修复（IntelligenceHook 触发）
- Agent 凭统计信息（而非原始数据）做规划

## Architecture

```
User: "Analyze Q3 sales, find outliers, export to Excel"

Agent Loop:
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐
│ Plan     │ → │ Generate │ → │ Verify   │ → │ Execute  │
│ (Macro)  │   │ YAML     │   │ infer_   │   │ (pass)   │
│          │   │ Workflow │   │ metadata │   │          │
└──────────┘   └──────────┘   └──────────┘   └──────────┘
                                    │ fail
                                    ▼
                               ┌──────────┐
                               │ Fix      │
                               │ (Atomic) │
                               │ patch node│
                               └──────────┘
```

## Tools

| Tool | Description |
|------|-------------|
| `generate_workflow_yaml` | 根据语义描述生成完整 YAML，含节点定义和连线 |
| `patch_node_config` | 修改指定节点的参数（不重建整个图） |
| `add_node` | 在 DAG 中插入新节点 |
| `connect` | 在两个节点间建立连线 |
| `infer_schema` | 推断节点输出 schema（已有） |
| `profile_data` | 计算列级统计（新增） |

## Statistical Profiling

Agent 做规划前先跑 profiler：

```
ProfilerNode output:
  columns:
    - name: amount
      type: float64
      min: 0.0
      max: 100000.0
      nulls: 12
      distinct: 892
      top_k: [500.0, 1000.0, 250.0]
```

Agent 凭此决定：需不需要过滤 NULL？需不需要分组？分桶粒度？

## Self-Healing

IntelligenceHook 集成：

1. 节点执行失败
2. Hook 捕获错误类型 + 错误消息
3. 调用 Agent 的 `patch_node_config` 或 `add_node`
4. Agent 重新验证 (`infer_metadata`)
5. 验证通过后重试执行
6. 记录自愈日志

最大自愈重试：3 次。超过则 fallback 到 human-in-the-loop。
