# Spec: ModelRouter

## Purpose

为 SwanFlow 运行时 AI 调用提供启发式复杂度评分 → 预算感知的模型选择。当 `IntelligenceHook` 需要调用 LLM 修复错误时，根据错误复杂度选择最经济的模型。

## Contract

### Router::route()
- **输入**: `NodeDiagnostic` (error_class, affected_nodes, schema_mismatch)
- **输出**: `ModelConfig` (model name, endpoint, timeout)
- **保证**: 
  - Simple 错误 → cheap 模型（默认 deepseek-v4-flash）
  - Medium 错误 → mid 模型（默认 gemini-2.5-flash）  
  - Complex 错误 → best 模型（默认 deepseek-v4-pro）
  - 模型列表可运行时配置（非硬编码）

### 复杂度评分算法

| 信号 | 加权 |
|------|------|
| `NodeErrorClass::Config` | +1 |
| `NodeErrorClass::Data` / `Schema` | +2 |
| `NodeErrorClass::Topology` | +3 |
| `affected_nodes > 1` | +1 |
| `schema_mismatch == true` | +1 |

| 得分 | 复杂度 | 模型 |
|------|--------|------|
| ≤1 | Simple | cheap |
| 2 | Medium | mid |
| ≥3 | Complex | best |

### RouterConfig

```rust
pub struct RouterConfig {
    pub cheap: ModelConfig,
    pub mid: ModelConfig,
    pub best: ModelConfig,
}
```

## Details

- 文件: `crates/swanflow-ai/src/router.rs`
- 依赖: `NodeDiagnostic`（已有），`NodeErrorClass`（已有）
- 配置来源: `RouterConfig` 从环境变量或配置 YAML 读取
- 借鉴: OpenFang `routing.rs:51-61` 启发式，但用 SwanFlow 的 `NodeDiagnostic` 替代 token 计数

## Boundary Cases

- **诊断信息为空/无分类**: 默认 Medium → mid 模型
- **配置缺失**: `RouterConfig::default()` 返回硬编码 fallback
- **所有模型相同**: 路由仍工作——返回同一模型（预算路由退化为固定路由）
- **模型不可用**: 由 `LlmClient` 层处理超时/重试，Router 层不感知
