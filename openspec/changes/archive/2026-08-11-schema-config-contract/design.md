# Design: Schema-Driven Config Contract

## 架构总览

```
                    ┌─────────────────────────────────────────────┐
                    │        NodeSpec.config_schema（唯一真源）    │
                    │  （已在 40+ 节点 spec() 中声明，无需迁移）     │
                    └──────────────────┬──────────────────────────┘
                                       │
              ┌────────────────────────┼────────────────────────┐
              ▼                        ▼                        ▼
   ┌──────────────────┐   ┌────────────────────┐   ┌─────────────────────────┐
   │ SchemaValidator  │   │ NodeBehavior::      │   │ adapter Inspect 响应    │
   │ (validation.rs)  │   │ validate 默认实现    │   │ config_schema 字段       │
   │ jsonschema crate │   │ （语义 override）    │   │ → 外部 GUI/编辑器生成表单 │
   │ JSON Pointer 定位 │   │                    │   │ → Agent 获取配置约束      │
   └──────────────────┘   └────────────────────┘   └─────────────────────────┘
        契约三角消费方 1      契约三角消费方 2          契约三角消费方 3（对外）
```

对应 Forge 的契约三角：validator（后端校验）+ compiler（本项目中 validate 发生在执行前的配置检查）+ SchemaForm（前端表单）。SwanFlow 无 GUI，「前端表单」一角由 adapter 对外暴露承担——外部编辑器（如未来 GUI 客户端）消费同一份 schema。

## 数据流

1. **注册期**：`NodeRegistry::register_core_nodes()` 注册节点 → `SchemaValidator::from_registry()` 收集全部 `config_schema` 建索引。
2. **校验期**：任何配置写入（adapter Execute / Agent 生成 / 外部导入）→ `SchemaValidator::validate_config(type_id, config)`：
   - 用 `jsonschema` crate 编译 `config_schema`，校验 `config`。
   - 失败 → `SwanFlowError::ConfigValidation { type_id, path, message }`（path 为 JSON Pointer）。
3. **执行前**：`NodeBehavior::validate()` 默认走 schema；节点有语义校验（Python 语法预检、跨字段依赖）时 override 并在 schema 加 `x-semantic-validate: true`。
4. **对外**：adapter `Inspect` 响应逐节点附带 `config_schema: Option<JsonValue>`（serde default 向后兼容）。

## 关键类型设计

### SchemaValidator（crates/swanflow-core/src/validation.rs）

```rust
use std::collections::HashMap;
use serde_json::Value as JsonValue;

pub struct SchemaValidator {
    schemas: HashMap<String, JsonValue>,
}

impl SchemaValidator {
    pub fn from_registry(registry: &NodeRegistry) -> Result<Self> {
        let mut schemas = HashMap::new();
        for spec in registry.list_specs() {
            schemas.insert(spec.type_id.clone(), spec.config_schema.clone());
        }
        Ok(Self { schemas })
    }

    pub fn validate_config(&self, type_id: &str, config: &JsonValue) -> Result<()> {
        let schema = self.schemas.get(type_id).ok_or_else(|| {
            anyhow!("Node type '{type_id}' has no config_schema in registry")
        })?;
        let compiled = jsonschema::validator_for(schema)
            .map_err(|e| anyhow!("Invalid JSON Schema for {type_id}: {e}"))?;
        if let Err(errors) = compiled.validate(config) {
            for err in errors {
                return Err(SwanFlowError::ConfigValidation {
                    type_id: type_id.to_string(),
                    path: err.instance_path.to_string(), // JSON Pointer
                    message: err.to_string(),
                }.into());
            }
        }
        Ok(())
    }

    pub fn schema_for(&self, type_id: &str) -> Option<&JsonValue> {
        self.schemas.get(type_id)
    }
}
```

### NodeBehavior::validate 默认实现（nodes.rs）

```rust
fn validate(&self, config: &JsonValue) -> Result<()> {
    // 默认：schema 驱动。语义 override 的节点在 schema 中标记 x-semantic-validate。
    SchemaValidator::validate_single(&self.spec(), config)
}
```

`validate_single` 为静态辅助（不依赖 registry），供 trait 默认方法直接调用。

### adapter 协议扩展（protocol.rs）

```rust
pub struct AdapterSchemaSummary {
    pub node_id: String,
    pub ports: Vec<Vec<String>>,
    #[serde(default)]
    pub config_schema: Option<JsonValue>, // 新增，向后兼容
}
```

## 依赖

- `jsonschema` crate：最新稳定版（当前 0.28.x / 0.29.x），纯 Rust、MIT 许可、支持 JSON Pointer 错误定位。`cargo add jsonschema` 于 swanflow-core。
- 不引入其他新依赖。

## 兼容性与迁移

1. **adapter 协议**：`config_schema` 带 `#[serde(default)]`，旧客户端反序列化不受影响；新客户端选择性消费。
2. **既有 validate 实现**：默认方法生效后，行为分两类：
   - 纯「必填/类型检查」类 → schema 已覆盖，删除手写实现（随测试验证，非本次全部清理）。
   - 语义检查类（Python 语法等）→ 保留 override，测试断言 `validate` 仍返回预期错误。
3. **测试**：既有 45 测试文件基线先行；新增 validation.rs 单测覆盖：合法配置通过、缺字段/错类型报错含 JSON Pointer、无 schema 节点报错、override 节点仍走语义校验。

## 风险

| 风险 | 缓解 |
|---|---|
| 既有节点 config_schema 与手写 validate 语义不一致（schema 未覆盖全部约束） | 默认方法只影响「之前未手写 validate」的节点；语义 override 节点不受影响。CI 加对比测试：schema 校验结果 ⊆ 手写校验结果 |
| jsonschema 版本 API 变化 | range-pinned（如 `jsonschema = "0.29"`），锁版本范围 |
| 主 crate 与 core 重复 NodeSpec 结构 | 先 grep 确认（nodes/mod.rs:44），若重复则两处同步加默认方法或 re-export |
