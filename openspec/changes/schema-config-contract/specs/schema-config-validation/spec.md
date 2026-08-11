# Spec: Schema-Driven Config Validation

## Purpose

将节点配置校验从「每节点手写 `validate()`」收敛为「JSON Schema 驱动」，使 `NodeSpec.config_schema` 成为配置契约的唯一真源——与 Forge 的共享 Schema 契约三角同构。外部消费者（adapter 客户端、Agent）通过同一份 schema 获得配置约束。

## Contract

1. **统一校验器**：`SchemaValidator::validate_config(type_id, config)` 对任意已注册节点类型，使用其 `config_schema` 校验配置。
2. **错误定位**：校验失败必须返回包含 JSON Pointer 路径的错误（`SwanFlowError::ConfigValidation { type_id, path, message }`）。
3. **默认方法**：`NodeBehavior::validate` 提供 schema 驱动默认实现；语义 override 的节点保持自定义实现。
4. **schema 暴露**：adapter `Inspect` 响应为每个节点返回 `config_schema`（`#[serde(default)]`，向后兼容）。
5. **无 schema 节点**：注册表中无 config_schema 的节点，`validate_config` 返回明确错误（非静默通过）。

## Details

### 文件与入口

| 项 | 位置 |
|---|---|
| 校验器 | `crates/swanflow-core/src/validation.rs`（新增） |
| trait 默认方法 | `crates/swanflow-core/src/nodes.rs`（NodeBehavior） |
| adapter 扩展 | `crates/swanflow/src/adapter/protocol.rs`（AdapterSchemaSummary + config_schema） |
| 依赖 | `crates/swanflow-core/Cargo.toml` + `jsonschema`（range-pinned） |

### 校验流程

```
validate_config(type_id, config)
  → schemas.get(type_id)          // 无 → Err("no config_schema")
  → jsonschema::validator_for(schema)   // 编译失败 → Err("invalid schema")
  → compiled.validate(config)
  → 失败取首个错误 → ConfigValidation { type_id, path: instance_path, message }
  → 通过 → Ok(())
```

### 语义 override 约定

- 节点需语义校验（Python 语法预检、跨字段依赖）时 override `validate()`，并在其 `config_schema` 顶层声明 `"x-semantic-validate": true` 作为文档标记。
- CI 对比测试：schema 校验结果必须是手写校验结果的子集（schema 不能放行手写校验拒绝的配置）。

## Boundary Cases

- **空配置**：schema 无 `required` 时允许空对象；有 required 时按 schema 拒绝。
- **未知节点类型**：`validate_config("no_such_type", ...)` → 明确错误，不静默通过。
- **非法 schema 本身**：`validator_for` 编译失败 → 明确错误（提示该节点 schema 损坏）。
- **无 config_schema 字段的节点**（若存在）：按 Contract 5 处理。
- **旧 adapter 客户端**：收到含 `config_schema` 的 Inspect 响应，因 serde default 反序列化成功，忽略新字段。
- **override 节点**：默认 schema 校验 + 语义校验都执行（override 内自行调用 super 或完整实现）。
- **JSON Pointer 为空路径**：根级错误（如类型不匹配）path 为 `""`，仍可定位到节点级。
