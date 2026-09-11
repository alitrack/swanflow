# Proposal: Schema-Driven Config Contract（契约三角补齐）

## Why

调研 Forge（nihalashetty/Forge，MIT，见 research/forge-nihalashetty-20260811.md）后确认其最值得借鉴的架构模式是**共享 Schema 契约三角**：一份 JSON Schema 同时驱动 ① 后端 validator（保存时拒绝坏配置）② 编译器（生成可执行图）③ 前端表单生成（`<SchemaForm>`）——前后端不可能漂移。

对照 SwanFlow 现状（本会话架构扫描实证）：

1. **声明已存在**：`NodeSpec.config_schema: JsonValue` 已在 `crates/swanflow-core/src/nodes.rs:30` 定义，每个节点在 `spec()` 内联声明配置 schema（boundary.rs、io/excel.rs、missing.rs 等 40+ 节点）。
2. **校验却是手写的**：`NodeBehavior::validate(&self, config)`（nodes.rs:228）由每个节点**各自实现**，无 jsonschema crate、无统一 schema 驱动校验。声明式 schema 与实际校验逻辑**双轨漂移**——正是 Forge 契约三角要消灭的问题。
3. **外部消费者拿不到 schema**：adapter 协议（crates/swanflow/src/adapter/protocol.rs）的 `Inspect` 只返回端口拓扑 `AdapterSchemaSummary`（node_id + ports），**不含 config_schema**。任何外部 GUI/编辑器/Agent 无法据此生成配置表单或校验配置——Forge 的 `<SchemaForm>` 一角在 SwanFlow 中缺失。

目标：把「声明式 schema」升级为「契约」——schema 成为校验与对外暴露的唯一真源，手写 validate 降级为可选的节点特定补充。

## What Changes

### New: `crates/swanflow-core/src/validation.rs`

```rust
/// 统一 schema 驱动校验器 —— 契约三角的第一消费方
pub struct SchemaValidator {
    /// 节点类型 id -> JSON Schema（从 registry list_specs 构建）
    schemas: HashMap<String, JsonValue>,
}

impl SchemaValidator {
    /// 从 NodeRegistry 收集所有节点的 config_schema
    pub fn from_registry(registry: &NodeRegistry) -> Result<Self>;
    /// 用 JSON Schema 校验配置；错误消息带 JSON Pointer 定位
    pub fn validate_config(&self, type_id: &str, config: &JsonValue) -> Result<()>;
    /// 导出单节点 schema（供 adapter 暴露给外部）
    pub fn schema_for(&self, type_id: &str) -> Option<&JsonValue>;
}
```

- 引入 `jsonschema` crate（纯 Rust，MIT）作为唯一校验引擎。
- 校验失败返回 `SwanFlowError::ConfigValidation { type_id, path, message }`，path 为 JSON Pointer（如 `/config/sandboxed`），可被 adapter 直接透传给外部编辑器定位。

### Modified: `crates/swanflow-core/src/nodes.rs`

- `NodeBehavior` trait 增加默认方法：
  ```rust
  /// 默认实现：用 spec().config_schema 做 schema 驱动校验。
  /// 节点需要额外语义校验（如跨字段依赖）时可 override。
  fn validate(&self, config: &JsonValue) -> Result<()> {
      SchemaValidator::validate_single(&self.spec(), config)
  }
  ```
- 现有各节点的手写 `validate()` 若只做「必填字段/类型检查」，可删除（由 schema 覆盖）；若含 schema 表达不了的语义（如 Python 脚本语法预检），保留 override 并在 schema 中声明 `x-semantic-validate: true` 标记。

### Modified: `crates/swanflow/src/adapter/protocol.rs`

- `AdapterSchemaSummary` 增加 `config_schema: Option<JsonValue>` 字段（`#[serde(default)]` 保证向后兼容）。
- `Inspect` 响应为每个节点附上 config_schema —— 外部 GUI 编辑器即可生成配置表单，Agent 也可获得准确配置约束。

### Modified: `crates/swanflow/src/nodes/mod.rs`（若存在重复 NodeSpec）

⚠️ 已知模式：SwanFlow 主 crate 与 swanflow-core 存在重复结构（见 openspec-workflow skill pitfall）。修改前先 grep 确认主 crate `crates/swanflow/src/nodes/mod.rs:44` 的 `NodeSpec` 是否与 core 重复定义，若重复则同改或改为 re-export（长期方案：主 crate re-export core）。

## Capabilities

### New Capabilities

- **统一 schema 驱动校验**：所有节点的 config 校验收敛到 JSON Schema，消除声明与校验双轨漂移。
- **schema 对外暴露**：adapter `Inspect` 返回每个节点的 config_schema，外部 GUI/编辑器/Agent 可生成表单、预校验配置。
- **校验错误定位**：JSON Pointer 级错误路径，外部编辑器可直接高亮出错字段。

### Modified Capabilities

- `NodeBehavior::validate` 从「每节点手写」改为「schema 默认 + 语义 override」。
- adapter `Inspect` 响应扩展（向后兼容，旧客户端不受影响）。

### Non-goals

- ❌ 不引入任何 GUI/前端框架（SwanFlow 无 GUI，schema 暴露到 adapter 即止）。
- ❌ 不重写 40+ 节点的既有 validate 实现——默认方法生效后逐步清理，非本次范围。
- ❌ 不引入 Forge 的 MCP server 双向/治理/RBAC/评估体系。
- ❌ 不做 schema 版本化/演进工具（本次只建契约，版本化留待后续 change）。
- ❌ 不触碰 `graph-repair-operators` 等并存 change 的文件。

## Impact

| 影响面 | 说明 |
|---|---|
| crates/swanflow-core | +validation.rs；nodes.rs 的 NodeBehavior 加默认方法；Cargo.toml +jsonschema 依赖 |
| crates/swanflow | adapter/protocol.rs 扩展 Inspect 响应；若存在重复 NodeSpec 需同步 |
| 既有测试 | 基线已记录（core 76 绿 / 主 crate 129 绿 + 2 存量失败）；validate 默认方法改变行为处需断言更新 |
| 外部消费者 | adapter 协议向后兼容（新字段 serde default），外部编辑器可选择性消费 |
| 文档 | README/CHANGES 更新 + docs/adr 记录借用（Forge 契约三角） |

> 备注：主 crate Cargo.toml 已有 `schemars = "1.2.1"` 死依赖（零使用点）。jsonschema 校验器对「手写 json! schema」与「未来 schemars derive 生成的标准 JSON Schema」均兼容，方案不冲突；死依赖的清理或启用留待后续 change。
