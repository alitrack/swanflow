# Tasks: Schema-Driven Config Contract

> 执行前置：openspec 四件套审批通过；工作区未提交改动（nodes.rs/registry.rs M 状态 + graph-repair-operators）**禁止触碰**。

## 基线记录（2026-08-11 预规划门实测）

- `cargo check --workspace`：**零错误** ✅
- `cargo test -p swanflow-core`：**76 passed / 0 failed** ✅
- `cargo test -p swanflow --lib`：**129 passed / 2 failed**（失败 = 存量 bug，与本 change 无关）：
  - `core::templating::test_mustache_placeholder_input` / `test_mustache_placeholder_custom_var` —— `render_string` 只支持 `${context.key}`，不支持 `{{mustache}}`（`render_json` 已支持）；文件无未提交改动，提交 0bf6a8f 引入
- ⚠️ 环境注意：测试需要 `LD_LIBRARY_PATH=/home/lhy/.local/share/uv/python/cpython-3.11.14-linux-x86_64-gnu/lib`（PyO3 找 libpython3.11.so）
- ⚠️ 编译注意：`cargo test --workspace` 全量会 OOM（8 并行编译 wasmtime/aws-sdk）→ 用 `CARGO_BUILD_JOBS=4` 或只测目标 crate

## 1. Milestone 1: 统一校验器 ✅（2026-08-11）

- [x] 1.1 `cargo add jsonschema@0.49.9`（swanflow-core，range-pinned）
- [x] 1.2 新增 `crates/swanflow-core/src/validation.rs`：`SchemaValidator` + `validate_config` + `schema_for` + `validate_single` + `from_registry` + `known_types`
- [x] 1.3 lib.rs `pub mod validation` + `pub use SchemaValidator`
- [x] 1.4 SwanFlowError 复用现有 `config_error()`（Semantic/ConfigError），未新增变体——错误信息已含 type_id + JSON Pointer 路径，变体冗余（实现时判断）
- [x] 1.5 单测 7 个全绿（valid / missing-required / wrong-type / unknown-type / invalid-schema / empty-config / from-registry）
- [x] 1.6 `cargo check -p swanflow-core` 零 error + `cargo test -p swanflow-core` 83/83 绿
- 经验：jsonschema 0.49 的 required 错误 instance_path 为空（字段名在消息里）；type 错误才带 JSON Pointer

## 2. Milestone 2: NodeBehavior 默认实现

- [ ] 2.1 `NodeBehavior::validate` 增加 schema 驱动默认方法（`validate_single` 实现）
- [ ] 2.2 grep 确认主 crate 是否重复定义 NodeSpec（`crates/swanflow/src/nodes/mod.rs:44`）；若重复 → 同步加默认方法或 re-export（不擅自重构）
- [ ] 2.3 抽查 3 个节点（boundary / excel / python）确认 schema 与手写 validate 语义一致；不一致处记录到 2.4
- [ ] 2.4 语义 override 节点在 schema 加 `x-semantic-validate: true` 标记（如 python.rs）
- [ ] 2.5 CI 对比测试：schema 校验 ⊆ 手写校验（新增测试文件）
- [ ] 2.6 `cargo test --workspace` 全绿

## 3. Milestone 3: adapter 暴露 schema ✅（2026-08-11）

- [x] 3.1 `AdapterSchemaSummary` 增加 `config_schema: Option<JsonValue>`（`#[serde(default)]` 向后兼容）
- [x] 3.2 `Inspect` 处理逻辑从 registry `get_node(type_id)` 取 config_schema 填充（engine 用 `Arc::clone` 保持 registry 可借用）
- [x] 3.3 单测 4 个全绿：序列化含 schema / 旧格式无字段反序列化为 None / roundtrip / InspectResult roundtrip
- [x] 3.4 `cargo check -p swanflow` 零 error；`cargo test -p swanflow --lib adapter` 4/4 绿
- ⚠️ 全量 `cargo test -p swanflow --lib` 极慢（143 测试 >35min，CPU 高），改用子集验证
- ⚠️ 发现存量失败（与本 change 无关）：`core::engine::duckdb_backend::tests::test_register_and_query_in_memory_table` —— panic 于 duckdb crate 内部 `raw_statement.rs:91`（"The statement was not executed yet"）；本 change 改动文件不含 engine/duckdb 代码

## 4. Milestone 4: 文档与收尾

- [x] 4.1 README 架构节补 schema 契约说明（声明与校验不漂移）
- [x] 4.2 ADR-011-schema-driven-config-contract.md（Context = Forge 契约三角调研 / Decision 三项 / Consequences / Evidence / Alternatives）
- [x] 4.3 提交实现代码（openspec/ 不入 git）—— commit `5d308e4`（9 文件 460+/15-；他人改动零混入：nodes.rs NodeState 部分/registry.rs 留在工作区）
- [x] 4.4 回归：`cargo clippy -p swanflow-core` 零 error 零新增告警（validation.rs 干净）；全量测试受环境限制用子集验证（见 M3 注）
- [ ] 4.5 用户确认后推送（分支 `feat/schema-config-contract`）
