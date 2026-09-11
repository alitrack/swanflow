# Spec: PromptBuilder

## Purpose

从模块化上下文组装 AI 提示词——每个来源独立文件/数据，运行时模板注入，非 NPP 式 200KB 内嵌字符串。

## Contract

### PromptBuilder::build_fix_prompt()

- **输入**: `PromptContext` (node_type, description, current_config, error_message, tools, instructions)
- **输出**: `String` — 完整提示词
- **保证**:
  - 总长度 ≤ 4096 字符
  - 包含：节点类型 + 描述 + 当前配置 + 错误信息 + 可用工具 + 修复指令 + 输出格式要求
  - 无硬编码工具名（从 registry 动态获取）
  - 无 session/开发者注释

### PromptContext

```rust
pub struct PromptContext {
    pub node_type: String,           // e.g. "FileSource"
    pub node_description: String,    // from NodeSpec
    pub current_config: Value,       // current settings JSON
    pub error_message: String,       // actual error
    pub available_tools: Vec<String>,// e.g. ["read_file", "set_node_params"]
    pub fix_instructions: String,    // domain-specific guidance
}
```

### Template 格式

```
You are fixing a data pipeline node.

Node: {node_type} — {description}
Error: {error_message}
Current config: {config_json}

Available tools: {tools}

Instructions: {instructions}

Return a JSON object: {{"fixed_config": {{...}}}}
Only change the fields that need fixing. Keep all other fields unchanged.
```

## Details

- 文件: `crates/swanflow-ai/src/prompt.rs`
- 模板引擎: 简单 `str::replace`（不需要完整模板引擎——字段数 <10）
- 模板来源: 代码内 `const`（≤20 行）或 `.md` 文件（需时再切）
- 借鉴: OpenFang `prompt_builder.rs` 的模块化上下文模式

## Boundary Cases

- **config 过大（>2000 chars）**: 截断到 2000 chars + "...(truncated)"
- **error_message 包含特殊字符**: JSON 序列化自动转义
- **提示词超过 4096 chars**: 优先截断 config，再截断 instructions
- **node_description 缺失**: 用 type_id 替代
