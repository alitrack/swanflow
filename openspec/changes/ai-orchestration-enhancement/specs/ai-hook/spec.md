# Spec: OpenAiHook

## Purpose

提供 `IntelligenceHook` trait 的生产级参考实现，通过 LLM 调用自动修复节点错误。

## Contract

### on_error()

- **输入**: node_id (Uuid), type_id (&str), config (&Value), error (&str)
- **输出**: `Option<Value>` — Some(修复后的 config) 或 None（无法修复）
- **保证**:
  - 每次错误最多 1 次 LLM 调用
  - 超时 15 秒内返回
  - 不修改原 config（return new）
  - 失败时返回 None（不抛异常）
  - 不阻塞 DAG 执行（tokio::time::timeout）

### on_data_quality()

- **输入**: node_id (Uuid), sample (&RecordBatch)
- **输出**: `Option<Value>` — 数据质量报告或修复建议
- **当前实现**: 返回 None（stub，Phase 2 实现）

## Details

- 文件: `crates/swanflow-ai/src/hook.rs`
- 使用 `ModelRouter` 选择模型
- 使用 `PromptBuilder` 构建修复提示词
- 使用 `LlmClient` 执行 HTTP 调用
- 响应解析: `serde_json::from_str` → 期望 `{"fixed_config": {...}}`

```rust
#[async_trait]
impl IntelligenceHook for OpenAiHook {
    async fn on_error(
        &self, node_id: Uuid, type_id: &str,
        config: &Value, error: &str,
    ) -> Option<Value> {
        let diagnostic = NodeDiagnostic::from_error(type_id, error);
        let model = self.router.route(&diagnostic);
        let ctx = PromptContext {
            node_type: type_id.to_string(),
            current_config: config.clone(),
            error_message: error.to_string(),
            ..
        };
        let prompt = self.prompt_builder.build_fix_prompt(&ctx);
        
        tokio::time::timeout(Duration::from_secs(15), 
            self.client.complete(&model, &prompt)
        ).await.ok()?
         .and_then(|resp| parse_fix_response(&resp))
    }
}
```

## Boundary Cases

- **LLM 返回无效 JSON**: 日志警告 → 返回 None
- **LLM 超时（15s）**: 返回 None，引擎使用 ErrorMode
- **LLM 返回空 config**: 返回 None
- **并发错误恢复**: 每个节点独立调用 hook（tokio 并发安全）
- **API key 缺失**: 返回 None，日志错误
