# Design: AI Orchestration Enhancement

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                  External AI Agent                        │
│             (Hermes / Claude Code / cc)                   │
│                    │ MCP (stdio/HTTP)                     │
└────────────────────┼─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│              SwanFlow MCP Server (已有, 零改动)           │
│  list_nodes | run_pipeline | get_schema | run_query      │
│  list_connectors | inspect_workflow | list_nodes         │
│  ┌──────────────────────────────────────────────────┐    │
│  │  run_query SELECT-only guard (新)                 │    │
│  │  regex: INSERT|UPDATE|DELETE|DROP|ALTER|CREATE    │    │
│  │  → -32602 Invalid params: "Write operations not   │    │
│  │     allowed via run_query"                        │    │
│  └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│              SwanFlow Engine                              │
│  ┌──────────────────────────────────────────────────┐    │
│  │  execute_workflow()                               │    │
│  │    └→ check_capability_gate(node, mode)  (已有)   │    │
│  │       ReadOnly → 拒绝非 ReadOnly 节点              │    │
│  └──────────────────────────────────────────────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │  IntelligenceHook (已有 trait)                    │    │
│  │    ├── on_error()    ← OpenAiHook 实现 (新)       │    │
│  │    └── on_data_quality()                          │    │
│  └──────────────────────────────────────────────────┘    │
│                         │                                │
│  ┌──────────────────────┴───────────────────────────┐   │
│  │           swanflow-ai (新 crate, optional)         │   │
│  │                                                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌──────────┐ │   │
│  │  │ ModelRouter │  │ OpenAiHook  │  │ Prompt   │ │   │
│  │  │ heuristic   │  │ implements  │  │ Builder  │ │   │
│  │  │ → budget    │  │ IntellHook  │  │ modular  │ │   │
│  │  └─────────────┘  └─────────────┘  └──────────┘ │   │
│  │                                                   │   │
│  │  ┌──────────────────────────────────────────┐    │   │
│  │  │  LlmClient (OpenAI-compatible HTTP)       │    │   │
│  │  │  POST /v1/chat/completions                │    │   │
│  │  │  KeyStore → API key resolution            │    │   │
│  │  └──────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

## Component Design

### 1. ModelRouter

```rust
// crates/swanflow-ai/src/router.rs
pub enum FixComplexity { Simple, Medium, Complex }

pub struct ModelRouter {
    cheap: ModelConfig,   // e.g. deepseek-v4-flash
    mid: ModelConfig,     // e.g. gemini-2.5-flash
    best: ModelConfig,    // e.g. deepseek-v4-pro
}

impl ModelRouter {
    /// Score a diagnostic and return the appropriate model.
    pub fn route(&self, diagnostic: &NodeDiagnostic) -> ModelConfig {
        match self.score(diagnostic) {
            Simple  => self.cheap.clone(),
            Medium  => self.mid.clone(),
            Complex => self.best.clone(),
        }
    }

    fn score(&self, d: &NodeDiagnostic) -> FixComplexity {
        // Heuristics:
        // 1. Error class: Config → Simple, Data → Medium, Topology → Complex
        // 2. +1 if multi-node affected
        // 3. +1 if schema mismatch
        // Thresholds: ≤1→Simple, 2→Medium, ≥3→Complex
    }
}
```

**借鉴来源**: OpenFang `routing.rs:51-61` 的 `TaskComplexity::score()` 启发式。

### 2. OpenAiHook

```rust
// crates/swanflow-ai/src/hook.rs
pub struct OpenAiHook {
    client: LlmClient,
    router: ModelRouter,
    prompt_builder: PromptBuilder,
}

#[async_trait]
impl IntelligenceHook for OpenAiHook {
    async fn on_error(&self, node_id, type_id, config, error) -> Option<Value> {
        let diagnostic = NodeDiagnostic { class: classify(error), .. };
        let model = self.router.route(&diagnostic);
        let prompt = self.prompt_builder.build_fix_prompt(type_id, config, error);
        let response = self.client.complete(&model, &prompt).await?;
        parse_fix_json(&response) // → new config or None
    }
}
```

### 3. PromptBuilder

```rust
// crates/swanflow-ai/src/prompt.rs
pub struct PromptContext {
    pub node_type: String,
    pub node_description: String,     // from NodeSpec
    pub current_config: serde_json::Value,
    pub error_message: String,
    pub available_tools: Vec<String>, // from MCP tool list
    pub fix_instructions: String,     // ≤10 lines, NOT 200KB
}

impl PromptBuilder {
    pub fn build_fix_prompt(&self, ctx: &PromptContext) -> String {
        // Template: 
        // "You are fixing a data pipeline node '{type}' ({desc}).
        //  Current config: {json}
        //  Error: {error}
        //  Instructions: {instructions}
        //  Return JSON: {{"fixed_config": {{...}}}}"
    }
}
```

**借鉴来源**: OpenFang `prompt_builder.rs` 的 `PromptContext` → 18 字段结构化上下文 → `build()` 组装。

### 4. SELECT-only Guard

```rust
// crates/swanflow/src/mcp/server.rs (in run_query handler)
static FORBIDDEN_SQL: &[&str] = &[
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", 
    "CREATE", "TRUNCATE", "REPLACE", "MERGE",
];

fn handler_run_query(params: &Value) -> Result<Value> {
    let sql = params["sql"].as_str().ok_or(...)?;
    let upper = sql.trim().to_uppercase();
    for keyword in FORBIDDEN_SQL {
        if upper.starts_with(keyword) || upper.contains(&format!(" {}", keyword)) {
            return Err(Error::invalid_params(
                format!("Write operation '{}' not allowed via run_query. Use run_pipeline for data modifications.", keyword)
            ));
        }
    }
    // ... execute SELECT
}
```

## Data Flow: Error Recovery

```
1. Node execution fails → NodeDiagnostic
2. SwanFlowEngine calls hook.on_error(node_id, type_id, config, error)
3. OpenAiHook:
   a. classify(error) → FixComplexity::Medium
   b. router.route(Medium) → ModelConfig { model: "gemini-2.5-flash" }
   c. prompt_builder.build_fix_prompt(type_id, config, error) → fix prompt
   d. client.complete(model, prompt) → response
   e. parse response → Some(new_config) or None
4. Engine retries node with new_config
5. If hook returns None → engine uses ErrorMode (Fail/Skip/Retry)
```

## Feature Gate

```toml
# crates/swanflow/Cargo.toml
[features]
default = []
ai = ["swanflow-ai"]

[dependencies]
swanflow-ai = { path = "../swanflow-ai", optional = true }
```

不启用 `ai` feature 则 `swanflow-ai` 不编译——零开销。
