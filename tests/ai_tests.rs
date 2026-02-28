use std::sync::Arc;
use async_trait::async_trait;
use rustflow::nodes::ai::providers::{LlmProvider, LlmConfig, ProviderKind};
use rustflow::nodes::ai::ConfigWizard;
use rustflow::nodes::transform::FilterNode;
use rustflow::nodes::NodeBehavior;
use anyhow::Result;

struct MockProvider {
    response: String,
}

#[async_trait]
impl LlmProvider for MockProvider {
    async fn complete(&self, _prompt: &str) -> Result<String> {
        Ok(self.response.clone())
    }
}

#[tokio::test]
async fn test_config_wizard_filter_suggestion() {
    let mock_response = r#"```json
    { "expr": "age > 30" }
    ```"#.to_string();
    
    let provider = Arc::new(MockProvider { response: mock_response });
    let wizard = ConfigWizard::new(provider);
    
    let filter_node = FilterNode;
    let spec = filter_node.spec();
    
    let suggestion = wizard.suggest_config(&spec, "Filter for users older than 30").await.unwrap();
    
    assert_eq!(suggestion["expr"], "age > 30");
}
