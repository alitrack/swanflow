use anyhow::Result;
use async_trait::async_trait;
use swanflow::agent::AnalystAgent;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::nodes::ai::providers::LlmProvider;
use swanflow::nodes::registry::NodeRegistry;
use std::sync::Arc;

struct MockAnalystProvider {
    responses: Vec<String>,
}

#[async_trait]
impl LlmProvider for MockAnalystProvider {
    async fn complete(&self, _prompt: &str) -> Result<String> {
        // Return first response for simplicity in test
        Ok(self.responses[0].clone())
    }
}

#[tokio::test]
async fn test_agent_reasoning_loop_foundation() {
    let mock_json = r#"{
        "thought": "I need to read the data and filter it.",
        "tool_calls": [
            { "name": "add_node", "arguments": { "type_id": "core.io.csv", "config": { "path": "test.csv" } } }
        ],
        "final_answer": "I have drafted the initial node."
    }"#.to_string();

    let provider = Arc::new(MockAnalystProvider {
        responses: vec![mock_json],
    });
    let registry = Arc::new(NodeRegistry::default());
    let engine = Arc::new(SwanFlowEngine::new(registry.clone()));

    let agent = AnalystAgent::new(provider, registry, engine);

    let workflow = agent.run_analysis("Read test.csv").await.unwrap();

    assert_eq!(workflow.graph.node_count(), 1);
}
