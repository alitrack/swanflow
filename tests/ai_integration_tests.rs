use dotenvy::dotenv;
use std::sync::Arc;
use swanflow::agent::AnalystAgent;
use swanflow::core::engine::SwanFlowEngine;
use swanflow::nodes::ai::providers::{LlmConfig, OpenAiProvider, ProviderKind};
use swanflow::nodes::registry::NodeRegistry;

#[tokio::test]
async fn test_real_ai_workflow_generation() {
    dotenv().ok();

    if std::env::var("RUSTFLOW_RUN_LIVE_LLM_TESTS").ok().as_deref() != Some("1") {
        println!(
            "Skipping live LLM integration test. Set RUSTFLOW_RUN_LIVE_LLM_TESTS=1 to enable."
        );
        return;
    }

    let api_key = std::env::var("LLM_API_KEY").expect("LLM_API_KEY not set");
    let api_base = std::env::var("LLM_API_BASE").expect("LLM_API_BASE not set");
    let model = std::env::var("LLM_MODEL").expect("LLM_MODEL not set");

    let config = LlmConfig {
        provider: ProviderKind::OpenAI,
        api_key: Some(api_key),
        model,
        endpoint: Some(api_base),
    };

    let provider: Arc<dyn swanflow::nodes::ai::providers::LlmProvider> =
        Arc::new(OpenAiProvider::new(config).unwrap());
    let registry = Arc::new(NodeRegistry::default());
    let engine = Arc::new(SwanFlowEngine::new(registry.clone()));

    let agent = AnalystAgent::new(provider, registry, engine.clone());

    // Complex Request: Multi-sheet report
    let prompt = "Create a professional dashboard from 'users.csv'. \
                  Sheet 1 should be 'Department Summary' with average salary. \
                  Sheet 2 should be 'Raw Data'. \
                  Save to 'pro_report.xlsx'.";

    println!("Asking Agent to plan multi-sheet workflow...");
    let workflow = agent.run_analysis(prompt).await.unwrap();

    println!("Executing Agent-planned pro workflow...");
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    while let Some(event) = rx.recv().await {
        println!("  Event: {:?}", event);
    }

    println!("✅ Pro Workflow execution finished. Check 'pro_report.xlsx' for results.");
}
