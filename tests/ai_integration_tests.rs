use std::sync::Arc;
use rustflow::agent::AnalystAgent;
use rustflow::nodes::ai::providers::{OpenAiProvider, LlmConfig, ProviderKind};
use rustflow::nodes::registry::NodeRegistry;
use rustflow::core::engine::SwanFlowEngine;
use dotenvy::dotenv;

#[tokio::test]
async fn test_real_ai_workflow_generation() {
    dotenv().ok();
    
    let api_key = std::env::var("LLM_API_KEY").expect("LLM_API_KEY not set");
    let api_base = std::env::var("LLM_API_BASE").expect("LLM_API_BASE not set");
    let model = std::env::var("LLM_MODEL").expect("LLM_MODEL not set");

    let config = LlmConfig {
        provider: ProviderKind::OpenAI,
        api_key,
        model,
        endpoint: Some(api_base),
    };

    let provider = Arc::new(OpenAiProvider::new(config));
    let registry = Arc::new(NodeRegistry::default());
    let engine = Arc::new(SwanFlowEngine::new(registry.clone()));
    
    let agent = AnalystAgent::new(provider, registry, engine.clone());
    
    // Request: Analyze a simple dataset
    let prompt = "Create a workflow that reads 'users.csv', groups by 'dept', calculates the total 'salary' per department, and saves the result to 'ai_report.xlsx' with a chart.";
    
    println!("Asking Agent to plan workflow...");
    let workflow = agent.run_analysis(prompt).await.unwrap();
    
    println!("Executing Agent-planned workflow...");
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();
    
    while let Some(event) = rx.recv().await {
        println!("  Event: {:?}", event);
    }
    
    println!("✅ Workflow execution finished. Check 'ai_report.xlsx' for results.");
}
