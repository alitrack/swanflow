use dotenvy::dotenv;
use rustflow::agent::AnalystAgent;
use rustflow::core::engine::SwanFlowEngine;
use rustflow::nodes::ai::providers::{LlmConfig, OpenAiProvider, ProviderKind};
use rustflow::nodes::registry::NodeRegistry;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn test_agent_with_variables() {
    dotenv().ok();

    let api_key = std::env::var("LLM_API_KEY").expect("LLM_API_KEY not set");
    let api_base = std::env::var("LLM_API_BASE").expect("LLM_API_BASE not set");
    let model = std::env::var("LLM_MODEL").expect("LLM_MODEL not set");

    let config = LlmConfig {
        provider: ProviderKind::OpenAI,
        api_key: Some(api_key),
        model,
        endpoint: Some(api_base),
    };

    let provider: Arc<dyn rustflow::nodes::ai::providers::LlmProvider> = Arc::new(OpenAiProvider::new(config).unwrap());
    let registry = Arc::new(NodeRegistry::default());
    let engine = Arc::new(SwanFlowEngine::new(registry.clone()));

    let agent = AnalystAgent::new(provider, registry, engine.clone());

    // Inject a variable into the initial workflow template if needed,
    // but here we just want the agent to KNOW it can use variables.
    let prompt = "Read 'users.csv', filter it using the global variable '${context.dept_filter}', and save to 'filtered_dept.xlsx'.";

    println!("Asking Agent to plan parameterized workflow...");
    let mut workflow = agent.run_analysis(prompt).await.unwrap();

    // Set the variable that the agent is expected to use
    workflow
        .context
        .insert("dept_filter".to_string(), json!("IT"));

    println!(
        "Executing Agent-planned workflow with context: {:?}",
        workflow.context
    );
    let mut rx = engine.execute_workflow(&workflow).await.unwrap();

    while let Some(event) = rx.recv().await {
        println!("  Event: {:?}", event);
    }

    println!("✅ Parameterized workflow execution finished.");
}
