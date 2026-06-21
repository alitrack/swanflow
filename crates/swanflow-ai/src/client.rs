//! Generic HTTP LLM client — OpenAI-compatible chat completions API.

use crate::{AiError, AiResult, KeyProvider, ModelConfig};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Minimal OpenAI-compatible chat completion client.
pub struct LlmClient {
    client: reqwest::Client,
    key_provider: Arc<dyn KeyProvider>,
}

#[derive(Debug, Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: f64,
    max_tokens: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatMessage,
}

impl LlmClient {
    pub fn new(key_provider: Arc<dyn KeyProvider>) -> Self {
        Self {
            client: reqwest::Client::new(),
            key_provider,
        }
    }

    /// Send a completion request and return the response text.
    pub async fn complete(&self, config: &ModelConfig, prompt: &str) -> AiResult<String> {
        let api_key = self
            .key_provider
            .get_key("default")
            .await
            .ok_or_else(|| AiError::MissingApiKey("default".into()))?;

        let request = ChatRequest {
            model: config.model.clone(),
            messages: vec![ChatMessage {
                role: "user".into(),
                content: prompt.to_string(),
            }],
            temperature: 0.0,
            max_tokens: 1024,
        };

        let url = format!("{}/chat/completions", config.endpoint.trim_end_matches('/'));

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&request)
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    AiError::Timeout(config.timeout_secs)
                } else {
                    AiError::RequestFailed(e.to_string())
                }
            })?;

        let body: ChatResponse = response
            .json()
            .await
            .map_err(|e| AiError::InvalidResponse(format!("Failed to parse response: {}", e)))?;

        body.choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| AiError::InvalidResponse("No choices in response".into()))
    }
}
