use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use anyhow::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProviderKind {
    OpenAI,
    Anthropic,
    Gemini,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub provider: ProviderKind,
    pub api_key: String,
    pub model: String,
    pub endpoint: Option<String>,
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, prompt: &str) -> Result<String>;
}

pub struct OpenAiProvider {
    config: LlmConfig,
    client: reqwest::Client,
}

impl OpenAiProvider {
    pub fn new(config: LlmConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, prompt: &str) -> Result<String> {
        let mut url = self.config.endpoint.as_deref().unwrap_or("https://api.openai.com/v1").to_string();
        if !url.ends_with("/chat/completions") {
            if url.ends_with("/") {
                url.push_str("chat/completions");
            } else {
                url.push_str("/chat/completions");
            }
        }
        
        println!("  [LLM] Calling {} with model {}", url, self.config.model);

        let response = self.client.post(&url)
            .header("Authorization", format!("Bearer {}", self.config.api_key))
            .json(&serde_json::json!({
                "model": self.config.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0
            }))
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;
        
        if !status.is_success() {
            anyhow::bail!("LLM API Error ({}): {}", status, text);
        }

        let body: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| anyhow::anyhow!("Failed to parse LLM JSON: {}. Response was: {}", e, text))?;
            
        Ok(body["choices"][0]["message"]["content"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing content in LLM response body: {}", text))?
            .to_string())
    }
}
