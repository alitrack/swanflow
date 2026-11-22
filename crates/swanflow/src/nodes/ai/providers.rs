use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ProviderKind {
    OpenAI,
    Anthropic,
    Gemini,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub provider: ProviderKind,
    /// API key. If not provided, will try to read from environment variable:
    /// - OPENAI_API_KEY for OpenAI
    /// - ANTHROPIC_API_KEY for Anthropic
    /// - GEMINI_API_KEY for Gemini
    pub api_key: Option<String>,
    pub model: String,
    pub endpoint: Option<String>,
}

impl LlmConfig {
    /// Create config with API key from environment variable if not provided.
    pub fn resolve_api_key(&mut self) -> Result<()> {
        if self.api_key.is_some() {
            return Ok(());
        }

        let env_key = match self.provider {
            ProviderKind::OpenAI => "OPENAI_API_KEY",
            ProviderKind::Anthropic => "ANTHROPIC_API_KEY",
            ProviderKind::Gemini => "GEMINI_API_KEY",
        };

        self.api_key = std::env::var(env_key).ok();

        if self.api_key.is_none() {
            anyhow::bail!(
                "API key not provided in config and {} environment variable not set",
                env_key
            );
        }

        Ok(())
    }
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
    pub fn new(mut config: LlmConfig) -> Result<Self> {
        config.resolve_api_key()?;
        Ok(Self {
            config,
            client: reqwest::Client::new(),
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, prompt: &str) -> Result<String> {
        let mut url = self
            .config
            .endpoint
            .as_deref()
            .unwrap_or("https://api.openai.com/v1")
            .to_string();
        if !url.ends_with("/chat/completions") {
            if url.ends_with("/") {
                url.push_str("chat/completions");
            } else {
                url.push_str("/chat/completions");
            }
        }

        eprintln!("  [LLM] Calling {} with model {}", url, self.config.model);

        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.api_key.as_ref().unwrap()),
            )
            .json(&serde_json::json!({
                "model": self.config.model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.0,
                "max_tokens": 32768
            }))
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            anyhow::bail!("LLM API Error ({}): {}", status, text);
        }

        let body: serde_json::Value = serde_json::from_str(&text).map_err(|e| {
            anyhow::anyhow!("Failed to parse LLM JSON: {}. Response was: {}", e, text)
        })?;

        // Log finish_reason + usage so silent truncation (finish_reason=length on
        // reasoning models sharing max_tokens between reasoning and content) is
        // diagnosable from stderr.
        eprintln!(
            "  [LLM] finish_reason={:?} usage={}",
            body["choices"][0]["finish_reason"].as_str().unwrap_or("?"),
            body["usage"]
        );

        Ok(body["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing content in LLM response body: {}", text))?
            .to_string())
    }
}
