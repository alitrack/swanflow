//! SwanFlow AI — optional AI enhancement crate.
//!
//! Provides production-ready implementations of:
//! - [`ModelRouter`] — heuristic complexity scoring → budget-aware model selection
//! - [`OpenAiHook`] — LLM-powered [`IntelligenceHook`] for error recovery
//! - [`PromptBuilder`] — modular context assembly (NOT monolithic 200KB prompts)
//! - [`LlmClient`] — generic HTTP LLM client (OpenAI-compatible API)
//! - [`RouterConfig`] — configuration loading and validation
//!
//! # Feature Gate
//!
//! This crate is an **optional** dependency of `swanflow`. When not enabled,
//! SwanFlow has zero AI-related code compiled in.
//!
//! ```toml
//! # Enable AI features:
//! swanflow = { path = "../swanflow", features = ["ai"] }
//! ```

pub mod client;
pub mod config;
pub mod hook;
pub mod prompt;
pub mod router;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Model configuration for routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelConfig {
    /// Model identifier (e.g. "deepseek-v4-flash", "gemini-2.5-flash")
    pub model: String,
    /// API endpoint (e.g. "https://api.deepseek.com/v1")
    pub endpoint: String,
    /// Request timeout
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
    15
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self {
            model: "deepseek-v4-flash".into(),
            endpoint: "https://api.deepseek.com/v1".into(),
            timeout_secs: 15,
        }
    }
}

/// Alias for the core kernel's `KeyStore` trait — provides API key resolution.
#[async_trait]
pub trait KeyProvider: Send + Sync {
    /// Get an API key by provider name.
    async fn get_key(&self, provider: &str) -> Option<String>;
}

/// Result type for AI operations.
pub type AiResult<T> = Result<T, AiError>;

#[derive(Debug, thiserror::Error)]
pub enum AiError {
    #[error("No API key configured for provider '{0}'")]
    MissingApiKey(String),

    #[error("LLM request failed: {0}")]
    RequestFailed(String),

    #[error("LLM returned invalid response: {0}")]
    InvalidResponse(String),

    #[error("Request timed out after {0}s")]
    Timeout(u64),

    #[error("Fix parsing failed: {0}")]
    ParseError(String),
}
