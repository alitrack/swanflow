//! AI inference kernel traits — LLMInfer, Embedding, Tokenize.
//!
//! These traits abstract LLM and embedding model access behind a testable interface.
//! Production impls wrap OpenAI-compatible APIs, Ollama, or local models.
//! Mock impls return pre-configured responses for unit testing.

use async_trait::async_trait;
use std::collections::HashMap;

use crate::core::error::CoreResult;

// ---------------------------------------------------------------------------
// LLMInfer
// ---------------------------------------------------------------------------

/// LLM inference — send a prompt + optional tools, get a completion.
///
/// Supports structured output via `response_schema` (JSON Schema format).
/// This is the core trait for SwanFlow's AI Agent nodes.
#[async_trait]
pub trait LLMInfer: Send + Sync {
    /// Send a completion request to an LLM.
    async fn complete(&self, request: LlmRequest) -> CoreResult<LlmResponse>;
}

/// A request to an LLM.
#[derive(Debug, Clone)]
pub struct LlmRequest {
    /// The model identifier (e.g., "gpt-4", "ollama/llama3", "claude-sonnet-4").
    pub model: String,
    /// System prompt.
    pub system: Option<String>,
    /// User messages — the conversation history or single-turn prompt.
    pub messages: Vec<LlmMessage>,
    /// Optional JSON Schema for structured output.
    pub response_schema: Option<serde_json::Value>,
    /// Temperature (0.0 = deterministic, 1.0 = creative).
    pub temperature: Option<f64>,
    /// Max output tokens.
    pub max_tokens: Option<u32>,
    /// Optional tools that the LLM can call.
    pub tools: Vec<serde_json::Value>,
}

/// A message in an LLM conversation.
#[derive(Debug, Clone)]
pub struct LlmMessage {
    pub role: LlmRole,
    pub content: String,
}

/// Role of a message participant.
#[derive(Debug, Clone, PartialEq)]
pub enum LlmRole {
    System,
    User,
    Assistant,
    Tool,
}

/// Response from an LLM.
#[derive(Debug, Clone)]
pub struct LlmResponse {
    /// The text content of the response.
    pub content: String,
    /// Structured output parsed from response_schema (if requested).
    pub structured: Option<serde_json::Value>,
    /// Tool calls requested by the LLM (if any).
    pub tool_calls: Vec<serde_json::Value>,
    /// Usage statistics.
    pub usage: Option<LlmUsage>,
}

/// Token usage statistics.
#[derive(Debug, Clone)]
pub struct LlmUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

// ---------------------------------------------------------------------------
// Embedding
// ---------------------------------------------------------------------------

/// Text embedding — convert text to vector embeddings.
#[async_trait]
pub trait Embedding: Send + Sync {
    /// Embed a single text string, returning a dense vector.
    async fn embed(&self, text: &str) -> CoreResult<Vec<f32>>;

    /// Embed multiple texts in a batch.
    async fn embed_batch(&self, texts: &[&str]) -> CoreResult<Vec<Vec<f32>>>;

    /// Get the dimensionality of the embedding vectors.
    fn dimension(&self) -> usize;
}

// ---------------------------------------------------------------------------
// Tokenize
// ---------------------------------------------------------------------------

/// Tokenization — count, encode, decode tokens.
#[async_trait]
pub trait Tokenize: Send + Sync {
    /// Count tokens in a text string.
    async fn count_tokens(&self, text: &str) -> CoreResult<u32>;

    /// Encode text into token IDs.
    async fn encode(&self, text: &str) -> CoreResult<Vec<u32>>;

    /// Decode token IDs back into text.
    async fn decode(&self, tokens: &[u32]) -> CoreResult<String>;
}
