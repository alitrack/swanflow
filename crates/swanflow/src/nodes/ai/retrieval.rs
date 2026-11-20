use crate::nodes::ai::ConfigWizard;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, DisplayAs, DisplayFormatType, ExecutionPlan,
    PlanProperties, RecordBatchStream,
};
use datafusion::physical_expr::EquivalenceProperties;
use futures::stream::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryPlanItem {
    pub query: String,
    pub rank: u32,
}

pub struct QueryPlanningNode;

impl QueryPlanningNode {
    pub fn plan_queries(&self, goal: &str, config: &JsonValue) -> Result<Vec<QueryPlanItem>> {
        let max_subqueries = config["max_subqueries"].as_u64().unwrap_or(3) as usize;
        if max_subqueries == 0 {
            return Err(anyhow!("max_subqueries must be greater than zero"));
        }

        let mut normalized = goal
            .replace(['\n', '\r'], " ")
            .replace('；', ";")
            .replace('，', ",")
            .replace('。', ".");
        normalized = normalized.replace(" and ", ";");

        let mut pieces: Vec<String> = normalized
            .split([';', '.', '?', '!'])
            .flat_map(|segment| segment.split(','))
            .map(str::trim)
            .filter(|segment| !segment.is_empty())
            .map(|segment| segment.to_string())
            .collect();

        if pieces.is_empty() {
            return Err(anyhow!("goal must not be empty"));
        }

        let mut seen = std::collections::HashSet::new();
        pieces.retain(|part| seen.insert(part.to_lowercase()));
        pieces.truncate(max_subqueries);

        Ok(pieces
            .into_iter()
            .enumerate()
            .map(|(idx, query)| QueryPlanItem {
                query,
                rank: (idx + 1) as u32,
            })
            .collect())
    }
}

#[async_trait]
impl NodeBehavior for QueryPlanningNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.query_planning".to_string(),
            display_name: "Query Planning".to_string(),
            description: "[SHELL NODE — no runtime behavior] Split a research goal into ranked sub-queries.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Goal input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Planned query output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_subqueries": { "type": "integer", "minimum": 1, "default": 3 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(max) = config["max_subqueries"].as_u64() {
            if max == 0 {
                return Err(anyhow!("max_subqueries must be greater than zero"));
            }
            return Ok(());
        }
        if config.get("max_subqueries").is_some() {
            return Err(anyhow!("max_subqueries must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Query Planning requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

pub struct EmbeddingGeneratorNode;

impl EmbeddingGeneratorNode {
    pub fn generate_embeddings(
        &self,
        texts: &[String],
        config: &JsonValue,
    ) -> Result<Vec<Vec<f32>>> {
        self.validate(config)?;

        let embedding_dim = config["embedding_dim"].as_u64().unwrap_or(384) as usize;
        let normalize = config["normalize"].as_bool().unwrap_or(true);
        let features_per_token = config["features_per_token"].as_u64().unwrap_or(2) as usize;

        let mut output = Vec::with_capacity(texts.len());
        for text in texts {
            let mut vector = vec![0.0_f32; embedding_dim];
            let lowered = text.to_lowercase();
            for (position, token) in lowered.split_whitespace().enumerate() {
                for feature_idx in 0..features_per_token {
                    let mut hasher = DefaultHasher::new();
                    token.hash(&mut hasher);
                    position.hash(&mut hasher);
                    feature_idx.hash(&mut hasher);
                    let hash = hasher.finish();
                    let index = (hash as usize) % embedding_dim;
                    let sign = if ((hash >> 63) & 1) == 0 { 1.0 } else { -1.0 };
                    let position_decay = 1.0 / (1.0 + position as f32 * 0.01);
                    vector[index] += sign * position_decay;
                }
            }

            if normalize {
                let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
                if norm > 0.0 {
                    for value in &mut vector {
                        *value /= norm;
                    }
                }
            }

            output.push(vector);
        }

        Ok(output)
    }
}

#[async_trait]
impl NodeBehavior for EmbeddingGeneratorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.embedding_generator".to_string(),
            display_name: "Embedding Generator".to_string(),
            description: "Generate deterministic embedding vectors for text payloads.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Text input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Embedding output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "embedding_dim": { "type": "integer", "minimum": 1, "default": 384 },
                    "normalize": { "type": "boolean", "default": true },
                    "features_per_token": { "type": "integer", "minimum": 1, "maximum": 8, "default": 2 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(dim) = config["embedding_dim"].as_u64() {
            if dim == 0 {
                return Err(anyhow!("embedding_dim must be greater than zero"));
            }
            if dim > 8192 {
                return Err(anyhow!("embedding_dim must be <= 8192"));
            }
        } else if config.get("embedding_dim").is_some() {
            return Err(anyhow!("embedding_dim must be an integer"));
        }

        if config.get("normalize").is_some() && config["normalize"].as_bool().is_none() {
            return Err(anyhow!("normalize must be a boolean"));
        }

        if let Some(features) = config["features_per_token"].as_u64() {
            if features == 0 {
                return Err(anyhow!("features_per_token must be greater than zero"));
            }
            if features > 8 {
                return Err(anyhow!("features_per_token must be <= 8"));
            }
        } else if config.get("features_per_token").is_some() {
            return Err(anyhow!("features_per_token must be an integer"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Embedding Generator requires one input plan"));
        }
        Ok(vec![xform_plan(inputs, config, XformMode::Embed)?])
    }
}

pub struct SemanticChunkerNode;

impl SemanticChunkerNode {
    pub fn chunk_text(&self, text: &str, config: &JsonValue) -> Result<Vec<String>> {
        let chunk_size = config["chunk_size"].as_u64().unwrap_or(200) as usize;
        let overlap = config["overlap"].as_u64().unwrap_or(20) as usize;

        if chunk_size == 0 {
            return Err(anyhow!("chunk_size must be greater than zero"));
        }
        if overlap >= chunk_size {
            return Err(anyhow!("overlap must be smaller than chunk_size"));
        }

        let tokens: Vec<&str> = text.split_whitespace().collect();
        if tokens.is_empty() {
            return Ok(vec![]);
        }

        let mut chunks = Vec::new();
        let step = chunk_size - overlap;
        let mut start = 0usize;
        while start < tokens.len() {
            let end = std::cmp::min(start + chunk_size, tokens.len());
            chunks.push(tokens[start..end].join(" "));
            if end == tokens.len() {
                break;
            }
            start += step;
        }

        Ok(chunks)
    }
}

#[async_trait]
impl NodeBehavior for SemanticChunkerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.semantic_chunker".to_string(),
            display_name: "Semantic Chunker".to_string(),
            description: "Split text content into overlapping chunks.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Chunked output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "chunk_size": { "type": "integer", "minimum": 1, "default": 200 },
                    "overlap": { "type": "integer", "minimum": 0, "default": 20 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let chunk_size = config["chunk_size"].as_u64().unwrap_or(200);
        let overlap = config["overlap"].as_u64().unwrap_or(20);

        if chunk_size == 0 {
            return Err(anyhow!("chunk_size must be greater than zero"));
        }
        if overlap >= chunk_size {
            return Err(anyhow!("overlap must be smaller than chunk_size"));
        }
        if config.get("chunk_size").is_some() && config["chunk_size"].as_u64().is_none() {
            return Err(anyhow!("chunk_size must be an integer"));
        }
        if config.get("overlap").is_some() && config["overlap"].as_u64().is_none() {
            return Err(anyhow!("overlap must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Semantic Chunker requires one input plan"));
        }
        Ok(vec![xform_plan(inputs, config, XformMode::Chunk)?])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetrievalCandidate {
    pub source_id: String,
    pub content: String,
    pub score: f32,
}

pub struct HybridRetrieverNode;
pub struct RerankerNode;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RerankBackend {
    Rule,
    Model,
    Llm,
}

impl HybridRetrieverNode {
    pub fn fuse_candidates(
        &self,
        sparse_candidates: Vec<RetrievalCandidate>,
        semantic_candidates: Vec<RetrievalCandidate>,
        config: &JsonValue,
    ) -> Result<Vec<RetrievalCandidate>> {
        self.validate(config)?;
        let alpha = config["alpha"].as_f64().unwrap_or(0.5) as f32;
        let top_k = config["top_k"].as_u64().unwrap_or(20) as usize;

        #[derive(Default)]
        struct FusionEntry {
            content: String,
            sparse_score: f32,
            semantic_score: f32,
        }

        let mut merged: HashMap<String, FusionEntry> = HashMap::new();
        for candidate in sparse_candidates {
            let entry = merged.entry(candidate.source_id.clone()).or_default();
            if entry.content.is_empty() && !candidate.content.is_empty() {
                entry.content = candidate.content.clone();
            }
            if candidate.score > entry.sparse_score {
                entry.sparse_score = candidate.score;
            }
        }
        for candidate in semantic_candidates {
            let entry = merged.entry(candidate.source_id.clone()).or_default();
            if entry.content.is_empty() && !candidate.content.is_empty() {
                entry.content = candidate.content.clone();
            }
            if candidate.score > entry.semantic_score {
                entry.semantic_score = candidate.score;
            }
        }

        let mut fused: Vec<RetrievalCandidate> = merged
            .into_iter()
            .map(|(source_id, entry)| RetrievalCandidate {
                source_id,
                content: entry.content,
                score: (1.0 - alpha) * entry.sparse_score + alpha * entry.semantic_score,
            })
            .collect();

        fused.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        fused.truncate(top_k);
        Ok(fused)
    }
}

impl RerankerNode {
    pub fn resolve_backend(&self, config: &JsonValue) -> Result<RerankBackend> {
        let backend = config["backend"].as_str().unwrap_or("rule");
        match backend {
            "rule" => Ok(RerankBackend::Rule),
            "model" => Ok(RerankBackend::Model),
            "llm" => Ok(RerankBackend::Llm),
            _ => Err(anyhow!("backend must be one of rule|model|llm")),
        }
    }

    fn rerank_with_overlap(
        &self,
        query: &str,
        candidates: Vec<RetrievalCandidate>,
        top_k: usize,
        term_boost: f32,
    ) -> Vec<RetrievalCandidate> {
        let query_terms: HashSet<String> = query
            .to_lowercase()
            .split_whitespace()
            .map(|token| {
                token
                    .trim_matches(|c: char| !c.is_alphanumeric())
                    .to_string()
            })
            .filter(|token| !token.is_empty())
            .collect();

        let mut reranked: Vec<RetrievalCandidate> = candidates
            .into_iter()
            .map(|mut candidate| {
                let overlap = candidate
                    .content
                    .to_lowercase()
                    .split_whitespace()
                    .map(|token| {
                        token
                            .trim_matches(|c: char| !c.is_alphanumeric())
                            .to_string()
                    })
                    .filter(|token| !token.is_empty() && query_terms.contains(token))
                    .count();

                candidate.score += overlap as f32 * term_boost;
                candidate
            })
            .collect();

        reranked.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        reranked.truncate(top_k);
        reranked
    }

    fn rerank_with_external_scores(
        &self,
        mut candidates: Vec<RetrievalCandidate>,
        external_scores: &HashMap<String, f32>,
        top_k: usize,
    ) -> Vec<RetrievalCandidate> {
        for candidate in &mut candidates {
            if let Some(score) = external_scores.get(&candidate.source_id) {
                candidate.score = *score;
            }
        }

        candidates.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_id.cmp(&right.source_id))
        });
        candidates.truncate(top_k);
        candidates
    }

    fn extract_external_scores(&self, body: &JsonValue) -> Result<HashMap<String, f32>> {
        let mut external_scores = HashMap::new();

        if let Some(results) = body["results"].as_array() {
            for (rank, item) in results.iter().enumerate() {
                if let Some(source_id) = item["source_id"].as_str() {
                    let score = item["score"]
                        .as_f64()
                        .map(|value| value as f32)
                        .unwrap_or((results.len().saturating_sub(rank)) as f32);
                    external_scores.insert(source_id.to_string(), score);
                }
            }
            return Ok(external_scores);
        }

        if let Some(ranked_ids) = body["ranked_ids"].as_array() {
            for (rank, value) in ranked_ids.iter().enumerate() {
                if let Some(source_id) = value.as_str() {
                    external_scores.insert(
                        source_id.to_string(),
                        (ranked_ids.len().saturating_sub(rank)) as f32,
                    );
                }
            }
            return Ok(external_scores);
        }

        if let Some(content) = body["choices"][0]["message"]["content"].as_str() {
            let json_str = ConfigWizard::extract_json_from_response(content).unwrap_or(content);
            if let Ok(nested_json) = serde_json::from_str::<JsonValue>(json_str) {
                return self.extract_external_scores(&nested_json);
            }
            return Err(anyhow!(
                "llm response content is not valid rerank JSON payload"
            ));
        }

        Err(anyhow!(
            "response missing 'results', 'ranked_ids', or chat completion content"
        ))
    }

    fn rerank_with_model_backend(
        &self,
        query: &str,
        candidates: Vec<RetrievalCandidate>,
        config: &JsonValue,
        top_k: usize,
        fallback_term_boost: f32,
    ) -> Result<Vec<RetrievalCandidate>> {
        let endpoint = config["model_endpoint"]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        if endpoint.is_none() {
            // Allow gradual rollout: if model endpoint is not configured, keep deterministic ranking.
            return Ok(self.rerank_with_overlap(query, candidates, top_k, fallback_term_boost));
        }
        let endpoint = endpoint.unwrap();

        let timeout_ms = config["timeout_ms"].as_u64().unwrap_or(3_000);
        let max_retries = config["max_retries"].as_u64().unwrap_or(2);
        let fail_open = config["fail_open"].as_bool().unwrap_or(true);
        let api_key = config["model_api_key"]
            .as_str()
            .map(str::to_string)
            .or_else(|| std::env::var("RERANK_MODEL_API_KEY").ok());

        let model_payload = serde_json::json!({
            "query": query,
            "top_k": top_k,
            "model_id": config["model_id"].as_str().unwrap_or("default"),
            "candidates": candidates.iter().map(|candidate| {
                serde_json::json!({
                    "source_id": candidate.source_id,
                    "content": candidate.content,
                    "score": candidate.score
                })
            }).collect::<Vec<_>>()
        });

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()?;

        let mut last_error = None;
        for attempt in 0..=max_retries {
            let mut request = client.post(endpoint).json(&model_payload);
            if let Some(key) = api_key.as_ref() {
                request = request.bearer_auth(key);
            }

            match request.send() {
                Ok(response) => {
                    let status = response.status();
                    let text = response.text()?;
                    if !status.is_success() {
                        last_error = Some(anyhow!(
                            "model backend returned non-success status {}: {}",
                            status,
                            text
                        ));
                    } else {
                        let body: JsonValue = serde_json::from_str(&text).map_err(|error| {
                            anyhow!(
                                "failed to parse model backend JSON: {}. response: {}",
                                error,
                                text
                            )
                        })?;
                        let external_scores = match self.extract_external_scores(&body) {
                            Ok(scores) => scores,
                            Err(error) => {
                                last_error =
                                    Some(anyhow!("model backend response parse failed: {}", error));
                                continue;
                            }
                        };

                        if external_scores.is_empty() {
                            last_error = Some(anyhow!(
                                "model backend response does not contain usable candidates"
                            ));
                            continue;
                        }

                        return Ok(self.rerank_with_external_scores(
                            candidates,
                            &external_scores,
                            top_k,
                        ));
                    }
                }
                Err(error) => {
                    last_error = Some(anyhow!("model backend request failed: {}", error));
                }
            }

            if attempt < max_retries {
                // Unified retry policy (core/retry.rs)
                let sleep_ms = crate::core::retry::backoff_delay_ms(100, attempt);
                std::thread::sleep(Duration::from_millis(sleep_ms));
            }
        }

        if fail_open {
            return Ok(self.rerank_with_overlap(query, candidates, top_k, fallback_term_boost));
        }

        Err(last_error.unwrap_or_else(|| anyhow!("model backend rerank failed")))
    }

    fn rerank_with_llm_backend(
        &self,
        query: &str,
        candidates: Vec<RetrievalCandidate>,
        config: &JsonValue,
        top_k: usize,
        fallback_term_boost: f32,
    ) -> Result<Vec<RetrievalCandidate>> {
        let endpoint = config["llm_endpoint"]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        if endpoint.is_none() {
            return Ok(self.rerank_with_overlap(query, candidates, top_k, fallback_term_boost));
        }
        let endpoint = endpoint.unwrap();

        let timeout_ms = config["timeout_ms"].as_u64().unwrap_or(3_000);
        let max_retries = config["max_retries"].as_u64().unwrap_or(2);
        let fail_open = config["fail_open"].as_bool().unwrap_or(true);
        let llm_model = config["llm_model"].as_str().unwrap_or("gpt-4o-mini");
        let llm_api_key = config["llm_api_key"]
            .as_str()
            .map(str::to_string)
            .or_else(|| std::env::var("LLM_RERANK_API_KEY").ok());
        let llm_max_candidates = config["llm_max_candidates"].as_u64().unwrap_or(20) as usize;

        let limited: Vec<&RetrievalCandidate> =
            candidates.iter().take(llm_max_candidates.max(1)).collect();
        let candidate_lines = limited
            .iter()
            .map(|candidate| {
                format!(
                    "- source_id: {}\n  score: {}\n  content: {}",
                    candidate.source_id, candidate.score, candidate.content
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let system_prompt = "You are a reranking engine. Return ONLY JSON with either `ranked_ids` (array of source_id in best-first order) or `results` (array of {source_id,score}).";
        let user_prompt = format!(
            "Query:\n{}\n\nCandidates:\n{}\n\nReturn top {} ids.",
            query, candidate_lines, top_k
        );
        let payload = serde_json::json!({
            "model": llm_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0.0
        });

        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()?;

        let mut last_error = None;
        for attempt in 0..=max_retries {
            let mut request = client.post(endpoint).json(&payload);
            if let Some(key) = llm_api_key.as_ref() {
                request = request.bearer_auth(key);
            }

            match request.send() {
                Ok(response) => {
                    let status = response.status();
                    let text = response.text()?;
                    if !status.is_success() {
                        last_error = Some(anyhow!(
                            "llm backend returned non-success status {}: {}",
                            status,
                            text
                        ));
                    } else {
                        let body: JsonValue = serde_json::from_str(&text).map_err(|error| {
                            anyhow!(
                                "failed to parse llm backend JSON: {}. response: {}",
                                error,
                                text
                            )
                        })?;

                        let external_scores = match self.extract_external_scores(&body) {
                            Ok(scores) => scores,
                            Err(error) => {
                                last_error =
                                    Some(anyhow!("llm backend response parse failed: {}", error));
                                continue;
                            }
                        };

                        if external_scores.is_empty() {
                            last_error = Some(anyhow!(
                                "llm backend response does not contain usable candidates"
                            ));
                            continue;
                        }

                        return Ok(self.rerank_with_external_scores(
                            candidates,
                            &external_scores,
                            top_k,
                        ));
                    }
                }
                Err(error) => {
                    last_error = Some(anyhow!("llm backend request failed: {}", error));
                }
            }

            if attempt < max_retries {
                // Unified retry policy (core/retry.rs)
                let sleep_ms = crate::core::retry::backoff_delay_ms(100, attempt);
                std::thread::sleep(Duration::from_millis(sleep_ms));
            }
        }

        if fail_open {
            return Ok(self.rerank_with_overlap(query, candidates, top_k, fallback_term_boost));
        }

        Err(last_error.unwrap_or_else(|| anyhow!("llm backend rerank failed")))
    }

    pub fn rerank_candidates(
        &self,
        query: &str,
        candidates: Vec<RetrievalCandidate>,
        config: &JsonValue,
    ) -> Result<Vec<RetrievalCandidate>> {
        self.validate(config)?;
        let top_k = config["top_k"].as_u64().unwrap_or(10) as usize;
        let query_term_boost = config["query_term_boost"].as_f64().unwrap_or(0.1) as f32;
        let backend = self.resolve_backend(config)?;

        let reranked = match backend {
            RerankBackend::Rule => {
                self.rerank_with_overlap(query, candidates, top_k, query_term_boost)
            }
            RerankBackend::Model => {
                let model_term_boost = config["model_term_boost"]
                    .as_f64()
                    .map(|v| v as f32)
                    .unwrap_or(query_term_boost);
                self.rerank_with_model_backend(query, candidates, config, top_k, model_term_boost)?
            }
            // Placeholder: llm backend currently shares deterministic overlap ranking path.
            RerankBackend::Llm => {
                let llm_term_boost = config["llm_term_boost"]
                    .as_f64()
                    .map(|v| v as f32)
                    .unwrap_or(query_term_boost);
                self.rerank_with_llm_backend(query, candidates, config, top_k, llm_term_boost)?
            }
        };

        Ok(reranked)
    }
}

#[async_trait]
impl NodeBehavior for HybridRetrieverNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.hybrid_retriever".to_string(),
            display_name: "Hybrid Retriever".to_string(),
            description: "Fuse sparse and semantic retrieval results.".to_string(),
            input_ports: vec![
                PortSpec {
                    id: 0,
                    name: "sparse".to_string(),
                    description: "Sparse retrieval input".to_string(),
                },
                PortSpec {
                    id: 1,
                    name: "semantic".to_string(),
                    description: "Semantic retrieval input".to_string(),
                },
            ],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Fused retrieval output".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "alpha": { "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.5 },
                    "top_k": { "type": "integer", "minimum": 1, "default": 20 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(alpha) = config["alpha"].as_f64() {
            if !(0.0..=1.0).contains(&alpha) {
                return Err(anyhow!("alpha must be within [0.0, 1.0]"));
            }
        } else if config.get("alpha").is_some() {
            return Err(anyhow!("alpha must be a number"));
        }

        if let Some(top_k) = config["top_k"].as_u64() {
            if top_k == 0 {
                return Err(anyhow!("top_k must be greater than zero"));
            }
        } else if config.get("top_k").is_some() {
            return Err(anyhow!("top_k must be an integer"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Hybrid Retriever requires at least one input plan"));
        }
        Ok(vec![xform_plan(inputs, config, XformMode::Hybrid)?])
    }
}

#[async_trait]
impl NodeBehavior for RerankerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.reranker".to_string(),
            display_name: "Reranker".to_string(),
            description: "Re-rank retrieval candidates by query relevance.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Candidate input".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Re-ranked output".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "backend": { "type": "string", "enum": ["rule", "model", "llm"], "default": "rule" },
                    "top_k": { "type": "integer", "minimum": 1, "default": 10 },
                    "query_term_boost": { "type": "number", "minimum": 0.0, "default": 0.1 },
                    "model_term_boost": { "type": "number", "minimum": 0.0, "default": 0.1 },
                    "llm_term_boost": { "type": "number", "minimum": 0.0, "default": 0.1 },
                    "model_id": { "type": "string" },
                    "llm_model": { "type": "string" },
                    "model_endpoint": { "type": "string" },
                    "model_api_key": { "type": "string" },
                    "llm_endpoint": { "type": "string" },
                    "llm_api_key": { "type": "string" },
                    "llm_max_candidates": { "type": "integer", "minimum": 1, "default": 20 },
                    "timeout_ms": { "type": "integer", "minimum": 1, "default": 3000 },
                    "max_retries": { "type": "integer", "minimum": 0, "default": 2 },
                    "fail_open": { "type": "boolean", "default": true }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        let _ = self.resolve_backend(config)?;

        if let Some(top_k) = config["top_k"].as_u64() {
            if top_k == 0 {
                return Err(anyhow!("top_k must be greater than zero"));
            }
        } else if config.get("top_k").is_some() {
            return Err(anyhow!("top_k must be an integer"));
        }

        if let Some(boost) = config["query_term_boost"].as_f64() {
            if boost < 0.0 {
                return Err(anyhow!("query_term_boost must be >= 0.0"));
            }
        } else if config.get("query_term_boost").is_some() {
            return Err(anyhow!("query_term_boost must be a number"));
        }

        if let Some(boost) = config["model_term_boost"].as_f64() {
            if boost < 0.0 {
                return Err(anyhow!("model_term_boost must be >= 0.0"));
            }
        } else if config.get("model_term_boost").is_some() {
            return Err(anyhow!("model_term_boost must be a number"));
        }

        if let Some(boost) = config["llm_term_boost"].as_f64() {
            if boost < 0.0 {
                return Err(anyhow!("llm_term_boost must be >= 0.0"));
            }
        } else if config.get("llm_term_boost").is_some() {
            return Err(anyhow!("llm_term_boost must be a number"));
        }

        if let Some(model_id) = config["model_id"].as_str() {
            if model_id.trim().is_empty() {
                return Err(anyhow!("model_id must not be empty when provided"));
            }
        } else if config.get("model_id").is_some() {
            return Err(anyhow!("model_id must be a string"));
        }

        if let Some(model_endpoint) = config["model_endpoint"].as_str() {
            if model_endpoint.trim().is_empty() {
                return Err(anyhow!("model_endpoint must not be empty when provided"));
            }
        } else if config.get("model_endpoint").is_some() {
            return Err(anyhow!("model_endpoint must be a string"));
        }

        if let Some(model_api_key) = config["model_api_key"].as_str() {
            if model_api_key.trim().is_empty() {
                return Err(anyhow!("model_api_key must not be empty when provided"));
            }
        } else if config.get("model_api_key").is_some() {
            return Err(anyhow!("model_api_key must be a string"));
        }

        if let Some(timeout_ms) = config["timeout_ms"].as_u64() {
            if timeout_ms == 0 {
                return Err(anyhow!("timeout_ms must be greater than zero"));
            }
        } else if config.get("timeout_ms").is_some() {
            return Err(anyhow!("timeout_ms must be an integer"));
        }

        if config.get("max_retries").is_some() && config["max_retries"].as_u64().is_none() {
            return Err(anyhow!("max_retries must be an integer"));
        }

        if config.get("fail_open").is_some() && config["fail_open"].as_bool().is_none() {
            return Err(anyhow!("fail_open must be a boolean"));
        }

        if let Some(llm_model) = config["llm_model"].as_str() {
            if llm_model.trim().is_empty() {
                return Err(anyhow!("llm_model must not be empty when provided"));
            }
        } else if config.get("llm_model").is_some() {
            return Err(anyhow!("llm_model must be a string"));
        }

        if let Some(llm_endpoint) = config["llm_endpoint"].as_str() {
            if llm_endpoint.trim().is_empty() {
                return Err(anyhow!("llm_endpoint must not be empty when provided"));
            }
        } else if config.get("llm_endpoint").is_some() {
            return Err(anyhow!("llm_endpoint must be a string"));
        }

        if let Some(llm_api_key) = config["llm_api_key"].as_str() {
            if llm_api_key.trim().is_empty() {
                return Err(anyhow!("llm_api_key must not be empty when provided"));
            }
        } else if config.get("llm_api_key").is_some() {
            return Err(anyhow!("llm_api_key must be a string"));
        }

        if let Some(llm_max_candidates) = config["llm_max_candidates"].as_u64() {
            if llm_max_candidates == 0 {
                return Err(anyhow!("llm_max_candidates must be greater than zero"));
            }
        } else if config.get("llm_max_candidates").is_some() {
            return Err(anyhow!("llm_max_candidates must be an integer"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Reranker requires one input plan"));
        }
        Ok(vec![xform_plan(inputs, config, XformMode::Rerank)?])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VectorContractRow {
    pub index_id: String,
    pub source_id: String,
    pub chunk_id: String,
    pub content: String,
    pub embedding_dim: u32,
    pub score: f32,
}

pub struct VectorIndexWriterNode;
pub struct VectorRetrieverNode;

fn validate_vector_contract_inputs(
    index_id: &str,
    source_id: &str,
    chunk_id: &str,
    embedding_dim: u32,
) -> Result<()> {
    if index_id.trim().is_empty() {
        return Err(anyhow!("index_id must not be empty"));
    }
    if source_id.trim().is_empty() {
        return Err(anyhow!("source_id must not be empty"));
    }
    if chunk_id.trim().is_empty() {
        return Err(anyhow!("chunk_id must not be empty"));
    }
    if embedding_dim == 0 {
        return Err(anyhow!("embedding_dim must be greater than zero"));
    }
    Ok(())
}

impl VectorIndexWriterNode {
    pub fn build_contract_row(
        &self,
        index_id: &str,
        source_id: &str,
        chunk_id: &str,
        content: &str,
        embedding_dim: u32,
        score: Option<f32>,
    ) -> Result<VectorContractRow> {
        validate_vector_contract_inputs(index_id, source_id, chunk_id, embedding_dim)?;
        let normalized_score = score.unwrap_or(1.0);
        if !(0.0..=1.0).contains(&normalized_score) {
            return Err(anyhow!("score must be within [0.0, 1.0]"));
        }

        Ok(VectorContractRow {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            chunk_id: chunk_id.to_string(),
            content: content.to_string(),
            embedding_dim,
            score: normalized_score,
        })
    }
}

impl VectorRetrieverNode {
    pub fn build_contract_row(
        &self,
        index_id: &str,
        source_id: &str,
        chunk_id: &str,
        content: &str,
        embedding_dim: u32,
        score: Option<f32>,
    ) -> Result<VectorContractRow> {
        validate_vector_contract_inputs(index_id, source_id, chunk_id, embedding_dim)?;
        let normalized_score = score.unwrap_or(0.0);
        if !(0.0..=1.0).contains(&normalized_score) {
            return Err(anyhow!("score must be within [0.0, 1.0]"));
        }

        Ok(VectorContractRow {
            index_id: index_id.to_string(),
            source_id: source_id.to_string(),
            chunk_id: chunk_id.to_string(),
            content: content.to_string(),
            embedding_dim,
            score: normalized_score,
        })
    }
}

#[async_trait]
impl NodeBehavior for VectorIndexWriterNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.vector_index_writer".to_string(),
            display_name: "Vector Index Writer".to_string(),
            description: "Normalize vector index write output contract.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Vector contract output".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "embedding_dim": { "type": "integer", "minimum": 1 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(dim) = config["embedding_dim"].as_u64() {
            if dim == 0 {
                return Err(anyhow!("embedding_dim must be greater than zero"));
            }
            return Ok(());
        }
        if config.get("embedding_dim").is_some() {
            return Err(anyhow!("embedding_dim must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Vector Index Writer requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[async_trait]
impl NodeBehavior for VectorRetrieverNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.vector_retriever".to_string(),
            display_name: "Vector Retriever".to_string(),
            description: "Normalize vector retrieval output contract.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Retrieved context output".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "embedding_dim": { "type": "integer", "minimum": 1 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(dim) = config["embedding_dim"].as_u64() {
            if dim == 0 {
                return Err(anyhow!("embedding_dim must be greater than zero"));
            }
            return Ok(());
        }
        if config.get("embedding_dim").is_some() {
            return Err(anyhow!("embedding_dim must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Vector Retriever requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

// ══════════════════════════════════════════════════════════════════
// TextXform: real execution for the retrieval shell nodes
// (chunk / embed / rerank / hybrid). Deterministic, no LLM required.
// ══════════════════════════════════════════════════════════════════

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum XformMode {
    Chunk,
    Embed,
    Rerank,
    Hybrid,
}

impl XformMode {
    fn tag(&self) -> &'static str {
        match self {
            XformMode::Chunk => "chunk",
            XformMode::Embed => "embed",
            XformMode::Rerank => "rerank",
            XformMode::Hybrid => "hybrid",
        }
    }
    fn out_fields(&self) -> Vec<Field> {
        match self {
            XformMode::Chunk => vec![
                Field::new("row_id", DataType::UInt32, true),
                Field::new("chunk_id", DataType::UInt32, true),
                Field::new("chunk_text", DataType::Utf8, true),
            ],
            XformMode::Embed => vec![
                Field::new("row_id", DataType::UInt32, true),
                Field::new("embed_norm", DataType::Utf8, true),
                Field::new("embed_json", DataType::Utf8, true),
            ],
            XformMode::Rerank | XformMode::Hybrid => vec![
                Field::new("rank", DataType::UInt32, true),
                Field::new("source_id", DataType::Utf8, true),
                Field::new("content", DataType::Utf8, true),
                // Numeric since 2026-09-28 (was Utf8 "{:.4}" — sigil finding #3);
                // downcasts must use Float64Array now.
                Field::new("score", DataType::Float64, true),
            ],
        }
    }
}

fn xform_plan(
    inputs: Vec<LogicalPlan>,
    config: &JsonValue,
    mode: XformMode,
) -> Result<LogicalPlan> {
    let column = config
        .get("column")
        .and_then(|v| v.as_str())
        .unwrap_or("text")
        .to_string();
    let cfg = config.clone();
    Ok(LogicalPlan::Extension(datafusion::logical_expr::Extension {
        node: Arc::new(TextXformLogical {
            input: inputs[0].clone(),
            column,
            mode,
            config: cfg,
        }),
    }))
}

#[derive(Clone)]
pub struct TextXformLogical {
    pub input: LogicalPlan,
    pub column: String,
    pub mode: XformMode,
    pub config: JsonValue,
}

impl fmt::Debug for TextXformLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextXformLogical")
            .field("column", &self.column)
            .field("mode", &self.mode.tag())
            .finish()
    }
}

impl PartialEq for TextXformLogical {
    fn eq(&self, other: &Self) -> bool {
        self.column == other.column && self.mode == other.mode
    }
}
impl Eq for TextXformLogical {}
impl Hash for TextXformLogical {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        self.mode.hash(state);
    }
}
impl PartialOrd for TextXformLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for TextXformLogical {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.column.clone(), self.mode.tag()).cmp(&(other.column.clone(), other.mode.tag()))
    }
}

impl UserDefinedLogicalNodeCore for TextXformLogical {
    fn name(&self) -> &str {
        "TextXformLogical"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref CHUNK_S: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(XformMode::Chunk.out_fields())).unwrap()
            );
            static ref EMBED_S: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(XformMode::Embed.out_fields())).unwrap()
            );
            static ref RERANK_S: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(XformMode::Rerank.out_fields())).unwrap()
            );
            static ref HYBRID_S: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(XformMode::Hybrid.out_fields())).unwrap()
            );
        }
        match self.mode {
            XformMode::Chunk => &CHUNK_S,
            XformMode::Embed => &EMBED_S,
            XformMode::Rerank => &RERANK_S,
            XformMode::Hybrid => &HYBRID_S,
        }
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TextXformLogical(column={}, mode={})", self.column, self.mode.tag())
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            column: self.column.clone(),
            mode: self.mode,
            config: self.config.clone(),
        })
    }
}

pub struct TextXformExec {
    input: Arc<dyn ExecutionPlan>,
    column: String,
    mode: XformMode,
    config: JsonValue,
    schema: SchemaRef,
    cache: PlanProperties,
}

impl TextXformExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, column: String, mode: XformMode, config: JsonValue) -> Self {
        let input: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(input));
        let schema = Arc::new(Schema::new(mode.out_fields()));
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self { input, column, mode, config, schema, cache }
    }
}

impl fmt::Debug for TextXformExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TextXformExec").field("mode", &self.mode.tag()).finish()
    }
}

impl DisplayAs for TextXformExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TextXformExec(mode={})", self.mode.tag())
    }
}

impl ExecutionPlan for TextXformExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> std::result::Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.column.clone(),
            self.mode,
            self.config.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(TextXformStream {
            input,
            column: self.column.clone(),
            mode: self.mode,
            config: self.config.clone(),
            schema: self.schema.clone(),
            state: XformStreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "TextXformExec"
    }
}

enum XformStreamState {
    PollingInput,
    Done,
}

struct TextXformStream {
    input: SendableRecordBatchStream,
    column: String,
    mode: XformMode,
    config: JsonValue,
    schema: SchemaRef,
    state: XformStreamState,
}

fn col_to_strings(batch: &RecordBatch, column: &str) -> Vec<String> {
    let idx = batch.schema().index_of(column).unwrap_or(0);
    let col = batch.column(idx);
    use datafusion::arrow::util::display::array_value_to_string;
    (0..batch.num_rows())
        .map(|i| array_value_to_string(col, i).unwrap_or_default())
        .collect()
}

impl Stream for TextXformStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            XformStreamState::PollingInput => match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let texts = col_to_strings(&batch, &self.column);
                    let mode = self.mode;
                    let config = self.config.clone();
                    let schema = self.schema.clone();
                    let out = build_xform_batch(texts, mode, &config, schema)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                    Poll::Ready(Some(Ok(out)))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.state = XformStreamState::Done;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
            XformStreamState::Done => Poll::Ready(None),
        }
    }
}

impl RecordBatchStream for TextXformStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn build_xform_batch(
    texts: Vec<String>,
    mode: XformMode,
    config: &JsonValue,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    match mode {
        XformMode::Chunk => {
            let chunker = SemanticChunkerNode;
            let mut rid = UInt32Builder::new();
            let mut cid = UInt32Builder::new();
            let mut txt = StringBuilder::new();
            for (i, text) in texts.iter().enumerate() {
                let chunks = chunker.chunk_text(text, config)?;
                for (j, c) in chunks.iter().enumerate() {
                    rid.append_value(i as u32);
                    cid.append_value(j as u32);
                    txt.append_value(c);
                }
            }
            Ok(RecordBatch::try_new(
                schema,
                vec![Arc::new(rid.finish()), Arc::new(cid.finish()), Arc::new(txt.finish())],
            )
            .map_err(|e| anyhow!("chunk batch: {}", e))?)
        }
        XformMode::Embed => {
            // Deterministic hashing embedding (no external service): FNV-1a
            // token hashing into a fixed-width vector, L2-normalized.
            // Spec advertises 'embedding_dim' (default 384); the legacy exec key
            // was 'dim' (default 64). Accept both, spec key wins.
            let dim = config["embedding_dim"]
                .as_u64()
                .or_else(|| config["dim"].as_u64())
                .unwrap_or(384) as usize;
            let mut rid = UInt32Builder::new();
            let mut norm = StringBuilder::new();
            let mut js = StringBuilder::new();
            for (i, text) in texts.iter().enumerate() {
                let mut v = vec![0f32; dim];
                for tok in text.to_lowercase().split_whitespace() {
                    let mut h = 2166136261u32;
                    for b in tok.as_bytes() {
                        h ^= *b as u32;
                        h = h.wrapping_mul(16777619);
                    }
                    v[(h as usize) % dim] += 1.0;
                }
                let l2 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                if l2 > 0.0 {
                    for x in v.iter_mut() {
                        *x /= l2;
                    }
                }
                rid.append_value(i as u32);
                norm.append_value(format!("{:.4}", l2));
                js.append_value(serde_json::to_string(&v).unwrap_or_default());
            }
            Ok(RecordBatch::try_new(
                schema,
                vec![Arc::new(rid.finish()), Arc::new(norm.finish()), Arc::new(js.finish())],
            )
            .map_err(|e| anyhow!("embed batch: {}", e))?)
        }
        XformMode::Rerank | XformMode::Hybrid => {
            // Input expectation: rows of (query, content) in the first two
            // string columns; single query row set reranked/hybrid-fused.
            // Build candidates from all rows: source_id = row index string.
            let query = texts.first().cloned().unwrap_or_default();
            let candidates: Vec<RetrievalCandidate> = texts
                .iter()
                .enumerate()
                .map(|(i, c)| RetrievalCandidate {
                    source_id: format!("s{}", i),
                    content: c.clone(),
                    score: 0.0,
                })
                .collect();
            let ranked = if mode == XformMode::Rerank {
                let top_k = config["top_k"].as_u64().unwrap_or(10) as usize;
                let term_boost = config["query_term_boost"]
                    .as_f64()
                    .or_else(|| config["term_boost"].as_f64())
                    .unwrap_or(0.1) as f32;
                RerankerNode.rerank_with_overlap_pub(&query, candidates, top_k, term_boost)
            } else {
                // Hybrid: treat candidates as the sparse leg; semantic leg
                // derives from content-hash similarity to the query.
                let semantic: Vec<RetrievalCandidate> = texts
                    .iter()
                    .enumerate()
                    .map(|(i, c)| RetrievalCandidate {
                        source_id: format!("s{}", i),
                        content: c.clone(),
                        score: hash_similarity(&query, c),
                    })
                    .collect();
                HybridRetrieverNode.fuse_candidates(candidates, semantic, config)?
            };
            let mut rank = UInt32Builder::new();
            let mut sid = StringBuilder::new();
            let mut content = StringBuilder::new();
            let mut score = datafusion::arrow::array::Float64Builder::new();
            for (i, c) in ranked.iter().enumerate() {
                rank.append_value(i as u32);
                sid.append_value(&c.source_id);
                content.append_value(&c.content);
                score.append_value(c.score as f64);
            }
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(rank.finish()),
                    Arc::new(sid.finish()),
                    Arc::new(content.finish()),
                    Arc::new(score.finish()),
                ],
            )
            .map_err(|e| anyhow!("rank batch: {}", e))?)
        }
    }
}

fn hash_similarity(a: &str, b: &str) -> f32 {
    let ta: HashSet<String> = a.to_lowercase().split_whitespace().map(str::to_string).collect();
    let tb: HashSet<String> = b.to_lowercase().split_whitespace().map(str::to_string).collect();
    if ta.is_empty() || tb.is_empty() {
        return 0.0;
    }
    let inter = ta.intersection(&tb).count() as f32;
    2.0 * inter / (ta.len() + tb.len()) as f32
}

impl RerankerNode {
    pub fn rerank_with_overlap_pub(
        &self,
        query: &str,
        candidates: Vec<RetrievalCandidate>,
        top_k: usize,
        term_boost: f32,
    ) -> Vec<RetrievalCandidate> {
        self.rerank_with_overlap(query, candidates, top_k, term_boost)
    }
}

#[cfg(test)]
mod xform_tests {
    use datafusion::arrow::array::StringArray;
    use super::*;

    #[test]
    fn test_xform_mode_schemas() {
        for mode in [XformMode::Chunk, XformMode::Embed, XformMode::Rerank, XformMode::Hybrid] {
            let logical = TextXformLogical {
                input: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
                }),
                column: "text".into(),
                mode,
                config: serde_json::json!({}),
            };
            assert_eq!(logical.schema().fields().len(), mode.out_fields().len());
        }
    }

    #[test]
    fn test_chunk_deterministic_window() {
        let texts = vec!["a b c d e f".to_string()];
        let batch = build_xform_batch(
            texts,
            XformMode::Chunk,
            &serde_json::json!({"chunk_size": 4, "overlap": 1}),
            Arc::new(Schema::new(XformMode::Chunk.out_fields())),
        )
        .unwrap();
        let col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let chunks: Vec<&str> = col.iter().map(|s| s.unwrap_or("")).collect();
        assert_eq!(chunks, vec!["a b c d", "d e f"]);
    }

    #[test]
    fn test_embed_l2_normalized() {
        let texts = vec!["x y z".to_string(), "x x y".to_string()];
        let batch = build_xform_batch(
            texts,
            XformMode::Embed,
            &serde_json::json!({"dim": 8}),
            Arc::new(Schema::new(XformMode::Embed.out_fields())),
        )
        .unwrap();
        let col = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        for v in col.iter() {
            let vec: Vec<f32> = serde_json::from_str(v.unwrap()).unwrap();
            let n: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((n - 1.0).abs() < 0.01, "not normalized: {}", n);
        }
    }

    #[test]
    fn test_hash_similarity_bounds() {
        assert_eq!(hash_similarity("a b", "a b"), 1.0);
        assert_eq!(hash_similarity("a b", "c d"), 0.0);
        assert!(hash_similarity("a b c", "a b d") > 0.0);
    }
}
