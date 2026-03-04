use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

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
            .replace('\n', " ")
            .replace('\r', " ")
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
            description: "Split a research goal into ranked sub-queries.".to_string(),
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
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Semantic Chunker requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
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
            .map(|token| token.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|token| !token.is_empty())
            .collect();

        let mut reranked: Vec<RetrievalCandidate> = candidates
            .into_iter()
            .map(|mut candidate| {
                let overlap = candidate
                    .content
                    .to_lowercase()
                    .split_whitespace()
                    .map(|token| token.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
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
            RerankBackend::Rule => self.rerank_with_overlap(query, candidates, top_k, query_term_boost),
            // Placeholder: model backend currently shares deterministic overlap ranking path.
            RerankBackend::Model => {
                let model_term_boost = config["model_term_boost"]
                    .as_f64()
                    .map(|v| v as f32)
                    .unwrap_or(query_term_boost);
                self.rerank_with_overlap(query, candidates, top_k, model_term_boost)
            }
            // Placeholder: llm backend currently shares deterministic overlap ranking path.
            RerankBackend::Llm => {
                let llm_term_boost = config["llm_term_boost"]
                    .as_f64()
                    .map(|v| v as f32)
                    .unwrap_or(query_term_boost);
                self.rerank_with_overlap(query, candidates, top_k, llm_term_boost)
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
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!(
                "Hybrid Retriever requires at least one input plan"
            ));
        }
        Ok(vec![inputs[0].clone()])
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
                    "llm_model": { "type": "string" }
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

        if let Some(llm_model) = config["llm_model"].as_str() {
            if llm_model.trim().is_empty() {
                return Err(anyhow!("llm_model must not be empty when provided"));
            }
        } else if config.get("llm_model").is_some() {
            return Err(anyhow!("llm_model must be a string"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Reranker requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
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
