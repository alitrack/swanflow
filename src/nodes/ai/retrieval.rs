use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
