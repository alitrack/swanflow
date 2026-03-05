use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ClaimVerificationResult {
    pub claim: String,
    pub score: f32,
    pub verdict: String,
    pub evidence_excerpt: Option<String>,
}

pub struct FactClaimVerifierNode;

impl FactClaimVerifierNode {
    fn tokenize(text: &str) -> HashSet<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|token| token.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
            .filter(|token| !token.is_empty())
            .collect()
    }

    pub fn verify_claims(
        &self,
        claims: &[String],
        evidences: &[String],
        config: &JsonValue,
    ) -> Result<Vec<ClaimVerificationResult>> {
        self.validate(config)?;

        let threshold = config["support_threshold"].as_f64().unwrap_or(0.6) as f32;
        let require_evidence = config["require_evidence"].as_bool().unwrap_or(true);

        let evidence_tokens: Vec<(String, HashSet<String>)> = evidences
            .iter()
            .cloned()
            .map(|evidence| {
                let tokens = Self::tokenize(&evidence);
                (evidence, tokens)
            })
            .collect();

        let mut results = Vec::with_capacity(claims.len());
        for claim in claims {
            if require_evidence && evidence_tokens.is_empty() {
                results.push(ClaimVerificationResult {
                    claim: claim.clone(),
                    score: 0.0,
                    verdict: "insufficient_evidence".to_string(),
                    evidence_excerpt: None,
                });
                continue;
            }

            let claim_tokens = Self::tokenize(claim);
            if claim_tokens.is_empty() {
                results.push(ClaimVerificationResult {
                    claim: claim.clone(),
                    score: 0.0,
                    verdict: "not_supported".to_string(),
                    evidence_excerpt: None,
                });
                continue;
            }

            let mut best_score = 0.0_f32;
            let mut best_evidence = None;

            for (evidence, tokens) in &evidence_tokens {
                let overlap = claim_tokens.intersection(tokens).count() as f32;
                let score = overlap / claim_tokens.len() as f32;
                if score > best_score {
                    best_score = score;
                    best_evidence = Some(evidence.clone());
                }
            }

            let verdict = if require_evidence && best_evidence.is_none() {
                "insufficient_evidence"
            } else if best_score >= threshold {
                "supported"
            } else {
                "not_supported"
            }
            .to_string();

            results.push(ClaimVerificationResult {
                claim: claim.clone(),
                score: best_score,
                verdict,
                evidence_excerpt: best_evidence,
            });
        }

        Ok(results)
    }
}

#[async_trait]
impl NodeBehavior for FactClaimVerifierNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.fact_claim_verifier".to_string(),
            display_name: "Fact & Claim Verifier".to_string(),
            description: "Verify claim support scores against provided evidence snippets."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Claims and evidence input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Claim verification output plan".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "support_threshold": { "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.6 },
                    "require_evidence": { "type": "boolean", "default": true }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(threshold) = config["support_threshold"].as_f64() {
            if !(0.0..=1.0).contains(&threshold) {
                return Err(anyhow!("support_threshold must be within [0.0, 1.0]"));
            }
        } else if config.get("support_threshold").is_some() {
            return Err(anyhow!("support_threshold must be a number"));
        }

        if config.get("require_evidence").is_some() && config["require_evidence"].as_bool().is_none() {
            return Err(anyhow!("require_evidence must be a boolean"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Fact & Claim Verifier requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}
