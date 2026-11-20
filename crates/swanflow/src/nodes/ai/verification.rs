use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{Array, RecordBatch, StringBuilder, StringArray};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{Extension, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_plan::RecordBatchStream;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::Stream;

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
            .map(|token| {
                token
                    .trim_matches(|c: char| !c.is_alphanumeric())
                    .to_string()
            })
            .filter(|token| !token.is_empty())
            .collect()
    }

    /// Token-overlap support scoring for one claim against one evidence text.
    /// Exposed as a free function so the execution stream (FactVerifyExec) can
    /// reuse the exact same scoring without constructing the node struct.
    pub fn score_claim(claim: &str, evidence: &str) -> f32 {
        let claim_tokens = Self::tokenize(claim);
        if claim_tokens.is_empty() {
            return 0.0;
        }
        let evidence_tokens = Self::tokenize(evidence);
        let overlap = claim_tokens.intersection(&evidence_tokens).count() as f32;
        overlap / claim_tokens.len() as f32
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
            description: "Verify claim support scores against the evidence corpus of the input table (claim/evidence Utf8 columns; token-overlap scoring, offline)."
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

        if config.get("require_evidence").is_some()
            && config["require_evidence"].as_bool().is_none()
        {
            return Err(anyhow!("require_evidence must be a boolean"));
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        mut inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Fact & Claim Verifier requires one input plan"));
        }
        self.validate(config)?;
        let threshold = config["support_threshold"].as_f64().unwrap_or(0.6) as f32;
        let require_evidence = config["require_evidence"].as_bool().unwrap_or(true);
        // Output schema: claim / score / verdict / evidence_excerpt
        let out_schema = Arc::new(DFSchema::try_from(datafusion::arrow::datatypes::Schema::new(
            vec![
                Field::new("claim", DataType::Utf8, false),
                Field::new("score", DataType::Float32, false),
                Field::new("verdict", DataType::Utf8, false),
                Field::new("evidence_excerpt", DataType::Utf8, true),
            ],
        ))?);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(FactVerifyLogical {
                threshold_bits: threshold.to_bits(),
                require_evidence,
                schema: out_schema,
                input: Arc::new(inputs.remove(0)),
            }),
        })])
    }
}

// --- Extension wiring (real implementation, 2026-09-28) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FactVerifyLogical {
    /// f32 as total-order bits (f32 lacks Eq/Hash; threshold is finite config)
    pub threshold_bits: u32,
    pub require_evidence: bool,
    pub schema: Arc<DFSchema>,
    /// The (single) upstream plan — MUST be carried so DataFusion lowers the
    /// input and the planner receives physical_inputs[0].
    pub input: Arc<LogicalPlan>,
}

impl PartialOrd for FactVerifyLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.threshold_bits.partial_cmp(&other.threshold_bits)
    }
}

impl UserDefinedLogicalNodeCore for FactVerifyLogical {
    fn name(&self) -> &str {
        "FactVerify"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FactVerify: threshold={}, require_evidence={}",
            f32::from_bits(self.threshold_bits), self.require_evidence
        )
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        let input = inputs
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("FactVerify requires its input".into()))?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct FactVerifyExec {
    pub threshold: f32,
    pub require_evidence: bool,
    pub claim_column: String,
    pub evidence_column: String,
    pub schema: datafusion::arrow::datatypes::SchemaRef,
    pub cache: PlanProperties,
    pub input: Arc<dyn ExecutionPlan>,
}

impl FactVerifyExec {
    pub fn new(
        threshold: f32,
        require_evidence: bool,
        claim_column: String,
        evidence_column: String,
        schema: datafusion::arrow::datatypes::SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            threshold,
            require_evidence,
            claim_column,
            evidence_column,
            schema,
            cache,
            input,
        }
    }
}

impl DisplayAs for FactVerifyExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "FactVerifyExec: claim_col={} evidence_col={} threshold={}",
            self.claim_column, self.evidence_column, self.threshold
        )
    }
}

impl ExecutionPlan for FactVerifyExec {
    fn name(&self) -> &str {
        "FactVerifyExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(FactVerifyExec::new(
            self.threshold,
            self.require_evidence,
            self.claim_column.clone(),
            self.evidence_column.clone(),
            self.schema.clone(),
            children.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal("FactVerifyExec requires its input child".into())
            })?,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(FactVerifyStream {
            schema: self.schema.clone(),
            threshold: self.threshold,
            require_evidence: self.require_evidence,
            claim_column: self.claim_column.clone(),
            evidence_column: self.evidence_column.clone(),
            input,
            done: false,
        }))
    }
}

pub struct FactVerifyStream {
    schema: datafusion::arrow::datatypes::SchemaRef,
    threshold: f32,
    require_evidence: bool,
    claim_column: String,
    evidence_column: String,
    input: SendableRecordBatchStream,
    done: bool,
}

impl Stream for FactVerifyStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        // Drain the entire input: claims column + evidence column accumulate
        // claims and the evidence corpus (all rows) before scoring.
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let claims = batch
                    .column_by_name(&self.claim_column)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "FactVerify: input missing claim column '{}'",
                            self.claim_column
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "FactVerify: claim column '{}' must be Utf8",
                            self.claim_column
                        ))
                    })?;
                let evidences = batch
                    .column_by_name(&self.evidence_column)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "FactVerify: input missing evidence column '{}'",
                            self.evidence_column
                        ))
                    })?
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "FactVerify: evidence column '{}' must be Utf8",
                            self.evidence_column
                        ))
                    })?;
                let claims_vec: Vec<String> =
                    (0..claims.len()).map(|i| claims.value(i).to_string()).collect();
                let evidences_vec: Vec<String> = (0..evidences.len())
                    .map(|i| evidences.value(i).to_string())
                    .collect();
                let batch_out = score_claims_to_batch(
                    &claims_vec,
                    &evidences_vec,
                    self.threshold,
                    self.require_evidence,
                    self.schema.clone(),
                )?;
                Poll::Ready(Some(Ok(batch_out)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for FactVerifyStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Core scoring (shared with FactClaimVerifierNode::verify_claims semantics):
/// each claim is scored against the FULL evidence corpus (all rows); the best
/// scoring evidence becomes the excerpt; verdict follows threshold rules.
pub fn score_claims_to_batch(
    claims: &[String],
    evidences: &[String],
    threshold: f32,
    require_evidence: bool,
    schema: datafusion::arrow::datatypes::SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let mut claim_b = StringBuilder::new();
    let mut score_b = datafusion::arrow::array::Float32Builder::new();
    let mut verdict_b = StringBuilder::new();
    let mut excerpt_b = StringBuilder::new();
    for claim in claims {
        let mut best_score = 0.0_f32;
        let mut best_evidence: Option<&String> = None;
        for evidence in evidences {
            let score = FactClaimVerifierNode::score_claim(claim, evidence);
            if best_evidence.is_none() || score > best_score {
                best_score = score;
                best_evidence = Some(evidence);
            }
        }
        let verdict = if require_evidence && best_evidence.is_none() {
            "insufficient_evidence"
        } else if best_score >= threshold {
            "supported"
        } else {
            "not_supported"
        };
        claim_b.append_value(claim);
        score_b.append_value(best_score);
        verdict_b.append_value(verdict);
        match best_evidence {
            Some(e) => excerpt_b.append_value(e),
            None => excerpt_b.append_null(),
        }
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(claim_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(verdict_b.finish()),
            Arc::new(excerpt_b.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod fact_verify_tests {
    use super::*;

    #[test]
    fn score_claim_overlap() {
        // claim tokens: {the, eiffel, tower, is, in, paris} (6)
        // evidence contains all 6 -> 1.0
        let s = FactClaimVerifierNode::score_claim(
            "The Eiffel Tower is in Paris.",
            "The Eiffel Tower is a landmark in Paris, France.",
        );
        assert!((s - 1.0).abs() < 1e-6, "got {s}");
        // partial evidence: {the, tower, is, very, tall} overlaps {the, tower, is} -> 3/6
        let p = FactClaimVerifierNode::score_claim(
            "The Eiffel Tower is in Paris.",
            "The tower is very tall.",
        );
        assert!((p - 0.5).abs() < 1e-6, "got {p}");
    }

    #[test]
    fn score_claim_empty() {
        assert_eq!(FactClaimVerifierNode::score_claim("", "anything"), 0.0);
    }

    #[test]
    fn batch_verdicts_follow_threshold() {
        let claims = vec![
            "The Eiffel Tower is in Paris.".to_string(),
            "Quantum giraffes govern Belgium.".to_string(),
        ];
        let evidences = vec![
            "The Eiffel Tower is a landmark in Paris, France.".to_string(),
            "The tower is very tall.".to_string(),  // partial, 0.5 — not the best
        ];
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("claim", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("verdict", DataType::Utf8, false),
            Field::new("evidence_excerpt", DataType::Utf8, true),
        ]));
        let batch =
            score_claims_to_batch(&claims, &evidences, 0.6, true, schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let verdicts = batch
            .column_by_name("verdict")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(verdicts.value(0), "supported"); // 5/6 >= 0.6
        assert_eq!(verdicts.value(1), "not_supported"); // 0 < 0.6
        let scores = batch
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Float32Array>()
            .unwrap();
        assert!((scores.value(0) - 1.0).abs() < 1e-6);
        assert_eq!(scores.value(1), 0.0);
    }
}
