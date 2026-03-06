use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

const CATEGORY_EMAIL: &str = "email";
const CATEGORY_PHONE: &str = "phone";
const CATEGORY_SSN: &str = "ssn";
const CATEGORY_CREDIT_CARD: &str = "credit_card";

lazy_static::lazy_static! {
    static ref EMAIL_RE: Regex = Regex::new(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b")
        .expect("valid email regex");
    static ref PHONE_RE: Regex = Regex::new(
        r"(?x)
        \b
        (?:\+?1[-.\s]?)?
        (?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]?\d{4}
        \b"
    )
    .expect("valid phone regex");
    static ref SSN_RE: Regex = Regex::new(r"\b\d{3}-\d{2}-\d{4}\b")
        .expect("valid ssn regex");
    static ref CREDIT_CARD_RE: Regex = Regex::new(r"\b(?:\d{4}[- ]?){3}\d{4}\b")
        .expect("valid credit card regex");
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PiiMatch {
    pub pii_type: String,
    pub value: String,
    pub start: usize,
    pub end: usize,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PiiDetectionResult {
    pub input_length: usize,
    pub matches: Vec<PiiMatch>,
}

pub struct PiiDetectorNode;

impl PiiDetectorNode {
    fn supported_categories() -> HashSet<&'static str> {
        HashSet::from([
            CATEGORY_EMAIL,
            CATEGORY_PHONE,
            CATEGORY_SSN,
            CATEGORY_CREDIT_CARD,
        ])
    }

    fn resolve_categories(config: &JsonValue) -> Result<HashSet<String>> {
        let supported = Self::supported_categories();
        if let Some(values) = config.get("categories") {
            let array = values
                .as_array()
                .ok_or_else(|| anyhow!("categories must be an array of strings"))?;

            let mut categories = HashSet::new();
            for value in array {
                let category = value
                    .as_str()
                    .ok_or_else(|| anyhow!("categories must contain only strings"))?;
                if !supported.contains(category) {
                    return Err(anyhow!(
                        "categories contains unsupported value '{}'",
                        category
                    ));
                }
                categories.insert(category.to_string());
            }
            Ok(categories)
        } else {
            Ok(supported.into_iter().map(str::to_string).collect())
        }
    }

    fn append_matches(
        text: &str,
        category: &str,
        regex: &Regex,
        confidence: f32,
        min_confidence: f32,
        output: &mut Vec<PiiMatch>,
    ) {
        if confidence < min_confidence {
            return;
        }
        for found in regex.find_iter(text) {
            output.push(PiiMatch {
                pii_type: category.to_string(),
                value: found.as_str().to_string(),
                start: found.start(),
                end: found.end(),
                confidence,
            });
        }
    }

    pub fn detect_pii(&self, text: &str, config: &JsonValue) -> Result<PiiDetectionResult> {
        self.validate(config)?;

        let min_confidence = config["min_confidence"].as_f64().unwrap_or(0.5) as f32;
        let categories = Self::resolve_categories(config)?;

        let mut matches = Vec::new();
        if categories.contains(CATEGORY_EMAIL) {
            Self::append_matches(
                text,
                CATEGORY_EMAIL,
                &EMAIL_RE,
                0.95,
                min_confidence,
                &mut matches,
            );
        }
        if categories.contains(CATEGORY_PHONE) {
            Self::append_matches(
                text,
                CATEGORY_PHONE,
                &PHONE_RE,
                0.90,
                min_confidence,
                &mut matches,
            );
        }
        if categories.contains(CATEGORY_SSN) {
            Self::append_matches(
                text,
                CATEGORY_SSN,
                &SSN_RE,
                0.98,
                min_confidence,
                &mut matches,
            );
        }
        if categories.contains(CATEGORY_CREDIT_CARD) {
            Self::append_matches(
                text,
                CATEGORY_CREDIT_CARD,
                &CREDIT_CARD_RE,
                0.96,
                min_confidence,
                &mut matches,
            );
        }

        matches.sort_by(|a, b| a.start.cmp(&b.start).then(a.end.cmp(&b.end)));
        let mut seen = HashSet::new();
        matches.retain(|item| {
            seen.insert((
                item.pii_type.clone(),
                item.start,
                item.end,
                item.value.clone(),
            ))
        });

        Ok(PiiDetectionResult {
            input_length: text.len(),
            matches,
        })
    }
}

#[async_trait]
impl NodeBehavior for PiiDetectorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.pii_detector".to_string(),
            display_name: "PII Detector".to_string(),
            description: "Detect common sensitive spans from text with deterministic matching."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan that includes textual content".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with PII detection results".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "min_confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.5 },
                    "categories": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["email", "phone", "ssn", "credit_card"]
                        }
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(confidence) = config.get("min_confidence") {
            let value = confidence
                .as_f64()
                .ok_or_else(|| anyhow!("min_confidence must be a number"))?;
            if !(0.0..=1.0).contains(&value) {
                return Err(anyhow!("min_confidence must be within [0.0, 1.0]"));
            }
        }

        if let Some(values) = config.get("categories") {
            let array = values
                .as_array()
                .ok_or_else(|| anyhow!("categories must be an array of strings"))?;
            let supported = Self::supported_categories();
            for value in array {
                let category = value
                    .as_str()
                    .ok_or_else(|| anyhow!("categories must contain only strings"))?;
                if !supported.contains(category) {
                    return Err(anyhow!(
                        "categories contains unsupported value '{}'",
                        category
                    ));
                }
            }
        }

        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("PII Detector requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PiiMaskReplacement {
    pub token: String,
    pub original: String,
    pub pii_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PiiMaskResult {
    pub masked_text: String,
    pub replacements: Vec<PiiMaskReplacement>,
}

pub struct PiiMaskerNode;

impl PiiMaskerNode {
    fn token_prefix(config: &JsonValue) -> String {
        config["token_prefix"].as_str().unwrap_or("PII").to_string()
    }

    pub fn mask_pii(&self, text: &str, config: &JsonValue) -> Result<PiiMaskResult> {
        self.validate(config)?;

        let detector = PiiDetectorNode;
        let detection = detector.detect_pii(text, config)?;
        if detection.matches.is_empty() {
            return Ok(PiiMaskResult {
                masked_text: text.to_string(),
                replacements: Vec::new(),
            });
        }

        let prefix = Self::token_prefix(config);
        let mut masked_text = text.to_string();
        let mut replacements = Vec::with_capacity(detection.matches.len());

        for (index, pii_match) in detection.matches.iter().enumerate() {
            let token = format!("<{}_{:03}>", prefix, index + 1);
            replacements.push(PiiMaskReplacement {
                token,
                original: pii_match.value.clone(),
                pii_type: pii_match.pii_type.clone(),
            });
        }

        for (pii_match, replacement) in detection.matches.iter().zip(replacements.iter()).rev() {
            masked_text.replace_range(pii_match.start..pii_match.end, &replacement.token);
        }

        Ok(PiiMaskResult {
            masked_text,
            replacements,
        })
    }

    pub fn demask_pii(&self, masked_text: &str, replacements: &[PiiMaskReplacement]) -> String {
        let mut restored = masked_text.to_string();
        for replacement in replacements {
            restored = restored.replace(&replacement.token, &replacement.original);
        }
        restored
    }
}

#[async_trait]
impl NodeBehavior for PiiMaskerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.pii_masker".to_string(),
            display_name: "PII Masker / De-masker".to_string(),
            description: "Mask and restore detected PII spans with deterministic placeholders."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan with text content".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with masked content and mappings".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "token_prefix": { "type": "string", "default": "PII" },
                    "min_confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.5 },
                    "categories": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["email", "phone", "ssn", "credit_card"]
                        }
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(prefix) = config.get("token_prefix") {
            let value = prefix
                .as_str()
                .ok_or_else(|| anyhow!("token_prefix must be a string"))?;
            if value.trim().is_empty() {
                return Err(anyhow!("token_prefix must not be empty"));
            }
        }

        let detector = PiiDetectorNode;
        detector.validate(config)
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("PII Masker requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PolicyDecision {
    pub allow: bool,
    pub reason: String,
    pub requires_approval: bool,
}

pub struct PolicyPrivacyGateNode;

impl PolicyPrivacyGateNode {
    fn parse_action_set(config: &JsonValue, field: &str) -> Result<HashSet<String>> {
        if let Some(values) = config.get(field) {
            let array = values
                .as_array()
                .ok_or_else(|| anyhow!("{} must be an array of strings", field))?;
            let mut actions = HashSet::new();
            for value in array {
                let action = value
                    .as_str()
                    .ok_or_else(|| anyhow!("{} must contain only strings", field))?;
                actions.insert(action.to_string());
            }
            Ok(actions)
        } else {
            Ok(HashSet::new())
        }
    }

    pub fn evaluate_policy(
        &self,
        action: &str,
        has_pii: bool,
        config: &JsonValue,
    ) -> Result<PolicyDecision> {
        self.validate(config)?;

        let block_when_pii = config["block_when_pii"].as_bool().unwrap_or(true);
        let blocked_actions = Self::parse_action_set(config, "blocked_actions")?;
        let approval_actions = Self::parse_action_set(config, "approval_actions")?;

        if block_when_pii && has_pii {
            return Ok(PolicyDecision {
                allow: false,
                reason: "pii_detected".to_string(),
                requires_approval: false,
            });
        }

        if blocked_actions.contains(action) {
            return Ok(PolicyDecision {
                allow: false,
                reason: "action_blocked".to_string(),
                requires_approval: false,
            });
        }

        Ok(PolicyDecision {
            allow: true,
            reason: "allowed".to_string(),
            requires_approval: approval_actions.contains(action),
        })
    }
}

#[async_trait]
impl NodeBehavior for PolicyPrivacyGateNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.policy_privacy_gate".to_string(),
            display_name: "Policy & Privacy Gate".to_string(),
            description: "Apply deterministic privacy and policy decisions for action execution."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for guarded execution".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with policy decision metadata".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "block_when_pii": { "type": "boolean", "default": true },
                    "blocked_actions": {
                        "type": "array",
                        "items": { "type": "string" }
                    },
                    "approval_actions": {
                        "type": "array",
                        "items": { "type": "string" }
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("block_when_pii") {
            if value.as_bool().is_none() {
                return Err(anyhow!("block_when_pii must be a boolean"));
            }
        }

        let _ = Self::parse_action_set(config, "blocked_actions")?;
        let _ = Self::parse_action_set(config, "approval_actions")?;
        Ok(())
    }

    async fn propose_plan(
        &self,
        inputs: Vec<LogicalPlan>,
        _config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Policy & Privacy Gate requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}
