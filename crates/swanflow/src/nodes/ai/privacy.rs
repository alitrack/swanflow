use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{StringArray, StringBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, DisplayAs, DisplayFormatType,
    ExecutionPlan, PlanProperties,
};
use datafusion::physical_expr::EquivalenceProperties;
use futures::stream::Stream;
use futures::StreamExt;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
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
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("PII Detector requires one input plan"));
        }
        let column = config
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();
        let node = Arc::new(PiiLogical {
            input: inputs[0].clone(),
            column,
            mode: PiiMode::Detect,
        });
        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node,
        })])
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum PiiMode {
    Detect,
    Mask,
    Gate(String, JsonValue),
}

#[derive(Clone)]
pub struct PiiLogical {
    pub input: LogicalPlan,
    pub column: String,
    pub mode: PiiMode,
}

impl fmt::Debug for PiiLogical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PiiLogical")
            .field("column", &self.column)
            .field("mode", &self.mode)
            .finish()
    }
}

impl PartialEq for PiiLogical {
    fn eq(&self, other: &Self) -> bool {
        if self.column != other.column { return false; }
        match (&self.mode, &other.mode) {
            (PiiMode::Detect, PiiMode::Detect) => true,
            (PiiMode::Mask, PiiMode::Mask) => true,
            (PiiMode::Gate(a, _), PiiMode::Gate(b, _)) => a == b,
            _ => false,
        }
    }
}
impl Eq for PiiLogical {}
impl std::hash::Hash for PiiLogical {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        let tag = match &self.mode {
            PiiMode::Detect => "detect".to_string(),
            PiiMode::Mask => "mask".to_string(),
            PiiMode::Gate(a, _) => format!("gate:{}", a),
        };
        tag.hash(state);
    }
}
impl PartialOrd for PiiLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for PiiLogical {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.column.cmp(&other.column).then_with(|| {
            let a = match &self.mode { PiiMode::Detect => 0, PiiMode::Mask => 1, PiiMode::Gate(..) => 2 };
            let b = match &other.mode { PiiMode::Detect => 0, PiiMode::Mask => 1, PiiMode::Gate(..) => 2 };
            a.cmp(&b)
        })
    }
}

impl datafusion::logical_expr::UserDefinedLogicalNodeCore for PiiLogical {
    fn name(&self) -> &str {
        "PiiLogical"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        lazy_static::lazy_static! {
            static ref DETECT_SCHEMA: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(vec![
                    Field::new("text", DataType::Utf8, true),
                    Field::new("match_count", DataType::UInt32, true),
                    Field::new("matches_json", DataType::Utf8, true),
                ]))
                .unwrap(),
            );
            static ref MASK_SCHEMA: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(vec![
                    Field::new("text", DataType::Utf8, true),
                    Field::new("masked_text", DataType::Utf8, true),
                    Field::new("replacements_json", DataType::Utf8, true),
                ]))
                .unwrap(),
            );
            static ref GATE_SCHEMA: DFSchemaRef = Arc::new(
                DFSchema::try_from(Schema::new(vec![
                    Field::new("text", DataType::Utf8, true),
                    Field::new("has_pii", DataType::UInt32, true),
                    Field::new("allow", DataType::UInt32, true),
                    Field::new("reason", DataType::Utf8, true),
                    Field::new("requires_approval", DataType::UInt32, true),
                ]))
                .unwrap(),
            );
        }
        match self.mode {
            PiiMode::Detect => &DETECT_SCHEMA,
            PiiMode::Mask => &MASK_SCHEMA,
            PiiMode::Gate(..) => &GATE_SCHEMA,
        }
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PiiLogical(column={}, mode={:?})", self.column, self.mode)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> std::result::Result<Self, DataFusionError> {
        Ok(Self {
            input: inputs[0].clone(),
            column: self.column.clone(),
            mode: self.mode.clone(),
        })
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
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("PII Masker requires one input plan"));
        }
        let column = config
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();
        let node = Arc::new(PiiLogical {
            input: inputs[0].clone(),
            column,
            mode: PiiMode::Mask,
        });
        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node,
        })])
    }
}


fn pii_mode_schema(mode: &PiiMode) -> SchemaRef {
    match mode {
        PiiMode::Detect => Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("match_count", DataType::UInt32, true),
            Field::new("matches_json", DataType::Utf8, true),
        ])),
        PiiMode::Mask => Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("masked_text", DataType::Utf8, true),
            Field::new("replacements_json", DataType::Utf8, true),
        ])),
        PiiMode::Gate(..) => Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("has_pii", DataType::UInt32, true),
            Field::new("allow", DataType::UInt32, true),
            Field::new("reason", DataType::Utf8, true),
            Field::new("requires_approval", DataType::UInt32, true),
        ])),
    }
}

pub struct PiiExec {
    input: Arc<dyn ExecutionPlan>,
    column: String,
    mode: PiiMode,
    cache: PlanProperties,
}

impl PiiExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, column: String, mode: PiiMode) -> Self {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(CoalescePartitionsExec::new(input));
        let schema = pii_mode_schema(&mode);
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            input,
            column,
            mode,
            cache,
        }
    }
    fn out_schema(&self) -> SchemaRef {
        pii_mode_schema(&self.mode)
    }
}

impl fmt::Debug for PiiExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PiiExec")
            .field("column", &self.column)
            .field("mode", &self.mode)
            .finish()
    }
}

impl DisplayAs for PiiExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PiiExec(column={}, mode={:?})", self.column, self.mode)
    }
}

impl ExecutionPlan for PiiExec {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.out_schema()
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
            self.mode.clone(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> std::result::Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(PiiStream {
            input,
            column: self.column.clone(),
            mode: self.mode.clone(),
            state: PiiStreamState::PollingInput,
        }))
    }
    fn properties(&self) -> &PlanProperties {
        &self.cache
    }
    fn name(&self) -> &str {
        "PiiExec"
    }
}

enum PiiStreamState {
    PollingInput,
    Done,
}

struct PiiStream {
    input: SendableRecordBatchStream,
    column: String,
    mode: PiiMode,
    state: PiiStreamState,
}

impl Stream for PiiStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match &mut self.state {
            PiiStreamState::PollingInput => match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    // resolve column index by name (fallback: first column)
                    let idx = batch
                        .schema()
                        .index_of(self.column.as_str())
                        .unwrap_or(0);
                    let col = batch.column(idx);
                    let texts = col.as_any().downcast_ref::<StringArray>();
                    let texts = match texts {
                        Some(a) => a,
                        None => {
                            // cast non-string columns to string via display
                            let n = batch.num_rows();
                            let owned: Vec<String> = (0..n)
                                .map(|i| arrow_scalar_to_string(col, i))
                                .collect();
                            &StringArray::from(owned)
                        }
                    };
                    let detector = PiiDetectorNode;
                    let masker = PiiMaskerNode;
                    let n = batch.num_rows();
                    let mut text_b = StringBuilder::new();
                    let mut count_b = UInt32Builder::new();
                    let mut mjson_b = StringBuilder::new();
                    let mut masked_b = StringBuilder::new();
                    let mut rjson_b = StringBuilder::new();
                    let mut haspii_b = UInt32Builder::new();
                    let mut allow_b = UInt32Builder::new();
                    let mut reason_b = StringBuilder::new();
                    let mut appr_b = UInt32Builder::new();
                    for i in 0..n {
                        let text = texts.value(i);
                        text_b.append_value(text);
                        match &self.mode {
                            PiiMode::Detect => {
                                let det = detector.detect_pii(text, &serde_json::json!({}))
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                count_b.append_value(det.matches.len() as u32);
                                mjson_b.append_value(serde_json::to_string(&det.matches).unwrap_or_default());
                            }
                            PiiMode::Mask => {
                                let mask_cfg = serde_json::json!({});
                                let m = masker.mask_pii(text, &mask_cfg)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                masked_b.append_value(m.masked_text);
                                rjson_b.append_value(serde_json::to_string(&m.replacements).unwrap_or_default());
                            }
                            PiiMode::Gate(action, cfg) => {
                                let det = detector.detect_pii(text, &serde_json::json!({}))
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                let gate = PolicyPrivacyGateNode;
                                let decision = gate.evaluate_policy(action, !det.matches.is_empty(), cfg)
                                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                                haspii_b.append_value(!det.matches.is_empty() as u32);
                                allow_b.append_value(decision.allow as u32);
                                reason_b.append_value(&decision.reason);
                                appr_b.append_value(decision.requires_approval as u32);
                            }
                        }
                    }
                    let schema = pii_mode_schema(&self.mode);
                    let batch_out = match &self.mode {
                        PiiMode::Detect => RecordBatch::try_new(
                            schema,
                            vec![
                                Arc::new(text_b.finish()),
                                Arc::new(count_b.finish()),
                                Arc::new(mjson_b.finish()),
                            ],
                        ),
                        PiiMode::Mask => RecordBatch::try_new(
                            schema,
                            vec![
                                Arc::new(text_b.finish()),
                                Arc::new(masked_b.finish()),
                                Arc::new(rjson_b.finish()),
                            ],
                        ),
                        PiiMode::Gate(..) => RecordBatch::try_new(
                            schema,
                            vec![
                                Arc::new(text_b.finish()),
                                Arc::new(haspii_b.finish()),
                                Arc::new(allow_b.finish()),
                                Arc::new(reason_b.finish()),
                                Arc::new(appr_b.finish()),
                            ],
                        ),
                    }
                    .map_err(|e| DataFusionError::Execution(format!("pii batch build: {}", e)))?;
                    Poll::Ready(Some(Ok(batch_out)))
                }
                Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.state = PiiStreamState::Done;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            },
            PiiStreamState::Done => Poll::Ready(None),
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for PiiStream {
    fn schema(&self) -> SchemaRef {
        pii_mode_schema(&self.mode)
    }
}

fn arrow_scalar_to_string(col: &dyn datafusion::arrow::array::Array, i: usize) -> String {
    use datafusion::arrow::util::display::array_value_to_string;
    array_value_to_string(col, i).unwrap_or_default()
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
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Policy & Privacy Gate requires one input plan"));
        }
        let column = config
            .get("column")
            .and_then(|v| v.as_str())
            .unwrap_or("text")
            .to_string();
        let action = config
            .get("action")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();
        let cfg = config.clone();
        let node = Arc::new(PiiLogical {
            input: inputs[0].clone(),
            column,
            mode: PiiMode::Gate(action, cfg),
        });
        Ok(vec![LogicalPlan::Extension(datafusion::logical_expr::Extension {
            node,
        })])
    }
}

#[cfg(test)]
mod exec_tests {
    use super::*;

    #[test]
    fn test_pii_logical_hash_eq() {
        let a = PiiLogical {
            input: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }),
            column: "text".to_string(),
            mode: PiiMode::Detect,
        };
        let b = PiiLogical {
            input: a.input.clone(),
            column: "text".to_string(),
            mode: PiiMode::Detect,
        };
        assert_eq!(a, b);
    }

    #[test]
    fn test_pii_mode_schemas_differ() {
        let det = PiiLogical {
            input: LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
                produce_one_row: false,
                schema: Arc::new(DFSchema::empty()),
            }),
            column: "t".into(),
            mode: PiiMode::Detect,
        };
        let msk = PiiLogical {
            input: det.input.clone(),
            column: "t".into(),
            mode: PiiMode::Mask,
        };
        use datafusion::logical_expr::UserDefinedLogicalNodeCore;
        assert_ne!(det.schema(), msk.schema());
        assert_eq!(det.schema().fields().len(), 3);
    }

    #[test]
    fn test_detect_mask_roundtrip_semantics() {
        let det = PiiDetectorNode;
        let msk = PiiMaskerNode;
        let text = "mail bob@x.io or 415-555-1234";
        let d = det.detect_pii(text, &serde_json::json!({})).unwrap();
        assert_eq!(d.matches.len(), 2);
        let m = msk.mask_pii(text, &serde_json::json!({})).unwrap();
        assert!(m.masked_text.contains("<PII_001>"));
        assert!(!m.masked_text.contains("bob@x.io"));
        let restored = msk.demask_pii(&m.masked_text, &m.replacements);
        assert_eq!(restored, text);
    }
}
