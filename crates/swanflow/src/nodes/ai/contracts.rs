use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, Float32Array, Float32Builder, RecordBatch, StringArray, StringBuilder, UInt64Array,
    UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DataFusionError};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::{Extension, UserDefinedLogicalNodeCore};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
};
use futures::Stream;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::Any;
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::nodes::{NodeBehavior, NodeCapability, NodeSpec, PortSpec};
use swanflow_core::validation::InstanceValidator;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CitationItem {
    pub source_id: String,
    pub snippet: String,
    pub score: f32,
    pub start: usize,
    pub end: usize,
}

pub struct StructuredIoContractNode;
pub struct CitationTrackerNode;

impl CitationTrackerNode {
    pub fn rank_citations(
        &self,
        mut citations: Vec<CitationItem>,
        config: &JsonValue,
    ) -> Result<Vec<CitationItem>> {
        let max_citations = config["max_citations"].as_u64().unwrap_or(5) as usize;
        if max_citations == 0 {
            return Err(anyhow!("max_citations must be greater than zero"));
        }

        citations.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.start.cmp(&right.start))
        });
        citations.truncate(max_citations);
        Ok(citations)
    }
}

// ============================================================================
// core.ai.structured_io_contract — REAL since 2026-10-16 (UA port batch 2;
// was a pass-through shell). Deterministic validation gate for raw LLM JSON
// output. Four layers, ported from the Understand-Anything Zod pipeline —
// "never let bad data into the graph":
//
//   1. sanitize    — strip BOM / trim / peel ```json fences (total fn)
//   2. alias-map   — recursively normalize object keys AND string values
//   3. auto-fix    — the alias substitutions themselves; each one is
//                    counted (K) so the diagnostic shows how many ran
//   4. validate    — every element through the JSON Schema (jsonschema 0.49
//                    via swanflow_core::validation::InstanceValidator);
//                    bad elements are dropped and counted (fail-soft, the
//                    default) — or with `strict: true` the first bad element
//                    errors the whole node (fail-fast).
//
// Output: one row per PASSED element — `item` = normalized JSON text,
// `rejected` = the source input row's dropped elements + reasons (empty
// when that input row lost none). Input rows with zero passed elements emit
// no rows: their rejection record lives in the diagnostic report (one
// `eprintln` per execution: passed N / dropped M / alias-fixes K).
//
// The legacy agent-side path `ConfigWizard::extract_json_from_response`
// (nodes/ai/mod.rs) stays untouched — separate case.
// ============================================================================

/// Layer 1: sanitize raw LLM text. Total — never fails: strips a leading
/// BOM, trims, and peels a ```json / ``` code fence when present (the
/// fence-aware twin of the legacy `extract_json_from_response` substring
/// scan). An unclosed fence keeps everything after the opening line.
pub fn sanitize_llm_text(raw: &str) -> String {
    let t = raw.strip_prefix('\u{feff}').unwrap_or(raw).trim();
    if !t.starts_with("```") {
        return t.to_string();
    }
    let rest = &t[3..];
    let body = match rest.find('\n') {
        Some(i) => &rest[i + 1..],
        // Single-line fence ("```json {...}```"): the body starts after the
        // (optional language) tag on the same line.
        None => match rest.find(char::is_whitespace) {
            Some(i) => &rest[i + 1..],
            None => rest,
        },
    };
    // Closing fence = the last "```" in the body (best effort).
    let end = body.rfind("```").unwrap_or(body.len());
    body[..end].trim().to_string()
}

/// Layers 2+3: apply `alias_map` recursively — object KEYS and string
/// VALUES are both looked up and substituted. Values are the common LLM
/// case (e.g. "组织" -> "organization"); key aliases also normalize schemas
/// the model spelled in the wrong language. Every substitution bumps
/// `fixes` so the diagnostic can report the auto-fix count.
fn apply_aliases(
    value: &JsonValue,
    alias_map: &[(String, String)],
    fixes: &mut usize,
) -> JsonValue {
    match value {
        JsonValue::Object(map) => {
            let mut out = serde_json::Map::new();
            for (key, val) in map {
                let new_key = match alias_map.iter().find(|(from, _)| from == key) {
                    Some((_, to)) => {
                        *fixes += 1;
                        to.clone()
                    }
                    None => key.clone(),
                };
                out.insert(new_key, apply_aliases(val, alias_map, fixes));
            }
            JsonValue::Object(out)
        }
        JsonValue::Array(items) => JsonValue::Array(
            items
                .iter()
                .map(|v| apply_aliases(v, alias_map, fixes))
                .collect(),
        ),
        JsonValue::String(s) => match alias_map.iter().find(|(from, _)| from == s) {
            Some((_, to)) => {
                *fixes += 1;
                JsonValue::String(to.clone())
            }
            None => value.clone(),
        },
        other => other.clone(),
    }
}

/// The compiled contract config — parsed/validated once at plan time and
/// shared by the logical node and the Exec.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompiledContract {
    /// Instance schema (JSON Schema; the practical subset is
    /// type/properties/required/enum/items — anything jsonschema 0.49
    /// accepts).
    pub schema: JsonValue,
    /// alias -> canonical, applied recursively to keys and string values.
    pub alias_map: Vec<(String, String)>,
    /// true = fail-fast: the first bad element errors the whole node.
    /// false = fail-soft (default): bad elements are dropped and counted.
    pub strict: bool,
    /// Name of the Utf8 input column holding the raw LLM text.
    /// None = first column.
    pub input_column: Option<String>,
}

impl CompiledContract {
    /// Parse + validate a node config. `schema` wins; a non-empty legacy
    /// `required_fields` array synthesizes `{"type":"object","required":[..]}`
    /// so pre-2026-10 shell-era workflows keep validating (error messages
    /// for the legacy paths are unchanged on purpose).
    pub fn from_config(config: &JsonValue) -> Result<Self> {
        let schema: JsonValue = if let Some(s) = config.get("schema") {
            match s.as_object() {
                Some(m) => JsonValue::Object(m.clone()),
                None => {
                    return Err(anyhow!(
                        "structured_io_contract: 'schema' must be a JSON Schema object"
                    ))
                }
            }
        } else if let Some(fields) = config.get("required_fields") {
            let arr = fields.as_array().ok_or_else(|| {
                anyhow!("structured_io_contract: required_fields must be an array")
            })?;
            if arr.is_empty() {
                return Err(anyhow!(
                    "structured_io_contract: required_fields must not be empty"
                ));
            }
            for field in arr {
                if field.as_str().is_none() {
                    return Err(anyhow!(
                        "structured_io_contract: required_fields must contain only strings"
                    ));
                }
            }
            let mut m = serde_json::Map::new();
            m.insert("type".to_string(), JsonValue::String("object".into()));
            m.insert("required".to_string(), JsonValue::Array(arr.clone()));
            JsonValue::Object(m)
        } else {
            return Err(anyhow!(
                "structured_io_contract: config requires 'schema' (a JSON Schema object) or a non-empty 'required_fields' array"
            ));
        };
        // Fail fast at load time if the schema does not even compile.
        InstanceValidator::compile(&schema).map_err(|e| anyhow!("structured_io_contract: {e}"))?;

        let alias_map = match config.get("alias_map") {
            None => Vec::new(),
            Some(v) => {
                let obj = v.as_object().ok_or_else(|| {
                    anyhow!(
                        "structured_io_contract: alias_map must be an object of alias -> canonical string"
                    )
                })?;
                let mut pairs = Vec::with_capacity(obj.len());
                for (k, val) in obj {
                    let canonical = val.as_str().ok_or_else(|| {
                        anyhow!(
                            "structured_io_contract: alias_map values must be strings (key '{k}')"
                        )
                    })?;
                    pairs.push((k.clone(), canonical.to_string()));
                }
                pairs
            }
        };

        if let Some(v) = config.get("strict") {
            if v.as_bool().is_none() {
                return Err(anyhow!(
                    "structured_io_contract: 'strict' must be a boolean"
                ));
            }
        }
        let strict = config["strict"].as_bool().unwrap_or(false);

        if let Some(v) = config.get("input_column") {
            if v.as_str().is_none() {
                return Err(anyhow!(
                    "structured_io_contract: 'input_column' must be a string"
                ));
            }
        }
        let input_column = config["input_column"].as_str().map(|s| s.to_string());

        Ok(Self {
            schema,
            alias_map,
            strict,
            input_column,
        })
    }
}

/// Per-batch counters; the streaming Exec folds them into the single
/// diagnostic report emitted per execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchContractStats {
    pub input_rows: usize,
    pub passed: usize,
    pub dropped: usize,
    pub alias_fixes: usize,
}

/// Run the four-layer pipeline over one input batch.
///
/// Fail-soft (default): bad elements are dropped and counted, the batch
/// completes. `strict`: the first bad element (parse-stage or schema-stage)
/// returns `Err` and the node — with it the run — fails.
pub fn process_batch(
    batch: &RecordBatch,
    contract: &CompiledContract,
    validator: &InstanceValidator,
) -> Result<(RecordBatch, BatchContractStats), DataFusionError> {
    if batch.num_columns() == 0 {
        return Err(DataFusionError::Execution(
            "structured_io_contract: input has no columns (expected one Utf8 column of raw LLM text)"
                .into(),
        ));
    }
    let idx = match &contract.input_column {
        Some(name) => batch.schema().index_of(name).map_err(|e| {
            DataFusionError::Execution(format!(
                "structured_io_contract: input column '{name}' not found: {e}"
            ))
        })?,
        None => 0,
    };
    // Sources may hand us the string family in different physical flavors
    // (Utf8View/LargeUtf8/Dictionary) — cast to plain Utf8 before reading.
    let text_arr =
        datafusion::arrow::compute::cast(batch.column(idx), &DataType::Utf8).map_err(|e| {
            DataFusionError::Execution(format!(
                "structured_io_contract: cannot cast input column {idx} to Utf8: {e}"
            ))
        })?;
    let text = text_arr
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "structured_io_contract: input column is not string-like".into(),
            )
        })?;

    let out_schema = Arc::new(Schema::new(vec![
        Field::new("item", DataType::Utf8, true),
        Field::new("rejected", DataType::Utf8, true),
    ]));

    let mut item_b = StringBuilder::new();
    let mut rejected_b = StringBuilder::new();
    let mut stats = BatchContractStats::default();

    for i in 0..text.len() {
        if text.is_null(i) {
            // A null raw cell is not a JSON element: treat it as a bad
            // element (fail-soft: counted; strict: fatal).
            stats.input_rows += 1;
            stats.dropped += 1;
            if contract.strict {
                return Err(DataFusionError::Execution(format!(
                    "structured_io_contract (strict): input row {i} is null"
                )));
            }
            continue;
        }
        stats.input_rows += 1;
        let raw = text.value(i);
        // Layer 1.
        let clean = sanitize_llm_text(raw);
        // Parse: a row whose text is not JSON at all is one dropped
        // element (fail-soft) or a hard error (strict).
        let elements: Vec<JsonValue> = match serde_json::from_str(&clean) {
            Ok(JsonValue::Array(a)) => a,
            Ok(other) => vec![other],
            Err(e) => {
                stats.dropped += 1;
                if contract.strict {
                    return Err(DataFusionError::Execution(format!(
                        "structured_io_contract (strict): input row {i} is not valid JSON: {e}"
                    )));
                }
                continue;
            }
        };
        // Layers 2+3 (normalize/auto-fix) then 4 (validate), per element.
        let mut row_drops: Vec<String> = Vec::new();
        let mut row_passes: Vec<JsonValue> = Vec::new();
        for element in &elements {
            let fixed = apply_aliases(element, &contract.alias_map, &mut stats.alias_fixes);
            match validator.validate(&fixed) {
                Ok(()) => row_passes.push(fixed),
                Err(reason) => {
                    stats.dropped += 1;
                    row_drops.push(format!("{} :: {reason}", compact_json(&fixed)));
                    if contract.strict {
                        return Err(DataFusionError::Execution(format!(
                            "structured_io_contract (strict): input row {i} failed schema validation: {reason}"
                        )));
                    }
                }
            }
        }
        stats.passed += row_passes.len();
        // One output row per passed element; the input row's rejection
        // summary rides along so downstream can see why a sibling was
        // dropped. A fully-rejected input row produces no rows — its record
        // lives in the diagnostic report.
        let rejected_cell = if row_drops.is_empty() {
            String::new()
        } else {
            row_drops.join("; ")
        };
        for item in &row_passes {
            item_b.append_value(item.to_string());
            rejected_b.append_value(&rejected_cell);
        }
    }

    let out = RecordBatch::try_new(
        out_schema,
        vec![Arc::new(item_b.finish()), Arc::new(rejected_b.finish())],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
    Ok((out, stats))
}

/// Compact (whitespace-free) JSON for rejection records.
fn compact_json(v: &JsonValue) -> String {
    serde_json::to_string(v).unwrap_or_else(|_| v.to_string())
}

#[async_trait]
impl NodeBehavior for StructuredIoContractNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.structured_io_contract".to_string(),
            display_name: "Structured IO Contract".to_string(),
            description: "Deterministic validation gate for raw LLM JSON output (UA 4-layer pipeline): strips BOM/whitespace/```json fences, normalizes alias keys and string values (auto-fixes, counted), validates each element against the JSON Schema (config `schema`, subset: type/properties/required/enum/items) or a legacy `required_fields` shorthand, then drops bad elements with a per-element reason (fail-soft default) or fails fast on the first one (config `strict: true`). Emits one `item` row per passed element (normalized JSON) plus a `rejected` column with the dropped elements and reasons, and logs one passed/dropped/alias-fixes diagnostic per run."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "One Utf8 column of raw LLM text — one JSON object or array per row; ```json fences, BOM and surrounding whitespace are tolerated".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "`item` (Utf8: passed elements as normalized JSON) + `rejected` (Utf8: dropped elements from the same input row + reasons)".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "schema": {
                        "type": "object",
                        "description": "JSON Schema (subset: type/properties/required/enum/items) each output element must satisfy"
                    },
                    "required_fields": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Legacy shorthand (shell era): synthesizes {\"type\":\"object\",\"required\":[...]} when `schema` is absent"
                    },
                    "alias_map": {
                        "type": "object",
                        "additionalProperties": { "type": "string" },
                        "description": "alias value -> canonical value, applied recursively to all keys and string values (auto-fix)"
                    },
                    "strict": {
                        "type": "boolean",
                        "default": false,
                        "description": "true = fail-fast: the first bad element errors the whole node (default false = fail-soft, drop and count)"
                    },
                    "input_column": {
                        "type": "string",
                        "description": "Utf8 column holding the raw LLM text (default: first column)"
                    }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        CompiledContract::from_config(config)?;
        Ok(())
    }

    /// Pure in-memory validation gate — no external side effects.
    fn capability(&self) -> NodeCapability {
        NodeCapability::ReadOnly
    }

    async fn propose_plan(
        &self,
        mut inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Structured IO Contract requires one input plan"));
        }
        let contract = CompiledContract::from_config(config)?;
        let out_schema = Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("item", DataType::Utf8, true),
            Field::new("rejected", DataType::Utf8, true),
        ]))?);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(StructuredIoContractLogical {
                contract,
                schema: out_schema,
                input: Arc::new(inputs.remove(0)),
            }),
        })])
    }
}

// --- Extension wiring (real implementation, 2026-10-16; was a pass-through shell) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StructuredIoContractLogical {
    pub contract: CompiledContract,
    pub schema: Arc<DFSchema>,
    /// The (single) upstream plan — carried so DataFusion lowers the input.
    pub input: Arc<LogicalPlan>,
}

impl PartialOrd for StructuredIoContractLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.contract.strict.cmp(&other.contract.strict))
    }
}

impl UserDefinedLogicalNodeCore for StructuredIoContractLogical {
    fn name(&self) -> &str {
        "StructuredIoContract"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StructuredIoContract(strict={})", self.contract.strict)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        let input = inputs.into_iter().next().ok_or_else(|| {
            DataFusionError::Internal("StructuredIoContract requires its input".into())
        })?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct StructuredIoContractExec {
    pub contract: CompiledContract,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
    pub input: Arc<dyn ExecutionPlan>,
}

impl StructuredIoContractExec {
    pub fn new(
        contract: CompiledContract,
        schema: SchemaRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        // CRITICAL: coalesce input partitions to 1 (same partition-drop hazard
        // as EntityExtractorExec) — a UNION ALL source like core.sql yields
        // several partitions; executing only the caller's partition index
        // would silently drop the other partitions' LLM rows.
        let input: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
        );
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            contract,
            schema,
            cache,
            input,
        }
    }
}

impl DisplayAs for StructuredIoContractExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StructuredIoContractExec(strict={})",
            self.contract.strict
        )
    }
}

impl ExecutionPlan for StructuredIoContractExec {
    fn name(&self) -> &str {
        "StructuredIoContractExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
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
        Ok(Arc::new(StructuredIoContractExec::new(
            self.contract.clone(),
            self.schema.clone(),
            children.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal(
                    "StructuredIoContractExec requires its input child".into(),
                )
            })?,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        // The schema was already compile-checked at plan time
        // (CompiledContract::from_config); a failure here can only come from
        // a hand-built plan — fail loudly instead of panicking.
        let validator = InstanceValidator::compile(&self.contract.schema)
            .map_err(|e| DataFusionError::Execution(format!("structured_io_contract: {e}")))?;
        Ok(Box::pin(StructuredIoContractStream {
            schema: self.schema.clone(),
            contract: self.contract.clone(),
            validator,
            input,
            done: false,
            passed: 0,
            dropped: 0,
            alias_fixes: 0,
            reported: false,
        }))
    }
}

pub struct StructuredIoContractStream {
    schema: SchemaRef,
    contract: CompiledContract,
    validator: InstanceValidator,
    input: SendableRecordBatchStream,
    done: bool,
    /// Running totals across all batches of this execution.
    passed: usize,
    dropped: usize,
    alias_fixes: usize,
    /// Guard so exactly one diagnostic report is emitted per execution.
    reported: bool,
}

impl StructuredIoContractStream {
    /// The one diagnostic report per execution: what the gate did —
    /// passed N / dropped M / alias-fixes K.
    fn report_diagnostic(&mut self) {
        if self.reported {
            return;
        }
        self.reported = true;
        eprintln!(
            "[structured_io_contract] passed {} / dropped {} / alias-fixes {}",
            self.passed, self.dropped, self.alias_fixes
        );
    }
}

impl Stream for StructuredIoContractStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                match process_batch(&batch, &self.contract, &self.validator) {
                    Ok((out, stats)) => {
                        self.passed += stats.passed;
                        self.dropped += stats.dropped;
                        self.alias_fixes += stats.alias_fixes;
                        Poll::Ready(Some(Ok(out)))
                    }
                    Err(e) => {
                        // strict mode: report what was seen before failing.
                        self.report_diagnostic();
                        Poll::Ready(Some(Err(e)))
                    }
                }
            }
            Poll::Ready(Some(Err(e))) => {
                self.report_diagnostic();
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.done = true;
                self.report_diagnostic();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for StructuredIoContractStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[async_trait]
impl NodeBehavior for CitationTrackerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.citation_tracker".to_string(),
            display_name: "Citation Tracker".to_string(),
            description: "Rank input citation rows (source_id/snippet/score/start/end) by score desc, tie-break source_id/start, truncate to max_citations (offline).".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Plan with citation contract".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_citations": { "type": "integer", "minimum": 1, "default": 5 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(max) = config["max_citations"].as_u64() {
            if max == 0 {
                return Err(anyhow!("max_citations must be greater than zero"));
            }
            return Ok(());
        }
        if config.get("max_citations").is_some() {
            return Err(anyhow!("max_citations must be an integer"));
        }
        Ok(())
    }

    async fn propose_plan(
        &self,
        mut inputs: Vec<LogicalPlan>,
        config: &JsonValue,
    ) -> Result<Vec<LogicalPlan>> {
        if inputs.is_empty() {
            return Err(anyhow!("Citation Tracker requires one input plan"));
        }
        self.validate(config)?;
        let max_citations = config["max_citations"].as_u64().unwrap_or(5) as usize;
        let out_schema = Arc::new(DFSchema::try_from(
            datafusion::arrow::datatypes::Schema::new(vec![
                Field::new("source_id", DataType::Utf8, false),
                Field::new("snippet", DataType::Utf8, false),
                Field::new("score", DataType::Float32, false),
                Field::new("start", DataType::UInt64, false),
                Field::new("end", DataType::UInt64, false),
            ]),
        )?);
        Ok(vec![LogicalPlan::Extension(Extension {
            node: Arc::new(CitationRankLogical {
                max_citations,
                schema: out_schema,
                input: Arc::new(inputs.remove(0)),
            }),
        })])
    }
}

// --- Extension wiring (real implementation, 2026-09-28; was a pass-through shell) ---

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CitationRankLogical {
    pub max_citations: usize,
    pub schema: Arc<DFSchema>,
    /// The (single) upstream plan — carried so DataFusion lowers the input.
    pub input: Arc<LogicalPlan>,
}

impl PartialOrd for CitationRankLogical {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.max_citations.partial_cmp(&other.max_citations)
    }
}

impl UserDefinedLogicalNodeCore for CitationRankLogical {
    fn name(&self) -> &str {
        "CitationRank"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<datafusion::logical_expr::Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CitationRank: max_citations={}", self.max_citations)
    }
    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<datafusion::logical_expr::Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> Result<Self, DataFusionError> {
        let input = inputs
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Internal("CitationRank requires its input".into()))?;
        Ok(Self {
            input: Arc::new(input),
            ..self.clone()
        })
    }
}

#[derive(Debug)]
pub struct CitationRankExec {
    pub max_citations: usize,
    pub schema: SchemaRef,
    pub cache: PlanProperties,
    pub input: Arc<dyn ExecutionPlan>,
}

impl CitationRankExec {
    pub fn new(max_citations: usize, schema: SchemaRef, input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            max_citations,
            schema,
            cache,
            input,
        }
    }
}

impl DisplayAs for CitationRankExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CitationRankExec: max_citations={}", self.max_citations)
    }
}

impl ExecutionPlan for CitationRankExec {
    fn name(&self) -> &str {
        "CitationRankExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
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
        Ok(Arc::new(CitationRankExec::new(
            self.max_citations,
            self.schema.clone(),
            children.into_iter().next().ok_or_else(|| {
                DataFusionError::Internal("CitationRankExec requires its input child".into())
            })?,
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(CitationRankStream {
            schema: self.schema.clone(),
            max_citations: self.max_citations,
            input,
            done: false,
        }))
    }
}

pub struct CitationRankStream {
    schema: SchemaRef,
    max_citations: usize,
    input: SendableRecordBatchStream,
    done: bool,
}

impl Stream for CitationRankStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let out = rank_batch(&batch, self.max_citations, self.schema.clone())?;
                Poll::Ready(Some(Ok(out)))
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

impl RecordBatchStream for CitationRankStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Rank one batch of citation rows: score desc, tie-break source_id asc then
/// start asc (mirrors CitationTrackerNode::rank_citations), truncate to max.
fn rank_batch(
    batch: &RecordBatch,
    max_citations: usize,
    schema: SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let col = |name: &str| {
        batch.column_by_name(name).ok_or_else(|| {
            DataFusionError::Execution(format!("CitationRank: input missing column '{name}'"))
        })
    };
    let source_id = col("source_id")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("source_id must be Utf8".into()))?;
    let snippet = col("snippet")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Execution("snippet must be Utf8".into()))?;
    // Accept Float32 (native) or Float64 (CSV sources infer f64) for score.
    let score_f64;
    let score_f32;
    let score_vals: &dyn Fn(usize) -> f64 = if let Some(a) = batch
        .column_by_name("score")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
    {
        score_f32 = a;
        &|i: usize| score_f32.value(i) as f64
    } else if let Some(a) = batch.column_by_name("score").and_then(|c| {
        c.as_any()
            .downcast_ref::<datafusion::arrow::array::Float64Array>()
    }) {
        score_f64 = a;
        &|i: usize| score_f64.value(i)
    } else {
        return Err(DataFusionError::Execution(
            "score must be Float32 or Float64".into(),
        ));
    };
    // Accept UInt64 (native) or Int64 (CSV inference) for start/end.
    let start_i64;
    let start_u64;
    let start_val: &dyn Fn(usize) -> u64 = if let Some(a) = batch
        .column_by_name("start")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    {
        start_u64 = a;
        &|i: usize| start_u64.value(i)
    } else if let Some(a) = batch.column_by_name("start").and_then(|c| {
        c.as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
    }) {
        start_i64 = a;
        &|i: usize| start_i64.value(i) as u64
    } else {
        return Err(DataFusionError::Execution(
            "start must be UInt64 or Int64".into(),
        ));
    };
    let end_i64;
    let end_u64;
    let end_val: &dyn Fn(usize) -> u64 = if let Some(a) = batch
        .column_by_name("end")
        .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
    {
        end_u64 = a;
        &|i: usize| end_u64.value(i)
    } else if let Some(a) = batch.column_by_name("end").and_then(|c| {
        c.as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
    }) {
        end_i64 = a;
        &|i: usize| end_i64.value(i) as u64
    } else {
        return Err(DataFusionError::Execution(
            "end must be UInt64 or Int64".into(),
        ));
    };

    let mut order: Vec<usize> = (0..source_id.len()).collect();
    order.sort_by(|&l, &r| {
        score_vals(r)
            .partial_cmp(&score_vals(l))
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| source_id.value(l).cmp(source_id.value(r)))
            .then_with(|| start_val(l).cmp(&start_val(r)))
    });
    order.truncate(max_citations);

    let mut sid_b = StringBuilder::new();
    let mut snip_b = StringBuilder::new();
    let mut score_b = Float32Builder::new();
    let mut start_b = UInt64Builder::new();
    let mut end_b = UInt64Builder::new();
    for i in order {
        sid_b.append_value(source_id.value(i));
        snip_b.append_value(snippet.value(i));
        score_b.append_value(score_vals(i) as f32);
        start_b.append_value(start_val(i));
        end_b.append_value(end_val(i));
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(sid_b.finish()),
            Arc::new(snip_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(start_b.finish()),
            Arc::new(end_b.finish()),
        ],
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod citation_rank_tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use std::sync::Arc;

    fn five_col_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("source_id", DataType::Utf8, false),
            Field::new("snippet", DataType::Utf8, false),
            Field::new("score", DataType::Float32, false),
            Field::new("start", DataType::UInt64, false),
            Field::new("end", DataType::UInt64, false),
        ]))
    }

    fn build_batch(rows: &[(&str, &str, f32, u64, u64)]) -> RecordBatch {
        let mut sid = StringBuilder::new();
        let mut snip = StringBuilder::new();
        let mut sc = Float32Builder::new();
        let mut st = UInt64Builder::new();
        let mut e = UInt64Builder::new();
        for (a, b, c, d, f) in rows {
            sid.append_value(*a);
            snip.append_value(*b);
            sc.append_value(*c);
            st.append_value(*d);
            e.append_value(*f);
        }
        RecordBatch::try_new(
            five_col_schema(),
            vec![
                Arc::new(sid.finish()),
                Arc::new(snip.finish()),
                Arc::new(sc.finish()),
                Arc::new(st.finish()),
                Arc::new(e.finish()),
            ],
        )
        .unwrap()
    }

    #[test]
    fn ranks_by_score_desc_truncates() {
        let batch = build_batch(&[
            ("srcA", "alpha snippet", 0.25, 0, 10),
            ("srcB", "bravo snippet", 0.90, 5, 15),
            ("srcC", "charlie snippet", 0.60, 2, 8),
        ]);
        let out = rank_batch(&batch, 2, five_col_schema()).unwrap();
        assert_eq!(out.num_rows(), 2);
        let sid = out
            .column_by_name("source_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let score = out
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(sid.value(0), "srcB");
        assert_eq!(sid.value(1), "srcC");
        assert!((score.value(0) - 0.90).abs() < 1e-6);
    }

    #[test]
    fn tie_breaks_source_id_then_start() {
        let batch = build_batch(&[
            ("srcB", "same score", 0.5, 9, 19),
            ("srcA", "same score", 0.5, 4, 14),
            ("srcA", "same score earlier", 0.5, 1, 11),
        ]);
        let out = rank_batch(&batch, 3, five_col_schema()).unwrap();
        let sid = out
            .column_by_name("source_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let start = out
            .column_by_name("start")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(sid.value(0), "srcA");
        assert_eq!(start.value(0), 1);
        assert_eq!(sid.value(1), "srcA");
        assert_eq!(start.value(1), 4);
        assert_eq!(sid.value(2), "srcB");
    }
}

#[cfg(test)]
mod structured_io_tests {
    use super::*;

    fn input_batch(rows: &[&str]) -> RecordBatch {
        let mut b = StringBuilder::new();
        for r in rows {
            b.append_value(*r);
        }
        let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(b.finish())]).unwrap()
    }

    fn run_pipeline(
        rows: &[&str],
        cfg: &JsonValue,
    ) -> Result<(RecordBatch, BatchContractStats), String> {
        let contract = CompiledContract::from_config(cfg).map_err(|e| e.to_string())?;
        let validator = InstanceValidator::compile(&contract.schema).map_err(|e| e)?;
        let batch = input_batch(rows);
        process_batch(&batch, &contract, &validator).map_err(|e| e.to_string())
    }

    fn col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
        batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    }

    /// ① 码栏包裹的 JSON 数组（外加一行 BOM+裸对象）→ 全部通过。
    #[test]
    fn fenced_array_all_pass() {
        let cfg = serde_json::json!({
            "schema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        });
        let rows = [
            "```json\n[{\"name\": \"a\"}, {\"name\": \"b\"}]\n```",
            "\u{feff}{\"name\": \"c\"}",
        ];
        let (out, stats) = run_pipeline(&rows, &cfg).unwrap();
        assert_eq!(stats.passed, 3, "fence/BOM must be stripped: {stats:?}");
        assert_eq!(stats.dropped, 0);
        assert_eq!(stats.alias_fixes, 0);
        assert_eq!(out.num_rows(), 3, "one output row per passed element");
        let item = col(&out, "item");
        let names: Vec<String> = (0..3)
            .map(|i| {
                let v: JsonValue = serde_json::from_str(item.value(i)).unwrap();
                v["name"].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(
            names,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
        let rejected = col(&out, "rejected");
        for i in 0..3 {
            assert_eq!(rejected.value(i), "", "no drops → empty rejected cell");
        }
    }

    /// ② 数组里混一个坏对象 → 只丢那个，其余通过（坏元素不进下游）。
    #[test]
    fn mixed_array_drops_only_the_bad_element() {
        let cfg = serde_json::json!({
            "schema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            }
        });
        let rows = ["[{\"name\": \"a\"}, {\"name\": 123}, {\"name\": \"c\"}]"];
        let (out, stats) = run_pipeline(&rows, &cfg).unwrap();
        assert_eq!(stats.passed, 2);
        assert_eq!(stats.dropped, 1);
        assert_eq!(
            out.num_rows(),
            2,
            "the bad element must not reach downstream"
        );
        let item = col(&out, "item");
        assert!(
            item.value(0).contains("\"a\""),
            "first pass: {}",
            item.value(0)
        );
        assert!(
            item.value(1).contains("\"c\""),
            "second pass: {}",
            item.value(1)
        );
        assert!(!item.value(0).contains("123"));
        assert!(!item.value(1).contains("123"));
        // The rejection record rides along in `rejected` on sibling rows.
        let rejected = col(&out, "rejected");
        assert!(
            rejected.value(0).contains("123"),
            "reason must name the bad element: {}",
            rejected.value(0)
        );
        assert!(rejected.value(0).contains("string"));
    }

    /// ③ alias_map 把 "组织"→"organization" 归一后过 enum 校验。
    #[test]
    fn alias_normalization_then_enum_validation() {
        let cfg = serde_json::json!({
            "schema": {
                "type": "object",
                "properties": { "kind": { "enum": ["organization", "person"] } },
                "required": ["kind"]
            },
            "alias_map": { "组织": "organization" }
        });
        let rows = ["{\"kind\": \"组织\"}"];
        let (out, stats) = run_pipeline(&rows, &cfg).unwrap();
        assert_eq!(stats.passed, 1, "alias fix must make the enum check pass");
        assert_eq!(stats.dropped, 0);
        assert_eq!(
            stats.alias_fixes, 1,
            "the 组织→organization fix must be counted"
        );
        let item = col(&out, "item");
        assert_eq!(item.value(0), "{\"kind\":\"organization\"}");
        assert_eq!(col(&out, "rejected").value(0), "");
    }

    /// ④ strict 模式：首个坏元素即整节点报错（解析阶段与 schema 阶段都要炸）。
    #[test]
    fn strict_mode_fails_fast_on_first_bad_element() {
        let cfg = serde_json::json!({
            "schema": {
                "type": "object",
                "properties": { "name": { "type": "string" } },
                "required": ["name"]
            },
            "strict": true
        });
        let rows = ["[{\"name\": \"a\"}, {\"name\": 123}]"];
        let err = run_pipeline(&rows, &cfg).unwrap_err();
        assert!(
            err.contains("strict"),
            "fail-fast error must name strict mode: {err}"
        );
        let rows2 = ["definitely not json"];
        let err2 = run_pipeline(&rows2, &cfg).unwrap_err();
        assert!(
            err2.contains("strict"),
            "parse-stage failure is fatal too: {err2}"
        );
    }

    /// ⑤ 空输入批次 → 输出 0 行不算错（sink 空输入 bail 的对偶：校验门要能过空）。
    #[test]
    fn empty_input_batch_yields_zero_rows_without_error() {
        let cfg = serde_json::json!({
            "schema": { "type": "object", "required": ["name"] }
        });
        let (out, stats) = run_pipeline(&[], &cfg).unwrap();
        assert_eq!(out.num_rows(), 0);
        assert_eq!(stats.input_rows, 0);
        assert_eq!(stats.passed, 0);
        assert_eq!(stats.dropped, 0);
        assert_eq!(stats.alias_fixes, 0);
    }

    /// 兼容面：legacy `required_fields` 合成 object schema，错误文案不变。
    #[test]
    fn legacy_required_fields_synthesizes_object_schema() {
        let cfg = serde_json::json!({ "required_fields": ["name"] });
        let (out, stats) = run_pipeline(&["{\"name\": \"x\"}"], &cfg).unwrap();
        assert_eq!(stats.passed, 1);
        assert_eq!(out.num_rows(), 1);
        let (_, stats2) = run_pipeline(&["{\"other\": 1}"], &cfg).unwrap();
        assert_eq!(stats2.dropped, 1, "missing required field drops fail-soft");

        let err = CompiledContract::from_config(&serde_json::json!({ "required_fields": [] }))
            .unwrap_err()
            .to_string();
        assert!(err.contains("must not be empty"));
    }

    /// 层1 直测：BOM / 码栏 / 纯文本的 sanitize 行为。
    #[test]
    fn sanitize_strips_bom_fence_and_whitespace() {
        assert_eq!(
            sanitize_llm_text("  ```json\n{\"a\": 1}\n```  "),
            "{\"a\": 1}"
        );
        assert_eq!(sanitize_llm_text("\u{feff}  {\"a\": 1}  "), "{\"a\": 1}");
        assert_eq!(sanitize_llm_text("plain json"), "plain json");
        // Degenerate: opening fence with no body keeps the tag text (parse
        // stage will reject it — the pipeline is total on sanitize).
        assert_eq!(sanitize_llm_text("```json"), "json");
    }
}
