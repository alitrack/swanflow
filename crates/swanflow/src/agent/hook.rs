use crate::core::engine::IntelligenceHook;
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use uuid::Uuid;

/// Rule-based intelligence hook.
///
/// The rule-based implementation only produces advisory output — it has no
/// graph-repair capability, so `on_error` always returns `None`, escalating
/// the failure to the existing human-in-the-loop path. LLM-powered repair
/// (the five GraSP operators) is provided by `swanflow-ai::hook::OpenAiHook`
/// + `swanflow::core::repair::apply_repair`.
pub struct DefaultIntelligenceHook {
    pub wizard: Arc<crate::nodes::ai::ConfigWizard>,
}

#[async_trait]
impl IntelligenceHook for DefaultIntelligenceHook {
    async fn on_error(
        &self,
        node_id: Uuid,
        type_id: &str,
        _config: &JsonValue,
        error: &str,
    ) -> Option<swanflow_core::repair::RepairOperator> {
        println!(
            "[Intelligence] Analyzing error for node {} (type: {}): {}\n\
             [Intelligence] This rule-based hook has no repair capability — \
             the error escalates to the human-in-the-loop path.",
            node_id, type_id, error
        );
        // Returning None escalates the error to the HITL path (unchanged).
        None
    }

    async fn on_data_quality(
        &self,
        _node_id: Uuid,
        _sample: &datafusion::arrow::record_batch::RecordBatch,
    ) -> Option<JsonValue> {
        // Quality checks could trigger semantic cleaning
        None
    }
}
