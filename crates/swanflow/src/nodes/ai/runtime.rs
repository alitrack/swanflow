use anyhow::{anyhow, Result};
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

use crate::nodes::{NodeBehavior, NodeSpec, PortSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RetryPlan {
    pub idempotency_key: String,
    pub duplicate: bool,
    pub delays_ms: Vec<u64>,
}

pub struct RetryBackoffGuardNode;

impl RetryBackoffGuardNode {
    pub fn plan_retry(
        &self,
        run_id: &str,
        operation: &str,
        seen_keys: &mut HashSet<String>,
        config: &JsonValue,
    ) -> Result<RetryPlan> {
        self.validate(config)?;

        let max_retries = config["max_retries"].as_u64().unwrap_or(3);
        let base_delay_ms = config["base_delay_ms"].as_u64().unwrap_or(100);
        let idempotency_key = format!("{}::{}", run_id, operation);

        if seen_keys.contains(&idempotency_key) {
            return Ok(RetryPlan {
                idempotency_key,
                duplicate: true,
                delays_ms: Vec::new(),
            });
        }
        seen_keys.insert(idempotency_key.clone());

        // Delegate to the shared retry policy (core/retry.rs) so the node and
        // the engine can never drift apart again.
        let delays = crate::core::retry::backoff_schedule(base_delay_ms, max_retries);

        Ok(RetryPlan {
            idempotency_key,
            duplicate: false,
            delays_ms: delays,
        })
    }
}

#[async_trait]
impl NodeBehavior for RetryBackoffGuardNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.retry_backoff_idempotency_guard".to_string(),
            display_name: "Retry/Backoff + Idempotency Guard".to_string(),
            description: "[SHELL NODE — no runtime behavior] Plan deterministic retry delays and suppress duplicate executions."
                .to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for guarded execution".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with retry metadata".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_retries": { "type": "integer", "minimum": 1, "default": 3 },
                    "base_delay_ms": { "type": "integer", "minimum": 1, "default": 100 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("max_retries") {
            let retries = value
                .as_u64()
                .ok_or_else(|| anyhow!("max_retries must be a positive integer"))?;
            if retries == 0 {
                return Err(anyhow!("max_retries must be greater than 0"));
            }
        }
        if let Some(value) = config.get("base_delay_ms") {
            let delay = value
                .as_u64()
                .ok_or_else(|| anyhow!("base_delay_ms must be a positive integer"))?;
            if delay == 0 {
                return Err(anyhow!("base_delay_ms must be greater than 0"));
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
            return Err(anyhow!(
                "Retry/Backoff + Idempotency Guard requires one input plan"
            ));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Default)]
pub struct CheckpointResumeControllerNode {
    checkpoints: HashMap<String, JsonValue>,
    sequence: u64,
}

impl CheckpointResumeControllerNode {
    pub fn save_checkpoint(&mut self, run_id: &str, snapshot: JsonValue) -> Result<String> {
        self.sequence = self.sequence.saturating_add(1);
        let token = format!("ckpt::{}::{}", run_id, self.sequence);
        self.checkpoints.insert(token.clone(), snapshot);
        Ok(token)
    }

    pub fn resume_checkpoint(&self, token: &str) -> Result<JsonValue> {
        self.checkpoints
            .get(token)
            .cloned()
            .ok_or_else(|| anyhow!("checkpoint token '{}' not found", token))
    }
}

#[async_trait]
impl NodeBehavior for CheckpointResumeControllerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.checkpoint_resume_controller".to_string(),
            display_name: "Checkpoint/Resume Controller".to_string(),
            description: "[SHELL NODE — no runtime behavior] Save and recover deterministic run checkpoints.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for checkpoint state".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with checkpoint metadata".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "token_ttl_seconds": { "type": "integer", "minimum": 1, "default": 3600 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("token_ttl_seconds") {
            let ttl = value
                .as_u64()
                .ok_or_else(|| anyhow!("token_ttl_seconds must be a positive integer"))?;
            if ttl == 0 {
                return Err(anyhow!("token_ttl_seconds must be greater than 0"));
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
            return Err(anyhow!(
                "Checkpoint/Resume Controller requires one input plan"
            ));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Default)]
pub struct RunStateStoreNode {
    state_by_run: HashMap<String, HashMap<String, JsonValue>>,
}

impl RunStateStoreNode {
    pub fn put_state(&mut self, run_id: &str, key: &str, value: JsonValue) -> Result<()> {
        let run_state = self.state_by_run.entry(run_id.to_string()).or_default();
        run_state.insert(key.to_string(), value);
        Ok(())
    }

    pub fn get_state(&self, run_id: &str, key: &str) -> Result<Option<JsonValue>> {
        Ok(self
            .state_by_run
            .get(run_id)
            .and_then(|run_state| run_state.get(key))
            .cloned())
    }

    pub fn delete_state(&mut self, run_id: &str, key: &str) -> Result<()> {
        if let Some(run_state) = self.state_by_run.get_mut(run_id) {
            run_state.remove(key);
            if run_state.is_empty() {
                self.state_by_run.remove(run_id);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl NodeBehavior for RunStateStoreNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.run_state_store".to_string(),
            display_name: "Run-State Store".to_string(),
            description: "[SHELL NODE — no runtime behavior] Persist deterministic run-scoped key-value state.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for state updates".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with state references".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_entries_per_run": { "type": "integer", "minimum": 1, "default": 1024 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("max_entries_per_run") {
            let max_entries = value
                .as_u64()
                .ok_or_else(|| anyhow!("max_entries_per_run must be a positive integer"))?;
            if max_entries == 0 {
                return Err(anyhow!("max_entries_per_run must be greater than 0"));
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
            return Err(anyhow!("Run-State Store requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditEvent {
    pub run_id: String,
    pub level: String,
    pub message: String,
    pub sequence: u64,
}

#[derive(Default)]
pub struct ObservabilityAuditLedgerNode {
    events: Vec<AuditEvent>,
    sequence: u64,
}

impl ObservabilityAuditLedgerNode {
    pub fn record_event(&mut self, run_id: &str, level: &str, message: &str) -> Result<()> {
        self.sequence = self.sequence.saturating_add(1);
        self.events.push(AuditEvent {
            run_id: run_id.to_string(),
            level: level.to_string(),
            message: message.to_string(),
            sequence: self.sequence,
        });
        Ok(())
    }

    pub fn query_events(&self, run_id: Option<&str>, level: Option<&str>) -> Vec<AuditEvent> {
        self.events
            .iter()
            .filter(|event| run_id.is_none_or(|v| event.run_id == v))
            .filter(|event| level.is_none_or(|v| event.level == v))
            .cloned()
            .collect()
    }
}

#[async_trait]
impl NodeBehavior for ObservabilityAuditLedgerNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.observability_audit_ledger".to_string(),
            display_name: "Observability & Audit Ledger".to_string(),
            description: "[SHELL NODE — no runtime behavior] Record append-only audit events for run observability.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for audit logging".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with audit metadata".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_events": { "type": "integer", "minimum": 1, "default": 10000 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("max_events") {
            let max_events = value
                .as_u64()
                .ok_or_else(|| anyhow!("max_events must be a positive integer"))?;
            if max_events == 0 {
                return Err(anyhow!("max_events must be greater than 0"));
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
            return Err(anyhow!(
                "Observability & Audit Ledger requires one input plan"
            ));
        }
        Ok(vec![inputs[0].clone()])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LoopReport {
    pub goal: String,
    pub iterations: usize,
    pub steps: Vec<String>,
    pub completed: bool,
}

pub struct AgentLoopOrchestratorNode;

impl AgentLoopOrchestratorNode {
    pub fn orchestrate<F>(
        &self,
        goal: &str,
        config: &JsonValue,
        mut step_fn: F,
    ) -> Result<LoopReport>
    where
        F: FnMut(usize) -> String,
    {
        self.validate(config)?;

        let max_iterations = config["max_iterations"].as_u64().unwrap_or(3) as usize;
        let mut steps = Vec::with_capacity(max_iterations);
        for iteration in 1..=max_iterations {
            steps.push(step_fn(iteration));
        }

        Ok(LoopReport {
            goal: goal.to_string(),
            iterations: max_iterations,
            steps,
            completed: true,
        })
    }
}

#[async_trait]
impl NodeBehavior for AgentLoopOrchestratorNode {
    fn spec(&self) -> NodeSpec {
        NodeSpec {
            type_id: "core.ai.agent_loop_orchestrator".to_string(),
            display_name: "Agent Loop Orchestrator".to_string(),
            description: "[SHELL NODE — no runtime behavior] Coordinate bounded iterative agent loop execution.".to_string(),
            input_ports: vec![PortSpec {
                id: 0,
                name: "input".to_string(),
                description: "Input plan for loop orchestration".to_string(),
            }],
            output_ports: vec![PortSpec {
                id: 0,
                name: "output".to_string(),
                description: "Output plan with loop report".to_string(),
            }],
            config_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "max_iterations": { "type": "integer", "minimum": 1, "default": 3 }
                }
            }),
        }
    }

    fn validate(&self, config: &JsonValue) -> Result<()> {
        if let Some(value) = config.get("max_iterations") {
            let max_iterations = value
                .as_u64()
                .ok_or_else(|| anyhow!("max_iterations must be a positive integer"))?;
            if max_iterations == 0 {
                return Err(anyhow!("max_iterations must be greater than 0"));
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
            return Err(anyhow!("Agent Loop Orchestrator requires one input plan"));
        }
        Ok(vec![inputs[0].clone()])
    }
}
