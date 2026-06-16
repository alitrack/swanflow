pub mod protocol;

use std::sync::Arc;

use anyhow::Result;
use protocol::{
    AdapterDiagnostic, AdapterExecutionEvent, AdapterMessage, AdapterRequest, AdapterSchemaSummary,
    CancellationStatus,
};

use crate::core::engine::SwanFlowEngine;
use crate::nodes::registry::NodeRegistry;

pub async fn handle_request(request: AdapterRequest) -> Result<Vec<AdapterMessage>> {
    match request {
        AdapterRequest::Inspect {
            request_id,
            flow_revision,
        } => {
            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);
            let metadata = engine.infer_metadata(&flow_revision.workflow).await?;
            let schema_summary = metadata
                .schemas
                .into_iter()
                .map(|(node_id, ports)| AdapterSchemaSummary {
                    node_id: node_id.to_string(),
                    ports: ports
                        .into_iter()
                        .map(|schema| {
                            schema
                                .fields()
                                .iter()
                                .map(|field| {
                                    format!("{}:{:?}", field.name(), field.data_type())
                                })
                                .collect()
                        })
                        .collect(),
                })
                .collect();

            Ok(vec![AdapterMessage::InspectResult {
                request_id,
                diagnostics: Vec::<AdapterDiagnostic>::new(),
                schema_summary,
            }])
        }
        AdapterRequest::Execute {
            request_id,
            run_id,
            flow_revision,
        } => {
            let registry = Arc::new(NodeRegistry::default());
            let engine = SwanFlowEngine::new(registry);
            let mut rx = engine.execute_workflow(&flow_revision.workflow).await?;
            let mut messages = vec![AdapterMessage::ExecuteAccepted {
                request_id: request_id.clone(),
                run_id: run_id.clone(),
            }];

            while let Some(event) = rx.recv().await {
                messages.push(AdapterMessage::ExecutionEvent {
                    request_id: request_id.clone(),
                    run_id: run_id.clone(),
                    event: AdapterExecutionEvent::from_node_event(event),
                });
            }

            messages.push(AdapterMessage::ExecutionEvent {
                request_id,
                run_id,
                event: AdapterExecutionEvent::RunCompleted {
                    status: "succeeded".to_owned(),
                    terminal_output_refs: Vec::new(),
                },
            });

            Ok(messages)
        }
        AdapterRequest::Cancel { request_id, run_id } => Ok(vec![AdapterMessage::CancelResult {
            request_id,
            run_id,
            status: CancellationStatus::NotFound,
        }]),
    }
}
