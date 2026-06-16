use swanflow::adapter::protocol::{
    AdapterExecutionEvent, AdapterMessage, AdapterRequest, CancellationStatus,
};
use swanflow::core::graph::Workflow;
use swanflow::nodes::{NodeEvent, NodeState};
use uuid::Uuid;

#[test]
fn deserializes_structured_machine_requests() {
    let workflow = Workflow::new("Adapter Test");
    let request_json = serde_json::json!({
        "op": "inspect",
        "request_id": "req-001",
        "flow_revision": {
            "workflow_id": "customers-clean",
            "revision_id": "rev-001",
            "workflow": workflow,
            "artifact_refs": ["artifact://dsl"]
        }
    });

    let request: AdapterRequest = serde_json::from_value(request_json).unwrap();

    match request {
        AdapterRequest::Inspect {
            request_id,
            flow_revision,
        } => {
            assert_eq!(request_id, "req-001");
            assert_eq!(flow_revision.workflow_id, "customers-clean");
            assert_eq!(flow_revision.revision_id, "rev-001");
            assert_eq!(flow_revision.artifact_refs, vec!["artifact://dsl"]);
        }
        other => panic!("expected inspect request, got {other:?}"),
    }
}

#[test]
fn converts_node_events_to_structured_execution_events() {
    let node_id = Uuid::new_v4();
    let event = AdapterExecutionEvent::from_node_event(NodeEvent::StateChanged {
        node_id,
        state: NodeState::Running,
    });
    let payload = serde_json::to_value(event).unwrap();

    assert_eq!(payload["kind"], "node_state_changed");
    assert_eq!(payload["node_id"], node_id.to_string());
    assert_eq!(payload["state"], "running");
}

#[test]
fn serializes_completed_message_with_terminal_output_refs() {
    let message = AdapterMessage::ExecutionEvent {
        request_id: "req-002".to_owned(),
        run_id: "run-002".to_owned(),
        event: AdapterExecutionEvent::RunCompleted {
            status: "succeeded".to_owned(),
            terminal_output_refs: vec![
                "parquet://outputs/customers.parquet".to_owned(),
                "excel://outputs/customers.xlsx".to_owned(),
            ],
        },
    };

    let payload = serde_json::to_value(message).unwrap();

    assert_eq!(payload["type"], "execution_event");
    assert_eq!(payload["event"]["kind"], "run_completed");
    assert_eq!(
        payload["event"]["terminal_output_refs"],
        serde_json::json!([
            "parquet://outputs/customers.parquet",
            "excel://outputs/customers.xlsx"
        ])
    );
}

#[test]
fn serializes_structured_cancel_result() {
    let message = AdapterMessage::CancelResult {
        request_id: "req-003".to_owned(),
        run_id: "run-003".to_owned(),
        status: CancellationStatus::Accepted,
    };

    let payload = serde_json::to_value(message).unwrap();

    assert_eq!(payload["type"], "cancel_result");
    assert_eq!(payload["status"], "accepted");
}
