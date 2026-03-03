use anyhow::Result;
use async_trait::async_trait;
use rustflow::nodes::ai::mcp_client::{McpClient, McpTransport};
use serde_json::{json, Value as JsonValue};
use tokio::sync::mpsc;

// Better mock that can simulate responses
struct StatefulMockTransport {
    outgoing: mpsc::UnboundedSender<JsonValue>,
    incoming: Mutex<mpsc::UnboundedReceiver<JsonValue>>,
}
use tokio::sync::Mutex;

#[async_trait]
impl McpTransport for StatefulMockTransport {
    async fn send_message(&self, message: JsonValue) -> Result<()> {
        self.outgoing.send(message).map_err(|e| anyhow::anyhow!(e.to_string()))
    }

    async fn receive_message(&self) -> Result<JsonValue> {
        let mut rx = self.incoming.lock().await;
        rx.recv().await.ok_or_else(|| anyhow::anyhow!("Mock closed"))
    }
}

#[tokio::test]
async fn test_mcp_initialization() -> Result<()> {
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    
    let transport = Box::new(StatefulMockTransport {
        outgoing: out_tx,
        incoming: Mutex::new(in_rx),
    });
    
    let mut client = McpClient::new(transport);
    
    // Start initialization in a task
    let init_handle = tokio::spawn(async move {
        client.initialize().await
    });
    
    // 1. Expect initialize request
    let req = out_rx.recv().await.unwrap();
    assert_eq!(req["method"], "initialize");
    let id = req["id"].clone();
    
    // 2. Send initialize response
    in_tx.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "serverInfo": {"name": "test-server", "version": "1.0"}
        }
    })).unwrap();
    
    // 3. Expect notifications/initialized
    let note = out_rx.recv().await.unwrap();
    assert_eq!(note["method"], "notifications/initialized");
    
    init_handle.await.unwrap()?;
    
    Ok(())
}

#[tokio::test]
async fn test_mcp_call_tool() -> Result<()> {
    let (out_tx, mut out_rx) = mpsc::unbounded_channel();
    let (in_tx, in_rx) = mpsc::unbounded_channel();
    
    let transport = Box::new(StatefulMockTransport {
        outgoing: out_tx,
        incoming: Mutex::new(in_rx),
    });
    
    let mut client = McpClient::new(transport);
    
    let call_handle = tokio::spawn(async move {
        client.call_tool("echo", json!({"msg": "hello"})).await
    });
    
    let req = out_rx.recv().await.unwrap();
    assert_eq!(req["method"], "tools/call");
    assert_eq!(req["params"]["name"], "echo");
    let id = req["id"].clone();
    
    in_tx.send(json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": {"content": [{"type": "text", "text": "hello"}]}
    })).unwrap();
    
    let result = call_handle.await.unwrap()?;
    assert_eq!(result["content"][0]["text"], "hello");
    
    Ok(())
}
