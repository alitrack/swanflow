use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tokio::process::{Command, Child};
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};
use futures::{SinkExt, StreamExt};
use std::process::Stdio;

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: JsonValue,
    pub id: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub result: Option<JsonValue>,
    pub error: Option<JsonValue>,
    pub id: JsonValue,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    pub params: Option<JsonValue>,
}

#[async_trait]
pub trait McpTransport: Send + Sync {
    async fn send_message(&self, message: JsonValue) -> Result<()>;
    async fn receive_message(&self) -> Result<JsonValue>;
}

pub struct StdioTransport {
    writer: Mutex<FramedWrite<tokio::process::ChildStdin, LinesCodec>>,
    reader: Mutex<FramedRead<tokio::process::ChildStdout, LinesCodec>>,
    _child: Child,
}

impl StdioTransport {
    pub fn spawn(command: &str, args: &[String]) -> Result<Self> {
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or_else(|| anyhow!("Failed to open stdin"))?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow!("Failed to open stdout"))?;

        let writer = FramedWrite::new(stdin, LinesCodec::new());
        let reader = FramedRead::new(stdout, LinesCodec::new());

        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            _child: child,
        })
    }
}

#[async_trait]
impl McpTransport for StdioTransport {
    async fn send_message(&self, message: JsonValue) -> Result<()> {
        let line = serde_json::to_string(&message)?;
        let mut writer = self.writer.lock().await;
        writer.send(line).await?;
        Ok(())
    }

    async fn receive_message(&self) -> Result<JsonValue> {
        let mut reader = self.reader.lock().await;
        if let Some(line) = reader.next().await {
            let line: String = line?;
            Ok(serde_json::from_str(&line)?)
        } else {
            Err(anyhow!("Stream closed"))
        }
    }
}

pub struct McpClient {
    transport: Box<dyn McpTransport>,
    next_id: i64,
}

impl McpClient {
    pub fn new(transport: Box<dyn McpTransport>) -> Self {
        Self {
            transport,
            next_id: 1,
        }
    }

    pub async fn call(&mut self, method: &str, params: JsonValue) -> Result<JsonValue> {
        let id = self.next_id;
        self.next_id += 1;

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: JsonValue::from(id),
        };

        self.transport.send_message(serde_json::to_value(request)?).await?;
        
        loop {
            let message = self.transport.receive_message().await?;
            if let Ok(response) = serde_json::from_value::<JsonRpcResponse>(message.clone()) {
                if response.id == JsonValue::from(id) {
                    if let Some(error) = response.error {
                        return Err(anyhow!("MCP Error: {:?}", error));
                    }
                    return Ok(response.result.unwrap_or(JsonValue::Null));
                }
            }
            // Ignore notifications and unrelated responses for now
        }
    }

    pub async fn initialize(&mut self) -> Result<()> {
        let params = serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {
                "name": "SwanFlow",
                "version": "0.1.0"
            }
        });

        self.call("initialize", params).await?;
        
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        self.transport.send_message(serde_json::to_value(notification)?).await?;
        
        Ok(())
    }

    pub async fn call_tool(&mut self, tool_name: &str, arguments: JsonValue) -> Result<JsonValue> {
        let params = serde_json::json!({
            "name": tool_name,
            "arguments": arguments
        });
        self.call("tools/call", params).await
    }
}
