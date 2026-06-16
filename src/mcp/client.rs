//! MCP Client — connect to external MCP servers and call their tools.

use crate::mcp::{
    ContentLengthTransport, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, McpTool,
};
use serde_json::Value as JsonValue;
use std::process::Stdio;
use tokio::process::{Child, Command};

/// A client for connecting to external MCP servers over stdio.
///
/// # Example
/// ```ignore
/// let mut client = McpClient::connect("python", &["mcp_server.py"]).await?;
/// let tools = client.list_tools().await?;
/// let result = client.call_tool("my_tool", json!({"arg": 1})).await?;
/// client.close().await?;
/// ```
pub struct McpClient {
    process: Option<Child>,
    transport: ContentLengthTransport<tokio::process::ChildStdout, tokio::process::ChildStdin>,
    next_id: i64,
}

impl McpClient {
    /// Spawn an external MCP server process and connect to it.
    pub async fn connect(command: &str, args: &[String]) -> anyhow::Result<Self> {
        let mut child = Command::new(command)
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to open stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("Failed to open stdout"))?;

        let transport = ContentLengthTransport::new(stdout, stdin);

        let mut client = Self {
            process: Some(child),
            transport,
            next_id: 1,
        };

        // Perform MCP initialization handshake
        client.initialize().await?;

        Ok(client)
    }

    /// Perform the MCP initialization handshake.
    async fn initialize(&mut self) -> anyhow::Result<()> {
        let params = serde_json::json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "SwanFlow", "version": "0.1.0" }
        });
        self.call_internal("initialize", params).await?;

        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        let body = serde_json::to_string(&notification)?;
        self.transport.write_message(&body).await?;

        Ok(())
    }

    /// Send a JSON-RPC request and wait for the matching response.
    async fn call_internal(
        &mut self,
        method: &str,
        params: JsonValue,
    ) -> anyhow::Result<JsonValue> {
        let id = self.next_id;
        self.next_id += 1;

        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: JsonValue::from(id),
        };

        let body = serde_json::to_string(&request)?;
        self.transport.write_message(&body).await?;

        loop {
            let body = self.transport.read_message().await?;

            // Try as a response
            if let Ok(response) = serde_json::from_str::<JsonRpcResponse>(&body) {
                if response.id.as_i64() == Some(id) {
                    if let Some(err) = response.error {
                        return Err(anyhow::anyhow!("MCP error: {}", err));
                    }
                    return Ok(response.result.unwrap_or(JsonValue::Null));
                }
            }
            // Ignore notifications (no id) or responses for other requests
        }
    }

    /// List all tools available from the MCP server.
    pub async fn list_tools(&mut self) -> anyhow::Result<Vec<McpTool>> {
        let result = self.call_internal("tools/list", JsonValue::Null).await?;
        let tools: Vec<McpTool> = serde_json::from_value(result["tools"].clone())
            .map_err(|e| anyhow::anyhow!("Failed to parse tools list: {e}"))?;
        Ok(tools)
    }

    /// Call a tool by name with the given arguments.
    pub async fn call_tool(
        &mut self,
        name: &str,
        arguments: JsonValue,
    ) -> anyhow::Result<JsonValue> {
        let params = serde_json::json!({
            "name": name,
            "arguments": arguments,
        });
        self.call_internal("tools/call", params).await
    }

    /// Close the connection and kill the child process.
    pub async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(mut child) = self.process.take() {
            child.kill().await.ok();
            child.wait().await.ok();
        }
        Ok(())
    }
}

impl Drop for McpClient {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            let _ = child.start_kill();
        }
    }
}
