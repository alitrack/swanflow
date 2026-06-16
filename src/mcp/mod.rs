//! MCP (Model Context Protocol) — JSON-RPC 2.0 over stdio.
//!
//! This module provides the protocol types, a Content-Length–delimited transport,
//! and both client and server implementations.
//!
//! ## MCP over stdio wire format
//! ```text
//! Content-Length: 123\r\n
//! \r\n
//! {"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::fmt;

// ---------------------------------------------------------------------------
// JSON-RPC 2.0 types
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default = "default_params")]
    pub params: JsonValue,
    #[serde(default = "default_id")]
    pub id: JsonValue,
}

fn default_params() -> JsonValue {
    JsonValue::Null
}
fn default_id() -> JsonValue {
    JsonValue::Null
}

/// JSON-RPC 2.0 response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: JsonValue,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
}

impl std::fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MCP error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for JsonRpcError {}

/// JSON-RPC 2.0 notification (no `id` field).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcNotification {
    pub jsonrpc: String,
    pub method: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub params: Option<JsonValue>,
}

// ---------------------------------------------------------------------------
// MCP domain types
// ---------------------------------------------------------------------------

/// MCP tool definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: JsonValue,
}

/// MCP resource definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResource {
    pub uri: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

/// MCP resource contents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResourceContents {
    pub uri: String,
    pub mime_type: String,
    pub text: String,
}

// ---------------------------------------------------------------------------
// Content-Length transport
// ---------------------------------------------------------------------------

/// Read-write transport for MCP over stdio with Content-Length framing.
pub struct ContentLengthTransport<R, W> {
    reader: tokio::io::BufReader<R>,
    writer: tokio::io::BufWriter<W>,
}

impl<R: tokio::io::AsyncRead + Unpin, W: tokio::io::AsyncWrite + Unpin> ContentLengthTransport<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self {
            reader: tokio::io::BufReader::new(reader),
            writer: tokio::io::BufWriter::new(writer),
        }
    }

    /// Read one JSON-RPC message (Content-Length framed).
    pub async fn read_message(&mut self) -> std::io::Result<String> {
        use tokio::io::AsyncBufReadExt;

        let mut header_line = String::new();
        let content_length = loop {
            header_line.clear();
            let n = self.reader.read_line(&mut header_line).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed",
                ));
            }
            let trimmed = header_line.trim();
            if trimmed.is_empty() {
                // End of headers — blank line
                break 0u64; // placeholder, will be set below
            }
            if let Some(len_str) = trimmed
                .strip_prefix("Content-Length:")
                .or_else(|| trimmed.strip_prefix("content-length:"))
            {
                let len = len_str.trim().parse::<u64>().map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid Content-Length: {e}"),
                    )
                })?;
                break len;
            }
            // Ignore other headers
        };

        if content_length == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing Content-Length header",
            ));
        }

        let mut body = vec![0u8; content_length as usize];
        use tokio::io::AsyncReadExt;
        self.reader.read_exact(&mut body).await?;

        String::from_utf8(body).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid UTF-8: {e}"))
        })
    }

    /// Write one JSON-RPC message (Content-Length framed).
    pub async fn write_message(&mut self, body: &str) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;

        let header = format!("Content-Length: {}\r\n\r\n", body.len());
        self.writer.write_all(header.as_bytes()).await?;
        self.writer.write_all(body.as_bytes()).await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Read and deserialize a JSON-RPC request.
    pub async fn read_request(&mut self) -> std::io::Result<JsonRpcRequest> {
        let body = self.read_message().await?;
        serde_json::from_str(&body).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid JSON-RPC request: {e}"),
            )
        })
    }

    /// Serialize and write a JSON-RPC response.
    pub async fn write_response(&mut self, response: &JsonRpcResponse) -> std::io::Result<()> {
        let body = serde_json::to_string(response).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Serialization error: {e}"),
            )
        })?;
        self.write_message(&body).await
    }
}

// ---------------------------------------------------------------------------
// Helper: build a success / error JSON-RPC response
// ---------------------------------------------------------------------------

/// Build a successful JSON-RPC response.
pub fn success_response(id: JsonValue, result: JsonValue) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: Some(result),
        error: None,
        id,
    }
}

/// Build an error JSON-RPC response.
pub fn error_response(id: JsonValue, code: i64, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
            data: None,
        }),
        id,
    }
}

pub mod client;
pub mod server;
