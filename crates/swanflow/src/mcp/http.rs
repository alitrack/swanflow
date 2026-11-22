//! MCP streamable HTTP transport (client side).
//!
//! Targets MCP servers that speak the "streamable HTTP" transport
//! (one POST per JSON-RPC message; response is either a single
//! `application/json` body or a `text/event-stream` stream). This is the
//! transport MotherDuck's `https://api.motherduck.com/mcp` endpoint uses,
//! authenticated with `Authorization: Bearer <token>`.
//!
//! Reuses the JSON-RPC types from [`crate::mcp`] — the stdio
//! [`crate::mcp::ContentLengthTransport`] is untouched.
//!
//! Design notes (openspec 2026-11-02-motherduck-flight-connector):
//! - One connection per logical session: `initialize` once, then N tool
//!   calls. A short-lived node execution creates one client, calls, drops.
//! - `Mcp-Session-Id` from the initialize response is echoed on subsequent
//!   requests (spec: Streamable HTTP transport).
//! - SSE frames (`event:`/`data:` lines) are parsed by a hand-rolled
//!   incremental parser — no new crates.

use crate::mcp::{JsonRpcNotification, JsonRpcRequest, JsonRpcResponse};
use anyhow::{anyhow, Context as _, Result};
use serde_json::Value as JsonValue;
use std::time::Duration;

/// Incremental SSE frame parser. Feed it raw text chunks; it yields
/// completed `data:` payloads (JSON strings) as frames complete.
///
/// Handles: multi-line `data:` concatenation (per SSE spec, joined with
/// `\n`), `event:`/`id:`/`retry:` lines (ignored except `event` is kept for
/// diagnostics), comment lines starting with `:`, and `\r\n` line endings.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: String,
    /// Parsed-out data lines of the frame currently being accumulated.
    current_data: Vec<String>,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed raw bytes/chunk; returns completed frame payloads in order.
    /// A frame completes on a blank line.
    pub fn feed(&mut self, chunk: &str) -> Vec<String> {
        let mut out = Vec::new();
        self.buffer.push_str(chunk);
        // Split on newlines, but keep a trailing partial line in the buffer.
        while let Some(nl) = self.buffer.find('\n') {
            let mut line = self.buffer[..nl].to_string();
            self.buffer.drain(..=nl);
            if line.ends_with('\r') {
                line.pop();
            }
            if line.is_empty() {
                // Blank line — frame boundary.
                if !self.current_data.is_empty() {
                    out.push(self.current_data.join("\n"));
                    self.current_data.clear();
                }
            } else if let Some(data) = line.strip_prefix("data:") {
                let data = data.strip_prefix(' ').unwrap_or(data);
                self.current_data.push(data.to_string());
            }
            // `event:`/`id:`/`retry:`/comments: intentionally ignored.
        }
        out
    }

    /// Flush any trailing frame not terminated by a blank line (some servers
    /// close the stream right after the last event without a final CRLF).
    pub fn flush(&mut self) -> Vec<String> {
        let rest = std::mem::take(&mut self.buffer);
        let mut out = Vec::new();
        for line in rest.lines() {
            let line = line.strip_suffix('\r').unwrap_or(line);
            if let Some(data) = line.strip_prefix("data:") {
                let data = data.strip_prefix(' ').unwrap_or(data);
                self.current_data.push(data.to_string());
            }
        }
        if !self.current_data.is_empty() {
            out.push(self.current_data.join("\n"));
        }
        out
    }
}

/// Configuration for an HTTP MCP endpoint.
#[derive(Debug, Clone)]
pub struct HttpMcpConfig {
    /// Full endpoint URL, e.g. `https://api.motherduck.com/mcp`.
    pub endpoint: String,
    /// Bearer token sent as `Authorization: Bearer <token>`.
    pub token: String,
    /// Optional endpoint override for tests (mock server).
    pub request_timeout: Duration,
}

impl Default for HttpMcpConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://api.motherduck.com/mcp".to_string(),
            token: String::new(),
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// An MCP client over the streamable HTTP transport.
///
/// Usage:
/// ```ignore
/// let mut client = HttpMcpClient::connect(config).await?;
/// let result = client.call_tool("run_flight", json!({...})).await?;
/// ```
pub struct HttpMcpClient {
    config: HttpMcpConfig,
    http: reqwest::Client,
    session_id: Option<String>,
    protocol_version: String,
    next_id: i64,
}

impl HttpMcpClient {
    /// Build the client and perform the MCP initialize handshake.
    pub async fn connect(config: HttpMcpConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .build()
            .context("building reqwest client for MCP HTTP transport")?;
        let mut client = Self {
            config,
            http,
            session_id: None,
            protocol_version: "2025-06-18".to_string(),
            next_id: 1,
        };
        client.initialize().await?;
        Ok(client)
    }

    /// Connect reading the bearer token from `MOTHERDUCK_TOKEN`
    /// (or `MCP_HTTP_TOKEN` as a transport-generic fallback).
    pub fn token_from_env() -> Result<String> {
        std::env::var("MOTHERDUCK_TOKEN")
            .or_else(|_| std::env::var("MCP_HTTP_TOKEN"))
            .map_err(|_| anyhow!("neither MOTHERDUCK_TOKEN nor MCP_HTTP_TOKEN is set"))
    }

    async fn initialize(&mut self) -> Result<()> {
        let params = serde_json::json!({
            "protocolVersion": self.protocol_version,
            "capabilities": {},
            "clientInfo": { "name": "SwanFlow", "version": "0.1.0" }
        });
        let (status, headers, body) = self.post_jsonrpc("initialize", params).await?;
        if !status.is_success() {
            return Err(anyhow!(
                "MCP initialize failed: HTTP {} — {}",
                status,
                truncate(&body, 300)
            ));
        }
        // Capture session id header if present.
        if let Some(sid) = headers
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
        {
            self.session_id = Some(sid);
        }
        // Capture negotiated protocol version, if the server cares.
        let _ = serde_json::from_str::<JsonValue>(&body)?;
        Ok(())
    }

    /// Send the `notifications/initialized` notification (no response).
    pub async fn send_initialized_notification(&mut self) -> Result<()> {
        let notification = JsonRpcNotification {
            jsonrpc: "2.0".to_string(),
            method: "notifications/initialized".to_string(),
            params: None,
        };
        let body = serde_json::to_string(&notification)?;
        let resp = self
            .build_request(body)?
            .send()
            .await
            .context("sending notifications/initialized")?;
        // 202 Accepted is the expected status for notifications.
        if !resp.status().is_success() {
            return Err(anyhow!(
                "notifications/initialized rejected: HTTP {}",
                resp.status()
            ));
        }
        Ok(())
    }

    fn build_request(&self, body: String) -> Result<reqwest::RequestBuilder> {
        let mut req = self
            .http
            .post(&self.config.endpoint)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json, text/event-stream")
            .header("Authorization", format!("Bearer {}", self.config.token))
            .header("Mcp-Protocol-Version", &self.protocol_version);
        if let Some(sid) = &self.session_id {
            req = req.header("Mcp-Session-Id", sid);
        }
        Ok(req.body(body))
    }

    /// POST one JSON-RPC request, return (status, headers, body-or-first-data-payload).
    async fn post_jsonrpc(
        &mut self,
        method: &str,
        params: JsonValue,
    ) -> Result<(reqwest::StatusCode, reqwest::header::HeaderMap, String)> {
        let id = self.next_id;
        self.next_id += 1;
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params,
            id: JsonValue::from(id),
        };
        let body = serde_json::to_string(&request)?;
        let builder = self.build_request(body)?;
        let resp = builder
            .send()
            .await
            .with_context(|| format!("MCP HTTP request '{method}' failed"))?;
        let status = resp.status();
        let headers = resp.headers().clone();
        let content_type = headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();

        if content_type.contains("text/event-stream") {
            // Stream: read the (finite) body, then walk SSE frames until we
            // find a JSON-RPC response (has "id" and a result/error); skip
            // ping/notifications. Note: we buffer the whole SSE body rather
            // than incrementally streaming — a POST response body is small
            // and bounded, and this avoids the `stream` reqwest feature.
            let bytes = resp
                .bytes()
                .await
                .context("reading MCP SSE response body")?;
            let text = String::from_utf8_lossy(&bytes);
            let mut parser = SseParser::new();
            for payload in parser.feed(&text) {
                if let Some(s) = Self::extract_response(&payload, id) {
                    return Ok((status, headers, s));
                }
            }
            for payload in parser.flush() {
                if let Some(s) = Self::extract_response(&payload, id) {
                    return Ok((status, headers, s));
                }
            }
            Err(anyhow!(
                "MCP SSE stream ended without a response for '{method}'"
            ))
        } else {
            // Single JSON body.
            let text = resp.text().await.context("reading MCP JSON response")?;
            Ok((status, headers, text))
        }
    }

    /// If `payload` is a JSON-RPC response matching `id`, return the raw
    /// string of the response body (caller re-parses). Also records errors.
    fn extract_response(payload: &str, id: i64) -> Option<String> {
        let val: JsonValue = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(_) => return None, // ping or non-JSON event — skip
        };
        let resp_id = val.get("id").and_then(|v| v.as_i64());
        if resp_id == Some(id) {
            return Some(payload.to_string());
        }
        None
    }

    /// Send a JSON-RPC request and return the `result` of the matching
    /// response.
    pub async fn call_internal(&mut self, method: &str, params: JsonValue) -> Result<JsonValue> {
        let (status, _headers, body) = self.post_jsonrpc(method, params).await?;
        if !status.is_success() {
            return Err(anyhow!(
                "MCP HTTP error: HTTP {status} — {}",
                truncate(&body, 300)
            ));
        }
        let response: JsonRpcResponse = serde_json::from_str(&body).with_context(|| {
            format!(
                "parsing JSON-RPC response for '{method}': {}",
                truncate(&body, 300)
            )
        })?;
        if let Some(err) = response.error {
            return Err(anyhow!("MCP error: {err}"));
        }
        Ok(response.result.unwrap_or(JsonValue::Null))
    }

    /// Call a tool by name with the given arguments.
    pub async fn call_tool(&mut self, name: &str, arguments: JsonValue) -> Result<JsonValue> {
        let params = serde_json::json!({ "name": name, "arguments": arguments });
        self.call_internal("tools/call", params).await
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_single_frame() {
        let mut p = SseParser::new();
        let frames = p.feed("event: message\ndata: {\"jsonrpc\":\"2.0\"}\n\n");
        assert_eq!(frames, vec!["{\"jsonrpc\":\"2.0\"}".to_string()]);
    }

    #[test]
    fn sse_split_chunks_half_packet() {
        let mut p = SseParser::new();
        assert!(p.feed("data: {\"a\"").is_empty()); // partial line
        let frames = p.feed(":1}\n\n");
        assert_eq!(frames, vec!["{\"a\":1}".to_string()]);
    }

    #[test]
    fn sse_multiple_events_one_chunk() {
        let mut p = SseParser::new();
        let frames = p.feed("data: one\n\ndata: two\n\n");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0], "one");
        assert_eq!(frames[1], "two");
    }

    #[test]
    fn sse_multiline_data_joined_with_newline() {
        let mut p = SseParser::new();
        let frames = p.feed("data: line1\ndata: line2\n\n");
        assert_eq!(frames, vec!["line1\nline2".to_string()]);
    }

    #[test]
    fn sse_crlf_endings() {
        let mut p = SseParser::new();
        let frames = p.feed("data: x\r\n\r\n");
        assert_eq!(frames, vec!["x".to_string()]);
    }

    #[test]
    fn sse_comment_ignored_and_ping_yields_payload() {
        let mut p = SseParser::new();
        // Comments (":" prefix) never yield anything.
        assert!(p.feed(": keepalive\n\n").is_empty());
        // The parser is event-type agnostic: a ping frame still yields its
        // data payload — filtering non-responses happens downstream in
        // `extract_response` (id matching), not here.
        let frames = p.feed("event: ping\ndata: {}\n\n");
        assert_eq!(frames, vec!["{}".to_string()]);
        let frames = p.feed("data: real\n\n");
        assert_eq!(frames, vec!["real".to_string()]);
    }

    #[test]
    fn sse_flush_without_trailing_blank_line() {
        let mut p = SseParser::new();
        assert!(p.feed("data: tail").is_empty());
        assert_eq!(p.flush(), vec!["tail".to_string()]);
    }

    #[test]
    fn extract_response_matches_id_only() {
        assert!(HttpMcpClient::extract_response("{\"id\":1,\"result\":{}}", 1).is_some());
        assert!(HttpMcpClient::extract_response("{\"id\":2,\"result\":{}}", 1).is_none());
        assert!(HttpMcpClient::extract_response("{\"method\":\"ping\"}", 1).is_none());
        assert!(HttpMcpClient::extract_response("not json", 1).is_none());
    }
}
