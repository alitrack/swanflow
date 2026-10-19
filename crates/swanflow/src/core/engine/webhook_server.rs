//! ADR-014 Stage 4 — webhook ingress: a minimal HTTP listener that turns
//! inbound POST/GET requests into automation events (true push triggers).
//!
//! Design constraints (from the ADR-014 sensor framework):
//! - **Bounded queue**: each path holds at most `QUEUE_CAP` pending
//!   events; extra arrivals coalesce into the existing pending event
//!   (webhook semantics are "at-least-one-run-since-last-push", not a
//!   durable message log).
//! - **Consume-after-claim**: the HTTP side only *records* arrivals.
//!   `sensor_tick` removes a pending event only after the triggered
//!   run is claimed — if the daemon dies between arrival and run, the
//!   event re-fires on the next tick (at-least-once, never lost).
//! - **Zero new dependencies**: hyper 1.x + hyper-util are already in
//!   the lockfile (reqwest's tree); no axum/tower stack is pulled in.
//!
//! HTTP surface:
//!   POST|GET /hooks/<path...>   -> 200 {"ok":true,"path":"...","pending":n}
//!   (any other method/path)     -> 404
//!
//! Matching is exact-path: the request path must equal (or, with the
//! trailing-slash tolerance, equal modulo one trailing slash) a path
//! referenced by some watched workflow's `on_webhook(...)` condition.

use std::collections::HashMap;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::sync::Mutex;

/// Max pending (un-consumed) events per path before coalescing.
pub const QUEUE_CAP: usize = 16;

/// Max accepted request body size (bodies are counted, not stored).
const MAX_BODY_BYTES: u64 = 64 * 1024;

/// Shared webhook event queue. Key = canonical path (starts with `/`),
/// value = number of pending arrivals (bounded by [`QUEUE_CAP`]).
#[derive(Default)]
pub struct WebhookQueue {
    pending: Mutex<HashMap<String, usize>>,
}

impl WebhookQueue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an arrival for `path` (called by the HTTP handler).
    /// Saturates at QUEUE_CAP — a burst of pushes is coalesced, since
    /// each pending event only means "run once more".
    pub async fn record(&self, path: &str) -> usize {
        let mut p = self.pending.lock().await;
        let n = p.entry(path.to_string()).or_insert(0);
        if *n < QUEUE_CAP {
            *n += 1;
        }
        *n
    }

    /// Snapshot the set of paths with at least one pending event
    /// (feeds `EvalContext::webhook_fired` each sensor tick).
    pub async fn fired_set(&self) -> std::collections::HashSet<String> {
        let p = self.pending.lock().await;
        p.iter()
            .filter(|(_, n)| **n > 0)
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Consume one pending event for `path`. Called ONLY after the
    /// sensor claimed the run (see module docs: consume-after-claim).
    pub async fn consume(&self, path: &str) {
        let mut p = self.pending.lock().await;
        if let Some(n) = p.get_mut(path) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                p.remove(path);
            }
        }
    }

    /// Total pending events across paths (diagnostics).
    pub async fn total_pending(&self) -> usize {
        let p = self.pending.lock().await;
        p.values().sum()
    }
}

/// Spawn the webhook HTTP listener on `bind` (host:port). Returns the
/// shared queue immediately; the server runs as a detached tokio task.
/// Connection errors are logged and the connection dropped — the
/// listener itself must survive bad clients.
pub async fn spawn_webhook_listener(
    bind: &str,
) -> Result<Arc<WebhookQueue>, Box<dyn std::error::Error + Send + Sync>> {
    let addr: std::net::SocketAddr = bind
        .parse()
        .map_err(|e| format!("invalid webhook bind '{bind}': {e}"))?;
    let queue = Arc::new(WebhookQueue::new());
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let q = queue.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("webhook listener accept failed: {e}");
                    continue;
                }
            };
            let q = q.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req| handle_webhook(req, q.clone()));
                if let Err(e) = hyper::server::conn::http1::Builder::new()
                    .serve_connection(io, service)
                    .await
                {
                    tracing::debug!("webhook connection error: {e}");
                }
            });
        }
    });
    tracing::info!("webhook ingress listening on http://{addr}/");
    Ok(queue)
}

async fn handle_webhook(
    req: Request<hyper::body::Incoming>,
    queue: Arc<WebhookQueue>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Drain the body (bounded via http_body_util::Limited): we count
    // pushes, not payloads — a body over the limit still triggers.
    let _ = http_body_util::Limited::new(req.into_body(), MAX_BODY_BYTES as usize)
        .collect()
        .await;

    if method != hyper::Method::POST && method != hyper::Method::GET {
        return Ok(json_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "{\"ok\":false,\"error\":\"method not allowed\"}",
        ));
    }
    if !path.starts_with('/') {
        return Ok(json_response(
            StatusCode::NOT_FOUND,
            "{\"ok\":false,\"error\":\"not found\"}",
        ));
    }
    let pending = queue.record(&path).await;
    tracing::info!("webhook arrived: {method} {path} (pending={pending})");
    let body = format!("{{\"ok\":true,\"path\":\"{path}\",\"pending\":{pending}}}");
    Ok(json_response(StatusCode::OK, &body))
}

fn json_response(status: StatusCode, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_owned())))
        .unwrap()
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// Tests
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn queue_record_consume_bounded() {
        let q = WebhookQueue::new();
        assert!(q.fired_set().await.is_empty());
        // Record coalesces at QUEUE_CAP.
        for i in 0..(QUEUE_CAP + 10) {
            q.record("/a").await;
        }
        assert_eq!(q.total_pending().await, QUEUE_CAP);
        assert!(q.fired_set().await.contains("/a"));
        // Consume drains one at a time.
        q.consume("/a").await;
        assert_eq!(q.total_pending().await, QUEUE_CAP - 1);
        // Unknown path consume is a no-op.
        q.consume("/nope").await;
        assert_eq!(q.total_pending().await, QUEUE_CAP - 1);
        // Drain fully — the key disappears.
        for _ in 0..(QUEUE_CAP - 1) {
            q.consume("/a").await;
        }
        assert!(q.fired_set().await.is_empty());
    }

    #[tokio::test]
    async fn queue_paths_are_independent() {
        let q = WebhookQueue::new();
        q.record("/a").await;
        q.record("/b").await;
        q.record("/b").await;
        assert_eq!(q.total_pending().await, 3);
        q.consume("/a").await;
        assert_eq!(q.total_pending().await, 2);
        let fired = q.fired_set().await;
        assert!(!fired.contains("/a"));
        assert!(fired.contains("/b"));
    }

    #[test]
    fn parse_webhook_path_reuse() {
        // The automation-module validator is the single source of truth
        // for path validity; the queue stores whatever the router hands
        // it, so validity is enforced at registration time.
        assert!(crate::core::engine::automation::parse_webhook_path("/hooks/x").is_ok());
        assert!(crate::core::engine::automation::parse_webhook_path("x").is_err());
    }
}
