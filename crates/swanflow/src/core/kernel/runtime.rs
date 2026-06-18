//! Runtime lifecycle kernel traits — CancellationToken, Timeout, Progress.
//!
//! These traits abstract the runtime environment's lifecycle concerns:
//! cooperative cancellation, time-bounded execution, and progress reporting.

use async_trait::async_trait;

use crate::core::error::CoreResult;

// ---------------------------------------------------------------------------
// CancellationToken
// ---------------------------------------------------------------------------

/// Cooperative cancellation — check if the current operation has been cancelled.
///
/// Follows Nika's nika-kernel-runtime pattern: a lightweight trait that
/// long-running operations check periodically. Implementation wraps
/// tokio_util::sync::CancellationToken or a simple atomic flag.
#[async_trait]
pub trait CancellationToken: Send + Sync {
    /// Returns true if cancellation has been requested.
    fn is_cancelled(&self) -> bool;

    /// Wait until cancellation is requested (returns immediately if already cancelled).
    async fn wait_cancelled(&self);

    /// Request cancellation.
    fn cancel(&self);
}

// ---------------------------------------------------------------------------
// Timeout
// ---------------------------------------------------------------------------

/// Time-bounded execution — apply a deadline to an async operation.
#[async_trait]
pub trait Timeout: Send + Sync {
    /// Execute a future with a deadline. Returns `Err` if the deadline is exceeded.
    async fn with_timeout<F, T>(&self, deadline_secs: f64, future: F) -> CoreResult<T>
    where
        F: std::future::Future<Output = CoreResult<T>> + Send;

    /// Get the remaining time before the deadline (in seconds). Returns 0 if exceeded.
    fn remaining_secs(&self) -> f64;
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

/// Progress reporting — report progress of a long-running operation.
#[async_trait]
pub trait Progress: Send + Sync {
    /// Report progress (0.0 = just started, 1.0 = complete).
    async fn report(&self, fraction: f64, message: Option<&str>);

    /// Get the last reported progress.
    fn last_reported(&self) -> Option<(f64, String)>;

    /// Mark as complete with an optional final message.
    async fn complete(&self, message: Option<&str>);
}
