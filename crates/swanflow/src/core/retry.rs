//! Shared retry/backoff policy — single source of truth.
//!
//! Before this module existed there were three hand-rolled copies with
//! divergent behaviour: the YAML-path `ErrorMode::Retry` loop retried
//! immediately with no delay at all, the staged path used a hardcoded
//! 500 ms sleep, and the reranker's model-backend calls computed
//! `100ms * 2^min(attempt, 4)` inline (twice). `RetryBackoffGuardNode`
//! could compute a proper schedule but nothing in the engine ever called
//! it. Everything now funnels through the helpers below.

pub const DEFAULT_MAX_RETRIES: u64 = 3;
pub const DEFAULT_BASE_DELAY_MS: u64 = 100;
/// Cap the exponent so a single delay never exceeds base * 2^4 (16x).
const BACKOFF_CAP_EXPONENT: u32 = 4;

/// Exponential backoff delay for `attempt` (0-based), in milliseconds.
pub fn backoff_delay_ms(base_delay_ms: u64, attempt: u64) -> u64 {
    base_delay_ms.saturating_mul(2u64.saturating_pow((attempt as u32).min(BACKOFF_CAP_EXPONENT)))
}

/// Full delay schedule covering `max_retries` retries (delays before each
/// retry, not before the first attempt).
pub fn backoff_schedule(base_delay_ms: u64, max_retries: u64) -> Vec<u64> {
    (0..max_retries)
        .map(|n| backoff_delay_ms(base_delay_ms, n))
        .collect()
}

/// Run an async operation with exponential backoff.
///
/// `op` is attempted up to `max_retries + 1` times (first attempt + retries).
/// `on_retry(attempt_0_based, delay_ms, &err)` fires before each backoff
/// sleep so callers can log or emit events.
pub async fn retry_with_backoff<F, Fut, T, E>(
    max_retries: u64,
    base_delay_ms: u64,
    mut op: F,
    mut on_retry: impl FnMut(u64, u64, &E),
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
{
    let mut last_err: Option<E> = None;
    for attempt in 0..=max_retries {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt < max_retries {
                    let delay = backoff_delay_ms(base_delay_ms, attempt);
                    on_retry(attempt, delay, &err);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                last_err = Some(err);
            }
        }
    }
    Err(last_err.expect("retry loop executes at least once"))
}

/// Blocking twin of [`retry_with_backoff`] for synchronous node code
/// (e.g. reranker model-backend calls inside `propose_plan`).
pub fn retry_with_backoff_blocking<F, T, E>(
    max_retries: u64,
    base_delay_ms: u64,
    mut op: F,
    mut on_retry: impl FnMut(u64, u64, &E),
) -> std::result::Result<T, E>
where
    F: FnMut() -> std::result::Result<T, E>,
{
    let mut last_err: Option<E> = None;
    for attempt in 0..=max_retries {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if attempt < max_retries {
                    let delay = backoff_delay_ms(base_delay_ms, attempt);
                    on_retry(attempt, delay, &err);
                    std::thread::sleep(std::time::Duration::from_millis(delay));
                }
                last_err = Some(err);
            }
        }
    }
    Err(last_err.expect("retry loop executes at least once"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_delay_doubles_with_cap() {
        assert_eq!(backoff_delay_ms(100, 0), 100);
        assert_eq!(backoff_delay_ms(100, 1), 200);
        assert_eq!(backoff_delay_ms(100, 2), 400);
        assert_eq!(backoff_delay_ms(100, 4), 1600);
        // capped at 2^4
        assert_eq!(backoff_delay_ms(100, 5), 1600);
        assert_eq!(backoff_delay_ms(100, 99), 1600);
    }

    #[test]
    fn schedule_has_one_entry_per_retry() {
        assert_eq!(backoff_schedule(100, 3), vec![100, 200, 400]);
        assert!(backoff_schedule(100, 0).is_empty());
    }

    #[test]
    fn saturates_instead_of_overflowing() {
        assert_eq!(backoff_delay_ms(u64::MAX, 3), u64::MAX);
    }

    #[tokio::test]
    async fn async_retry_succeeds_after_failures() {
        let mut calls = 0u32;
        let result: std::result::Result<u32, &'static str> = retry_with_backoff(
            3,
            1, // 1ms base to keep the test fast
            || {
                calls += 1;
                async move {
                    if calls < 3 {
                        Err("transient")
                    } else {
                        Ok(calls)
                    }
                }
            },
            |attempt, _delay, err: &&'static str| {
                assert!(attempt < 2, "on_retry must not fire after final attempt");
                assert_eq!(*err, "transient");
            },
        )
        .await;
        assert_eq!(result, Ok(3));
    }

    #[tokio::test]
    async fn async_retry_exhausts_and_returns_last_error() {
        let mut retries_seen = 0u32;
        let result: std::result::Result<(), &'static str> = retry_with_backoff(
            2,
            1,
            || async { Err("permanent") },
            |_, _, _| retries_seen += 1,
        )
        .await;
        assert_eq!(result, Err("permanent"));
        assert_eq!(
            retries_seen, 2,
            "on_retry fires once per retry, not per attempt"
        );
    }

    #[test]
    fn blocking_retry_succeeds_after_failures() {
        let mut calls = 0u32;
        let result: std::result::Result<u32, &'static str> = retry_with_backoff_blocking(
            3,
            1,
            || {
                calls += 1;
                if calls < 2 {
                    Err("transient")
                } else {
                    Ok(calls)
                }
            },
            |_, _, _| {},
        );
        assert_eq!(result, Ok(2));
    }
}
