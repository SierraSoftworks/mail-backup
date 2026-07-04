//! Shared resilience primitives for network transports: retries with
//! exponential backoff, and a circuit breaker which pauses traffic to a
//! server that keeps failing.
//!
//! The two compose through [`retry`]: every attempt first asks the breaker
//! for permission and then reports its outcome back, so all call sites
//! sharing a breaker contribute to (and benefit from) the same picture of
//! the server's health. A transport embeds one breaker per server (see
//! `MailClient`) and supplies an error classifier saying which of its
//! errors are transient — only transient failures trip the breaker, since
//! an auth or not-found error says nothing about the server's health.

use std::fmt::Display;
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use tracing_batteries::prelude::*;

/// How a transiently-failing operation is retried: the delay starts at
/// `initial_delay`, doubles after every failure, and is capped at
/// `max_delay`, across at most `max_attempts` attempts.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    /// Eight attempts with delays of 0.5s, 1s, 2s, 4s, 8s, 16s and 30s
    /// (about a minute of waiting in total) — long enough to ride out the
    /// short 502/503 bursts a provider emits during rolling restarts and
    /// load-balancer failovers, without stalling a run indefinitely when
    /// the server is genuinely down.
    fn default() -> Self {
        Self {
            max_attempts: 8,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
        }
    }
}

/// The reason a [`retry`] call gave up.
#[derive(Debug)]
pub enum RetryError<E> {
    /// The operation itself failed: either fatally (a non-transient error,
    /// returned immediately) or transiently after exhausting the policy's
    /// attempts or tripping the circuit breaker. Carries the last error.
    Operation(E),
    /// The circuit breaker was already open before the first attempt, so
    /// the operation was never run; `retry_after` is how long the breaker
    /// will stay open (barring further failures).
    CircuitOpen { retry_after: Duration },
}

impl<E> RetryError<E> {
    /// The underlying operation error, when there is one to inspect.
    pub fn inner(&self) -> Option<&E> {
        match self {
            RetryError::Operation(error) => Some(error),
            RetryError::CircuitOpen { .. } => None,
        }
    }
}

impl<E: Display> Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryError::Operation(error) => error.fmt(f),
            RetryError::CircuitOpen { retry_after } => write!(
                f,
                "the server has been failing repeatedly; requests are paused for the next {retry_after:?}"
            ),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for RetryError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RetryError::Operation(error) => Some(error),
            RetryError::CircuitOpen { .. } => None,
        }
    }
}

/// The circuit breaker's state machine.
#[derive(Debug)]
enum BreakerState {
    /// Traffic flows normally; the failure streak is tracked so repeated
    /// transient failures can open the breaker.
    Closed { consecutive_failures: u32 },
    /// Traffic is paused until the cooldown deadline passes.
    Open { until: tokio::time::Instant },
    /// The cooldown has passed and a single probe request is in flight; its
    /// outcome decides whether the breaker closes again or re-opens.
    HalfOpen,
}

/// A circuit breaker shared by every request a transport makes to one
/// server.
///
/// After `threshold` consecutive transient failures the breaker opens and
/// [`try_acquire`](Self::try_acquire) rejects requests for `cooldown`,
/// so a struggling server is not hammered by dozens of concurrent retry
/// loops each backing off independently. Once the cooldown passes, a
/// single probe request is let through: if it succeeds the breaker closes,
/// if it fails the breaker re-opens for another cooldown.
///
/// All methods take `&self` (the state lives behind a mutex), so a single
/// breaker can serve concurrent requests.
pub struct CircuitBreaker {
    threshold: u32,
    cooldown: Duration,
    state: Mutex<BreakerState>,
}

impl Default for CircuitBreaker {
    /// Opens after 10 consecutive transient failures — more than one
    /// [`retry`] call's default 8 attempts, so a single unlucky call never
    /// opens the breaker on its own — and pauses traffic for 60 seconds,
    /// comfortably within the daemon's reconnect backoff ceiling.
    fn default() -> Self {
        Self::new(10, Duration::from_secs(60))
    }
}

impl CircuitBreaker {
    pub fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold,
            cooldown,
            state: Mutex::new(BreakerState::Closed {
                consecutive_failures: 0,
            }),
        }
    }

    /// Asks permission to issue a request. `Err` carries roughly how long
    /// to wait before asking again: the remaining cooldown when the breaker
    /// is open, or a full cooldown when a recovery probe is already in
    /// flight. When an open breaker's cooldown has passed, the caller is
    /// granted the probe slot.
    pub fn try_acquire(&self) -> Result<(), Duration> {
        let mut state = self.state.lock().unwrap();
        match *state {
            BreakerState::Closed { .. } => Ok(()),
            BreakerState::HalfOpen => Err(self.cooldown),
            BreakerState::Open { until } => {
                let now = tokio::time::Instant::now();
                if now >= until {
                    *state = BreakerState::HalfOpen;
                    Ok(())
                } else {
                    Err(until - now)
                }
            }
        }
    }

    /// Reports a successful request: the failure streak resets and the
    /// breaker closes (a success is proof enough of recovery, whether it
    /// was the designated probe or a request that was already in flight).
    pub fn record_success(&self) {
        *self.state.lock().unwrap() = BreakerState::Closed {
            consecutive_failures: 0,
        };
    }

    /// Reports a transient request failure. Non-transient failures (auth,
    /// not-found, invalid requests) must not be reported: they say nothing
    /// about the server's health.
    pub fn record_failure(&self) {
        let mut state = self.state.lock().unwrap();
        match &mut *state {
            BreakerState::Closed {
                consecutive_failures,
            } => {
                *consecutive_failures += 1;
                if *consecutive_failures >= self.threshold {
                    warn!(
                        "The server has failed {} times in a row; pausing requests to it for {:?}.",
                        self.threshold, self.cooldown
                    );
                    *state = BreakerState::Open {
                        until: tokio::time::Instant::now() + self.cooldown,
                    };
                }
            }
            BreakerState::HalfOpen => {
                warn!(
                    "The server is still failing after its cooldown; pausing requests to it for another {:?}.",
                    self.cooldown
                );
                *state = BreakerState::Open {
                    until: tokio::time::Instant::now() + self.cooldown,
                };
            }
            // Failures of requests that were already in flight when the
            // breaker opened carry no new information; the cooldown is not
            // extended.
            BreakerState::Open { .. } => {}
        }
    }
}

/// Runs `operation`, retrying transient failures (as classified by
/// `is_transient`) per `policy` and coordinating with `breaker`.
///
/// Every attempt asks the breaker for permission first and reports its
/// outcome back. When the breaker rejects an attempt after earlier attempts
/// of this same call have failed, that underlying error is returned (it is
/// the actual cause); when the breaker rejects the very first attempt the
/// call fails fast with [`RetryError::CircuitOpen`] without touching the
/// network.
pub async fn retry<T, E, F, Fut>(
    policy: &RetryPolicy,
    breaker: &CircuitBreaker,
    description: &str,
    is_transient: impl Fn(&E) -> bool,
    mut operation: F,
) -> Result<T, RetryError<E>>
where
    E: Display,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut delay = policy.initial_delay;
    let mut attempt = 1;
    let mut last_error: Option<E> = None;

    loop {
        if let Err(retry_after) = breaker.try_acquire() {
            return Err(match last_error {
                Some(error) => RetryError::Operation(error),
                None => {
                    warn!(
                        "{} skipped: the server has been failing repeatedly; requests are paused for the next {:?}",
                        description, retry_after
                    );
                    RetryError::CircuitOpen { retry_after }
                }
            });
        }

        match operation().await {
            Ok(value) => {
                breaker.record_success();
                return Ok(value);
            }
            Err(error) if attempt < policy.max_attempts && is_transient(&error) => {
                breaker.record_failure();
                warn!(
                    "{} failed (attempt {}/{}): {}; retrying in {:?}",
                    description, attempt, policy.max_attempts, error, delay
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(policy.max_delay);
                attempt += 1;
                last_error = Some(error);
            }
            Err(error) => {
                if is_transient(&error) {
                    breaker.record_failure();
                }
                return Err(RetryError::Operation(error));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A policy with short, test-friendly delays: 10ms doubling to a 40ms
    /// cap.
    fn fast_policy(max_attempts: u32) -> RetryPolicy {
        RetryPolicy {
            max_attempts,
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(40),
        }
    }

    /// Errors starting with "transient" are retryable; anything else is
    /// fatal.
    fn is_transient(error: &&'static str) -> bool {
        error.starts_with("transient")
    }

    #[tokio::test]
    async fn a_successful_operation_runs_once() {
        let breaker = CircuitBreaker::default();
        let calls = AtomicU32::new(0);

        let result = retry(&fast_policy(5), &breaker, "op", is_transient, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Ok::<_, &'static str>(42) }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn transient_failures_are_retried_until_success() {
        let breaker = CircuitBreaker::default();
        let calls = AtomicU32::new(0);

        let result = retry(&fast_policy(5), &breaker, "op", is_transient, || {
            let attempt = calls.fetch_add(1, Ordering::Relaxed);
            async move {
                if attempt < 2 {
                    Err("transient blip")
                } else {
                    Ok("recovered")
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), "recovered");
        assert_eq!(calls.load(Ordering::Relaxed), 3);
        assert!(
            breaker.try_acquire().is_ok(),
            "the success closed the streak"
        );
    }

    #[tokio::test]
    async fn fatal_errors_are_not_retried() {
        let breaker = CircuitBreaker::default();
        let calls = AtomicU32::new(0);

        let result: Result<(), _> = retry(&fast_policy(5), &breaker, "op", is_transient, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err("fatal: unauthorized") }
        })
        .await;

        assert!(matches!(
            result,
            Err(RetryError::Operation("fatal: unauthorized"))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausting_the_attempts_returns_the_last_error() {
        let breaker = CircuitBreaker::default();
        let calls = AtomicU32::new(0);

        let result: Result<(), _> = retry(&fast_policy(3), &breaker, "op", is_transient, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err("transient outage") }
        })
        .await;

        assert!(matches!(
            result,
            Err(RetryError::Operation("transient outage"))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn the_backoff_doubles_and_caps_at_the_maximum() {
        let breaker = CircuitBreaker::default();
        let started = tokio::time::Instant::now();
        let waits = Mutex::new(Vec::new());

        let _: Result<(), _> = retry(&fast_policy(5), &breaker, "op", is_transient, || {
            waits.lock().unwrap().push(started.elapsed());
            async { Err("transient outage") }
        })
        .await;

        // Attempt start times: 0, then after 10ms, 20ms, 40ms, 40ms waits.
        let waits = waits.lock().unwrap();
        let millis: Vec<u128> = waits.iter().map(|w| w.as_millis()).collect();
        assert_eq!(millis, vec![0, 10, 30, 70, 110]);
    }

    #[tokio::test]
    async fn non_transient_failures_do_not_trip_the_breaker() {
        let breaker = CircuitBreaker::new(1, Duration::from_secs(60));

        let _: Result<(), _> = retry(&fast_policy(3), &breaker, "op", is_transient, || async {
            Err("fatal: bad request")
        })
        .await;

        assert!(
            breaker.try_acquire().is_ok(),
            "a client error says nothing about the server's health"
        );
    }

    #[tokio::test]
    async fn an_open_breaker_fails_fast_without_running_the_operation() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(60));
        breaker.record_failure();
        breaker.record_failure();

        let calls = AtomicU32::new(0);
        let result: Result<(), _> = retry(&fast_policy(3), &breaker, "op", is_transient, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Ok(()) }
        })
        .await;

        assert!(matches!(result, Err(RetryError::CircuitOpen { .. })));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn opening_the_breaker_mid_call_surfaces_the_real_error() {
        // The call's own failures open the breaker after two attempts; the
        // caller still sees the underlying error, not a breaker rejection.
        let breaker = CircuitBreaker::new(2, Duration::from_secs(60));
        let calls = AtomicU32::new(0);

        let result: Result<(), _> = retry(&fast_policy(5), &breaker, "op", is_transient, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err("transient outage") }
        })
        .await;

        assert!(matches!(
            result,
            Err(RetryError::Operation("transient outage"))
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn the_breaker_recovers_through_a_half_open_probe() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(60));
        breaker.record_failure();
        breaker.record_failure();
        assert!(breaker.try_acquire().is_err(), "open after the threshold");

        tokio::time::advance(Duration::from_secs(61)).await;

        // The first caller after the cooldown gets the probe slot; anyone
        // else keeps being rejected until the probe resolves.
        assert!(breaker.try_acquire().is_ok(), "the probe is admitted");
        assert!(breaker.try_acquire().is_err(), "only one probe at a time");

        breaker.record_success();
        assert!(
            breaker.try_acquire().is_ok(),
            "the success closed the breaker"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_probe_reopens_the_breaker() {
        let breaker = CircuitBreaker::new(2, Duration::from_secs(60));
        breaker.record_failure();
        breaker.record_failure();

        tokio::time::advance(Duration::from_secs(61)).await;
        assert!(breaker.try_acquire().is_ok(), "the probe is admitted");
        breaker.record_failure();

        let retry_after = breaker
            .try_acquire()
            .expect_err("the failed probe re-opened the breaker");
        assert!(retry_after > Duration::from_secs(59));
    }

    #[test]
    fn the_circuit_open_error_explains_itself() {
        let error: RetryError<&'static str> = RetryError::CircuitOpen {
            retry_after: Duration::from_secs(42),
        };
        let message = error.to_string();
        assert!(message.contains("failing repeatedly"), "got: {message}");
        assert!(message.contains("42"), "got: {message}");
    }
}
