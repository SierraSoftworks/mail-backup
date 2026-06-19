//! Optional HTTP cron-monitoring ("ping") for scheduled backup runs.
//!
//! When a backup policy configures a [`PingConfig`], the URL for the matching
//! state is fetched as each backup run starts, succeeds, or fails — a
//! best-effort signal to an external cron/uptime monitor such as [Sentry Crons]
//! or [Healthchecks.io]. Pings are intentionally plain HTTP GET requests, and a
//! ping that fails or times out is logged but never affects the backup itself
//! (monitoring must never be able to take a backup down).
//!
//! Each ping carries the W3C trace context (`traceparent`/`tracestate`) of the
//! backup run it reports, so a monitor that understands trace context can join
//! the ping to the same distributed trace.
//!
//! Only full backup *runs* — one-shot backups, and in the daemon the initial
//! pass and each scheduled snapshot refresh — are reported. The daemon's
//! incremental live syncs (notification- and safety-poll-driven) are
//! deliberately ignored, since a cron monitor tracks scheduled runs rather than
//! every incremental change.
//!
//! [Sentry Crons]: https://docs.sentry.io/product/crons/
//! [Healthchecks.io]: https://healthchecks.io/

use std::future::Future;
use std::time::Duration;

use serde::Deserialize;
// The tracing-batteries prelude re-exports the OpenTelemetry pieces we need —
// `OpenTelemetrySpanExt` (for the current span's context), the global
// `get_text_map_propagator`, the `TextMapPropagator`/`Injector` traits — which
// keeps us pinned to the exact same `opentelemetry` version it uses.
use tracing_batteries::prelude::*;
use url::Url;

/// How long a single ping may take before it is abandoned, so an unresponsive
/// monitoring endpoint can never stall a backup for long.
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// HTTP cron-monitoring endpoints, pinged as a scheduled backup run reaches
/// each lifecycle state. Designed for services such as [Sentry Crons] or
/// [Healthchecks.io].
///
/// Each state has its own URL, so the same shape works both for services that
/// distinguish states with a query string (Sentry uses `?status=in_progress`,
/// `?status=ok` and `?status=error`) and for those that use a path suffix
/// (Healthchecks uses `/start` and `/fail`). Any state left unset is simply not
/// reported.
///
/// [Sentry Crons]: https://docs.sentry.io/product/crons/
/// [Healthchecks.io]: https://healthchecks.io/
#[derive(Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct PingConfig {
    /// Pinged when a backup run begins.
    #[serde(default)]
    pub start: Option<Url>,
    /// Pinged when a backup run completes successfully.
    #[serde(default)]
    pub success: Option<Url>,
    /// Pinged when a backup run fails.
    #[serde(default)]
    pub failure: Option<Url>,
}

/// The lifecycle state of a backup run, each mapped to a configured URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PingState {
    /// A run has started.
    Start,
    /// A run finished successfully.
    Success,
    /// A run failed.
    Failure,
}

impl PingState {
    fn label(self) -> &'static str {
        match self {
            PingState::Start => "start",
            PingState::Success => "success",
            PingState::Failure => "failure",
        }
    }
}

/// A carrier that lets the OpenTelemetry propagator write the outgoing trace
/// context into a [`reqwest`] header map.
struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

impl OpenTelemetryPropagationInjector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(key.as_bytes()),
            reqwest::header::HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// Injects `context`'s W3C trace context (`traceparent`/`tracestate`) into
/// `headers` using the globally configured propagator. A no-op when the context
/// has no valid active span, so nothing is sent when there is no trace to join.
fn inject_trace_context(
    context: &opentelemetry::Context,
    headers: &mut reqwest::header::HeaderMap,
) {
    get_text_map_propagator(|propagator| {
        propagator.inject_context(context, &mut HeaderInjector(headers));
    });
}

/// Builds the request headers for a ping, carrying the current span's trace
/// context so the monitoring service can join the ping to the backup run's
/// distributed trace. Empty when no trace is active (e.g. when OpenTelemetry
/// export is not configured).
fn trace_context_headers() -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    inject_trace_context(&Span::current().context(), &mut headers);
    headers
}

/// Pings the HTTP cron monitor configured for a backup policy as its runs start
/// and complete.
pub struct Pinger {
    client: reqwest::Client,
    config: PingConfig,
}

impl Pinger {
    /// Builds a pinger for the given configuration. The HTTP client carries a
    /// short timeout so an unresponsive endpoint can never stall a backup.
    pub fn new(config: PingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(PING_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self { client, config }
    }

    /// Runs `task`, reporting its lifecycle to the configured endpoints: pings
    /// `start` before it begins, then `success` or `failure` based on the
    /// result. `report_success` decides whether a successful result warrants a
    /// `success` ping — a backup cut short by shutdown returns `false`, so
    /// neither success nor failure is reported and the run is simply retried.
    pub async fn observe<T, E>(
        &self,
        task: impl Future<Output = Result<T, E>>,
        report_success: impl FnOnce(&T) -> bool,
    ) -> Result<T, E> {
        self.ping(PingState::Start).await;
        match task.await {
            Ok(value) => {
                if report_success(&value) {
                    self.ping(PingState::Success).await;
                }
                Ok(value)
            }
            Err(e) => {
                self.ping(PingState::Failure).await;
                Err(e)
            }
        }
    }

    fn url_for(&self, state: PingState) -> Option<&Url> {
        match state {
            PingState::Start => self.config.start.as_ref(),
            PingState::Success => self.config.success.as_ref(),
            PingState::Failure => self.config.failure.as_ref(),
        }
    }

    /// Sends a best-effort GET to the URL for `state` (a no-op when no URL is
    /// configured for it). Transport errors and non-success responses are
    /// logged at warn level and otherwise swallowed.
    async fn ping(&self, state: PingState) {
        let Some(url) = self.url_for(state) else {
            return;
        };

        debug!("Pinging the {} cron monitor at {}", state.label(), url);
        let request = self
            .client
            .get(url.clone())
            .headers(trace_context_headers());
        match request.send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => warn!(
                "The {} cron monitor at {} responded with HTTP {}",
                state.label(),
                url,
                response.status().as_u16()
            ),
            Err(e) => warn!(
                "Failed to ping the {} cron monitor at {}: {}",
                state.label(),
                url,
                e
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracing_batteries::prelude::opentelemetry::Context;
    use tracing_batteries::prelude::opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState,
    };
    use tracing_batteries::prelude::{TraceContextPropagator, set_text_map_propagator};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// An OpenTelemetry context carrying a known, valid remote span context, so
    /// the propagator has a trace to inject. Returns it with the `traceparent`
    /// value it should produce.
    fn test_trace_context() -> (Context, String) {
        let trace_id = "0af7651916cd43dd8448eb211c80319c";
        let span_id = "b7ad6b7169203331";
        let span_context = SpanContext::new(
            TraceId::from_hex(trace_id).unwrap(),
            SpanId::from_hex(span_id).unwrap(),
            TraceFlags::SAMPLED,
            true,
            TraceState::default(),
        );
        let context = Context::new().with_remote_span_context(span_context);
        (context, format!("00-{trace_id}-{span_id}-01"))
    }

    fn url(base: &str, suffix: &str) -> Url {
        format!("{base}{suffix}").parse().unwrap()
    }

    #[test]
    fn state_labels() {
        assert_eq!(PingState::Start.label(), "start");
        assert_eq!(PingState::Success.label(), "success");
        assert_eq!(PingState::Failure.label(), "failure");
    }

    /// Mounts a `GET {suffix}` expectation that must be hit exactly `times`.
    async fn expect(server: &MockServer, suffix: &str, times: u64) {
        Mock::given(method("GET"))
            .and(path(suffix))
            .respond_with(ResponseTemplate::new(200))
            .expect(times)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn pings_the_url_for_each_state() {
        let server = MockServer::start().await;
        expect(&server, "/start", 1).await;
        expect(&server, "/ok", 1).await;
        expect(&server, "/fail", 1).await;

        let pinger = Pinger::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            success: Some(url(&server.uri(), "/ok")),
            failure: Some(url(&server.uri(), "/fail")),
        });

        pinger.ping(PingState::Start).await;
        pinger.ping(PingState::Success).await;
        pinger.ping(PingState::Failure).await;
        // MockServer verifies each `expect(1)` when it is dropped here.
    }

    #[tokio::test]
    async fn unconfigured_states_are_a_noop() {
        let server = MockServer::start().await;
        // Only `start` is configured; the others must not touch the network.
        expect(&server, "/start", 1).await;

        let pinger = Pinger::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            ..Default::default()
        });

        pinger.ping(PingState::Start).await;
        pinger.ping(PingState::Success).await;
        pinger.ping(PingState::Failure).await;
    }

    #[tokio::test]
    async fn a_disabled_pinger_does_nothing() {
        let pinger = Pinger::new(PingConfig::default());
        // None of these may panic or touch the network with nothing configured.
        pinger.ping(PingState::Start).await;
        pinger.ping(PingState::Success).await;
        pinger.ping(PingState::Failure).await;
    }

    #[tokio::test]
    async fn an_error_response_is_swallowed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let pinger = Pinger::new(PingConfig {
            start: Some(server.uri().parse().unwrap()),
            ..Default::default()
        });
        // A 5xx from the monitor is logged, not surfaced.
        pinger.ping(PingState::Start).await;
    }

    #[tokio::test]
    async fn a_transport_error_is_swallowed() {
        // Nothing is listening on this port, so the GET fails to connect; the
        // error must be swallowed rather than propagated.
        let pinger = Pinger::new(PingConfig {
            start: Some("http://127.0.0.1:1/start".parse().unwrap()),
            ..Default::default()
        });
        pinger.ping(PingState::Start).await;
    }

    #[tokio::test]
    async fn observe_reports_start_and_success() {
        let server = MockServer::start().await;
        expect(&server, "/start", 1).await;
        expect(&server, "/ok", 1).await;

        let pinger = Pinger::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            success: Some(url(&server.uri(), "/ok")),
            failure: Some(url(&server.uri(), "/fail")),
        });

        let result = pinger
            .observe(async { Ok::<_, ()>(42) }, |value| {
                assert_eq!(*value, 42);
                true
            })
            .await;
        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn observe_skips_success_when_predicate_is_false() {
        let server = MockServer::start().await;
        // Only `start` should be pinged; `success` is suppressed (e.g. an
        // interrupted run) and `failure` never fires for an `Ok`.
        expect(&server, "/start", 1).await;

        let pinger = Pinger::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            success: Some(url(&server.uri(), "/ok")),
            failure: Some(url(&server.uri(), "/fail")),
        });

        let ran = AtomicBool::new(false);
        let result: Result<(), ()> = pinger
            .observe(async { Ok(()) }, |_| {
                ran.store(true, Ordering::SeqCst);
                false
            })
            .await;
        assert_eq!(result, Ok(()));
        assert!(ran.load(Ordering::SeqCst), "the predicate was consulted");
    }

    #[tokio::test]
    async fn observe_reports_failure_on_error() {
        let server = MockServer::start().await;
        expect(&server, "/start", 1).await;
        expect(&server, "/fail", 1).await;

        let pinger = Pinger::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            success: Some(url(&server.uri(), "/ok")),
            failure: Some(url(&server.uri(), "/fail")),
        });

        let result: Result<(), &str> = pinger
            .observe(async { Err("boom") }, |_| panic!("not called on error"))
            .await;
        assert_eq!(result, Err("boom"));
    }

    #[test]
    fn injects_the_active_traceparent() {
        set_text_map_propagator(TraceContextPropagator::new());
        let (context, expected) = test_trace_context();

        let mut headers = reqwest::header::HeaderMap::new();
        inject_trace_context(&context, &mut headers);

        let traceparent = headers
            .get("traceparent")
            .expect("a traceparent header is injected for the active trace")
            .to_str()
            .unwrap();
        assert_eq!(traceparent, expected);
    }

    #[test]
    fn injects_nothing_without_an_active_trace() {
        set_text_map_propagator(TraceContextPropagator::new());
        // An empty context has no valid span, so nothing is propagated.
        let mut headers = reqwest::header::HeaderMap::new();
        inject_trace_context(&Context::new(), &mut headers);
        assert!(headers.get("traceparent").is_none());
    }

    #[tokio::test]
    async fn a_ping_without_an_active_trace_sends_no_trace_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let pinger = Pinger::new(PingConfig {
            start: Some(server.uri().parse().unwrap()),
            ..Default::default()
        });
        pinger.ping(PingState::Start).await;

        let requests = server
            .received_requests()
            .await
            .expect("the server records requests");
        assert_eq!(requests.len(), 1);
        assert!(
            requests[0].headers.get("traceparent").is_none(),
            "no traceparent should be sent when no trace is active"
        );
    }
}
