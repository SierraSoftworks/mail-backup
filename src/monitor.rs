//! Optional HTTP cron-monitoring for scheduled backup runs.
//!
//! When a backup policy configures a [`PingConfig`], the URL for the matching
//! state is pinged as each backup run starts, succeeds, or fails — a best-effort
//! signal to an external cron/uptime monitor such as [Sentry Crons] or
//! [Healthchecks.io]. Pings are intentionally plain HTTP GET requests, and a
//! ping that fails or times out is logged but never affects the backup itself
//! (monitoring must never be able to take a backup down).
//!
//! Only full backup *runs* are reported; the daemon's live streaming syncs are
//! deliberately ignored, since a cron monitor tracks scheduled runs rather than
//! every incremental change.
//!
//! [Sentry Crons]: https://docs.sentry.io/product/crons/
//! [Healthchecks.io]: https://healthchecks.io/

use std::time::Duration;

use tracing_batteries::prelude::*;
use url::Url;

use crate::policy::PingConfig;

/// How long a single monitor ping may take before it is abandoned, so an
/// unresponsive monitoring endpoint can never stall a backup for long.
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// The lifecycle state of a backup run, each mapped to a configured URL.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonitorState {
    /// A run has started.
    Start,
    /// A run finished successfully.
    Success,
    /// A run failed.
    Fail,
}

impl MonitorState {
    fn label(self) -> &'static str {
        match self {
            MonitorState::Start => "start",
            MonitorState::Success => "success",
            MonitorState::Fail => "fail",
        }
    }
}

/// Pings the HTTP cron monitor configured for a backup policy as its runs start
/// and complete.
pub struct Monitor {
    client: reqwest::Client,
    config: PingConfig,
}

impl Monitor {
    /// Builds a monitor for the given configuration. The HTTP client carries a
    /// short timeout so an unresponsive endpoint can never stall a backup.
    pub fn new(config: PingConfig) -> Self {
        let client = reqwest::Client::builder()
            .timeout(PING_TIMEOUT)
            .build()
            .unwrap_or_default();
        Self { client, config }
    }

    fn url_for(&self, state: MonitorState) -> Option<&Url> {
        match state {
            MonitorState::Start => self.config.start.as_ref(),
            MonitorState::Success => self.config.success.as_ref(),
            MonitorState::Fail => self.config.fail.as_ref(),
        }
    }

    /// Reports that a backup run has started.
    pub async fn started(&self) {
        self.ping(MonitorState::Start).await;
    }

    /// Reports that a backup run completed successfully.
    pub async fn succeeded(&self) {
        self.ping(MonitorState::Success).await;
    }

    /// Reports that a backup run failed.
    pub async fn failed(&self) {
        self.ping(MonitorState::Fail).await;
    }

    /// Sends a best-effort GET to the URL for `state` (a no-op when no URL is
    /// configured for it). Transport errors and non-success responses are
    /// logged at warn level and otherwise swallowed.
    async fn ping(&self, state: MonitorState) {
        let Some(url) = self.url_for(state) else {
            return;
        };

        debug!("Pinging the {} cron monitor at {}", state.label(), url);
        match self.client.get(url.clone()).send().await {
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
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn url(base: &str, suffix: &str) -> Url {
        format!("{base}{suffix}").parse().unwrap()
    }

    #[tokio::test]
    async fn pings_the_url_for_each_configured_state() {
        let server = MockServer::start().await;
        for suffix in ["/start", "/ok", "/fail"] {
            Mock::given(method("GET"))
                .and(path(suffix))
                .respond_with(ResponseTemplate::new(200))
                .expect(1)
                .mount(&server)
                .await;
        }

        let monitor = Monitor::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            success: Some(url(&server.uri(), "/ok")),
            fail: Some(url(&server.uri(), "/fail")),
        });

        monitor.started().await;
        monitor.succeeded().await;
        monitor.failed().await;
        // MockServer verifies each `expect(1)` when it is dropped here.
    }

    #[tokio::test]
    async fn unconfigured_states_are_a_noop() {
        let server = MockServer::start().await;
        // Only `start` is configured; success/fail must not touch the network.
        Mock::given(method("GET"))
            .and(path("/start"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let monitor = Monitor::new(PingConfig {
            start: Some(url(&server.uri(), "/start")),
            ..Default::default()
        });

        monitor.started().await;
        monitor.succeeded().await;
        monitor.failed().await;
    }

    #[tokio::test]
    async fn a_disabled_monitor_does_nothing() {
        let monitor = Monitor::new(PingConfig::default());
        // None of these may panic or touch the network with nothing configured.
        monitor.started().await;
        monitor.succeeded().await;
        monitor.failed().await;
    }

    #[tokio::test]
    async fn an_error_response_does_not_panic() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let monitor = Monitor::new(PingConfig {
            start: Some(server.uri().parse().unwrap()),
            ..Default::default()
        });
        // A 5xx from the monitor is logged, not surfaced.
        monitor.started().await;
    }
}
