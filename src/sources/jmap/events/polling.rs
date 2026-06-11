//! Change notifications by periodically probing the server's Email and
//! Mailbox state strings. The fallback of last resort: it works against
//! any JMAP server, at the cost of latency and a little request traffic.

use std::time::Duration;

use super::{EventStrategy, EventStream};
use crate::entities::mail::SourceNotification;
use crate::helpers::jmap::MailClient;

/// How often the server's state is probed.
const DEFAULT_INTERVAL: Duration = Duration::from_secs(120);

/// The shortest permitted probe interval, protecting servers from
/// accidental hammering.
const MIN_INTERVAL: Duration = Duration::from_secs(10);

pub struct PollingStrategy {
    interval: Duration,
}

impl PollingStrategy {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval: interval.max(MIN_INTERVAL),
        }
    }
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self::new(DEFAULT_INTERVAL)
    }
}

impl EventStrategy for PollingStrategy {
    fn name(&self) -> &'static str {
        "polling"
    }

    fn supported(&self, _client: &MailClient) -> bool {
        true
    }

    async fn subscribe<'a>(
        &'a self,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error> {
        // The baseline probe doubles as the subscription check: a server we
        // cannot probe falls through to the chain's error handling. Anything
        // changing between the engine's last sync and this baseline is
        // covered by its catch-up sync and safety poll.
        let mut last_email = client.email_state().await?;
        let mut last_mailbox = client.mailbox_state().await?;
        let interval = self.interval;

        Ok(Box::pin(async_stream::stream! {
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                ticker.tick().await;

                let email_state = match client.email_state().await {
                    Ok(state) => state,
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                };
                let mailbox_state = match client.mailbox_state().await {
                    Ok(state) => state,
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                };

                let email = email_state != last_email;
                let mailbox = mailbox_state != last_mailbox;
                last_email = email_state;
                last_mailbox = mailbox_state;

                if email || mailbox {
                    yield Ok(SourceNotification::Changed { email, mailbox });
                } else {
                    // Liveness for the consumer's cancellation check.
                    yield Ok(SourceNotification::Ping);
                }
            }
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio_stream::StreamExt;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::sources::jmap::testing::{
        connected_client, method_response, mock_session_document, session_json,
    };

    /// Mounts a one-shot (or unlimited, when `times` is `None`) state
    /// response for the given object type at the given priority.
    async fn mount_state(
        server: &MockServer,
        object: &str,
        state: &str,
        times: Option<u64>,
        priority: u8,
    ) {
        let mut mock = Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains(format!("{object}/get")))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                &format!("{object}/get"),
                json!({ "accountId": "acc-primary", "state": state, "list": [], "notFound": [] }),
            )))
            .with_priority(priority);
        if let Some(times) = times {
            mock = mock.up_to_n_times(times);
        }
        mock.mount(server).await;
    }

    #[tokio::test]
    async fn detects_state_transitions() {
        let server = MockServer::start().await;
        mock_session_document(&server, session_json(&server.uri())).await;

        // Email state: baseline "e1", steady "e1", then moves to "e2".
        mount_state(&server, "Email", "e1", Some(2), 1).await;
        mount_state(&server, "Email", "e2", None, 2).await;
        mount_state(&server, "Mailbox", "m1", None, 1).await;

        let client = connected_client(&server).await;
        let strategy = PollingStrategy {
            interval: Duration::from_millis(25),
        };
        let stream = strategy
            .subscribe(&client)
            .await
            .expect("the baseline probe succeeds");
        tokio::pin!(stream);

        assert!(
            matches!(stream.next().await, Some(Ok(SourceNotification::Ping))),
            "a steady state yields a ping"
        );
        assert!(
            matches!(
                stream.next().await,
                Some(Ok(SourceNotification::Changed {
                    email: true,
                    mailbox: false
                }))
            ),
            "a moved state yields a change notification"
        );
    }

    #[tokio::test]
    async fn ends_on_persistent_probe_failures() {
        let server = MockServer::start().await;
        mock_session_document(&server, session_json(&server.uri())).await;

        // The baseline succeeds; every later probe fails with a
        // non-transient error (no retries, so the test stays fast).
        mount_state(&server, "Email", "e1", Some(1), 1).await;
        mount_state(&server, "Mailbox", "m1", Some(1), 1).await;
        Mock::given(method("POST"))
            .and(path("/api"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = connected_client(&server).await;
        let strategy = PollingStrategy {
            interval: Duration::from_millis(25),
        };
        let stream = strategy
            .subscribe(&client)
            .await
            .expect("the baseline probe succeeds");
        tokio::pin!(stream);

        assert!(
            matches!(stream.next().await, Some(Err(_))),
            "a failed probe surfaces as a stream error"
        );
        assert!(
            stream.next().await.is_none(),
            "the stream ends after an error"
        );
    }

    #[test]
    fn the_interval_is_clamped_to_a_safe_minimum() {
        let strategy = PollingStrategy::new(Duration::from_millis(1));
        assert_eq!(strategy.interval, MIN_INTERVAL);
    }
}
