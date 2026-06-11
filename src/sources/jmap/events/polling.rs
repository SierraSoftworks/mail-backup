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
