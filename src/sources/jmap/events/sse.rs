//! Change notifications over EventSource/SSE (RFC 8620 §7.3).

use tokio_stream::StreamExt;
use tracing_batteries::prelude::*;

use super::{EventStrategy, EventStream};
use crate::entities::mail::SourceNotification;
use crate::errors::HumanizableError;
use crate::helpers::jmap::{MailClient, is_keepalive_artifact};

/// Server-side keep-alive interval, in seconds.
const PING_INTERVAL_SECS: u32 = 30;

pub struct SseStrategy;

impl EventStrategy for SseStrategy {
    fn name(&self) -> &'static str {
        "sse"
    }

    fn supported(&self, client: &MailClient) -> bool {
        !client.inner().session().event_source_url().is_empty()
    }

    async fn subscribe<'a>(
        &'a self,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error> {
        let stream = client
            .inner()
            .event_source(
                Some(vec![
                    jmap_client::DataType::Email,
                    jmap_client::DataType::Mailbox,
                ]),
                false,
                Some(PING_INTERVAL_SECS),
                None,
            )
            .await
            .map_err(|e| e.to_human_error())?;

        Ok(Box::pin(async_stream::stream! {
            tokio::pin!(stream);

            while let Some(event) = stream.next().await {
                match event {
                    Ok(jmap_client::event_source::PushNotification::StateChange(changes)) => {
                        let email = changes.has_type(jmap_client::DataType::Email);
                        let mailbox = changes.has_type(jmap_client::DataType::Mailbox);
                        if email || mailbox {
                            yield Ok(SourceNotification::Changed { email, mailbox });
                        } else {
                            yield Ok(SourceNotification::Ping);
                        }
                    }
                    Ok(_) => {
                        yield Ok(SourceNotification::Ping);
                    }
                    Err(e) if is_keepalive_artifact(&e) => {
                        // A keep-alive comment mis-parsed by jmap-client; the
                        // connection is healthy, so stay on the stream rather
                        // than tearing it down and re-syncing.
                        yield Ok(SourceNotification::Ping);
                    }
                    Err(e) if matches!(e, jmap_client::Error::Parse(_)) => {
                        // The server pushed an event payload jmap-client
                        // couldn't decode — notably, Fastmail omits the
                        // "@type" member (RFC 8620 §7.1) from its EventSource
                        // StateChange payloads, which jmap-client requires.
                        // Notifications are only hints, so treat it as
                        // "something changed" and let the state-driven sync
                        // work out what, rather than tearing down a healthy
                        // stream.
                        debug!("Treating an undecodable push event as a change hint: {}", e);
                        yield Ok(SourceNotification::Changed {
                            email: true,
                            mailbox: true,
                        });
                    }
                    Err(e) => {
                        yield Err(e.to_human_error());
                        break;
                    }
                }
            }
        }))
    }
}
