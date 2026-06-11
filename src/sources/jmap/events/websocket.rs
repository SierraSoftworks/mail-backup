//! Change notifications over a JMAP websocket connection (RFC 8887).
//!
//! Preferred over SSE where the server advertises push support on the
//! `urn:ietf:params:jmap:websocket` capability: a single long-lived
//! connection with lower latency and explicit client-driven keep-alives.

use std::time::Duration;

use jmap_client::PushObject;
use jmap_client::client_ws::WebSocketMessage;
use tokio_stream::StreamExt;
use tracing_batteries::prelude::*;

use super::{EventStrategy, EventStream};
use crate::entities::mail::SourceNotification;
use crate::errors::HumanizableError;
use crate::helpers::jmap::MailClient;

/// How often a Ping frame is sent to keep the connection alive (and detect
/// its death), matching the SSE keep-alive cadence.
const PING_INTERVAL: Duration = Duration::from_secs(30);

pub struct WebSocketStrategy;

impl EventStrategy for WebSocketStrategy {
    fn name(&self) -> &'static str {
        "websocket"
    }

    fn supported(&self, client: &MailClient) -> bool {
        client
            .inner()
            .session()
            .websocket_capabilities()
            .is_some_and(|capabilities| capabilities.supports_push())
    }

    async fn subscribe<'a>(
        &'a self,
        client: &'a MailClient,
    ) -> Result<EventStream<'a>, human_errors::Error> {
        // NOTE: connect_ws stores the connection's write half inside the
        // client, so a MailClient supports at most one websocket
        // subscription at a time. The daemon's one-stream-at-a-time
        // lifecycle guarantees that here.
        let mut ws = client
            .inner()
            .connect_ws()
            .await
            .map_err(|e| e.to_human_error())?;

        // Push enablement is per-connection (RFC 8887 §4.3.5.1) and must be
        // re-sent after every reconnect, which re-subscribing does for us.
        client
            .inner()
            .enable_push_ws(
                Some([
                    jmap_client::DataType::Email,
                    jmap_client::DataType::Mailbox,
                ]),
                None::<String>,
            )
            .await
            .map_err(|e| e.to_human_error())?;

        Ok(Box::pin(async_stream::stream! {
            let mut ping = tokio::time::interval_at(
                tokio::time::Instant::now() + PING_INTERVAL,
                PING_INTERVAL,
            );
            ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    message = ws.next() => match message {
                        Some(Ok(WebSocketMessage::PushNotification(push))) => {
                            let (email, mailbox) = collect_changes(&push);
                            if email || mailbox {
                                yield Ok(SourceNotification::Changed { email, mailbox });
                            } else {
                                yield Ok(SourceNotification::Ping);
                            }
                        }
                        Some(Ok(WebSocketMessage::Response(_))) => {
                            // We never send method calls on this connection,
                            // so responses carry nothing actionable.
                            debug!("Ignoring an unsolicited method response on the websocket connection.");
                        }
                        Some(Err(e)) => {
                            yield Err(e.to_human_error());
                            break;
                        }
                        None => break,
                    },
                    _ = ping.tick() => {
                        if let Err(e) = client.inner().ws_ping().await {
                            yield Err(e.to_human_error());
                            break;
                        }
                        yield Ok(SourceNotification::Ping);
                    }
                }
            }
        }))
    }
}

/// Folds a push object into "did Email and/or Mailbox state change"
/// flags, matching the account-agnostic semantics of the SSE path.
fn collect_changes(push: &PushObject) -> (bool, bool) {
    match push {
        PushObject::StateChange { changed } => (
            changed
                .values()
                .any(|types| types.contains_key(&jmap_client::DataType::Email)),
            changed
                .values()
                .any(|types| types.contains_key(&jmap_client::DataType::Mailbox)),
        ),
        PushObject::EmailPush { .. } => (true, false),
        PushObject::Group { entries } => entries.iter().fold((false, false), |acc, entry| {
            let (email, mailbox) = collect_changes(entry);
            (acc.0 || email, acc.1 || mailbox)
        }),
        PushObject::CalendarAlert(_) => (false, false),
    }
}
