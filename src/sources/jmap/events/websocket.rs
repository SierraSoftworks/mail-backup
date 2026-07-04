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
                Some([jmap_client::DataType::Email, jmap_client::DataType::Mailbox]),
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use tokio_stream::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    use wiremock::MockServer;

    use crate::sources::jmap::testing::{
        connected_client, mock_session_document, session_json, session_json_with_websocket,
    };

    async fn client_with_session(session: serde_json::Value) -> (MockServer, MailClient) {
        let server = MockServer::start().await;
        mock_session_document(&server, session).await;
        let client = connected_client(&server).await;
        (server, client)
    }

    #[tokio::test]
    async fn the_capability_gates_the_strategy() {
        let server = MockServer::start().await;
        let (_server, client) = client_with_session(session_json(&server.uri())).await;
        assert!(
            !WebSocketStrategy.supported(&client),
            "no websocket capability means no websocket strategy"
        );

        let server = MockServer::start().await;
        let (_server, client) = client_with_session(session_json_with_websocket(
            &server.uri(),
            "ws://127.0.0.1:1",
            false,
        ))
        .await;
        assert!(
            !WebSocketStrategy.supported(&client),
            "a websocket endpoint without push support is useless to us"
        );

        let server = MockServer::start().await;
        let (_server, client) = client_with_session(session_json_with_websocket(
            &server.uri(),
            "ws://127.0.0.1:1",
            true,
        ))
        .await;
        assert!(WebSocketStrategy.supported(&client));
    }

    #[tokio::test]
    async fn delivers_push_notifications_end_to_end() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a local listener binds");
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());

        // A minimal RFC 8887 server: accept the handshake (capturing the
        // headers for assertion), read the push-enable frame, deliver one
        // StateChange, then close.
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("the client connects");

            let mut authorization = None;
            let mut protocol = None;
            // The Result type (and its large Err variant) is dictated by
            // tungstenite's handshake Callback trait.
            #[allow(clippy::result_large_err)]
            let callback =
                |request: &Request, mut response: Response| -> Result<Response, ErrorResponse> {
                    authorization = request
                        .headers()
                        .get("Authorization")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    protocol = request
                        .headers()
                        .get("Sec-WebSocket-Protocol")
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string);
                    response
                        .headers_mut()
                        .insert("Sec-WebSocket-Protocol", "jmap".parse().unwrap());
                    Ok(response)
                };
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .expect("the handshake succeeds");

            let push_enable = loop {
                match ws
                    .next()
                    .await
                    .expect("a frame arrives")
                    .expect("frame is ok")
                {
                    Message::Text(text) => break text.to_string(),
                    _ => continue,
                }
            };

            ws.send(Message::text(
                r#"{"@type":"StateChange","changed":{"acc-primary":{"Email":"es-2"}}}"#,
            ))
            .await
            .expect("the state change sends");
            ws.send(Message::Close(None))
                .await
                .expect("the close sends");
            // Complete the close handshake by reading until the client's
            // Close reply ends the stream. Dropping the socket with that
            // reply still in flight makes Windows send an RST, which the
            // client sees as WSAECONNABORTED instead of a clean EOF.
            while ws.next().await.is_some() {}

            (authorization, protocol, push_enable)
        });

        let server = MockServer::start().await;
        let (_server, client) =
            client_with_session(session_json_with_websocket(&server.uri(), &ws_url, true)).await;

        let stream = WebSocketStrategy
            .subscribe(&client)
            .await
            .expect("the subscription succeeds");
        tokio::pin!(stream);

        let mut notifications = Vec::new();
        while let Some(item) = stream.next().await {
            notifications.push(item.expect("the stream stays clean until the server closes"));
        }
        assert!(
            notifications.contains(&SourceNotification::Changed {
                email: true,
                mailbox: false
            }),
            "got: {notifications:?}"
        );

        let (authorization, protocol, push_enable) =
            server_task.await.expect("the server task completes");
        assert_eq!(authorization.as_deref(), Some("Bearer test-token"));
        assert_eq!(protocol.as_deref(), Some("jmap"));
        assert!(
            push_enable.contains("WebSocketPushEnable"),
            "got: {push_enable}"
        );
        assert!(
            push_enable.contains("Email") && push_enable.contains("Mailbox"),
            "got: {push_enable}"
        );
    }

    #[test]
    fn group_pushes_are_flattened() {
        let push: PushObject = serde_json::from_value(serde_json::json!({
            "@type": "Group",
            "entries": [
                { "@type": "StateChange", "changed": { "acc-primary": { "Mailbox": "ms-1" } } },
                { "@type": "EmailPush", "accountId": "acc-primary", "email": null },
            ],
        }))
        .expect("a valid push object");
        assert_eq!(collect_changes(&push), (true, true));
    }
}
