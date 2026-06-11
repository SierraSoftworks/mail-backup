//! Shared wiremock fixtures for JMAP source and event-strategy tests.

use serde_json::{Value, json};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::JmapMailSource;
use crate::entities::mail::SourceState;
use crate::helpers::jmap::MailClient;
use crate::policy::SourceConfig;
use crate::sources::MailSource;

pub fn session_json(base: &str) -> Value {
    let mail_capabilities = json!({
        "maxMailboxesPerEmail": null,
        "maxMailboxDepth": 10,
        "maxSizeMailboxName": 200,
        "maxSizeAttachmentsPerEmail": 50_000_000u64,
        "emailQuerySortOptions": ["receivedAt"],
        "mayCreateTopLevelMailbox": true
    });

    json!({
        "capabilities": {
            "urn:ietf:params:jmap:core": {
                "maxSizeUpload": 50_000_000u64,
                "maxConcurrentUpload": 4,
                "maxSizeRequest": 10_000_000u64,
                "maxConcurrentRequests": 4,
                "maxCallsInRequest": 16,
                "maxObjectsInGet": 256,
                "maxObjectsInSet": 128,
                "collationAlgorithms": []
            },
            "urn:ietf:params:jmap:mail": mail_capabilities
        },
        "accounts": {
            "acc-primary": {
                "name": "user@example.com",
                "isPersonal": true,
                "isReadOnly": false,
                "accountCapabilities": { "urn:ietf:params:jmap:mail": mail_capabilities }
            },
            "acc-other": {
                "name": "other@example.com",
                "isPersonal": false,
                "isReadOnly": false,
                "accountCapabilities": { "urn:ietf:params:jmap:mail": mail_capabilities }
            }
        },
        "primaryAccounts": { "urn:ietf:params:jmap:mail": "acc-primary" },
        "username": "user@example.com",
        "apiUrl": format!("{base}/api"),
        "downloadUrl": format!("{base}/download/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}"),
        "uploadUrl": format!("{base}/upload/{{accountId}}/"),
        "eventSourceUrl": format!("{base}/eventsource/?types={{types}}&closeafter={{closeafter}}&ping={{ping}}"),
        "state": "session-1"
    })
}

/// A session document which additionally advertises RFC 8887 websocket
/// support.
pub fn session_json_with_websocket(base: &str, ws_url: &str, supports_push: bool) -> Value {
    let mut session = session_json(base);
    session["capabilities"]["urn:ietf:params:jmap:websocket"] = json!({
        "url": ws_url,
        "supportsPush": supports_push,
    });
    session
}

pub fn method_response(name: &str, body: Value) -> Value {
    json!({ "methodResponses": [[name, body, "s0"]], "sessionState": "session-1" })
}

/// Matches only requests whose body does NOT contain the given needle.
pub struct BodyNotContains(pub &'static str);

impl wiremock::Match for BodyNotContains {
    fn matches(&self, request: &wiremock::Request) -> bool {
        !String::from_utf8_lossy(&request.body).contains(self.0)
    }
}

/// Serves the given session document at the well-known path.
pub async fn mock_session_document(server: &MockServer, session: Value) {
    Mock::given(method("GET"))
        .and(path("/.well-known/jmap"))
        .respond_with(ResponseTemplate::new(200).set_body_json(session))
        .mount(server)
        .await;
}

/// Serves the Email/get + Mailbox/get state probes which `connect()` (and
/// the polling strategy) issue, with both states held constant.
pub async fn mock_state_probes(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/api"))
        .and(body_string_contains("Email/get"))
        .and(body_string_contains("\"ids\":[]"))
        .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
            "Email/get",
            json!({ "accountId": "acc-primary", "state": "email-state-1", "list": [], "notFound": [] }),
        )))
        .mount(server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api"))
        .and(body_string_contains("Mailbox/get"))
        .and(body_string_contains("\"ids\":[]"))
        .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
            "Mailbox/get",
            json!({ "accountId": "acc-primary", "state": "mailbox-state-1", "list": [], "notFound": [] }),
        )))
        .mount(server)
        .await;
}

/// Serves the session document and the state probes: the standard setup
/// for a connectable source.
pub async fn mock_session(server: &MockServer) {
    mock_session_document(server, session_json(&server.uri())).await;
    mock_state_probes(server).await;
}

pub fn test_source(server: &MockServer, token: &str, account: Option<&str>) -> JmapMailSource {
    JmapMailSource::from_config(&SourceConfig::Jmap {
        url: server.uri(),
        token: token.to_string(),
        account: account.map(str::to_string),
    })
}

pub async fn connected_source(
    server: &MockServer,
    account: Option<&str>,
) -> (JmapMailSource, SourceState) {
    let mut source = test_source(server, "test-token", account);
    let state = source.connect().await.expect("connect succeeds");
    (source, state)
}

/// A bare `MailClient` connected to the mock server. Unlike
/// [`connected_source`], no state probes are issued during setup.
pub async fn connected_client(server: &MockServer) -> MailClient {
    MailClient::connect(&server.uri(), "test-token", None)
        .await
        .expect("client connects")
}

pub fn email_json(id: &str, received: &str) -> Value {
    json!({
        "id": id,
        "blobId": format!("blob-{id}"),
        "threadId": format!("thread-{id}"),
        "mailboxIds": { "mb-inbox": true },
        "keywords": { "$seen": true },
        "size": 1234,
        "receivedAt": received,
        "messageId": [format!("<{id}@example.com>")],
        "subject": format!("Subject {id}"),
        "from": [{ "name": "Sender", "email": "sender@example.com" }]
    })
}
