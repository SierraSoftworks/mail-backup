//! The JMAP mail source: Fastmail and any other RFC 8620/8621 provider.
//!
//! Backups are strictly read-only against the server — this source only ever
//! issues query/get/changes/download requests.

use std::sync::atomic::AtomicBool;

use tokio_stream::{Stream, StreamExt};
use tracing_batteries::prelude::*;

use super::{ChangesResult, MailSource};
use crate::entities::mail::{
    ChangeSet, DateRange, MailboxInfo, MessageMeta, SourceNotification, SourceState,
};
use crate::errors::HumanizableError;
use crate::helpers::jmap::{
    MailClient, email_to_meta, is_anchor_not_found, is_cannot_calculate_changes,
    is_keepalive_artifact, mailbox_to_info, retry,
};
use crate::policy::SourceConfig;

/// How many message ids each Email/query page requests.
const QUERY_PAGE_SIZE: usize = 500;

/// The Email properties needed for a full-fidelity backup.
fn backup_properties() -> Vec<jmap_client::email::Property> {
    use jmap_client::email::Property;
    vec![
        Property::Id,
        Property::BlobId,
        Property::ThreadId,
        Property::MailboxIds,
        Property::Keywords,
        Property::Size,
        Property::ReceivedAt,
        Property::MessageId,
        Property::Subject,
        Property::From,
    ]
}

pub struct JmapMailSource {
    session_url: String,
    token: String,
    account: Option<String>,
    client: Option<MailClient>,
}

impl JmapMailSource {
    pub fn from_config(config: &SourceConfig) -> Self {
        Self {
            session_url: config.session_url(),
            token: config.token().to_string(),
            account: config.account().map(str::to_string),
            client: None,
        }
    }

    fn client(&self) -> Result<&MailClient, human_errors::Error> {
        self.client.as_ref().ok_or_else(|| {
            human_errors::system(
                "The mail source has not been connected yet.",
                &["This is a bug in mail-backup; please report it to us on GitHub."],
            )
        })
    }

    /// The server's current Email state string (an Email/get with no ids).
    async fn email_state(&self) -> Result<String, human_errors::Error> {
        let client = self.client()?;
        retry("Fetching the mail state", || async {
            let mut request = client.inner().build();
            request.get_email().ids(Vec::<String>::new());
            request
                .send_single::<jmap_client::core::response::EmailGetResponse>()
                .await
                .map(|mut r| r.take_state())
        })
        .await
        .map_err(|e| e.to_human_error())
    }

    /// The server's current Mailbox state string.
    async fn mailbox_state(&self) -> Result<String, human_errors::Error> {
        let client = self.client()?;
        retry("Fetching the mailbox state", || async {
            let mut request = client.inner().build();
            request.get_mailbox().ids(Vec::<String>::new());
            request
                .send_single::<jmap_client::core::response::MailboxGetResponse>()
                .await
                .map(|mut r| r.take_state())
        })
        .await
        .map_err(|e| e.to_human_error())
    }

    /// One page of an Email/query enumeration sorted by receivedAt ascending.
    async fn query_page(
        &self,
        range: &DateRange,
        anchor: Option<&str>,
        position: i32,
    ) -> Result<Vec<String>, jmap_client::Error> {
        let client = self.client().expect("checked by callers");
        let mut request = client.inner().build();
        {
            let query = request.query_email();

            let mut conditions = Vec::new();
            if let Some(start) = range.start {
                // JMAP `after` is inclusive of the given UTCDate.
                conditions.push(jmap_client::email::query::Filter::after(start.timestamp()));
            }
            if let Some(end) = range.end {
                conditions.push(jmap_client::email::query::Filter::before(
                    end.timestamp() + 1,
                ));
            }
            if !conditions.is_empty() {
                query.filter(jmap_client::core::query::Filter::and(conditions));
            }

            query.sort([jmap_client::email::query::Comparator::received_at().ascending()]);
            match anchor {
                Some(anchor) => {
                    query.anchor(anchor).anchor_offset(1);
                }
                None => {
                    query.position(position);
                }
            }
            query.limit(QUERY_PAGE_SIZE);
            query.calculate_total(false);
        }

        request
            .send_single::<jmap_client::core::query::QueryResponse>()
            .await
            .map(|mut r| r.take_ids())
    }

    /// Fetches full backup metadata for up to `maxObjectsInGet` ids.
    async fn get_message_chunk(
        &self,
        ids: &[String],
    ) -> Result<Vec<MessageMeta>, human_errors::Error> {
        let client = self.client()?;
        let emails = retry("Fetching message metadata", || async {
            let mut request = client.inner().build();
            request
                .get_email()
                .ids(ids.iter().cloned())
                .properties(backup_properties());
            request
                .send_single::<jmap_client::core::response::EmailGetResponse>()
                .await
                .map(|mut r| r.take_list())
        })
        .await
        .map_err(|e| e.to_human_error())?;

        emails.iter().map(email_to_meta).collect()
    }
}

impl MailSource for JmapMailSource {
    fn kind(&self) -> &'static str {
        "jmap/mail"
    }

    async fn connect(&mut self) -> Result<SourceState, human_errors::Error> {
        let client =
            MailClient::connect(&self.session_url, &self.token, self.account.as_deref()).await?;
        info!(
            "Connected to {} as account {}",
            self.session_url,
            client.account_id()
        );
        self.client = Some(client);

        Ok(SourceState {
            account_id: self.client()?.account_id().to_string(),
            email_state: Some(self.email_state().await?),
            mailbox_state: Some(self.mailbox_state().await?),
        })
    }

    async fn list_mailboxes(&self) -> Result<Vec<MailboxInfo>, human_errors::Error> {
        let client = self.client()?;
        let mailboxes = retry("Listing mailboxes", || async {
            let mut request = client.inner().build();
            request.get_mailbox().properties([
                jmap_client::mailbox::Property::Id,
                jmap_client::mailbox::Property::Name,
                jmap_client::mailbox::Property::ParentId,
                jmap_client::mailbox::Property::Role,
                jmap_client::mailbox::Property::SortOrder,
            ]);
            request
                .send_single::<jmap_client::core::response::MailboxGetResponse>()
                .await
                .map(|mut r| r.take_list())
        })
        .await
        .map_err(|e| e.to_human_error())?;

        Ok(mailboxes.iter().map(mailbox_to_info).collect())
    }

    fn enumerate<'a>(
        &'a self,
        range: DateRange,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<MessageMeta, human_errors::Error>> + 'a {
        async_stream::try_stream! {
            let client = self.client()?;
            let chunk_size = client.max_objects_in_get();

            let mut anchor: Option<String> = None;
            let mut position: i32 = 0;

            loop {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

                let ids = match retry("Enumerating messages", || {
                    self.query_page(&range, anchor.as_deref(), position)
                })
                .await
                {
                    Ok(ids) => ids,
                    Err(e) if is_anchor_not_found(&e) && anchor.is_some() => {
                        // The anchor message was deleted mid-enumeration; fall
                        // back to positional paging (idempotent application
                        // absorbs any overlap).
                        debug!("Query anchor disappeared; falling back to positional paging");
                        anchor = None;
                        continue;
                    }
                    Err(e) => Err(e.to_human_error())?,
                };

                if ids.is_empty() {
                    break;
                }

                anchor = ids.last().cloned();
                position += ids.len() as i32;

                for chunk in ids.chunks(chunk_size) {
                    let metas = self.get_message_chunk(chunk).await?;
                    for meta in metas {
                        yield meta;
                    }
                }
            }
        }
    }

    async fn changes(&self, since: &SourceState) -> Result<ChangesResult, human_errors::Error> {
        let client = self.client()?;

        let Some(email_state) = since.email_state.clone() else {
            return Ok(ChangesResult::StateTooOld);
        };

        let email_changes = match retry("Fetching mail changes", || {
            client.inner().email_changes(email_state.clone(), None)
        })
        .await
        {
            Ok(changes) => changes,
            Err(e) if is_cannot_calculate_changes(&e) => return Ok(ChangesResult::StateTooOld),
            Err(e) => return Err(e.to_human_error()),
        };

        // Mailbox changes carry no payload we use directly — any change just
        // triggers a full mailbox re-list — so only the flag and the new
        // state matter here.
        let mut mailboxes_changed = false;
        let mailbox_state = match since.mailbox_state.clone() {
            None => {
                mailboxes_changed = true;
                Some(self.mailbox_state().await?)
            }
            Some(state) => {
                let mut current = state;
                loop {
                    // Built by hand rather than via the `mailbox_changes`
                    // convenience method: that method always sends a
                    // `maxChanges` argument, and RFC 8620 requires it to be a
                    // *positive* integer when present (Fastmail rejects 0).
                    match retry("Fetching mailbox changes", || async {
                        let mut request = client.inner().build();
                        request.changes_mailbox(current.clone());
                        request.send_changes_mailbox().await
                    })
                    .await
                    {
                        Ok(changes) => {
                            mailboxes_changed |= !changes.created().is_empty()
                                || !changes.updated().is_empty()
                                || !changes.destroyed().is_empty();
                            current = changes.new_state().to_string();
                            if !changes.has_more_changes() {
                                break Some(current);
                            }
                        }
                        Err(e) if is_cannot_calculate_changes(&e) => {
                            mailboxes_changed = true;
                            break Some(self.mailbox_state().await?);
                        }
                        Err(e) => return Err(e.to_human_error()),
                    }
                }
            }
        };

        Ok(ChangesResult::Changes(ChangeSet {
            state: SourceState {
                account_id: since.account_id.clone(),
                email_state: Some(email_changes.new_state().to_string()),
                mailbox_state,
            },
            mailboxes_changed,
            created: email_changes.created().to_vec(),
            updated: email_changes.updated().to_vec(),
            destroyed: email_changes.destroyed().to_vec(),
            has_more: email_changes.has_more_changes(),
        }))
    }

    async fn get_messages(&self, ids: &[String]) -> Result<Vec<MessageMeta>, human_errors::Error> {
        let chunk_size = self.client()?.max_objects_in_get();
        let mut metas = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(chunk_size) {
            metas.extend(self.get_message_chunk(chunk).await?);
        }
        Ok(metas)
    }

    async fn fetch_blob(
        &self,
        blob_id: &str,
        _cancel: &AtomicBool,
    ) -> Result<Vec<u8>, human_errors::Error> {
        let client = self.client()?;
        retry("Downloading a message", || client.inner().download(blob_id))
            .await
            .map_err(|e| e.to_human_error())
    }

    fn events<'a>(
        &'a self,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<SourceNotification, human_errors::Error>> + 'a {
        async_stream::stream! {
            let client = match self.client() {
                Ok(client) => client,
                Err(e) => {
                    yield Err(e);
                    return;
                }
            };

            let stream = match client
                .inner()
                .event_source(
                    Some(vec![
                        jmap_client::DataType::Email,
                        jmap_client::DataType::Mailbox,
                    ]),
                    false,
                    Some(30),
                    None,
                )
                .await
            {
                Ok(stream) => stream,
                Err(e) => {
                    yield Err(e.to_human_error());
                    return;
                }
            };

            tokio::pin!(stream);

            while let Some(event) = stream.next().await {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }

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
                    Err(e) => {
                        yield Err(e.to_human_error());
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn session_json(base: &str) -> serde_json::Value {
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

    fn method_response(name: &str, body: serde_json::Value) -> serde_json::Value {
        json!({ "methodResponses": [[name, body, "s0"]], "sessionState": "session-1" })
    }

    /// Matches only requests whose body does NOT contain the given needle.
    struct BodyNotContains(&'static str);

    impl wiremock::Match for BodyNotContains {
        fn matches(&self, request: &wiremock::Request) -> bool {
            !String::from_utf8_lossy(&request.body).contains(self.0)
        }
    }

    /// Serves the session document and the Email/get + Mailbox/get state
    /// probes which `connect()` issues.
    async fn mock_session(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/.well-known/jmap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(&server.uri())))
            .mount(server)
            .await;

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

    async fn connected_source(
        server: &MockServer,
        account: Option<&str>,
    ) -> (JmapMailSource, SourceState) {
        let mut source = JmapMailSource {
            session_url: server.uri(),
            token: "test-token".to_string(),
            account: account.map(str::to_string),
            client: None,
        };
        let state = source.connect().await.expect("connect succeeds");
        (source, state)
    }

    fn email_json(id: &str, received: &str) -> serde_json::Value {
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

    #[tokio::test]
    async fn connect_uses_bearer_token_and_resolves_accounts() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        // Session fetch must carry the bearer token.
        Mock::given(method("GET"))
            .and(path("/.well-known/jmap"))
            .and(header("Authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(&server.uri())))
            .mount(&server)
            .await;

        // Default: the primary mail account.
        let (_, state) = connected_source(&server, None).await;
        assert_eq!(state.account_id, "acc-primary");
        assert_eq!(state.email_state.as_deref(), Some("email-state-1"));
        assert_eq!(state.mailbox_state.as_deref(), Some("mailbox-state-1"));

        // Selection by account name (e-mail address).
        let (_, state) = connected_source(&server, Some("other@example.com")).await;
        assert_eq!(state.account_id, "acc-other");

        // Selection by account id.
        let (_, state) = connected_source(&server, Some("acc-other")).await;
        assert_eq!(state.account_id, "acc-other");
    }

    #[tokio::test]
    async fn connect_rejects_unknown_account() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        let mut source = JmapMailSource {
            session_url: server.uri(),
            token: "test-token".to_string(),
            account: Some("missing@example.com".to_string()),
            client: None,
        };
        let error = source.connect().await.expect_err("unknown account fails");
        assert!(error.to_string().contains("missing@example.com"));
    }

    #[tokio::test]
    async fn connect_explains_authentication_failures() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/.well-known/jmap"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;

        let mut source = JmapMailSource {
            session_url: server.uri(),
            token: "bad-token".to_string(),
            account: None,
            client: None,
        };
        let error = source.connect().await.expect_err("401 fails");
        let message = format!("{error}");
        assert!(
            message.contains("credentials") || message.contains("token"),
            "got: {message}"
        );
    }

    #[tokio::test]
    async fn list_mailboxes_converts_models() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Mailbox/get"))
            .and(body_string_contains("properties"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Mailbox/get",
                json!({
                    "accountId": "acc-primary",
                    "state": "mailbox-state-1",
                    "list": [
                        { "id": "mb-inbox", "name": "Inbox", "role": "inbox", "parentId": null, "sortOrder": 1 },
                        { "id": "mb-sub", "name": "Receipts", "role": null, "parentId": "mb-inbox", "sortOrder": 7 }
                    ],
                    "notFound": []
                }),
            )))
            .mount(&server)
            .await;

        let (source, _) = connected_source(&server, None).await;
        let mailboxes = source.list_mailboxes().await.unwrap();
        assert_eq!(mailboxes.len(), 2);
        assert_eq!(
            mailboxes[0],
            MailboxInfo {
                id: "mb-inbox".to_string(),
                name: "Inbox".to_string(),
                role: Some("inbox".to_string()),
                parent_id: None,
                sort_order: 1,
            }
        );
        assert_eq!(mailboxes[1].parent_id.as_deref(), Some("mb-inbox"));
    }

    #[tokio::test]
    async fn enumerate_pages_through_query_results() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        // First query page returns two ids; every later page is empty.
        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/query",
                json!({
                    "accountId": "acc-primary",
                    "queryState": "q1",
                    "canCalculateChanges": true,
                    "position": 0,
                    "ids": ["M1", "M2"]
                }),
            )))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/query"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/query",
                json!({
                    "accountId": "acc-primary",
                    "queryState": "q1",
                    "canCalculateChanges": true,
                    "position": 2,
                    "ids": []
                }),
            )))
            .with_priority(2)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/get"))
            .and(body_string_contains("\"M1\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/get",
                json!({
                    "accountId": "acc-primary",
                    "state": "email-state-1",
                    "list": [
                        email_json("M1", "2023-01-01T08:00:00Z"),
                        email_json("M2", "2023-01-02T09:30:00Z")
                    ],
                    "notFound": []
                }),
            )))
            .mount(&server)
            .await;

        let (source, _) = connected_source(&server, None).await;
        let cancel = AtomicBool::new(false);
        let stream = source.enumerate(DateRange::all(), &cancel);
        tokio::pin!(stream);

        let mut metas = Vec::new();
        while let Some(meta) = stream.next().await {
            metas.push(meta.unwrap());
        }

        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].id, "M1");
        assert_eq!(metas[0].blob_id, "blob-M1");
        assert_eq!(
            metas[0].mailbox_ids.iter().collect::<Vec<_>>(),
            ["mb-inbox"]
        );
        assert!(metas[0].keywords.contains("$seen"));
        assert_eq!(
            metas[0].received_at,
            chrono::DateTime::parse_from_rfc3339("2023-01-01T08:00:00Z").unwrap()
        );
        assert_eq!(metas[0].size, 1234);
        assert_eq!(metas[0].subject.as_deref(), Some("Subject M1"));
        assert_eq!(metas[0].from, vec!["sender@example.com".to_string()]);
        assert_eq!(metas[1].id, "M2");
    }

    #[tokio::test]
    async fn changes_returns_changeset() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/changes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/changes",
                json!({
                    "accountId": "acc-primary",
                    "oldState": "email-state-1",
                    "newState": "email-state-2",
                    "hasMoreChanges": false,
                    "created": ["M9"],
                    "updated": ["M1"],
                    "destroyed": ["M2"]
                }),
            )))
            .mount(&server)
            .await;

        // RFC 8620 forbids `maxChanges: 0`; a request carrying a maxChanges
        // argument here goes unmatched and fails the test.
        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Mailbox/changes"))
            .and(BodyNotContains("maxChanges"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Mailbox/changes",
                json!({
                    "accountId": "acc-primary",
                    "oldState": "mailbox-state-1",
                    "newState": "mailbox-state-2",
                    "hasMoreChanges": false,
                    "created": ["mb-new"],
                    "updated": [],
                    "destroyed": []
                }),
            )))
            .mount(&server)
            .await;

        let (source, state) = connected_source(&server, None).await;
        let result = source.changes(&state).await.unwrap();

        match result {
            ChangesResult::Changes(changes) => {
                assert_eq!(changes.created, vec!["M9".to_string()]);
                assert_eq!(changes.updated, vec!["M1".to_string()]);
                assert_eq!(changes.destroyed, vec!["M2".to_string()]);
                assert!(changes.mailboxes_changed);
                assert!(!changes.has_more);
                assert_eq!(changes.state.email_state.as_deref(), Some("email-state-2"));
                assert_eq!(
                    changes.state.mailbox_state.as_deref(),
                    Some("mailbox-state-2")
                );
            }
            other => panic!("expected changes, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn changes_state_too_old_triggers_reconcile_signal() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/changes"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "methodResponses": [["error", { "type": "cannotCalculateChanges" }, "s0"]],
                "sessionState": "session-1"
            })))
            .mount(&server)
            .await;

        let (source, state) = connected_source(&server, None).await;
        let result = source.changes(&state).await.unwrap();
        assert!(matches!(result, ChangesResult::StateTooOld));
    }

    #[tokio::test]
    async fn event_stream_survives_keepalive_comments() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        // Comment keep-alives (which Fastmail sends) trip a bug in
        // jmap-client's SSE parser: it dispatches the resulting empty events
        // instead of ignoring them, and they fail JSON parsing. They must
        // surface as pings, not errors, or the daemon tears down a healthy
        // connection on every keep-alive.
        let body = concat!(
            ": connected\n\n",
            "event: state\nid: e1\ndata: {\"@type\":\"StateChange\",\"changed\":{\"acc-primary\":{\"Email\":\"es-2\"}}}\n\n",
            ":\n\n",
            "event: state\ndata: {\"@type\":\"StateChange\",\"changed\":{\"acc-primary\":{\"Mailbox\":\"ms-2\"}}}\n\n",
        );
        Mock::given(method("GET"))
            .and(path("/eventsource/"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(body.as_bytes(), "text/event-stream"),
            )
            .mount(&server)
            .await;

        let (source, _) = connected_source(&server, None).await;
        let cancel = AtomicBool::new(false);
        let stream = source.events(&cancel);
        tokio::pin!(stream);

        let mut notifications = Vec::new();
        while let Some(item) = stream.next().await {
            notifications.push(item.expect("keep-alives must not surface as stream errors"));
        }

        assert!(
            notifications.contains(&SourceNotification::Changed {
                email: true,
                mailbox: false
            }),
            "got: {notifications:?}"
        );
        assert!(
            notifications.contains(&SourceNotification::Changed {
                email: false,
                mailbox: true
            }),
            "got: {notifications:?}"
        );
        assert!(
            notifications.contains(&SourceNotification::Ping),
            "keep-alive artifacts become pings: {notifications:?}"
        );
    }

    #[tokio::test]
    async fn fetch_blob_downloads_raw_bytes() {
        let server = MockServer::start().await;
        mock_session(&server).await;

        Mock::given(method("GET"))
            .and(path("/download/acc-primary/blob-M1/none"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"raw rfc5322 bytes".to_vec()))
            .mount(&server)
            .await;

        let (source, _) = connected_source(&server, None).await;
        let cancel = AtomicBool::new(false);
        let raw = source.fetch_blob("blob-M1", &cancel).await.unwrap();
        assert_eq!(raw, b"raw rfc5322 bytes");
    }
}
