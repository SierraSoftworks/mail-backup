//! The JMAP restore target: recreates mailboxes via Mailbox/set and imports
//! raw messages via blob upload + Email/import, preserving keywords, mailbox
//! memberships, and the original receivedAt timestamps.

use tracing_batteries::prelude::*;

use super::RestoreTarget;
use crate::entities::mail::{MailboxInfo, MessageMeta};
use crate::errors::HumanizableError;
use crate::helpers::jmap::{MailClient, mailbox_to_info, retry};
use crate::policy::SourceConfig;

pub struct JmapRestoreTarget {
    session_url: String,
    token: String,
    account: Option<String>,
    client: Option<MailClient>,
}

impl JmapRestoreTarget {
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
                "The restore target has not been connected yet.",
                &["This is a bug in mail-backup; please report it to us on GitHub."],
            )
        })
    }
}

fn role_from_string(role: Option<&str>) -> jmap_client::mailbox::Role {
    use jmap_client::mailbox::Role;
    match role.map(|r| r.to_ascii_lowercase()).as_deref() {
        None => Role::None,
        Some("archive") => Role::Archive,
        Some("drafts") => Role::Drafts,
        Some("important") => Role::Important,
        Some("inbox") => Role::Inbox,
        Some("junk") => Role::Junk,
        Some("sent") => Role::Sent,
        Some("trash") => Role::Trash,
        Some(other) => Role::Other(other.to_string()),
    }
}

impl RestoreTarget for JmapRestoreTarget {
    fn kind(&self) -> &'static str {
        "jmap/mail"
    }

    async fn connect(&mut self) -> Result<String, human_errors::Error> {
        let client =
            MailClient::connect(&self.session_url, &self.token, self.account.as_deref()).await?;
        info!(
            "Connected to {} as account {}",
            self.session_url,
            client.account_id()
        );
        self.client = Some(client);
        Ok(self.client()?.account_id().to_string())
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

    async fn create_mailbox(
        &mut self,
        name: &str,
        parent_id: Option<&str>,
        role: Option<&str>,
    ) -> Result<String, human_errors::Error> {
        let client = self.client()?;
        let mut mailbox = retry("Creating a mailbox", || {
            client
                .inner()
                .mailbox_create(name, parent_id, role_from_string(role))
        })
        .await
        .map_err(|e| e.to_human_error())?;

        let id = mailbox.take_id();
        debug!("Created mailbox {} ({})", name, id);
        Ok(id)
    }

    async fn message_exists(&self, meta: &MessageMeta) -> Result<bool, human_errors::Error> {
        let client = self.client()?;

        // Prefer the Message-ID header (globally unique by convention); fall
        // back to a receivedAt+size probe for messages without one.
        let filter = match meta.message_id.first() {
            Some(message_id) => jmap_client::core::query::Filter::and(vec![
                jmap_client::email::query::Filter::header("Message-ID", Some(message_id.clone())),
            ]),
            None => jmap_client::core::query::Filter::and(vec![
                jmap_client::email::query::Filter::after(meta.received_at.timestamp() - 1),
                jmap_client::email::query::Filter::before(meta.received_at.timestamp() + 1),
                jmap_client::email::query::Filter::min_size(meta.size as u32),
                jmap_client::email::query::Filter::max_size(meta.size as u32),
            ]),
        };

        let ids = retry("Checking for an existing message", || {
            let filter = filter.clone();
            async {
                let mut request = client.inner().build();
                {
                    let query = request.query_email();
                    query.filter(filter);
                    query.limit(1);
                    query.calculate_total(false);
                }
                request
                    .send_single::<jmap_client::core::query::QueryResponse>()
                    .await
                    .map(|mut r| r.take_ids())
            }
        })
        .await
        .map_err(|e| e.to_human_error())?;

        Ok(!ids.is_empty())
    }

    async fn import(
        &mut self,
        raw: Vec<u8>,
        mailbox_ids: Vec<String>,
        keywords: Vec<String>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<String, human_errors::Error> {
        let client = self.client()?;

        // Always pass receivedAt explicitly: some servers default it to the
        // import time when omitted, destroying the original date.
        // No automatic retry here — an import retried after an ambiguous
        // failure could duplicate the message; re-running the restore (with
        // dedupe) is the safe retry path.
        let mut email = client
            .inner()
            .email_import(
                raw,
                mailbox_ids,
                Some(keywords),
                Some(received_at.timestamp()),
            )
            .await
            .map_err(|e| e.to_human_error())?;

        Ok(email.take_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_string_contains, method, path};
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
                "acc-1": {
                    "name": "user@example.com",
                    "isPersonal": true,
                    "isReadOnly": false,
                    "accountCapabilities": { "urn:ietf:params:jmap:mail": mail_capabilities }
                }
            },
            "primaryAccounts": { "urn:ietf:params:jmap:mail": "acc-1" },
            "username": "user@example.com",
            "apiUrl": format!("{base}/api"),
            "downloadUrl": format!("{base}/download/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}"),
            "uploadUrl": format!("{base}/upload/{{accountId}}/"),
            "eventSourceUrl": format!("{base}/eventsource/?types={{types}}&closeafter={{closeafter}}&ping={{ping}}"),
            "state": "session-1"
        })
    }

    fn method_response(name: &str, body: serde_json::Value, call_id: &str) -> serde_json::Value {
        json!({ "methodResponses": [[name, body, call_id]], "sessionState": "session-1" })
    }

    async fn connected_target(server: &MockServer) -> JmapRestoreTarget {
        Mock::given(method("GET"))
            .and(path("/.well-known/jmap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session_json(&server.uri())))
            .mount(server)
            .await;

        let mut target = JmapRestoreTarget {
            session_url: server.uri(),
            token: "token".to_string(),
            account: None,
            client: None,
        };
        assert_eq!(target.connect().await.unwrap(), "acc-1");
        target
    }

    #[tokio::test]
    async fn create_mailbox_issues_mailbox_set() {
        let server = MockServer::start().await;
        let mut target = connected_target(&server).await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Mailbox/set"))
            .and(body_string_contains("\"name\":\"Receipts\""))
            .and(body_string_contains("\"parentId\":\"mb-archive\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Mailbox/set",
                json!({
                    "accountId": "acc-1",
                    "oldState": "m1",
                    "newState": "m2",
                    "created": { "c0": { "id": "mb-new", "sortOrder": 0 } }
                }),
                "s0",
            )))
            .mount(&server)
            .await;

        let id = target
            .create_mailbox("Receipts", Some("mb-archive"), None)
            .await
            .unwrap();
        assert_eq!(id, "mb-new");
    }

    #[tokio::test]
    async fn import_uploads_blob_and_preserves_metadata() {
        let server = MockServer::start().await;
        let mut target = connected_target(&server).await;

        Mock::given(method("POST"))
            .and(path("/upload/acc-1/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "accountId": "acc-1",
                "blobId": "B99",
                "type": "application/octet-stream",
                "size": 11
            })))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/import"))
            .and(body_string_contains("\"blobId\":\"B99\""))
            .and(body_string_contains("mb-inbox"))
            .and(body_string_contains("$seen"))
            .and(body_string_contains("\"receivedAt\":\"2023-01-02T09:00:00Z\""))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/import",
                json!({
                    "accountId": "acc-1",
                    "oldState": "e1",
                    "newState": "e2",
                    "created": { "i0": { "id": "M-new", "blobId": "B99", "threadId": "T1", "size": 11 } }
                }),
                "s0",
            )))
            .mount(&server)
            .await;

        let id = target
            .import(
                b"raw message".to_vec(),
                vec!["mb-inbox".to_string()],
                vec!["$seen".to_string()],
                chrono::DateTime::parse_from_rfc3339("2023-01-02T09:00:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
            )
            .await
            .unwrap();
        assert_eq!(id, "M-new");
    }

    #[tokio::test]
    async fn message_exists_queries_by_message_id_header() {
        let server = MockServer::start().await;
        let target = connected_target(&server).await;

        Mock::given(method("POST"))
            .and(path("/api"))
            .and(body_string_contains("Email/query"))
            .and(body_string_contains("Message-ID"))
            .respond_with(ResponseTemplate::new(200).set_body_json(method_response(
                "Email/query",
                json!({
                    "accountId": "acc-1",
                    "queryState": "q1",
                    "canCalculateChanges": false,
                    "position": 0,
                    "ids": ["M-existing"]
                }),
                "s0",
            )))
            .mount(&server)
            .await;

        let meta = MessageMeta {
            id: "M1".to_string(),
            blob_id: "B1".to_string(),
            thread_id: "T1".to_string(),
            mailbox_ids: Default::default(),
            keywords: Default::default(),
            received_at: chrono::Utc::now(),
            size: 10,
            message_id: vec!["<M1@example.com>".to_string()],
            subject: None,
            from: vec![],
        };
        assert!(target.message_exists(&meta).await.unwrap());
    }
}
