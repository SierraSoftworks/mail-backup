use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use super::Metadata;
use crate::{FilterValue, Filterable};

/// A mailbox (folder) as it exists on the mail server.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxInfo {
    pub id: String,
    /// The true (unsanitized) name of this mailbox as it appears on the server.
    pub name: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub sort_order: u32,
}

/// The metadata for a single message, as needed to back it up and restore it
/// with full fidelity. The raw RFC5322 content is stored separately.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMeta {
    /// The server-assigned (JMAP) id of this message.
    pub id: String,
    pub blob_id: String,
    pub thread_id: String,
    /// Every mailbox this message belongs to (it lives in exactly one
    /// directory on disk — its primary mailbox — but membership of all
    /// mailboxes is preserved here).
    pub mailbox_ids: BTreeSet<String>,
    /// JMAP keywords such as `$seen`, `$flagged`, `$draft`, `$answered`,
    /// along with any custom keywords.
    pub keywords: BTreeSet<String>,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    /// The RFC5322 Message-ID header values of this message.
    #[serde(default)]
    pub message_id: Vec<String>,
    #[serde(default)]
    pub subject: Option<String>,
    /// The email addresses of the message's senders.
    #[serde(default)]
    pub from: Vec<String>,
}

impl MessageMeta {
    /// The UTC day this message was received on, used to group backfilled
    /// mail into daily snapshot commits.
    pub fn received_day(&self) -> chrono::NaiveDate {
        self.received_at.date_naive()
    }
}

/// A record of a mailbox known to the local store, along with the directory
/// it has been assigned. The assigned directory is decided once, when the
/// mailbox is first seen, and persisted — it never changes due to
/// re-sanitization (only due to a rename on the server).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MailboxRecord {
    pub info: MailboxInfo,
    /// The repository-relative, `/`-separated directory path holding this
    /// mailbox's messages (each segment is Windows-safe sanitized).
    pub dir_path: String,
}

/// An index of every mailbox known to the local store, keyed by mailbox id.
#[derive(Clone, Debug, Default)]
pub struct MailboxIndex {
    boxes: HashMap<String, MailboxRecord>,
}

impl MailboxIndex {
    pub fn insert(&mut self, record: MailboxRecord) {
        self.boxes.insert(record.info.id.clone(), record);
    }

    pub fn remove(&mut self, id: &str) -> Option<MailboxRecord> {
        self.boxes.remove(id)
    }

    pub fn get(&self, id: &str) -> Option<&MailboxRecord> {
        self.boxes.get(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = &MailboxRecord> {
        self.boxes.values()
    }

    pub fn len(&self) -> usize {
        self.boxes.len()
    }

    /// The full human-readable name path of a mailbox (e.g. `Archive/2023`),
    /// built from the true mailbox names by walking the parent chain.
    pub fn name_path(&self, id: &str) -> Option<String> {
        let mut segments = Vec::new();
        let mut current = self.boxes.get(id)?;
        segments.push(current.info.name.clone());

        // Guard against parent-chain cycles by bounding the walk.
        for _ in 0..self.boxes.len() {
            match current
                .info
                .parent_id
                .as_deref()
                .and_then(|p| self.boxes.get(p))
            {
                Some(parent) => {
                    segments.push(parent.info.name.clone());
                    current = parent;
                }
                None => break,
            }
        }

        segments.reverse();
        Some(segments.join("/"))
    }

    /// Updates the directory path of every mailbox under `old_prefix` after a
    /// directory rename, returning the affected mailbox ids.
    pub fn rebase_dir_paths(&mut self, old_prefix: &str, new_prefix: &str) -> Vec<String> {
        let mut affected = Vec::new();
        for record in self.boxes.values_mut() {
            if record.dir_path == old_prefix {
                record.dir_path = new_prefix.to_string();
                affected.push(record.info.id.clone());
            } else if let Some(rest) = record.dir_path.strip_prefix(&format!("{old_prefix}/")) {
                record.dir_path = format!("{new_prefix}/{rest}");
                affected.push(record.info.id.clone());
            }
        }
        affected
    }
}

/// A message together with the filterable metadata view of it. This is the
/// object which filter expressions are evaluated against, for both backups
/// and restores.
#[derive(Clone, Debug)]
pub struct MailMessage {
    pub meta: MessageMeta,
    metadata: Metadata,
}

impl MailMessage {
    pub fn new(meta: MessageMeta, mailboxes: &MailboxIndex) -> Self {
        let mut metadata = Metadata::default();
        meta.inject_metadata_with(&mut metadata, mailboxes);
        Self { meta, metadata }
    }
}

impl MessageMeta {
    fn inject_metadata_with(&self, metadata: &mut Metadata, mailboxes: &MailboxIndex) {
        metadata.insert("message.id", self.id.clone());
        metadata.insert("message.thread", self.thread_id.clone());
        metadata.insert("message.blob", self.blob_id.clone());

        let mut paths: Vec<String> = self
            .mailbox_ids
            .iter()
            .filter_map(|id| mailboxes.name_path(id))
            .collect();
        paths.sort();

        if let Some(primary) = crate::stores::layout::primary_mailbox(&self.mailbox_ids, mailboxes)
            && let Some(path) = mailboxes.name_path(&primary.info.id)
        {
            metadata.insert("message.mailbox", path);
        }

        metadata.insert(
            "message.mailboxes",
            paths.into_iter().map(FilterValue::from).collect::<Vec<_>>(),
        );

        let keywords: Vec<FilterValue<'static>> = self
            .keywords
            .iter()
            .map(|k| FilterValue::from(k.clone()))
            .collect();
        metadata.insert("message.keyword", keywords.clone());
        metadata.insert("message.keywords", keywords);

        metadata.insert(
            "message.received",
            self.received_at
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        );
        metadata.insert(
            "message.date",
            self.received_at.format("%Y-%m-%d").to_string(),
        );
        metadata.insert("message.size", self.size as u32);

        if let Some(subject) = &self.subject {
            metadata.insert("message.subject", subject.clone());
        }
        metadata.insert(
            "message.from",
            self.from
                .iter()
                .map(|f| FilterValue::from(f.clone()))
                .collect::<Vec<_>>(),
        );
    }
}

impl Filterable for MailMessage {
    fn get(&self, key: &str) -> FilterValue<'_> {
        self.metadata.get(key)
    }
}

impl std::fmt::Display for MailMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.meta.subject {
            Some(subject) => write!(
                f,
                "{} {} ({})",
                self.meta.received_at.format("%Y-%m-%d %H:%M:%S"),
                self.meta.id,
                subject
            ),
            None => write!(
                f,
                "{} {}",
                self.meta.received_at.format("%Y-%m-%d %H:%M:%S"),
                self.meta.id
            ),
        }
    }
}

/// A single change which a [`crate::stores::MailStore`] should apply to its
/// local copy of the mail account.
#[derive(Debug)]
pub enum MailEvent {
    /// A mailbox was created or its name/parent/role changed.
    MailboxUpserted(MailboxInfo),
    /// A mailbox was deleted on the server.
    MailboxDeleted { id: String },
    /// A new message arrived (the raw RFC5322 content is provided).
    MessageAdded { message: MailMessage, raw: Vec<u8> },
    /// A message's mutable state (keywords and/or mailbox membership) changed.
    MessageUpdated { message: MailMessage },
    /// A message was deleted on the server.
    MessageDeleted { id: String },
}

/// The synchronization cursor for a mail source, identifying both the account
/// and the last fully-applied server state.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SourceState {
    pub account_id: String,
    #[serde(default)]
    pub email_state: Option<String>,
    #[serde(default)]
    pub mailbox_state: Option<String>,
}

/// One page of changes reported by a mail source since a previous state.
#[derive(Clone, Debug, Default)]
pub struct ChangeSet {
    /// The new cursor after applying this page of changes.
    pub state: SourceState,
    /// Whether any mailbox-level changes occurred (created/updated/destroyed).
    pub mailboxes_changed: bool,
    pub created: Vec<String>,
    pub updated: Vec<String>,
    pub destroyed: Vec<String>,
    /// Whether another page of changes should be requested.
    pub has_more: bool,
}

/// A notification from a mail source's real-time event stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceNotification {
    /// Server state changed for the given data types.
    Changed { email: bool, mailbox: bool },
    /// A keep-alive ping; no action needed.
    Ping,
}

/// An inclusive range of received-at timestamps used to bound enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateRange {
    pub start: Option<chrono::DateTime<chrono::Utc>>,
    pub end: Option<chrono::DateTime<chrono::Utc>>,
}

impl DateRange {
    pub fn all() -> Self {
        Self {
            start: None,
            end: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Filter;
    use rstest::rstest;

    pub fn mailbox(id: &str, name: &str, role: Option<&str>, parent: Option<&str>) -> MailboxInfo {
        MailboxInfo {
            id: id.to_string(),
            name: name.to_string(),
            role: role.map(str::to_string),
            parent_id: parent.map(str::to_string),
            sort_order: 0,
        }
    }

    fn test_index() -> MailboxIndex {
        let mut index = MailboxIndex::default();
        index.insert(MailboxRecord {
            info: mailbox("mb-inbox", "Inbox", Some("inbox"), None),
            dir_path: "Inbox".to_string(),
        });
        index.insert(MailboxRecord {
            info: mailbox("mb-archive", "Archive", Some("archive"), None),
            dir_path: "Archive".to_string(),
        });
        index.insert(MailboxRecord {
            info: mailbox("mb-2023", "2023", None, Some("mb-archive")),
            dir_path: "Archive/2023".to_string(),
        });
        index
    }

    fn test_message() -> MailMessage {
        MailMessage::new(
            MessageMeta {
                id: "M123".to_string(),
                blob_id: "G456".to_string(),
                thread_id: "T789".to_string(),
                mailbox_ids: ["mb-inbox", "mb-2023"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                keywords: ["$seen"].iter().map(|s| s.to_string()).collect(),
                received_at: chrono::DateTime::parse_from_rfc3339("2023-06-15T10:30:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                size: 2048,
                message_id: vec!["<abc@example.com>".to_string()],
                subject: Some("Quarterly invoice".to_string()),
                from: vec!["billing@example.com".to_string()],
            },
            &test_index(),
        )
    }

    #[test]
    fn name_path_resolution() {
        let index = test_index();
        assert_eq!(index.name_path("mb-2023").as_deref(), Some("Archive/2023"));
        assert_eq!(index.name_path("mb-inbox").as_deref(), Some("Inbox"));
        assert_eq!(index.name_path("mb-missing"), None);
    }

    #[test]
    fn rebase_dir_paths_moves_subtree() {
        let mut index = test_index();
        let affected = index.rebase_dir_paths("Archive", "Stored");
        assert_eq!(affected.len(), 2);
        assert_eq!(index.get("mb-archive").unwrap().dir_path, "Stored");
        assert_eq!(index.get("mb-2023").unwrap().dir_path, "Stored/2023");
        assert_eq!(index.get("mb-inbox").unwrap().dir_path, "Inbox");
    }

    #[rstest]
    #[case("message.id == \"M123\"", true)]
    #[case("message.mailbox == \"Inbox\"", true)] // primary: inbox role outranks others
    #[case("message.mailboxes contains \"Archive/2023\"", true)]
    #[case("message.mailboxes contains \"Archive\"", false)]
    #[case("message.keyword contains \"$seen\"", true)]
    #[case("message.keywords contains \"$flagged\"", false)]
    #[case("message.received > \"2023-01-01\"", true)]
    #[case("message.received > \"2024-01-01\"", false)]
    #[case("message.date == \"2023-06-15\"", true)]
    #[case("message.date startswith \"2023-\"", true)]
    #[case("message.size < 4096", true)]
    #[case("message.size >= 4096", false)]
    #[case("message.subject contains \"invoice\"", true)]
    #[case("message.from contains \"billing@example.com\"", true)]
    #[case("message.thread == \"T789\"", true)]
    #[case("message.missing", false)]
    fn message_filtering(#[case] filter: &str, #[case] matches: bool) {
        let message = test_message();
        assert_eq!(
            Filter::new(filter)
                .expect("parse filter")
                .matches(&message)
                .expect("run filter"),
            matches,
            "filter: {filter}"
        );
    }

    #[test]
    fn display_message() {
        let message = test_message();
        assert_eq!(
            format!("{}", message),
            "2023-06-15 10:30:00 M123 (Quarterly invoice)"
        );
    }
}
