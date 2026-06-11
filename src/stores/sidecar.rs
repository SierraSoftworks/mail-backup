//! Canonical (de)serialization of the per-message metadata sidecars and the
//! per-directory mailbox metadata files.
//!
//! Serialization must be canonical — the same logical content always produces
//! the same bytes — because idempotent crash recovery relies on re-written
//! sidecars being byte-identical (and therefore producing identical git
//! blobs). This is guaranteed by using fixed-order structs, sorted sets, and
//! seconds-precision timestamps.

use std::collections::BTreeSet;

use human_errors::ResultExt;
use serde::{Deserialize, Serialize};

use crate::entities::mail::{MailboxInfo, MessageMeta};

/// The current sidecar schema version.
const SIDECAR_VERSION: u32 = 1;

/// The committed metadata sidecar stored next to each `.eml` file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageSidecar {
    pub v: u32,
    pub id: String,
    pub blob_id: String,
    pub thread_id: String,
    pub mailbox_ids: BTreeSet<String>,
    pub keywords: BTreeSet<String>,
    pub received_at: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    #[serde(default)]
    pub message_id: Vec<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub from: Vec<String>,
    /// The full hex sha256 digest of the raw message bytes, used to verify
    /// the adjacent `.eml` file and to skip re-downloads during recovery.
    pub sha256: String,
}

impl MessageSidecar {
    pub fn new(meta: &MessageMeta, sha256_hex: &str) -> Self {
        Self {
            v: SIDECAR_VERSION,
            id: meta.id.clone(),
            blob_id: meta.blob_id.clone(),
            thread_id: meta.thread_id.clone(),
            mailbox_ids: meta.mailbox_ids.clone(),
            keywords: meta.keywords.clone(),
            // Seconds precision keeps serialization canonical (and matches
            // the precision JMAP itself uses for receivedAt).
            received_at: truncate_to_seconds(meta.received_at),
            size: meta.size,
            message_id: meta.message_id.clone(),
            subject: meta.subject.clone(),
            from: meta.from.clone(),
            sha256: sha256_hex.to_string(),
        }
    }

    pub fn to_meta(&self) -> MessageMeta {
        MessageMeta {
            id: self.id.clone(),
            blob_id: self.blob_id.clone(),
            thread_id: self.thread_id.clone(),
            mailbox_ids: self.mailbox_ids.clone(),
            keywords: self.keywords.clone(),
            received_at: self.received_at,
            size: self.size,
            message_id: self.message_id.clone(),
            subject: self.subject.clone(),
            from: self.from.clone(),
        }
    }

    /// Serializes this sidecar to its canonical byte representation.
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        serde_yaml::to_string(self)
            .expect("sidecar serialization cannot fail")
            .into_bytes()
    }

    pub fn parse(content: &[u8]) -> Result<Self, human_errors::Error> {
        serde_yaml::from_slice(content).wrap_system_err(
            "Failed to parse a message metadata sidecar file.",
            &[
                "The sidecar file may have been modified or corrupted. Run `mail-backup index` to rebuild the store's index, or restore the file from git history.",
            ],
        )
    }
}

/// The committed `.mailbox.yaml` file stored inside each mailbox directory.
/// Its location *is* the persisted mailbox-id to directory-path mapping.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MailboxSidecar {
    pub v: u32,
    pub id: String,
    /// The true mailbox name as it appears on the server (the directory name
    /// is the sanitized form of this).
    pub name: String,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub sort_order: u32,
}

impl MailboxSidecar {
    pub fn new(info: &MailboxInfo) -> Self {
        Self {
            v: SIDECAR_VERSION,
            id: info.id.clone(),
            name: info.name.clone(),
            role: info.role.clone(),
            parent_id: info.parent_id.clone(),
            sort_order: info.sort_order,
        }
    }

    pub fn to_info(&self) -> MailboxInfo {
        MailboxInfo {
            id: self.id.clone(),
            name: self.name.clone(),
            role: self.role.clone(),
            parent_id: self.parent_id.clone(),
            sort_order: self.sort_order,
        }
    }

    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        serde_yaml::to_string(self)
            .expect("mailbox sidecar serialization cannot fail")
            .into_bytes()
    }

    pub fn parse(content: &[u8]) -> Result<Self, human_errors::Error> {
        serde_yaml::from_slice(content).wrap_system_err(
            "Failed to parse a mailbox metadata file.",
            &[
                "The .mailbox.yaml file may have been modified or corrupted. Run `mail-backup index` to rebuild the store's index, or restore the file from git history.",
            ],
        )
    }
}

fn truncate_to_seconds(dt: chrono::DateTime<chrono::Utc>) -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    dt.trunc_subsecs(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_meta() -> MessageMeta {
        MessageMeta {
            id: "M123".to_string(),
            blob_id: "G456".to_string(),
            thread_id: "T789".to_string(),
            mailbox_ids: ["mb-b", "mb-a"].iter().map(|s| s.to_string()).collect(),
            keywords: ["$seen", "$flagged"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            received_at: chrono::DateTime::parse_from_rfc3339("2023-06-15T10:30:00.123456Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            size: 2048,
            message_id: vec!["<abc@example.com>".to_string()],
            subject: Some("Hello".to_string()),
            from: vec!["a@example.com".to_string()],
        }
    }

    #[test]
    fn round_trip() {
        let sidecar = MessageSidecar::new(&test_meta(), "ab12cd34");
        let bytes = sidecar.to_canonical_bytes();
        let parsed = MessageSidecar::parse(&bytes).unwrap();
        assert_eq!(parsed, sidecar);

        let meta = parsed.to_meta();
        assert_eq!(meta.id, "M123");
        // Subsecond precision is deliberately dropped.
        assert_eq!(
            meta.received_at,
            chrono::DateTime::parse_from_rfc3339("2023-06-15T10:30:00Z").unwrap()
        );
    }

    #[test]
    fn serialization_is_canonical() {
        let a = MessageSidecar::new(&test_meta(), "ab12cd34");
        let b = MessageSidecar::new(&test_meta(), "ab12cd34");
        assert_eq!(a.to_canonical_bytes(), b.to_canonical_bytes());

        // Sets serialize in sorted order regardless of insertion order.
        let yaml = String::from_utf8(a.to_canonical_bytes()).unwrap();
        let flagged = yaml.find("$flagged").unwrap();
        let seen = yaml.find("$seen").unwrap();
        assert!(flagged < seen, "keywords should be sorted: {yaml}");
    }

    #[test]
    fn mailbox_sidecar_round_trip() {
        let info = MailboxInfo {
            id: "mb-1".to_string(),
            name: "Archive: 2023".to_string(),
            role: Some("archive".to_string()),
            parent_id: Some("mb-0".to_string()),
            sort_order: 10,
        };
        let sidecar = MailboxSidecar::new(&info);
        let parsed = MailboxSidecar::parse(&sidecar.to_canonical_bytes()).unwrap();
        assert_eq!(parsed.to_info(), info);
    }

    #[test]
    fn parse_rejects_garbage() {
        assert!(MessageSidecar::parse(b"not: [valid").is_err());
        assert!(MailboxSidecar::parse(b"{{{{").is_err());
    }
}
