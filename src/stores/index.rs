//! The in-memory index of a store's contents, with persistence and
//! rebuild-from-disk support.
//!
//! The index is purely a cache: the committed sidecar files are the source of
//! truth, and the index can always be rebuilt by scanning them.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use human_errors::ResultExt;
use serde::{Deserialize, Serialize};

use super::layout::{MAILBOX_META_FILE, MESSAGE_SUFFIX, SIDECAR_SUFFIX};
use super::sidecar::{MailboxSidecar, MessageSidecar};
use super::{StoreState, StoredMessage};
use crate::entities::mail::{MailboxIndex, MailboxRecord};

/// Directory names which are never part of the mail tree and are skipped when
/// scanning.
const SKIPPED_DIRS: &[&str] = &[".git", ".mail-backup"];

#[derive(Debug, Default)]
pub struct StoreIndex {
    emails: HashMap<String, StoredMessage>,
    by_path: HashMap<String, String>,
    pub mailboxes: MailboxIndex,
}

impl StoreIndex {
    pub fn insert_message(&mut self, message: StoredMessage) {
        if let Some(previous) = self.emails.get(&message.meta.id) {
            self.by_path.remove(&previous.path);
        }
        self.by_path
            .insert(message.path.clone(), message.meta.id.clone());
        self.emails.insert(message.meta.id.clone(), message);
    }

    pub fn remove_message(&mut self, id: &str) -> Option<StoredMessage> {
        let removed = self.emails.remove(id);
        if let Some(message) = &removed {
            self.by_path.remove(&message.path);
        }
        removed
    }

    pub fn get_message(&self, id: &str) -> Option<&StoredMessage> {
        self.emails.get(id)
    }

    pub fn messages(&self) -> impl Iterator<Item = &StoredMessage> {
        self.emails.values()
    }

    /// The id of the message whose `.eml` file lives at the given path.
    pub fn path_owner(&self, path: &str) -> Option<&str> {
        self.by_path.get(path).map(String::as_str)
    }

    /// All messages whose paths sit under the given directory prefix.
    pub fn messages_under(&self, dir_prefix: &str) -> Vec<String> {
        let prefix = format!("{dir_prefix}/");
        self.by_path
            .iter()
            .filter(|(path, _)| path.starts_with(&prefix))
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// Whether any mailbox directory sits strictly under the given prefix.
    pub fn has_mailboxes_under(&self, dir_prefix: &str) -> bool {
        let prefix = format!("{dir_prefix}/");
        self.mailboxes
            .iter()
            .any(|r| r.dir_path.starts_with(&prefix))
    }

    /// Rebuilds the index by walking a directory tree and parsing every
    /// mailbox metadata file and message sidecar found.
    pub fn rebuild_from_dir(root: &Path) -> Result<Self, human_errors::Error> {
        let mut index = StoreIndex::default();
        let mut pending = vec![PathBuf::new()];

        while let Some(rel_dir) = pending.pop() {
            let abs_dir = root.join(&rel_dir);
            let entries = std::fs::read_dir(&abs_dir).wrap_system_err(
                format!(
                    "Failed to list the directory {} while rebuilding the store index.",
                    abs_dir.display()
                ),
                &["Make sure the store directory is readable by the process."],
            )?;

            for entry in entries {
                let entry = entry.wrap_system_err(
                    "Failed to read a directory entry while rebuilding the store index.",
                    &["Make sure the store directory is readable by the process."],
                )?;
                let name = entry.file_name().to_string_lossy().to_string();
                let file_type = entry.file_type().wrap_system_err(
                    format!(
                        "Failed to inspect {} while rebuilding the store index.",
                        entry.path().display()
                    ),
                    &["Make sure the store directory is readable by the process."],
                )?;

                let rel_path = if rel_dir.as_os_str().is_empty() {
                    PathBuf::from(&name)
                } else {
                    rel_dir.join(&name)
                };

                if file_type.is_dir() {
                    if !SKIPPED_DIRS.contains(&name.as_str()) {
                        pending.push(rel_path);
                    }
                } else if name == MAILBOX_META_FILE {
                    let content = std::fs::read(entry.path()).wrap_system_err(
                        format!("Failed to read {}.", entry.path().display()),
                        &["Make sure the store directory is readable by the process."],
                    )?;
                    let sidecar = MailboxSidecar::parse(&content)?;
                    let dir_path = to_slash_path(&rel_dir);
                    index.mailboxes.insert(MailboxRecord {
                        info: sidecar.to_info(),
                        dir_path,
                    });
                } else if name.ends_with(SIDECAR_SUFFIX) {
                    let content = std::fs::read(entry.path()).wrap_system_err(
                        format!("Failed to read {}.", entry.path().display()),
                        &["Make sure the store directory is readable by the process."],
                    )?;
                    let sidecar = MessageSidecar::parse(&content)?;
                    let eml_path = to_slash_path(&rel_path)
                        .strip_suffix(SIDECAR_SUFFIX)
                        .map(|stem| format!("{stem}{MESSAGE_SUFFIX}"))
                        .expect("sidecar paths always end with the sidecar suffix");
                    index.insert_message(StoredMessage {
                        meta: sidecar.to_meta(),
                        path: eml_path,
                        sha256: sidecar.sha256,
                    });
                }
            }
        }

        Ok(index)
    }
}

/// Describes every difference between an expected index (e.g. the committed
/// state) and an actual one (e.g. a scan of the worktree). An empty result
/// means the two views are consistent.
pub fn diff_indexes(expected: &StoreIndex, actual: &StoreIndex) -> Vec<String> {
    let mut issues = Vec::new();

    for message in expected.messages() {
        match actual.get_message(&message.meta.id) {
            None => issues.push(format!(
                "message {} ({}) is missing",
                message.meta.id, message.path
            )),
            Some(found) => {
                if found.path != message.path {
                    issues.push(format!(
                        "message {} is at {} instead of {}",
                        message.meta.id, found.path, message.path
                    ));
                }
                if found.sha256 != message.sha256 {
                    issues.push(format!(
                        "message {} ({}) has different content",
                        message.meta.id, message.path
                    ));
                }
            }
        }
    }

    for message in actual.messages() {
        if expected.get_message(&message.meta.id).is_none() {
            issues.push(format!(
                "message {} ({}) is present but not expected",
                message.meta.id, message.path
            ));
        }
    }

    for record in expected.mailboxes.iter() {
        match actual.mailboxes.get(&record.info.id) {
            None => issues.push(format!(
                "mailbox {} ({}) is missing",
                record.info.id, record.dir_path
            )),
            Some(found) if found.dir_path != record.dir_path => issues.push(format!(
                "mailbox {} is at {} instead of {}",
                record.info.id, found.dir_path, record.dir_path
            )),
            _ => {}
        }
    }

    for record in actual.mailboxes.iter() {
        if expected.mailboxes.get(&record.info.id).is_none() {
            issues.push(format!(
                "mailbox {} ({}) is present but not expected",
                record.info.id, record.dir_path
            ));
        }
    }

    issues.sort();
    issues
}

fn to_slash_path(path: &Path) -> String {
    path.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The serialized form of the index cache file.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexFile {
    emails: Vec<StoredMessage>,
    mailboxes: Vec<MailboxFileRecord>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MailboxFileRecord {
    info: crate::entities::mail::MailboxInfo,
    dir_path: String,
}

/// Persistence for a store's state and index cache, rooted at a state
/// directory (`.mail-backup/` for plain stores, `.git/mail-backup/` for git
/// stores).
pub struct StatePersistence {
    state_dir: PathBuf,
}

impl StatePersistence {
    pub fn new(state_dir: PathBuf) -> Self {
        Self { state_dir }
    }

    fn state_path(&self) -> PathBuf {
        self.state_dir.join("state.json")
    }

    fn index_path(&self) -> PathBuf {
        self.state_dir.join("index.json")
    }

    /// Loads the persisted state, returning `None` when no state has ever
    /// been saved (a fresh store).
    pub fn load_state(&self) -> Result<Option<StoreState>, human_errors::Error> {
        let path = self.state_path();
        if !path.exists() {
            return Ok(None);
        }
        let content = std::fs::read(&path).wrap_system_err(
            format!("Failed to read the store state file {}.", path.display()),
            &["Make sure the store directory is readable by the process."],
        )?;
        let state = serde_json::from_slice(&content).wrap_system_err(
            format!("Failed to parse the store state file {}.", path.display()),
            &[
                "The state file may be corrupted. Deleting it will force a full reconciliation against the server on the next run.",
            ],
        )?;
        Ok(Some(state))
    }

    pub fn save_state(&self, state: &StoreState) -> Result<(), human_errors::Error> {
        let content = serde_json::to_vec_pretty(state).expect("state serialization cannot fail");
        self.write_atomic(&self.state_path(), &content)
    }

    /// Loads the index cache, returning `None` when it is missing or
    /// unreadable (callers fall back to rebuilding from disk).
    pub fn load_index(&self) -> Option<StoreIndex> {
        let content = std::fs::read(self.index_path()).ok()?;
        let file: IndexFile = serde_json::from_slice(&content).ok()?;
        let mut index = StoreIndex::default();
        for record in file.mailboxes {
            index.mailboxes.insert(MailboxRecord {
                info: record.info,
                dir_path: record.dir_path,
            });
        }
        for message in file.emails {
            index.insert_message(message);
        }
        Some(index)
    }

    pub fn save_index(&self, index: &StoreIndex) -> Result<(), human_errors::Error> {
        let file = IndexFile {
            emails: {
                let mut emails: Vec<_> = index.messages().cloned().collect();
                emails.sort_by(|a, b| a.meta.id.cmp(&b.meta.id));
                emails
            },
            mailboxes: {
                let mut mailboxes: Vec<_> = index
                    .mailboxes
                    .iter()
                    .map(|r| MailboxFileRecord {
                        info: r.info.clone(),
                        dir_path: r.dir_path.clone(),
                    })
                    .collect();
                mailboxes.sort_by(|a, b| a.info.id.cmp(&b.info.id));
                mailboxes
            },
        };
        let content = serde_json::to_vec(&file).expect("index serialization cannot fail");
        self.write_atomic(&self.index_path(), &content)
    }

    fn write_atomic(&self, path: &Path, content: &[u8]) -> Result<(), human_errors::Error> {
        std::fs::create_dir_all(&self.state_dir).wrap_system_err(
            format!(
                "Failed to create the store state directory {}.",
                self.state_dir.display()
            ),
            &["Make sure the store directory is writable by the process."],
        )?;
        let tmp = path.with_extension("tmp-mb");
        std::fs::write(&tmp, content).wrap_system_err(
            format!("Failed to write {}.", tmp.display()),
            &["Make sure the store directory is writable by the process."],
        )?;
        if path.exists() {
            std::fs::remove_file(path).wrap_system_err(
                format!("Failed to replace {}.", path.display()),
                &["Make sure the store directory is writable by the process."],
            )?;
        }
        std::fs::rename(&tmp, path).wrap_system_err(
            format!("Failed to finalize writing {}.", path.display()),
            &["Make sure the store directory is writable by the process."],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::{MailboxInfo, MessageMeta};
    use std::collections::BTreeSet;

    fn stored(id: &str, path: &str) -> StoredMessage {
        StoredMessage {
            meta: MessageMeta {
                id: id.to_string(),
                blob_id: format!("blob-{id}"),
                thread_id: format!("thread-{id}"),
                mailbox_ids: BTreeSet::new(),
                keywords: BTreeSet::new(),
                received_at: chrono::DateTime::parse_from_rfc3339("2023-06-15T10:30:00Z")
                    .unwrap()
                    .with_timezone(&chrono::Utc),
                size: 10,
                message_id: vec![],
                subject: None,
                from: vec![],
            },
            path: path.to_string(),
            sha256: "ab".repeat(32),
        }
    }

    #[test]
    fn insert_replaces_previous_path() {
        let mut index = StoreIndex::default();
        index.insert_message(stored("M1", "Inbox/a.eml"));
        assert_eq!(index.path_owner("Inbox/a.eml"), Some("M1"));

        index.insert_message(stored("M1", "Archive/a.eml"));
        assert_eq!(index.path_owner("Inbox/a.eml"), None);
        assert_eq!(index.path_owner("Archive/a.eml"), Some("M1"));
        assert_eq!(index.messages().count(), 1);
    }

    #[test]
    fn messages_under_prefix() {
        let mut index = StoreIndex::default();
        index.insert_message(stored("M1", "Inbox/a.eml"));
        index.insert_message(stored("M2", "Inbox/Sub/b.eml"));
        index.insert_message(stored("M3", "InboxOther/c.eml"));

        let mut under = index.messages_under("Inbox");
        under.sort();
        assert_eq!(under, vec!["M1".to_string(), "M2".to_string()]);
    }

    #[test]
    fn state_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let persistence = StatePersistence::new(dir.path().join(".mail-backup"));

        assert!(persistence.load_state().unwrap().is_none());

        let mut state = StoreState::default();
        state.source.account_id = "u1".to_string();
        state.source.email_state = Some("s100".to_string());
        state.current_commit_day = chrono::NaiveDate::from_ymd_opt(2026, 6, 11);
        persistence.save_state(&state).unwrap();

        let loaded = persistence.load_state().unwrap().expect("state saved");
        assert_eq!(loaded, state);
    }

    #[test]
    fn index_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let persistence = StatePersistence::new(dir.path().join(".mail-backup"));

        let mut index = StoreIndex::default();
        index.insert_message(stored("M1", "Inbox/a.eml"));
        index.mailboxes.insert(MailboxRecord {
            info: MailboxInfo {
                id: "mb-1".to_string(),
                name: "Inbox".to_string(),
                role: Some("inbox".to_string()),
                parent_id: None,
                sort_order: 1,
            },
            dir_path: "Inbox".to_string(),
        });
        persistence.save_index(&index).unwrap();

        let loaded = persistence.load_index().expect("index saved");
        assert_eq!(loaded.messages().count(), 1);
        assert_eq!(loaded.path_owner("Inbox/a.eml"), Some("M1"));
        assert_eq!(loaded.mailboxes.get("mb-1").unwrap().dir_path, "Inbox");
    }

    #[test]
    fn rebuild_from_dir_scans_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();

        std::fs::create_dir_all(root.join("Inbox")).unwrap();
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::create_dir_all(root.join(".mail-backup")).unwrap();

        let mailbox = crate::stores::sidecar::MailboxSidecar::new(&MailboxInfo {
            id: "mb-1".to_string(),
            name: "Inbox".to_string(),
            role: Some("inbox".to_string()),
            parent_id: None,
            sort_order: 0,
        });
        std::fs::write(
            root.join("Inbox").join(MAILBOX_META_FILE),
            mailbox.to_canonical_bytes(),
        )
        .unwrap();

        let message = stored("M1", "Inbox/20230615-103000-aabbccddeeff.eml");
        let sidecar = crate::stores::sidecar::MessageSidecar::new(&message.meta, &message.sha256);
        std::fs::write(
            root.join("Inbox").join("20230615-103000-aabbccddeeff.eml"),
            b"raw message",
        )
        .unwrap();
        std::fs::write(
            root.join("Inbox")
                .join("20230615-103000-aabbccddeeff.meta.yaml"),
            sidecar.to_canonical_bytes(),
        )
        .unwrap();

        // Decoys in skipped directories must not be indexed.
        std::fs::write(root.join(".git").join("decoy.meta.yaml"), b"garbage").unwrap();

        let index = StoreIndex::rebuild_from_dir(root).unwrap();
        assert_eq!(index.messages().count(), 1);
        assert_eq!(
            index.path_owner("Inbox/20230615-103000-aabbccddeeff.eml"),
            Some("M1")
        );
        assert_eq!(index.mailboxes.get("mb-1").unwrap().dir_path, "Inbox");
    }
}
