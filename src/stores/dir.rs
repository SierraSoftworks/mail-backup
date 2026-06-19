//! A mail store backed by a plain directory tree.
//!
//! This store owns all worktree manipulation; the git store wraps it and
//! mirrors the recorded [`PathEdit`]s into git tree edits. Every operation is
//! idempotent: re-applying an event which has (partially or fully) been
//! applied before converges on the same result, which is what makes crash
//! recovery via at-least-once redelivery safe.
//!
//! Callers are expected to order events within a batch: mailbox upserts
//! (parents before children) first, then message events, then mailbox
//! deletions.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use human_errors::ResultExt;
use sha2::{Digest, Sha256};
use tracing_batteries::prelude::*;

use super::index::{StatePersistence, StoreIndex};
use super::layout::{self, MAILBOX_META_FILE, UNFILED_DIR};
use super::sidecar::{MailboxSidecar, MessageSidecar};
use super::{Checkpoint, EventOutcome, MailStore, PathEdit, StoreState, StoredMessage};
use crate::entities::mail::{MailEvent, MailboxIndex, MailboxInfo, MailboxRecord, MessageMeta};

pub struct DirMailStore {
    root: PathBuf,
    persistence: StatePersistence,
    state: StoreState,
    index: StoreIndex,
    path_edits: Vec<PathEdit>,
    opened: bool,
}

impl DirMailStore {
    pub fn new(root: PathBuf) -> Self {
        let state_dir = root.join(".mail-backup");
        Self::with_state_dir(root, state_dir)
    }

    /// Creates a store whose state lives in a custom directory (the git store
    /// keeps it under `.git/mail-backup/` so the worktree holds only mail).
    pub fn with_state_dir(root: PathBuf, state_dir: PathBuf) -> Self {
        Self {
            root,
            persistence: StatePersistence::new(state_dir),
            state: StoreState::default(),
            index: StoreIndex::default(),
            path_edits: Vec::new(),
            opened: false,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Drains the worktree mutations recorded since the last call. Layered
    /// stores use this to mirror filesystem changes into their own structures.
    pub fn take_path_edits(&mut self) -> Vec<PathEdit> {
        std::mem::take(&mut self.path_edits)
    }

    /// Discards the cached index and rebuilds it by scanning the directory
    /// tree's sidecar files.
    pub fn rebuild_index(&mut self) -> Result<(), human_errors::Error> {
        self.index = StoreIndex::rebuild_from_dir(&self.root)?;
        self.persistence.save_index(&self.index)?;
        Ok(())
    }

    /// Compares the cached index against a scan of the directory tree,
    /// returning every inconsistency found.
    pub fn verify(&self) -> Result<Vec<String>, human_errors::Error> {
        let on_disk = StoreIndex::rebuild_from_dir(&self.root)?;
        Ok(super::index::diff_indexes(&self.index, &on_disk))
    }

    /// Opens the store without loading an index, for layered stores which
    /// supply their own (e.g. the git store rebuilds it from the committed
    /// tree rather than the worktree).
    pub(crate) fn open_without_index(&mut self) -> Result<(), human_errors::Error> {
        std::fs::create_dir_all(&self.root).wrap_user_err(
            format!(
                "Failed to create the backup directory {}.",
                self.root.display()
            ),
            &["Make sure the configured backup path is valid and writable by the process."],
        )?;
        self.state = self.persistence.load_state()?.unwrap_or_default();
        self.opened = true;
        Ok(())
    }

    pub(crate) fn load_cached_index(&self) -> Option<StoreIndex> {
        self.persistence.load_index()
    }

    pub(crate) fn set_index(&mut self, index: StoreIndex) {
        self.index = index;
    }

    fn abs(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    fn write_file(&mut self, rel: &str, content: &[u8]) -> Result<(), human_errors::Error> {
        let path = self.abs(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).wrap_system_err(
                format!("Failed to create the directory {}.", parent.display()),
                &["Make sure the store directory is writable by the process."],
            )?;
        }

        let tmp = path.with_extension("tmp-mb");
        std::fs::write(&tmp, content).wrap_system_err(
            format!("Failed to write {}.", tmp.display()),
            &["Make sure the store directory is writable by the process."],
        )?;
        if path.exists() {
            std::fs::remove_file(&path).wrap_system_err(
                format!("Failed to replace {}.", path.display()),
                &["Make sure the store directory is writable by the process."],
            )?;
        }
        std::fs::rename(&tmp, &path).wrap_system_err(
            format!("Failed to finalize writing {}.", path.display()),
            &["Make sure the store directory is writable by the process."],
        )?;

        self.path_edits.push(PathEdit::Upsert(rel.to_string()));
        Ok(())
    }

    fn remove_file(&mut self, rel: &str) -> Result<(), human_errors::Error> {
        let path = self.abs(rel);
        if path.exists() {
            std::fs::remove_file(&path).wrap_system_err(
                format!("Failed to remove {}.", path.display()),
                &["Make sure the store directory is writable by the process."],
            )?;
        }
        self.path_edits.push(PathEdit::Remove(rel.to_string()));
        Ok(())
    }

    /// Whether the file at `rel` already holds exactly `content`.
    fn file_matches(&self, rel: &str, content: &[u8]) -> bool {
        std::fs::read(self.abs(rel))
            .map(|existing| existing == content)
            .unwrap_or(false)
    }

    /// The case-folded directory names already taken by *other* mailboxes
    /// directly under the given parent directory prefix.
    fn sibling_dir_names(&self, parent_prefix: &str, exclude_id: &str) -> BTreeSet<String> {
        let mut taken: BTreeSet<String> = self
            .index
            .mailboxes
            .iter()
            .filter(|r| r.info.id != exclude_id)
            .filter_map(|r| {
                let rest = if parent_prefix.is_empty() {
                    r.dir_path.as_str()
                } else {
                    r.dir_path.strip_prefix(&format!("{parent_prefix}/"))?
                };
                if rest.is_empty() || rest.contains('/') {
                    None
                } else {
                    Some(rest.to_lowercase())
                }
            })
            .collect();

        if parent_prefix.is_empty() {
            taken.insert(UNFILED_DIR.to_lowercase());
        }

        taken
    }

    /// The directory prefix a mailbox should live under, based on its parent.
    /// Unknown parents fall back to the root (the engine refreshes the full
    /// mailbox list whenever mailboxes change, so this is a defensive path).
    fn parent_prefix(&self, info: &MailboxInfo) -> String {
        match info.parent_id.as_deref() {
            None => String::new(),
            Some(parent_id) => match self.index.mailboxes.get(parent_id) {
                Some(parent) => parent.dir_path.clone(),
                None => {
                    warn!(
                        "Mailbox {} references unknown parent {}; placing it at the top level",
                        info.id, parent_id
                    );
                    String::new()
                }
            },
        }
    }

    fn apply_mailbox_upserted(
        &mut self,
        info: &MailboxInfo,
    ) -> Result<EventOutcome, human_errors::Error> {
        match self.index.mailboxes.get(&info.id).cloned() {
            None => {
                let prefix = self.parent_prefix(info);
                let taken = self.sibling_dir_names(&prefix, &info.id);
                let mut segment = layout::assign_dir_name(&info.name, &info.id, &taken);

                // Defensive: if a directory of that name exists on disk but is
                // not ours (stale index), fall back to the suffixed name.
                let candidate = join_dir(&prefix, &segment);
                if self.dir_belongs_to_other(&candidate, &info.id) {
                    let mut taken = taken;
                    taken.insert(segment.to_lowercase());
                    segment = layout::assign_dir_name(&info.name, &info.id, &taken);
                }

                let dir_path = join_dir(&prefix, &segment);
                let meta_path = format!("{dir_path}/{MAILBOX_META_FILE}");
                self.write_file(&meta_path, &MailboxSidecar::new(info).to_canonical_bytes())?;
                self.index.mailboxes.insert(MailboxRecord {
                    info: info.clone(),
                    dir_path,
                });
                Ok(EventOutcome::Added)
            }
            Some(existing) => {
                let structural_change =
                    existing.info.name != info.name || existing.info.parent_id != info.parent_id;

                let mut outcome = EventOutcome::Unchanged;

                if structural_change {
                    let prefix = self.parent_prefix(info);
                    let taken = self.sibling_dir_names(&prefix, &info.id);
                    let segment = layout::assign_dir_name(&info.name, &info.id, &taken);
                    let new_dir = join_dir(&prefix, &segment);

                    if new_dir != existing.dir_path {
                        self.rename_dir(&existing.dir_path, &new_dir)?;
                        outcome = EventOutcome::Moved;
                    }
                }

                let record = self
                    .index
                    .mailboxes
                    .get(&info.id)
                    .expect("record exists: checked above")
                    .clone();

                if record.info != *info {
                    let meta_path = format!("{}/{MAILBOX_META_FILE}", record.dir_path);
                    self.write_file(&meta_path, &MailboxSidecar::new(info).to_canonical_bytes())?;
                    self.index.mailboxes.insert(MailboxRecord {
                        info: info.clone(),
                        dir_path: record.dir_path,
                    });
                    if outcome == EventOutcome::Unchanged {
                        outcome = EventOutcome::Updated;
                    }
                }

                Ok(outcome)
            }
        }
    }

    /// Whether the directory at `rel` exists and holds a `.mailbox.yaml`
    /// belonging to a different mailbox.
    fn dir_belongs_to_other(&self, rel: &str, mailbox_id: &str) -> bool {
        let meta = self.abs(rel).join(MAILBOX_META_FILE);
        match std::fs::read(&meta) {
            Ok(content) => MailboxSidecar::parse(&content)
                .map(|sidecar| sidecar.id != mailbox_id)
                .unwrap_or(true),
            Err(_) => false,
        }
    }

    /// Renames a mailbox directory, moving its entire subtree (messages,
    /// sidecars, and descendant mailboxes) and recording per-file edits.
    fn rename_dir(&mut self, old_dir: &str, new_dir: &str) -> Result<(), human_errors::Error> {
        let old_abs = self.abs(old_dir);
        let new_abs = self.abs(new_dir);

        if old_abs.exists() {
            if let Some(parent) = new_abs.parent() {
                std::fs::create_dir_all(parent).wrap_system_err(
                    format!("Failed to create the directory {}.", parent.display()),
                    &["Make sure the store directory is writable by the process."],
                )?;
            }
            std::fs::rename(&old_abs, &new_abs).wrap_system_err(
                format!(
                    "Failed to rename the mailbox directory {} to {}.",
                    old_abs.display(),
                    new_abs.display()
                ),
                &["Make sure no other process is holding files in the store open."],
            )?;
        } else if !new_abs.exists() {
            warn!(
                "Mailbox directory {} is missing; recreating {} from scratch",
                old_dir, new_dir
            );
            std::fs::create_dir_all(&new_abs).wrap_system_err(
                format!("Failed to create the directory {}.", new_abs.display()),
                &["Make sure the store directory is writable by the process."],
            )?;
        }

        // Record per-file edits for every indexed file under the subtree.
        for message_id in self.index.messages_under(old_dir) {
            let message = self
                .index
                .get_message(&message_id)
                .expect("message ids under a prefix are indexed")
                .clone();
            let new_path = format!(
                "{new_dir}/{}",
                message
                    .path
                    .strip_prefix(&format!("{old_dir}/"))
                    .expect("paths under a prefix start with it")
            );

            self.path_edits.push(PathEdit::Remove(message.path.clone()));
            self.path_edits
                .push(PathEdit::Remove(layout::sidecar_path(&message.path)));
            self.path_edits.push(PathEdit::Upsert(new_path.clone()));
            self.path_edits
                .push(PathEdit::Upsert(layout::sidecar_path(&new_path)));

            self.index.insert_message(StoredMessage {
                path: new_path,
                ..message
            });
        }

        for affected in [old_dir.to_string()].into_iter().chain(
            self.index
                .mailboxes
                .iter()
                .filter(|r| r.dir_path.starts_with(&format!("{old_dir}/")))
                .map(|r| r.dir_path.clone())
                .collect::<Vec<_>>(),
        ) {
            let relocated = if affected == old_dir {
                new_dir.to_string()
            } else {
                format!(
                    "{new_dir}/{}",
                    affected
                        .strip_prefix(&format!("{old_dir}/"))
                        .expect("paths under a prefix start with it")
                )
            };
            self.path_edits
                .push(PathEdit::Remove(format!("{affected}/{MAILBOX_META_FILE}")));
            self.path_edits
                .push(PathEdit::Upsert(format!("{relocated}/{MAILBOX_META_FILE}")));
        }

        self.index.mailboxes.rebase_dir_paths(old_dir, new_dir);
        Ok(())
    }

    fn apply_mailbox_deleted(&mut self, id: &str) -> Result<EventOutcome, human_errors::Error> {
        let Some(record) = self.index.mailboxes.get(id).cloned() else {
            return Ok(EventOutcome::Unchanged);
        };

        if !self.index.messages_under(&record.dir_path).is_empty()
            || self.index.has_mailboxes_under(&record.dir_path)
        {
            debug!(
                "Mailbox {} ({}) still has contents; deferring its removal",
                id, record.dir_path
            );
            return Ok(EventOutcome::Skipped);
        }

        self.remove_file(&format!("{}/{MAILBOX_META_FILE}", record.dir_path))?;

        let abs = self.abs(&record.dir_path);
        if abs.exists()
            && let Err(e) = std::fs::remove_dir(&abs)
        {
            warn!(
                "Could not remove the mailbox directory {}: {}; leaving it in place",
                record.dir_path, e
            );
        }

        self.index.mailboxes.remove(id);
        Ok(EventOutcome::Removed)
    }

    fn apply_message_added(
        &mut self,
        meta: &MessageMeta,
        raw: &[u8],
    ) -> Result<EventOutcome, human_errors::Error> {
        let sha256 = hex_digest(raw);

        if let Some(existing) = self.index.get_message(&meta.id).cloned() {
            // Redelivery of a message we already hold: make sure the raw file
            // exists, then treat the rest as a metadata update.
            if existing.sha256 == sha256 {
                if !self.abs(&existing.path).exists() {
                    self.write_file(&existing.path.clone(), raw)?;
                }
                let outcome = self.apply_message_updated(meta)?;
                return Ok(match outcome {
                    EventOutcome::Skipped => EventOutcome::Unchanged,
                    other => other,
                });
            }

            // The server reused an id with different content (should never
            // happen — JMAP blobs are immutable). Replace our copy entirely.
            warn!(
                "Message {} content changed on the server; replacing the local copy",
                meta.id
            );
            self.remove_file(&existing.path.clone())?;
            self.remove_file(&layout::sidecar_path(&existing.path))?;
            self.index.remove_message(&meta.id);
        }

        let dir = layout::primary_mailbox(&meta.mailbox_ids, &self.index.mailboxes)
            .map(|r| r.dir_path.clone())
            .unwrap_or_else(|| UNFILED_DIR.to_string());

        let path = layout::message_path(&dir, meta, &sha256, |candidate| {
            self.index
                .path_owner(candidate)
                .map(|owner| owner != meta.id)
                .unwrap_or(false)
        });

        if !self.file_matches(&path, raw) {
            self.write_file(&path, raw)?;
        } else {
            // The file already exists from an interrupted earlier run: no
            // rewrite needed, but layered stores still need to see the edit.
            self.path_edits.push(PathEdit::Upsert(path.clone()));
        }

        let sidecar = MessageSidecar::new(meta, &sha256);
        let sidecar_rel = layout::sidecar_path(&path);
        let sidecar_bytes = sidecar.to_canonical_bytes();
        if !self.file_matches(&sidecar_rel, &sidecar_bytes) {
            self.write_file(&sidecar_rel, &sidecar_bytes)?;
        } else {
            self.path_edits.push(PathEdit::Upsert(sidecar_rel.clone()));
        }

        self.index.insert_message(StoredMessage {
            meta: sidecar.to_meta(),
            path,
            sha256,
        });

        Ok(EventOutcome::Added)
    }

    fn apply_message_updated(
        &mut self,
        meta: &MessageMeta,
    ) -> Result<EventOutcome, human_errors::Error> {
        let Some(existing) = self.index.get_message(&meta.id).cloned() else {
            debug!(
                "Update for message {} which is not in the store; it needs a full fetch",
                meta.id
            );
            return Ok(EventOutcome::Skipped);
        };

        let new_dir = layout::primary_mailbox(&meta.mailbox_ids, &self.index.mailboxes)
            .map(|r| r.dir_path.clone())
            .unwrap_or_else(|| UNFILED_DIR.to_string());

        let new_path = layout::message_path(&new_dir, meta, &existing.sha256, |candidate| {
            self.index
                .path_owner(candidate)
                .map(|owner| owner != meta.id)
                .unwrap_or(false)
        });

        let mut moved = false;
        let mut current_path = existing.path.clone();

        if new_path != existing.path {
            let old_abs = self.abs(&existing.path);
            let new_abs = self.abs(&new_path);

            if old_abs.exists() {
                if let Some(parent) = new_abs.parent() {
                    std::fs::create_dir_all(parent).wrap_system_err(
                        format!("Failed to create the directory {}.", parent.display()),
                        &["Make sure the store directory is writable by the process."],
                    )?;
                }
                std::fs::rename(&old_abs, &new_abs).wrap_system_err(
                    format!(
                        "Failed to move {} to {}.",
                        old_abs.display(),
                        new_abs.display()
                    ),
                    &["Make sure no other process is holding files in the store open."],
                )?;
                // Move the old sidecar along; it gets rewritten below if stale.
                let old_sidecar = self.abs(&layout::sidecar_path(&existing.path));
                let new_sidecar = self.abs(&layout::sidecar_path(&new_path));
                if old_sidecar.exists() {
                    if new_sidecar.exists() {
                        std::fs::remove_file(&new_sidecar).ok();
                    }
                    std::fs::rename(&old_sidecar, &new_sidecar).wrap_system_err(
                        format!("Failed to move {}.", old_sidecar.display()),
                        &["Make sure no other process is holding files in the store open."],
                    )?;
                }
            } else if new_abs.exists() {
                // Already moved by an interrupted earlier run.
                debug!(
                    "Message {} already moved to {}; treating the move as applied",
                    meta.id, new_path
                );
            } else {
                warn!(
                    "Message {} is missing from the store (expected at {}); it needs a full fetch",
                    meta.id, existing.path
                );
                return Ok(EventOutcome::Skipped);
            }

            self.path_edits
                .push(PathEdit::Remove(existing.path.clone()));
            self.path_edits
                .push(PathEdit::Remove(layout::sidecar_path(&existing.path)));
            self.path_edits.push(PathEdit::Upsert(new_path.clone()));
            self.path_edits
                .push(PathEdit::Upsert(layout::sidecar_path(&new_path)));

            current_path = new_path;
            moved = true;
        }

        let sidecar = MessageSidecar::new(meta, &existing.sha256);
        let sidecar_rel = layout::sidecar_path(&current_path);
        let sidecar_bytes = sidecar.to_canonical_bytes();
        let metadata_changed = !self.file_matches(&sidecar_rel, &sidecar_bytes);
        if metadata_changed {
            self.write_file(&sidecar_rel, &sidecar_bytes)?;
        }

        self.index.insert_message(StoredMessage {
            meta: sidecar.to_meta(),
            path: current_path,
            sha256: existing.sha256,
        });

        Ok(if moved {
            EventOutcome::Moved
        } else if metadata_changed {
            EventOutcome::Updated
        } else {
            EventOutcome::Unchanged
        })
    }

    fn apply_message_deleted(&mut self, id: &str) -> Result<EventOutcome, human_errors::Error> {
        let Some(existing) = self.index.remove_message(id) else {
            return Ok(EventOutcome::Unchanged);
        };

        self.remove_file(&existing.path)?;
        self.remove_file(&layout::sidecar_path(&existing.path))?;
        Ok(EventOutcome::Removed)
    }
}

fn join_dir(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}/{segment}")
    }
}

fn hex_digest(content: &[u8]) -> String {
    let digest = Sha256::digest(content);
    let mut out = String::with_capacity(64);
    for byte in digest.iter() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

impl MailStore for DirMailStore {
    fn kind(&self) -> &'static str {
        "dir"
    }

    async fn open(&mut self) -> Result<(), human_errors::Error> {
        std::fs::create_dir_all(&self.root).wrap_user_err(
            format!(
                "Failed to create the backup directory {}.",
                self.root.display()
            ),
            &["Make sure the configured backup path is valid and writable by the process."],
        )?;

        self.state = self.persistence.load_state()?.unwrap_or_default();
        self.index = match self.persistence.load_index() {
            Some(index) => index,
            None => StoreIndex::rebuild_from_dir(&self.root)?,
        };
        self.opened = true;
        Ok(())
    }

    fn state(&self) -> &StoreState {
        &self.state
    }

    fn state_mut(&mut self) -> &mut StoreState {
        &mut self.state
    }

    fn mailboxes(&self) -> &MailboxIndex {
        &self.index.mailboxes
    }

    fn lookup(&self, message_id: &str) -> Option<&StoredMessage> {
        self.index.get_message(message_id)
    }

    fn list(&self) -> impl Iterator<Item = &StoredMessage> {
        self.index.messages()
    }

    async fn apply(
        &mut self,
        events: Vec<MailEvent>,
    ) -> Result<Vec<EventOutcome>, human_errors::Error> {
        debug_assert!(self.opened, "the store must be opened before use");

        let mut outcomes = Vec::with_capacity(events.len());
        for event in events.iter() {
            let outcome = match event {
                MailEvent::MailboxUpserted(info) => self.apply_mailbox_upserted(info)?,
                MailEvent::MailboxDeleted { id } => self.apply_mailbox_deleted(id)?,
                MailEvent::MessageAdded { message, raw } => {
                    self.apply_message_added(&message.meta, raw)?
                }
                MailEvent::MessageUpdated { message } => {
                    self.apply_message_updated(&message.meta)?
                }
                MailEvent::MessageDeleted { id } => self.apply_message_deleted(id)?,
            };
            outcomes.push(outcome);
        }
        Ok(outcomes)
    }

    async fn checkpoint(&mut self, _checkpoint: &Checkpoint) -> Result<(), human_errors::Error> {
        self.persistence.save_state(&self.state)?;
        self.persistence.save_index(&self.index)?;
        self.path_edits.clear();
        Ok(())
    }

    async fn save_state(&mut self) -> Result<(), human_errors::Error> {
        self.persistence.save_state(&self.state)?;
        self.persistence.save_index(&self.index)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::MailMessage;

    fn mailbox(id: &str, name: &str, role: Option<&str>, parent: Option<&str>) -> MailboxInfo {
        MailboxInfo {
            id: id.to_string(),
            name: name.to_string(),
            role: role.map(str::to_string),
            parent_id: parent.map(str::to_string),
            sort_order: 0,
        }
    }

    fn meta(id: &str, mailboxes: &[&str], keywords: &[&str], received: &str) -> MessageMeta {
        MessageMeta {
            id: id.to_string(),
            blob_id: format!("blob-{id}"),
            thread_id: format!("thread-{id}"),
            mailbox_ids: mailboxes.iter().map(|s| s.to_string()).collect(),
            keywords: keywords.iter().map(|s| s.to_string()).collect(),
            received_at: chrono::DateTime::parse_from_rfc3339(received)
                .unwrap()
                .with_timezone(&chrono::Utc),
            size: 10,
            message_id: vec![format!("<{id}@example.com>")],
            subject: Some(format!("Subject {id}")),
            from: vec!["sender@example.com".to_string()],
        }
    }

    fn added(store: &DirMailStore, meta: MessageMeta, raw: &[u8]) -> MailEvent {
        MailEvent::MessageAdded {
            message: MailMessage::new(meta, store.mailboxes()),
            raw: raw.to_vec(),
        }
    }

    fn updated(store: &DirMailStore, meta: MessageMeta) -> MailEvent {
        MailEvent::MessageUpdated {
            message: MailMessage::new(meta, store.mailboxes()),
        }
    }

    async fn open_store() -> (tempfile::TempDir, DirMailStore) {
        let dir = tempfile::tempdir().unwrap();
        let mut store = DirMailStore::new(dir.path().to_path_buf());
        store.open().await.unwrap();
        (dir, store)
    }

    #[tokio::test]
    async fn add_message_creates_files() {
        let (dir, mut store) = open_store().await;

        let outcomes = store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
                None,
            ))])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Added]);

        let m = meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z");
        let outcomes = store
            .apply(vec![added(&store, m, b"raw message content")])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Added]);

        let stored = store.lookup("M1").expect("message indexed");
        assert!(stored.path.starts_with("Inbox/20230615-103000-"));
        assert!(dir.path().join(&stored.path).exists());
        assert!(dir.path().join(layout::sidecar_path(&stored.path)).exists());
        assert!(dir.path().join("Inbox").join(MAILBOX_META_FILE).exists());

        // Re-applying the same event is a no-op (idempotency).
        let m = meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z");
        let outcomes = store
            .apply(vec![added(&store, m, b"raw message content")])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Unchanged]);
    }

    #[tokio::test]
    async fn keyword_update_rewrites_sidecar_only() {
        let (dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
                None,
            ))])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                b"raw",
            )])
            .await
            .unwrap();

        let path_before = store.lookup("M1").unwrap().path.clone();
        store.take_path_edits();

        let outcomes = store
            .apply(vec![updated(
                &store,
                meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z"),
            )])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Updated]);

        let stored = store.lookup("M1").unwrap();
        assert_eq!(stored.path, path_before);
        assert!(stored.meta.keywords.contains("$seen"));

        let edits = store.take_path_edits();
        assert_eq!(
            edits,
            vec![PathEdit::Upsert(layout::sidecar_path(&path_before))]
        );

        let sidecar = MessageSidecar::parse(
            &std::fs::read(dir.path().join(layout::sidecar_path(&path_before))).unwrap(),
        )
        .unwrap();
        assert!(sidecar.keywords.contains("$seen"));
    }

    #[tokio::test]
    async fn mailbox_change_moves_message_file() {
        let (dir, mut store) = open_store().await;
        store
            .apply(vec![
                MailEvent::MailboxUpserted(mailbox("mb-inbox", "Inbox", Some("inbox"), None)),
                MailEvent::MailboxUpserted(mailbox("mb-archive", "Archive", Some("archive"), None)),
            ])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z"),
                b"raw content",
            )])
            .await
            .unwrap();

        let old_path = store.lookup("M1").unwrap().path.clone();
        assert!(old_path.starts_with("Inbox/"));

        let outcomes = store
            .apply(vec![updated(
                &store,
                meta("M1", &["mb-archive"], &["$seen"], "2023-06-15T10:30:00Z"),
            )])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Moved]);

        let stored = store.lookup("M1").unwrap();
        assert!(stored.path.starts_with("Archive/"));
        // The basename never changes across moves: byte-identical content at
        // the new path is what makes git detect the move as a rename.
        assert_eq!(
            old_path.rsplit('/').next().unwrap(),
            stored.path.rsplit('/').next().unwrap()
        );
        assert!(!dir.path().join(&old_path).exists());
        assert!(dir.path().join(&stored.path).exists());
        assert_eq!(
            std::fs::read(dir.path().join(&stored.path)).unwrap(),
            b"raw content"
        );

        // Replaying the same update converges (idempotency after crash).
        let outcomes = store
            .apply(vec![updated(
                &store,
                meta("M1", &["mb-archive"], &["$seen"], "2023-06-15T10:30:00Z"),
            )])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Unchanged]);
    }

    #[tokio::test]
    async fn delete_message_removes_files() {
        let (dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
                None,
            ))])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                b"raw",
            )])
            .await
            .unwrap();

        let path = store.lookup("M1").unwrap().path.clone();
        let outcomes = store
            .apply(vec![MailEvent::MessageDeleted {
                id: "M1".to_string(),
            }])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Removed]);
        assert!(store.lookup("M1").is_none());
        assert!(!dir.path().join(&path).exists());
        assert!(!dir.path().join(layout::sidecar_path(&path)).exists());

        // Deleting again is a no-op.
        let outcomes = store
            .apply(vec![MailEvent::MessageDeleted {
                id: "M1".to_string(),
            }])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Unchanged]);
    }

    #[tokio::test]
    async fn mailbox_rename_moves_subtree() {
        let (dir, mut store) = open_store().await;
        store
            .apply(vec![
                MailEvent::MailboxUpserted(mailbox("mb-a", "Alpha", None, None)),
                MailEvent::MailboxUpserted(mailbox("mb-sub", "Sub", None, Some("mb-a"))),
            ])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-sub"], &[], "2023-06-15T10:30:00Z"),
                b"raw",
            )])
            .await
            .unwrap();

        let outcomes = store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-a", "Beta", None, None,
            ))])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Moved]);

        assert_eq!(store.mailboxes().get("mb-a").unwrap().dir_path, "Beta");
        assert_eq!(
            store.mailboxes().get("mb-sub").unwrap().dir_path,
            "Beta/Sub"
        );
        let stored = store.lookup("M1").unwrap();
        assert!(stored.path.starts_with("Beta/Sub/"), "got: {}", stored.path);
        assert!(dir.path().join(&stored.path).exists());
        assert!(!dir.path().join("Alpha").exists());
        assert!(dir.path().join("Beta").join(MAILBOX_META_FILE).exists());
        let sidecar = MailboxSidecar::parse(
            &std::fs::read(dir.path().join("Beta").join(MAILBOX_META_FILE)).unwrap(),
        )
        .unwrap();
        assert_eq!(sidecar.name, "Beta");
    }

    #[tokio::test]
    async fn mailbox_delete_deferred_until_empty() {
        let (dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
                None,
            ))])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                b"raw",
            )])
            .await
            .unwrap();

        let outcomes = store
            .apply(vec![MailEvent::MailboxDeleted {
                id: "mb-inbox".to_string(),
            }])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Skipped]);
        assert!(store.mailboxes().get("mb-inbox").is_some());

        store
            .apply(vec![MailEvent::MessageDeleted {
                id: "M1".to_string(),
            }])
            .await
            .unwrap();

        let outcomes = store
            .apply(vec![MailEvent::MailboxDeleted {
                id: "mb-inbox".to_string(),
            }])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Removed]);
        assert!(store.mailboxes().get("mb-inbox").is_none());
        assert!(!dir.path().join("Inbox").exists());
    }

    #[tokio::test]
    async fn message_without_known_mailbox_goes_unfiled() {
        let (dir, mut store) = open_store().await;
        let outcomes = store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-unknown"], &[], "2023-06-15T10:30:00Z"),
                b"raw",
            )])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Added]);
        let stored = store.lookup("M1").unwrap();
        assert!(stored.path.starts_with(&format!("{UNFILED_DIR}/")));
        assert!(dir.path().join(&stored.path).exists());

        // Once the mailbox becomes known, an update moves it into place.
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-unknown",
                "Found",
                None,
                None,
            ))])
            .await
            .unwrap();
        let outcomes = store
            .apply(vec![updated(
                &store,
                meta("M1", &["mb-unknown"], &[], "2023-06-15T10:30:00Z"),
            )])
            .await
            .unwrap();
        assert_eq!(outcomes, vec![EventOutcome::Moved]);
        assert!(store.lookup("M1").unwrap().path.starts_with("Found/"));
    }

    #[tokio::test]
    async fn duplicate_content_messages_get_distinct_paths() {
        let (_dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
                None,
            ))])
            .await
            .unwrap();

        let raw = b"identical content";
        store
            .apply(vec![added(
                &store,
                meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                raw,
            )])
            .await
            .unwrap();
        store
            .apply(vec![added(
                &store,
                meta("M2", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                raw,
            )])
            .await
            .unwrap();

        let p1 = store.lookup("M1").unwrap().path.clone();
        let p2 = store.lookup("M2").unwrap().path.clone();
        assert_ne!(p1, p2);
        assert!(p2.contains('~'), "duplicate gets a suffixed name: {p2}");
    }

    #[tokio::test]
    async fn index_survives_reopen_and_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = DirMailStore::new(dir.path().to_path_buf());
            store.open().await.unwrap();
            store
                .apply(vec![MailEvent::MailboxUpserted(mailbox(
                    "mb-inbox",
                    "Inbox",
                    Some("inbox"),
                    None,
                ))])
                .await
                .unwrap();
            let m = meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z");
            let event = MailEvent::MessageAdded {
                message: MailMessage::new(m, store.mailboxes()),
                raw: b"raw".to_vec(),
            };
            store.apply(vec![event]).await.unwrap();
            store
                .checkpoint(&Checkpoint {
                    date: chrono::NaiveDate::from_ymd_opt(2023, 6, 15).unwrap(),
                    kind: crate::stores::SnapshotKind::Live,
                    description: "test".to_string(),
                })
                .await
                .unwrap();
        }

        // Reopen: loads the cached index.
        let mut store = DirMailStore::new(dir.path().to_path_buf());
        store.open().await.unwrap();
        assert!(store.lookup("M1").is_some());

        // Rebuild from sidecars produces the same view.
        store.rebuild_index().unwrap();
        let stored = store.lookup("M1").expect("rebuilt index has the message");
        assert!(stored.meta.keywords.contains("$seen"));
        assert_eq!(store.mailboxes().get("mb-inbox").unwrap().dir_path, "Inbox");
    }
}
