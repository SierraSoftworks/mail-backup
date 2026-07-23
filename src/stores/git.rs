//! A mail store backed by a local git repository, producing one commit per
//! day of mail.
//!
//! The git store wraps [`DirMailStore`] (which owns all worktree
//! manipulation) and mirrors every recorded [`PathEdit`] into incremental git
//! tree edits — no index files, no status scans, since this process is the
//! only writer. Backfilled days become backdated commits; the current day's
//! commit is amended as new mail streams in.

use std::collections::HashMap;
use std::path::PathBuf;

use gix::bstr::BStr;
use human_errors::ResultExt;
use tracing_batteries::prelude::*;

use super::dir::DirMailStore;
use super::index::StoreIndex;
use super::layout::{MAILBOX_META_FILE, MESSAGE_SUFFIX, SIDECAR_SUFFIX};
use super::sidecar::{MailboxSidecar, MessageSidecar};
use super::{
    Checkpoint, EventOutcome, MailStore, PathEdit, SnapshotKind, StoreState, StoredMessage,
};
use crate::entities::mail::{MailEvent, MailboxIndex, MailboxRecord};

/// The committer/author identity used when none is configured.
const DEFAULT_COMMIT_NAME: &str = "mail-backup";
const DEFAULT_COMMIT_EMAIL: &str = "mail-backup@sierrasoftworks.github.io";

pub struct GitMailStore {
    inner: DirMailStore,
    repo: Option<gix::Repository>,
    commit_name: String,
    commit_email: String,
    /// Worktree edits accumulated since the last checkpoint, in application
    /// order. Only the final edit per path matters.
    pending: Vec<PathEdit>,
}

impl GitMailStore {
    pub fn new(root: PathBuf, commit_name: Option<String>, commit_email: Option<String>) -> Self {
        let state_dir = root.join(".git").join("mail-backup");
        Self {
            inner: DirMailStore::with_state_dir(root, state_dir),
            repo: None,
            commit_name: commit_name.unwrap_or_else(|| DEFAULT_COMMIT_NAME.to_string()),
            commit_email: commit_email.unwrap_or_else(|| DEFAULT_COMMIT_EMAIL.to_string()),
            pending: Vec::new(),
        }
    }

    fn repo(&self) -> &gix::Repository {
        self.repo
            .as_ref()
            .expect("the store must be opened before use")
    }

    #[cfg(test)]
    pub fn root(&self) -> &std::path::Path {
        self.inner.root()
    }

    /// The current HEAD commit id, or `None` when the branch is unborn.
    fn head_id(&self) -> Option<gix::ObjectId> {
        self.repo().head_id().ok().map(|id| id.detach())
    }

    fn head_tree_id(&self) -> gix::ObjectId {
        self.repo()
            .head_tree_id()
            .map(|id| id.detach())
            .unwrap_or_else(|_| gix::ObjectId::empty_tree(self.repo().object_hash()))
    }

    fn signature_time(at: chrono::DateTime<chrono::Utc>) -> String {
        format!("{} +0000", at.timestamp())
    }

    fn signature<'a>(&'a self, time: &'a str) -> gix::actor::SignatureRef<'a> {
        gix::actor::SignatureRef {
            name: self.commit_name.as_str().into(),
            email: self.commit_email.as_str().into(),
            time,
        }
    }

    /// Applies an update to the repository's local configuration *file*.
    /// Callers must [`Self::reload_repository`] afterwards — the open
    /// repository handle keeps the configuration it was opened with.
    fn update_config(
        &self,
        update: impl FnOnce(&mut gix::config::File) -> Result<(), human_errors::Error>,
    ) -> Result<(), human_errors::Error> {
        let config_path = self.repo().path().join("config");
        let mut config = gix::config::File::from_path_no_includes(
            config_path.clone(),
            gix::config::Source::Local,
        )
        .wrap_system_err(
            "Unable to load the git configuration for the backup repository.",
            &["Make sure that the git repository has been correctly initialized."],
        )?;

        update(&mut config)?;

        let mut file = std::fs::File::create(&config_path).wrap_system_err(
            "Unable to write the git configuration for the backup repository.",
            &["Make sure that the git repository is writable by the process."],
        )?;
        config.write_to(&mut file).wrap_system_err(
            "Unable to write the git configuration for the backup repository.",
            &["Make sure that the git repository is writable by the process."],
        )?;

        Ok(())
    }

    /// Re-opens the repository so configuration written to disk becomes
    /// visible to this handle.
    fn reload_repository(&mut self) -> Result<(), human_errors::Error> {
        self.repo = Some(gix::open(self.inner.root()).map_err(humanize_git)?);
        Ok(())
    }

    /// Initializes a brand-new repository: local configuration which protects
    /// the raw mail bytes from line-ending mangling, plus an initial commit
    /// holding `.gitattributes` so the protection travels with clones.
    fn initialize_repository(&mut self) -> Result<(), human_errors::Error> {
        let commit_name = self.commit_name.clone();
        let commit_email = self.commit_email.clone();
        self.update_config(move |config| {
            for (section, key, value) in [
                ("user", "name", commit_name.as_str()),
                ("user", "email", commit_email.as_str()),
                // The raw .eml bytes must never be rewritten by git's
                // line-ending conversion, no matter what the user's global
                // config says.
                ("core", "autocrlf", "false"),
                ("core", "eol", "lf"),
                // Mailbox names can produce deep paths on Windows.
                ("core", "longpaths", "true"),
                // Don't let a user-invoked git trigger gc while we hold the
                // repo.
                ("gc", "auto", "0"),
            ] {
                config
                    .set_raw_value_by(section, None::<&BStr>, key, value)
                    .wrap_system_err(
                        "Unable to update the git configuration for the backup repository.",
                        &["Make sure that the git repository has been correctly initialized."],
                    )?;
            }
            Ok(())
        })
    }

    /// Guarantees the repository can resolve a committer identity, which
    /// reflog writes (every amend) require. Pre-existing repositories on
    /// machines without a global git identity have none, so persist a
    /// fallback — the same mechanism github-backup uses.
    fn ensure_committer(&mut self) -> Result<(), human_errors::Error> {
        if self.repo().committer().is_none() {
            let commit_name = self.commit_name.clone();
            let commit_email = self.commit_email.clone();
            self.update_config(move |config| {
                config
                    .set_raw_value(
                        gix::config::tree::gitoxide::Committer::NAME_FALLBACK,
                        commit_name.as_str(),
                    )
                    .wrap_system_err(
                        "Unable to update the git configuration for the backup repository.",
                        &["Make sure that the git repository has been correctly initialized."],
                    )?;
                config
                    .set_raw_value(
                        gix::config::tree::gitoxide::Committer::EMAIL_FALLBACK,
                        commit_email.as_str(),
                    )
                    .wrap_system_err(
                        "Unable to update the git configuration for the backup repository.",
                        &["Make sure that the git repository has been correctly initialized."],
                    )?;
                Ok(())
            })?;
            self.reload_repository()?;
        }
        Ok(())
    }

    fn ensure_initial_commit(&mut self) -> Result<(), human_errors::Error> {
        if self.head_id().is_some() {
            return Ok(());
        }

        let attributes = "* -text\n";
        std::fs::write(self.inner.root().join(".gitattributes"), attributes).wrap_system_err(
            "Unable to write the .gitattributes file for the backup repository.",
            &["Make sure that the backup directory is writable by the process."],
        )?;

        let repo = self.repo();
        let blob = repo
            .write_blob(attributes.as_bytes())
            .map_err(humanize_git)?;
        let mut editor = repo
            .edit_tree(gix::ObjectId::empty_tree(repo.object_hash()))
            .map_err(humanize_git)?;
        editor
            .upsert(".gitattributes", gix::object::tree::EntryKind::Blob, blob)
            .map_err(humanize_git)?;
        let tree = editor.write().map_err(humanize_git)?;

        let time = Self::signature_time(chrono::Utc::now());
        let signature = self.signature(&time);
        repo.commit_as(
            signature,
            signature,
            "HEAD",
            "Initialize mail backup store",
            tree,
            Vec::<gix::ObjectId>::new(),
        )
        .map_err(humanize_git)?;

        Ok(())
    }

    /// Rebuilds the store index from the committed HEAD tree (never from the
    /// worktree: after a crash the worktree may be ahead of the last
    /// checkpoint, and indexing those files would suppress the tree edits
    /// they still need).
    fn rebuild_index_from_head(&self) -> Result<StoreIndex, human_errors::Error> {
        let mut index = StoreIndex::default();
        let repo = self.repo();
        if self.head_id().is_none() {
            return Ok(index);
        }

        let mut pending: Vec<(String, gix::ObjectId)> = vec![(String::new(), self.head_tree_id())];

        while let Some((prefix, tree_id)) = pending.pop() {
            let tree = repo.find_tree(tree_id).map_err(humanize_git)?;
            for entry in tree.iter() {
                let entry = entry.map_err(humanize_git)?;
                let name = entry.filename().to_string();
                let path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };

                if entry.mode().is_tree() {
                    pending.push((path, entry.object_id()));
                } else if name == MAILBOX_META_FILE {
                    let blob = repo.find_object(entry.object_id()).map_err(humanize_git)?;
                    let sidecar = MailboxSidecar::parse(&blob.data)?;
                    index.mailboxes.insert(MailboxRecord {
                        info: sidecar.to_info(),
                        dir_path: prefix.clone(),
                    });
                } else if name.ends_with(SIDECAR_SUFFIX) {
                    let blob = repo.find_object(entry.object_id()).map_err(humanize_git)?;
                    let sidecar = MessageSidecar::parse(&blob.data)?;
                    let eml_path = path
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

    /// Applies the pending worktree edits to HEAD's tree and returns the new
    /// tree id. Only the final edit per path is applied, since an upserted
    /// file may have been removed later in the same batch (or vice versa).
    fn build_tree(&mut self) -> Result<gix::ObjectId, human_errors::Error> {
        let mut finals: HashMap<String, bool> = HashMap::new();
        for edit in self.pending.drain(..) {
            match edit {
                PathEdit::Upsert(path) => finals.insert(path, true),
                PathEdit::Remove(path) => finals.insert(path, false),
            };
        }

        let repo = self
            .repo
            .as_ref()
            .expect("the store must be opened before use");
        let mut editor = repo.edit_tree(self.head_tree_id()).map_err(humanize_git)?;

        for (path, is_upsert) in finals {
            if is_upsert {
                let absolute = self.inner.root().join(&path);
                let content = std::fs::read(&absolute).wrap_system_err(
                    format!(
                        "Failed to read {} while committing the backup.",
                        absolute.display()
                    ),
                    &["Make sure no other process is modifying the backup directory."],
                )?;
                let blob = repo.write_blob(&content).map_err(humanize_git)?;
                editor
                    .upsert(path.as_str(), gix::object::tree::EntryKind::Blob, blob)
                    .map_err(humanize_git)?;
            } else {
                editor.remove(path.as_str()).map_err(humanize_git)?;
            }
        }

        Ok(editor.write().map_err(humanize_git)?.detach())
    }

    fn commit_message(checkpoint: &Checkpoint) -> String {
        if checkpoint.description.is_empty() {
            format!("Mail backup for {}", checkpoint.date)
        } else {
            format!(
                "Mail backup for {}\n\n{}",
                checkpoint.date, checkpoint.description
            )
        }
    }

    /// Creates a new commit on HEAD with both signature times set to the
    /// given moment (backdated for backfill snapshots).
    fn commit_new(
        &mut self,
        tree: gix::ObjectId,
        message: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<gix::ObjectId, human_errors::Error> {
        let time = Self::signature_time(at);
        let signature = self.signature(&time);
        let parents: Vec<gix::ObjectId> = self.head_id().into_iter().collect();

        let id = self
            .repo()
            .commit_as(signature, signature, "HEAD", message, tree, parents)
            .map_err(humanize_git)?;
        Ok(id.detach())
    }

    /// Rewrites the current HEAD commit with an updated tree and message,
    /// keeping its parents and author (so the day's first-change time is
    /// preserved) while refreshing the committer time.
    fn amend_head(
        &mut self,
        head: gix::ObjectId,
        tree: gix::ObjectId,
        message: &str,
    ) -> Result<gix::ObjectId, human_errors::Error> {
        let repo = self
            .repo
            .as_ref()
            .expect("the store must be opened before use");

        let old = repo.find_commit(head).map_err(humanize_git)?;
        let parents: Vec<gix::ObjectId> = old.parent_ids().map(|id| id.detach()).collect();
        let author = old.author().map_err(humanize_git)?;
        let author_name = author.name.to_string();
        let author_email = author.email.to_string();
        let author_time = author.time.to_string();
        drop(old);

        let committer_time = Self::signature_time(chrono::Utc::now());
        let committer = self.signature(&committer_time);
        let author = gix::actor::SignatureRef {
            name: author_name.as_str().into(),
            email: author_email.as_str().into(),
            time: author_time.as_str(),
        };

        let new_commit = repo
            .new_commit_as(committer, author, message, tree, parents)
            .map_err(humanize_git)?;
        let new_id = new_commit.id().detach();
        drop(new_commit);

        let branch = repo
            .head_name()
            .map_err(humanize_git)?
            .expect("amend is only attempted on a born, attached HEAD");

        use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit, RefLog};
        repo.edit_reference(RefEdit {
            change: Change::Update {
                log: LogChange {
                    mode: RefLog::AndReference,
                    force_create_reflog: false,
                    message: "mail-backup: amend daily snapshot".into(),
                },
                expected: PreviousValue::MustExistAndMatch(gix::refs::Target::Object(head)),
                new: gix::refs::Target::Object(new_id),
            },
            name: branch,
            deref: false,
        })
        .map_err(humanize_git)?;

        Ok(new_id)
    }

    /// Compares the committed HEAD tree against a scan of the worktree,
    /// returning every inconsistency found. Files written but not yet
    /// committed (a pending batch) show up here, so this is only meaningful
    /// at rest.
    pub fn verify(&self) -> Result<Vec<String>, human_errors::Error> {
        let committed = self.rebuild_index_from_head()?;
        let on_disk = StoreIndex::rebuild_from_dir(self.inner.root())?;
        Ok(super::index::diff_indexes(&committed, &on_disk))
    }

    /// Refreshes `.git/index` from the committed tree so that a user running
    /// `git status` in the backup repository sees a clean worktree. Failure
    /// is harmless (the index is purely cosmetic for this store).
    fn refresh_git_index(&self, tree: gix::ObjectId) {
        let repo = self.repo();
        match repo.index_from_tree(&tree) {
            Ok(mut index) => {
                if let Err(e) = index.write(Default::default()) {
                    debug!("Could not refresh the git index after committing: {}", e);
                }
            }
            Err(e) => debug!("Could not build the git index after committing: {}", e),
        }
    }
}

fn humanize_git(err: impl std::error::Error + Send + Sync + 'static) -> human_errors::Error {
    human_errors::wrap_system(
        err,
        "A git operation on the backup repository failed.",
        &[
            "Make sure the backup repository has not been modified by another process, and report this issue on GitHub if it persists.",
        ],
    )
}

impl MailStore for GitMailStore {
    fn kind(&self) -> &'static str {
        "git"
    }

    async fn open(&mut self) -> Result<(), human_errors::Error> {
        std::fs::create_dir_all(self.inner.root()).wrap_user_err(
            format!(
                "Failed to create the backup directory {}.",
                self.inner.root().display()
            ),
            &["Make sure the configured backup path is valid and writable by the process."],
        )?;

        let root = self.inner.root().to_path_buf();
        let fresh = !root.join(".git").exists();
        let repo = if fresh {
            gix::init(&root).map_err(humanize_git)?
        } else {
            gix::open(&root).map_err(humanize_git)?
        };
        self.repo = Some(repo);

        if fresh {
            self.initialize_repository()?;
            self.reload_repository()?;
        }
        self.ensure_committer()?;
        self.ensure_initial_commit()?;

        self.inner.open_without_index()?;

        // Trust the cached index only when it was saved against the current
        // HEAD; otherwise rebuild from the committed tree (never the
        // worktree, which may be ahead after a crash).
        let head = self.head_id().map(|id| id.to_string());
        let index = if self.inner.state().head_at_save == head {
            match self.inner.load_cached_index() {
                Some(index) => index,
                None => self.rebuild_index_from_head()?,
            }
        } else {
            if self.inner.state().head_at_save.is_some() {
                info!(
                    "The backup repository's HEAD does not match the saved state; rebuilding the index from the committed tree"
                );
            }
            self.rebuild_index_from_head()?
        };
        self.inner.set_index(index);

        Ok(())
    }

    fn state(&self) -> &StoreState {
        self.inner.state()
    }

    fn state_mut(&mut self) -> &mut StoreState {
        self.inner.state_mut()
    }

    fn mailboxes(&self) -> &MailboxIndex {
        self.inner.mailboxes()
    }

    fn lookup(&self, message_id: &str) -> Option<&StoredMessage> {
        self.inner.lookup(message_id)
    }

    fn list(&self) -> impl Iterator<Item = &StoredMessage> {
        self.inner.list()
    }

    async fn apply(
        &mut self,
        events: Vec<MailEvent>,
    ) -> Result<Vec<EventOutcome>, human_errors::Error> {
        let outcomes = self.inner.apply(events).await?;
        self.pending.extend(self.inner.take_path_edits());
        Ok(outcomes)
    }

    async fn checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<(), human_errors::Error> {
        let head = self.head_id();
        let head_tree = self.head_tree_id();
        let new_tree = if self.pending.is_empty() {
            head_tree
        } else {
            self.build_tree()?
        };

        let message = Self::commit_message(checkpoint);

        let new_head = match checkpoint.kind {
            _ if new_tree == head_tree => {
                // Nothing changed in the tree; don't create an empty commit.
                head
            }
            SnapshotKind::Backfill => {
                let end_of_day = checkpoint
                    .date
                    .and_hms_opt(23, 59, 59)
                    .expect("23:59:59 is a valid time")
                    .and_utc();
                Some(self.commit_new(new_tree, &message, end_of_day)?)
            }
            SnapshotKind::Live => {
                let amendable = self.inner.state().current_commit_day == Some(checkpoint.date)
                    && self.inner.state().head_at_save.as_deref()
                        == head.map(|h| h.to_string()).as_deref()
                    && head.is_some();

                let id = if amendable {
                    self.amend_head(head.expect("checked above"), new_tree, &message)?
                } else {
                    self.commit_new(new_tree, &message, chrono::Utc::now())?
                };
                self.inner.state_mut().current_commit_day = Some(checkpoint.date);
                Some(id)
            }
        };

        self.inner.state_mut().head_at_save = new_head.map(|id| id.to_string());
        self.inner.checkpoint(checkpoint).await?;

        if let Some(_id) = new_head {
            self.refresh_git_index(new_tree);
        }

        Ok(())
    }

    async fn save_state(&mut self) -> Result<(), human_errors::Error> {
        // Persist state and index only; any pending worktree edits stay queued
        // for the next checkpoint rather than producing a commit here.
        self.inner.save_state().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::{MailMessage, MailboxInfo, MessageMeta};
    use crate::stores::layout;

    fn mailbox(id: &str, name: &str, role: Option<&str>) -> MailboxInfo {
        MailboxInfo {
            id: id.to_string(),
            name: name.to_string(),
            role: role.map(str::to_string),
            parent_id: None,
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

    fn added(store: &GitMailStore, meta: MessageMeta, raw: &[u8]) -> MailEvent {
        MailEvent::MessageAdded {
            message: MailMessage::new(meta, store.mailboxes()),
            raw: raw.to_vec(),
        }
    }

    fn date(s: &str) -> chrono::NaiveDate {
        s.parse().unwrap()
    }

    fn backfill_checkpoint(day: &str) -> Checkpoint {
        Checkpoint {
            date: date(day),
            kind: SnapshotKind::Backfill,
            description: format!("backfill {day}"),
        }
    }

    fn live_checkpoint(day: &str) -> Checkpoint {
        Checkpoint {
            date: date(day),
            kind: SnapshotKind::Live,
            description: "live".to_string(),
        }
    }

    async fn open_store() -> (tempfile::TempDir, GitMailStore) {
        let dir = tempfile::tempdir().unwrap();
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();
        (dir, store)
    }

    /// Collects (message summary, author timestamp) for HEAD's ancestry,
    /// oldest first.
    fn log(store: &GitMailStore) -> Vec<(String, i64)> {
        let repo = store.repo();
        let mut commits = Vec::new();
        let mut current = store.head_id();
        while let Some(id) = current {
            let commit = repo.find_commit(id).unwrap();
            let message = commit.message_raw().unwrap().to_string();
            let author_time = commit
                .author()
                .unwrap()
                .time
                .split_whitespace()
                .next()
                .unwrap()
                .parse::<i64>()
                .unwrap();
            current = commit.parent_ids().next().map(|p| p.detach());
            commits.push((message, author_time));
        }
        commits.reverse();
        commits
    }

    /// Finds the blob id of a path within the tree of the given commit.
    fn blob_at(store: &GitMailStore, commit: gix::ObjectId, path: &str) -> Option<gix::ObjectId> {
        let repo = store.repo();
        let commit = repo.find_commit(commit).unwrap();
        let mut tree_id = commit.tree_id().unwrap().detach();
        let mut segments: Vec<&str> = path.split('/').collect();
        let file = segments.pop().unwrap();

        for segment in segments {
            let tree = repo.find_tree(tree_id).unwrap();
            let entry = tree
                .iter()
                .filter_map(|e| e.ok())
                .find(|e| e.filename() == segment)?;
            tree_id = entry.object_id();
        }

        let tree = repo.find_tree(tree_id).unwrap();
        tree.iter()
            .filter_map(|e| e.ok())
            .find(|e| e.filename() == file)
            .map(|e| e.object_id())
    }

    #[tokio::test]
    async fn initial_commit_protects_line_endings() {
        let (dir, store) = open_store().await;
        assert!(dir.path().join(".gitattributes").exists());

        let head = store.head_id().expect("an initial commit exists");
        let blob = blob_at(&store, head, ".gitattributes").expect(".gitattributes committed");
        let repo = store.repo();
        assert_eq!(
            repo.find_object(blob).unwrap().data.as_slice(),
            b"* -text\n"
        );

        let config = std::fs::read_to_string(dir.path().join(".git/config")).unwrap();
        assert!(config.contains("autocrlf = false"));
        assert!(config.contains("longpaths = true"));
    }

    #[tokio::test]
    async fn backfill_produces_backdated_daily_commits() {
        let (_dir, mut store) = open_store().await;

        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
            ))])
            .await
            .unwrap();

        for (i, day) in ["2023-01-01", "2023-01-02", "2023-01-03"]
            .iter()
            .enumerate()
        {
            let m = meta(
                &format!("M{i}"),
                &["mb-inbox"],
                &[],
                &format!("{day}T08:00:00Z"),
            );
            let raw = format!("message {i}");
            let event = added(&store, m, raw.as_bytes());
            store.apply(vec![event]).await.unwrap();
            store.checkpoint(&backfill_checkpoint(day)).await.unwrap();
        }

        let history = log(&store);
        // Initial commit + 3 daily snapshots.
        assert_eq!(history.len(), 4);
        assert!(history[1].0.starts_with("Mail backup for 2023-01-01"));
        assert!(history[2].0.starts_with("Mail backup for 2023-01-02"));
        assert!(history[3].0.starts_with("Mail backup for 2023-01-03"));

        // Author times are the historical day-ends, in chronological order.
        let expected_day_1 = chrono::DateTime::parse_from_rfc3339("2023-01-01T23:59:59Z")
            .unwrap()
            .timestamp();
        assert_eq!(history[1].1, expected_day_1);
        assert!(history[1].1 < history[2].1);
        assert!(history[2].1 < history[3].1);
    }

    #[tokio::test]
    async fn live_checkpoints_amend_the_days_commit() {
        let (_dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
            ))])
            .await
            .unwrap();

        let event = added(
            &store,
            meta("M1", &["mb-inbox"], &[], "2026-06-11T08:00:00Z"),
            b"first",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();
        let after_first = log(&store);

        let event = added(
            &store,
            meta("M2", &["mb-inbox"], &[], "2026-06-11T09:00:00Z"),
            b"second",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();
        let after_second = log(&store);

        // Amended: same number of commits, same author time, both messages present.
        assert_eq!(after_first.len(), after_second.len());
        assert_eq!(
            after_first.last().unwrap().1,
            after_second.last().unwrap().1
        );
        assert_eq!(store.list().count(), 2);

        // A new day produces a new commit instead.
        let event = added(
            &store,
            meta("M3", &["mb-inbox"], &[], "2026-06-12T01:00:00Z"),
            b"third",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-12"))
            .await
            .unwrap();
        assert_eq!(log(&store).len(), after_second.len() + 1);
    }

    #[tokio::test]
    async fn checkpoint_without_changes_creates_no_commit() {
        let (_dir, mut store) = open_store().await;
        let before = log(&store).len();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();
        assert_eq!(log(&store).len(), before);
    }

    #[tokio::test]
    async fn moves_keep_blob_identity_for_rename_detection() {
        let (_dir, mut store) = open_store().await;
        store
            .apply(vec![
                MailEvent::MailboxUpserted(mailbox("mb-inbox", "Inbox", Some("inbox"))),
                MailEvent::MailboxUpserted(mailbox("mb-archive", "Archive", Some("archive"))),
            ])
            .await
            .unwrap();

        let event = added(
            &store,
            meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
            b"movable content",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&backfill_checkpoint("2023-06-15"))
            .await
            .unwrap();

        let old_path = store.lookup("M1").unwrap().path.clone();
        let old_head = store.head_id().unwrap();
        let old_blob = blob_at(&store, old_head, &old_path).expect("blob committed");

        let update = MailEvent::MessageUpdated {
            message: MailMessage::new(
                meta("M1", &["mb-archive"], &[], "2023-06-15T10:30:00Z"),
                store.mailboxes(),
            ),
        };
        store.apply(vec![update]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();

        let new_path = store.lookup("M1").unwrap().path.clone();
        assert_ne!(new_path, old_path);

        let new_head = store.head_id().unwrap();
        let new_blob = blob_at(&store, new_head, &new_path).expect("blob at new path");
        assert_eq!(
            old_blob, new_blob,
            "byte-identical blob enables rename detection"
        );
        assert!(
            blob_at(&store, new_head, &old_path).is_none(),
            "old path removed"
        );
        // History still holds the old location.
        assert!(blob_at(&store, old_head, &old_path).is_some());
    }

    #[tokio::test]
    async fn index_rebuilds_from_head_tree() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
            store.open().await.unwrap();
            store
                .apply(vec![MailEvent::MailboxUpserted(mailbox(
                    "mb-inbox",
                    "Inbox",
                    Some("inbox"),
                ))])
                .await
                .unwrap();
            let event = added(
                &store,
                meta("M1", &["mb-inbox"], &["$seen"], "2023-06-15T10:30:00Z"),
                b"raw",
            );
            store.apply(vec![event]).await.unwrap();
            store
                .checkpoint(&backfill_checkpoint("2023-06-15"))
                .await
                .unwrap();
        }

        // Delete the cached index; the store must rebuild from the HEAD tree.
        std::fs::remove_file(dir.path().join(".git/mail-backup/index.json")).unwrap();

        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();
        let stored = store.lookup("M1").expect("index rebuilt from HEAD");
        assert!(stored.meta.keywords.contains("$seen"));
        assert_eq!(store.mailboxes().get("mb-inbox").unwrap().dir_path, "Inbox");
    }

    #[tokio::test]
    async fn crash_before_checkpoint_recovers_via_redelivery() {
        let dir = tempfile::tempdir().unwrap();

        // First run: apply but crash before checkpointing (no commit, no
        // state save) — the worktree is ahead of HEAD.
        {
            let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
            store.open().await.unwrap();
            store
                .apply(vec![MailEvent::MailboxUpserted(mailbox(
                    "mb-inbox",
                    "Inbox",
                    Some("inbox"),
                ))])
                .await
                .unwrap();
            let event = added(
                &store,
                meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
                b"raw content",
            );
            store.apply(vec![event]).await.unwrap();
            // Dropped without checkpoint: simulates a crash.
        }

        // Recovery: the source redelivers the same changes (state was never
        // advanced); applying them again must converge and commit everything.
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();
        assert!(
            store.lookup("M1").is_none(),
            "index reflects HEAD, not the worktree"
        );

        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
            ))])
            .await
            .unwrap();
        let event = added(
            &store,
            meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
            b"raw content",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&backfill_checkpoint("2023-06-15"))
            .await
            .unwrap();

        let stored = store.lookup("M1").expect("message recovered");
        let head = store.head_id().unwrap();
        assert!(
            blob_at(&store, head, &stored.path).is_some(),
            "file committed"
        );
        assert!(
            blob_at(&store, head, &layout::sidecar_path(&stored.path)).is_some(),
            "sidecar committed"
        );
    }

    /// Runs the real `git` CLI in a directory, returning stdout (or `None`
    /// when git is not installed, in which case the test is skipped).
    fn git_cli(root: &std::path::Path, args: &[&str]) -> Option<String> {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .output()
            .ok()?;
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        Some(String::from_utf8_lossy(&output.stdout).to_string())
    }

    #[tokio::test]
    async fn repositories_are_valid_to_the_real_git_cli() {
        let (dir, mut store) = open_store().await;
        if git_cli(dir.path(), &["--version"]).is_none() {
            eprintln!("git is not installed; skipping CLI compatibility test");
            return;
        }

        store
            .apply(vec![
                MailEvent::MailboxUpserted(mailbox("mb-inbox", "Inbox", Some("inbox"))),
                MailEvent::MailboxUpserted(mailbox("mb-archive", "Archive", Some("archive"))),
            ])
            .await
            .unwrap();
        let event = added(
            &store,
            meta("M1", &["mb-inbox"], &[], "2023-06-15T10:30:00Z"),
            b"a message body that is long enough for rename similarity detection to work with",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&backfill_checkpoint("2023-06-15"))
            .await
            .unwrap();

        let old_path = store.lookup("M1").unwrap().path.clone();

        // Move the message to another mailbox in a second commit.
        let update = MailEvent::MessageUpdated {
            message: MailMessage::new(
                meta("M1", &["mb-archive"], &[], "2023-06-15T10:30:00Z"),
                store.mailboxes(),
            ),
        };
        store.apply(vec![update]).await.unwrap();
        store
            .checkpoint(&backfill_checkpoint("2023-06-16"))
            .await
            .unwrap();
        let new_path = store.lookup("M1").unwrap().path.clone();

        // The repository is intact and the worktree is clean.
        git_cli(dir.path(), &["fsck", "--strict"]);
        let status = git_cli(dir.path(), &["status", "--porcelain"]).unwrap();
        assert_eq!(status.trim(), "", "git sees a clean worktree");

        // History is shaped as expected and backdated.
        let log = git_cli(
            dir.path(),
            &["log", "--format=%s|%ad", "--date=format:%Y-%m-%d"],
        )
        .unwrap();
        assert!(
            log.contains("Mail backup for 2023-06-15|2023-06-15"),
            "log: {log}"
        );

        // git's rename detection identifies the move (byte-identical content).
        let diff = git_cli(
            dir.path(),
            &[
                "diff",
                "-M",
                "--name-status",
                "HEAD~1",
                "HEAD",
                "--",
                "*.eml",
            ],
        )
        .unwrap();
        let rename_line = diff
            .lines()
            .find(|l| l.starts_with('R'))
            .unwrap_or_else(|| panic!("no rename detected in: {diff}"));
        assert!(
            rename_line.starts_with("R100"),
            "100% similarity: {rename_line}"
        );
        assert!(rename_line.contains(&old_path));
        assert!(rename_line.contains(&new_path));
    }

    #[tokio::test]
    async fn keyword_change_amends_with_sidecar_diff_only() {
        let (_dir, mut store) = open_store().await;
        store
            .apply(vec![MailEvent::MailboxUpserted(mailbox(
                "mb-inbox",
                "Inbox",
                Some("inbox"),
            ))])
            .await
            .unwrap();
        let event = added(
            &store,
            meta("M1", &["mb-inbox"], &[], "2026-06-11T08:00:00Z"),
            b"raw",
        );
        store.apply(vec![event]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();

        let path = store.lookup("M1").unwrap().path.clone();
        let head_before = store.head_id().unwrap();
        let eml_before = blob_at(&store, head_before, &path).unwrap();

        let update = MailEvent::MessageUpdated {
            message: MailMessage::new(
                meta("M1", &["mb-inbox"], &["$seen"], "2026-06-11T08:00:00Z"),
                store.mailboxes(),
            ),
        };
        store.apply(vec![update]).await.unwrap();
        store
            .checkpoint(&live_checkpoint("2026-06-11"))
            .await
            .unwrap();

        let head_after = store.head_id().unwrap();
        assert_ne!(head_before, head_after, "the day's commit was rewritten");
        assert_eq!(
            blob_at(&store, head_after, &path).unwrap(),
            eml_before,
            "the .eml blob is untouched"
        );
        assert_ne!(
            blob_at(&store, head_after, &layout::sidecar_path(&path)),
            blob_at(&store, head_before, &layout::sidecar_path(&path)),
            "the sidecar blob changed"
        );
    }
}
