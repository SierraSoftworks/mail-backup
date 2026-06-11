//! Reads a mail archive (git repository or plain directory) back into memory
//! for restoration, optionally as it existed at a historical point in time.

use std::collections::HashMap;
use std::path::PathBuf;

use human_errors::ResultExt;
use tracing_batteries::prelude::*;

use crate::entities::mail::{MailboxIndex, MailboxRecord, MessageMeta};
use crate::policy::StoreConfig;
use crate::stores::index::StoreIndex;
use crate::stores::layout::{MAILBOX_META_FILE, MESSAGE_SUFFIX, SIDECAR_SUFFIX};
use crate::stores::sidecar::{MailboxSidecar, MessageSidecar};

pub struct ArchiveMessage {
    pub meta: MessageMeta,
    pub path: String,
    pub sha256: String,
    content: ContentKey,
}

enum ContentKey {
    File(PathBuf),
    Blob(gix::ObjectId),
}

#[allow(clippy::large_enum_variant)] // exactly one instance exists per restore
enum ArchiveSource {
    Dir,
    Git(gix::Repository),
}

/// A point-in-time view of a mail archive.
pub struct Archive {
    source: ArchiveSource,
    pub mailboxes: Vec<MailboxRecord>,
    pub messages: Vec<ArchiveMessage>,
    /// The commit the archive was read from (git archives only).
    pub commit: Option<String>,
}

impl Archive {
    /// Opens the archive described by a store configuration. `at` selects a
    /// historical snapshot: either a `YYYY-MM-DD` date (the snapshot at the
    /// end of that day) or a git revision; it requires a git archive.
    pub fn open(config: &StoreConfig, at: Option<&str>) -> Result<Self, human_errors::Error> {
        match config {
            StoreConfig::LocalDir { path } => {
                if at.is_some() {
                    return Err(human_errors::user(
                        "Point-in-time restore (--at) requires a git archive.",
                        &[
                            "Plain directory stores keep no history; restore them without the --at option.",
                        ],
                    ));
                }
                Self::open_dir(path.clone())
            }
            StoreConfig::LocalGit { path, .. } => Self::open_git(path.clone(), at),
        }
    }

    fn open_dir(root: PathBuf) -> Result<Self, human_errors::Error> {
        let index = StoreIndex::rebuild_from_dir(&root)?;

        let mailboxes: Vec<MailboxRecord> = index.mailboxes.iter().cloned().collect();
        let messages = index
            .messages()
            .map(|stored| ArchiveMessage {
                meta: stored.meta.clone(),
                path: stored.path.clone(),
                sha256: stored.sha256.clone(),
                content: ContentKey::File(root.join(&stored.path)),
            })
            .collect();

        Ok(Self {
            source: ArchiveSource::Dir,
            mailboxes,
            messages,
            commit: None,
        })
    }

    fn open_git(root: PathBuf, at: Option<&str>) -> Result<Self, human_errors::Error> {
        let repo = gix::open(&root).wrap_user_err(
            format!(
                "Failed to open the backup repository at {}.",
                root.display()
            ),
            &["Make sure the configured path points at an existing mail backup repository."],
        )?;

        let commit_id = resolve_commit(&repo, at)?;
        let commit = repo.find_commit(commit_id).map_err(git_error)?;
        let tree_id = commit.tree_id().map_err(git_error)?.detach();
        drop(commit);

        // Walk the committed tree, collecting every blob; messages are then
        // assembled by pairing each sidecar with its adjacent .eml blob.
        let mut blobs: HashMap<String, gix::ObjectId> = HashMap::new();
        let mut mailboxes = Vec::new();
        let mut sidecars: Vec<(String, MessageSidecar)> = Vec::new();

        let mut pending: Vec<(String, gix::ObjectId)> = vec![(String::new(), tree_id)];
        while let Some((prefix, tree_id)) = pending.pop() {
            let tree = repo.find_tree(tree_id).map_err(git_error)?;
            for entry in tree.iter() {
                let entry = entry.map_err(git_error)?;
                let name = entry.filename().to_string();
                let path = if prefix.is_empty() {
                    name.clone()
                } else {
                    format!("{prefix}/{name}")
                };

                if entry.mode().is_tree() {
                    pending.push((path, entry.object_id()));
                } else if name == MAILBOX_META_FILE {
                    let blob = repo.find_object(entry.object_id()).map_err(git_error)?;
                    let sidecar = MailboxSidecar::parse(&blob.data)?;
                    mailboxes.push(MailboxRecord {
                        info: sidecar.to_info(),
                        dir_path: prefix.clone(),
                    });
                } else if name.ends_with(SIDECAR_SUFFIX) {
                    let blob = repo.find_object(entry.object_id()).map_err(git_error)?;
                    sidecars.push((path, MessageSidecar::parse(&blob.data)?));
                } else {
                    blobs.insert(path, entry.object_id());
                }
            }
        }

        let mut messages = Vec::with_capacity(sidecars.len());
        for (sidecar_path, sidecar) in sidecars {
            let eml_path = sidecar_path
                .strip_suffix(SIDECAR_SUFFIX)
                .map(|stem| format!("{stem}{MESSAGE_SUFFIX}"))
                .expect("sidecar paths always end with the sidecar suffix");

            match blobs.get(&eml_path) {
                Some(oid) => messages.push(ArchiveMessage {
                    meta: sidecar.to_meta(),
                    path: eml_path,
                    sha256: sidecar.sha256,
                    content: ContentKey::Blob(*oid),
                }),
                None => warn!(
                    "The archive has a metadata sidecar at {} but no matching message file; skipping it",
                    sidecar_path
                ),
            }
        }

        Ok(Self {
            source: ArchiveSource::Git(repo),
            mailboxes,
            messages,
            commit: Some(commit_id.to_string()),
        })
    }

    /// A mailbox index over the archive's mailboxes (used for filter
    /// evaluation and primary-mailbox resolution).
    pub fn mailbox_index(&self) -> MailboxIndex {
        let mut index = MailboxIndex::default();
        for record in &self.mailboxes {
            index.insert(record.clone());
        }
        index
    }

    /// Reads and verifies the raw bytes of an archived message. A checksum
    /// mismatch means the archive is corrupt (or was modified outside
    /// mail-backup) and the message is not safe to restore.
    pub fn read(&self, message: &ArchiveMessage) -> Result<Vec<u8>, human_errors::Error> {
        let raw = match (&message.content, &self.source) {
            (ContentKey::File(path), _) => std::fs::read(path).wrap_system_err(
                format!("Failed to read the archived message {}.", path.display()),
                &["Make sure the archive has not been modified and is readable by the process."],
            )?,
            (ContentKey::Blob(oid), ArchiveSource::Git(repo)) => {
                repo.find_object(*oid).map_err(git_error)?.data.clone()
            }
            (ContentKey::Blob(_), ArchiveSource::Dir) => {
                unreachable!("blob keys only exist for git archives")
            }
        };

        let digest = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(&raw);
            let mut out = String::with_capacity(64);
            for byte in digest.iter() {
                out.push_str(&format!("{byte:02x}"));
            }
            out
        };

        if digest != message.sha256 {
            return Err(human_errors::system(
                format!(
                    "The archived message at {} does not match its recorded checksum.",
                    message.path
                ),
                &[
                    "The archive may be corrupt or modified; restore the file from git history (or skip it) before retrying.",
                ],
            ));
        }

        Ok(raw)
    }
}

fn git_error(err: impl std::error::Error + Send + Sync + 'static) -> human_errors::Error {
    human_errors::wrap_system(
        err,
        "A git operation on the backup repository failed while reading the archive.",
        &[
            "Make sure the backup repository is intact, and report this issue on GitHub if it persists.",
        ],
    )
}

/// Resolves the commit a restore should read from.
fn resolve_commit(
    repo: &gix::Repository,
    at: Option<&str>,
) -> Result<gix::ObjectId, human_errors::Error> {
    let head = repo
        .head_id()
        .map_err(|_| {
            human_errors::user(
                "The backup repository has no commits to restore from.",
                &["Run a backup first to populate the archive."],
            )
        })?
        .detach();

    let Some(at) = at else {
        return Ok(head);
    };

    if let Ok(date) = at.parse::<chrono::NaiveDate>() {
        // Daily snapshots are linear: walk first-parent history back to the
        // most recent commit at or before the end of the requested day.
        let cutoff = date
            .and_hms_opt(23, 59, 59)
            .expect("23:59:59 is a valid time")
            .and_utc()
            .timestamp();

        let mut current = Some(head);
        while let Some(id) = current {
            let commit = repo.find_commit(id).map_err(git_error)?;
            let committed_at: i64 = commit
                .committer()
                .map_err(git_error)?
                .time
                .split_whitespace()
                .next()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0);

            if committed_at <= cutoff {
                return Ok(id);
            }
            current = commit.parent_ids().next().map(|p| p.detach());
        }

        return Err(human_errors::user(
            format!("The archive has no snapshot at or before {}.", date),
            &["Pick a later date, or omit --at to restore the latest state."],
        ));
    }

    let id = repo
        .rev_parse_single(at)
        .map_err(|e| {
            human_errors::wrap_user(
                e,
                format!("'{at}' is not a date (YYYY-MM-DD) or a known git revision in the archive."),
                &["Pass a date like 2026-01-31, a commit hash, or omit --at to restore the latest state."],
            )
        })?
        .detach();
    Ok(id)
}
