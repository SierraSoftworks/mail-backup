//! Restores a local mail archive to a mail server, recreating the mailbox
//! tree and re-importing messages with their original keywords, mailbox
//! memberships, and received dates.

pub mod jmap;
pub mod reader;

use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::sync::atomic::AtomicBool;

use tracing_batteries::prelude::*;

use crate::entities::mail::{MailMessage, MailboxInfo, MessageMeta};
use crate::policy::{DedupeMode, RestorePolicy};
use reader::Archive;

/// A destination which archived mail can be restored to.
///
/// Implementations only ever *add* to the target account (create mailboxes,
/// import messages); a restore never modifies or removes existing mail.
pub trait RestoreTarget: Send {
    fn kind(&self) -> &'static str;

    /// Connects and returns the target account id.
    async fn connect(&mut self) -> Result<String, human_errors::Error>;

    async fn list_mailboxes(&self) -> Result<Vec<MailboxInfo>, human_errors::Error>;

    async fn create_mailbox(
        &mut self,
        name: &str,
        parent_id: Option<&str>,
        role: Option<&str>,
    ) -> Result<String, human_errors::Error>;

    /// Whether a message matching this metadata already exists on the target
    /// (used to skip duplicates).
    async fn message_exists(&self, meta: &MessageMeta) -> Result<bool, human_errors::Error>;

    /// Uploads and imports a raw message. Note that imports are not retried
    /// automatically (a retry after an ambiguous failure could duplicate the
    /// message); failed messages are reported and picked up by a re-run,
    /// where deduplication skips everything that made it through.
    async fn import(
        &mut self,
        raw: Vec<u8>,
        mailbox_ids: Vec<String>,
        keywords: Vec<String>,
        received_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<String, human_errors::Error>;
}

#[derive(Clone, Debug, Default)]
pub struct RestoreOptions {
    /// Restore the archive as it was at this date (YYYY-MM-DD) or commit.
    pub at: Option<String>,
    /// Overrides the policy's filter expression.
    pub filter: Option<String>,
    /// Import messages even when they already exist on the target.
    pub force: bool,
    pub dry_run: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreSummary {
    /// Messages selected by the filter.
    pub selected: usize,
    pub imported: usize,
    pub skipped_existing: usize,
    pub skipped_filter: usize,
    pub failed: usize,
    pub mailboxes_created: usize,
    pub dry_run: bool,
}

impl Display for RestoreSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.dry_run {
            write!(
                f,
                "[dry-run] would import {} messages ({} already on the server, {} filtered out) and create {} mailboxes",
                self.selected - self.skipped_existing,
                self.skipped_existing,
                self.skipped_filter,
                self.mailboxes_created
            )
        } else {
            write!(
                f,
                "{} imported, {} already present, {} filtered out, {} failed, {} mailboxes created",
                self.imported,
                self.skipped_existing,
                self.skipped_filter,
                self.failed,
                self.mailboxes_created
            )
        }
    }
}

/// Runs a restore: read the archive (optionally at a point in time), select
/// messages with the filter, recreate the mailbox tree on the target, and
/// import everything that isn't already there.
pub async fn run_restore<T: RestoreTarget>(
    target: &mut T,
    policy: &RestorePolicy,
    options: &RestoreOptions,
    cancel: &AtomicBool,
) -> Result<RestoreSummary, human_errors::Error> {
    let mut summary = RestoreSummary {
        dry_run: options.dry_run,
        ..Default::default()
    };

    let filter = match &options.filter {
        Some(expression) => &crate::Filter::new(expression.as_str())?,
        None => &policy.filter,
    };

    // 1. Read the archive.
    let archive = Archive::open(&policy.from, options.at.as_deref())?;
    if let Some(commit) = &archive.commit {
        info!("Restoring from snapshot {}", commit);
    }
    let mailbox_index = archive.mailbox_index();

    // 2. Select messages.
    let mut selected = Vec::new();
    for message in &archive.messages {
        let view = MailMessage::new(message.meta.clone(), &mailbox_index);
        if filter.matches(&view)? {
            selected.push(message);
        } else {
            summary.skipped_filter += 1;
        }
    }
    summary.selected = selected.len();
    info!(
        "Selected {} of {} archived messages for restore",
        selected.len(),
        archive.messages.len()
    );

    // 3. Plan the mailboxes each message restores into. Mailbox ids in a
    //    sidecar which no longer resolve (the mailbox was later deleted) fall
    //    back to the mailbox owning the message's directory; messages with no
    //    resolvable mailbox at all go to a synthetic "Unfiled" mailbox.
    let by_dir: HashMap<&str, &str> = archive
        .mailboxes
        .iter()
        .map(|r| (r.dir_path.as_str(), r.info.id.as_str()))
        .collect();

    let mut needed: BTreeSet<String> = BTreeSet::new();
    let mut unfiled_needed = false;
    let mut message_mailboxes: Vec<Vec<String>> = Vec::with_capacity(selected.len());

    for message in &selected {
        let mut resolved: Vec<String> = message
            .meta
            .mailbox_ids
            .iter()
            .filter(|id| mailbox_index.get(id).is_some())
            .cloned()
            .collect();

        if resolved.is_empty() {
            let dir = message.path.rsplit_once('/').map(|(d, _)| d).unwrap_or("");
            match by_dir.get(dir) {
                Some(id) => resolved.push((*id).to_string()),
                None => {
                    unfiled_needed = true;
                    resolved.push(UNFILED_ARCHIVE_ID.to_string());
                }
            }
        }

        needed.extend(resolved.iter().cloned());
        message_mailboxes.push(resolved);
    }

    // Include all ancestors so the tree can be built parents-first.
    let mut with_ancestors = needed.clone();
    for id in &needed {
        let mut current = mailbox_index.get(id);
        while let Some(record) = current {
            current = record
                .info
                .parent_id
                .as_deref()
                .and_then(|p| mailbox_index.get(p));
            if let Some(parent) = &current {
                with_ancestors.insert(parent.info.id.clone());
            }
        }
    }

    // 4. Connect and recreate the mailbox tree.
    let account = target.connect().await?;
    info!("Restoring to account {} via {}", account, target.kind());

    let mapping = ensure_mailboxes(
        target,
        &archive,
        &with_ancestors,
        unfiled_needed,
        policy.mailbox_prefix.as_deref(),
        options.dry_run,
        &mut summary,
    )
    .await?;

    // 5. Import, skipping messages the target already has.
    let mut failures = Vec::new();
    for (message, archive_mailboxes) in selected.iter().zip(message_mailboxes) {
        if super::engine::cancelled(cancel) {
            info!("Restore interrupted; re-running it will skip everything already imported");
            break;
        }

        let dedupe = !options.force && policy.dedupe == DedupeMode::MessageId;
        if dedupe && target.message_exists(&message.meta).await? {
            summary.skipped_existing += 1;
            continue;
        }

        if options.dry_run {
            continue;
        }

        let mailbox_ids: Vec<String> = archive_mailboxes
            .iter()
            .filter_map(|id| mapping.get(id).cloned())
            .collect();

        let raw = archive.read(message)?;
        match target
            .import(
                raw,
                mailbox_ids,
                message.meta.keywords.iter().cloned().collect(),
                message.meta.received_at,
            )
            .await
        {
            Ok(_) => summary.imported += 1,
            Err(e) => {
                summary.failed += 1;
                warn!("Failed to import {}: {}", message.path, e);
                failures.push(message.path.clone());
            }
        }
    }

    if !failures.is_empty() {
        warn!(
            "{} messages failed to import; re-run the restore to retry them (already-imported mail is skipped): {}",
            failures.len(),
            failures
                .iter()
                .take(10)
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    info!("Restore complete: {}", summary);
    Ok(summary)
}

/// The synthetic archive-mailbox id used for messages whose mailboxes cannot
/// be resolved from the archive.
const UNFILED_ARCHIVE_ID: &str = "\u{0}unfiled";

/// Recreates (or matches) the needed mailbox tree on the target, returning
/// the archive-mailbox-id to target-mailbox-id mapping.
async fn ensure_mailboxes<T: RestoreTarget>(
    target: &mut T,
    archive: &Archive,
    needed: &BTreeSet<String>,
    unfiled_needed: bool,
    prefix: Option<&str>,
    dry_run: bool,
    summary: &mut RestoreSummary,
) -> Result<HashMap<String, String>, human_errors::Error> {
    // Existing target mailboxes, addressable by their full (true) name path,
    // case-insensitively.
    let existing = target.list_mailboxes().await?;
    let mut existing_index = crate::entities::mail::MailboxIndex::default();
    for info in &existing {
        existing_index.insert(crate::entities::mail::MailboxRecord {
            info: info.clone(),
            dir_path: String::new(),
        });
    }
    let mut by_path: HashMap<String, String> = existing
        .iter()
        .filter_map(|info| {
            existing_index
                .name_path(&info.id)
                .map(|path| (path.to_lowercase(), info.id.clone()))
        })
        .collect();

    let mut mapping: HashMap<String, String> = HashMap::new();
    let mut dry_run_counter = 0;
    let create = async |target: &mut T,
                        name: &str,
                        parent: Option<&str>,
                        role: Option<&str>,
                        counter: &mut usize,
                        summary: &mut RestoreSummary|
           -> Result<String, human_errors::Error> {
        summary.mailboxes_created += 1;
        if dry_run {
            *counter += 1;
            Ok(format!("dry-run-{counter}"))
        } else {
            target.create_mailbox(name, parent, role).await
        }
    };

    // The prefix mailbox, when configured, roots everything we restore.
    let prefix_id = match prefix {
        Some(prefix) => match by_path.get(&prefix.to_lowercase()) {
            Some(id) => Some(id.clone()),
            None => {
                let id = create(target, prefix, None, None, &mut dry_run_counter, summary).await?;
                by_path.insert(prefix.to_lowercase(), id.clone());
                Some(id)
            }
        },
        None => None,
    };

    // Parents before children: archive mailboxes ordered by path depth.
    let mut ordered: Vec<_> = archive
        .mailboxes
        .iter()
        .filter(|r| needed.contains(&r.info.id))
        .collect();
    ordered.sort_by_key(|r| r.dir_path.matches('/').count());

    let archive_index = archive.mailbox_index();
    for record in ordered {
        let name_path = archive_index
            .name_path(&record.info.id)
            .expect("archive mailboxes resolve their own paths");
        let target_path = match prefix {
            Some(prefix) => format!("{prefix}/{name_path}").to_lowercase(),
            None => name_path.to_lowercase(),
        };

        if let Some(id) = by_path.get(&target_path) {
            mapping.insert(record.info.id.clone(), id.clone());
            continue;
        }

        let parent_target = match record.info.parent_id.as_deref() {
            Some(parent) => mapping.get(parent).cloned().or_else(|| prefix_id.clone()),
            None => prefix_id.clone(),
        };

        // Roles are unique per account; only claim one when restoring into
        // an account which doesn't have it yet (and not under a prefix).
        let role = record.info.role.as_deref().filter(|role| {
            prefix.is_none() && !existing.iter().any(|m| m.role.as_deref() == Some(*role))
        });

        let id = create(
            target,
            &record.info.name,
            parent_target.as_deref(),
            role,
            &mut dry_run_counter,
            summary,
        )
        .await?;
        by_path.insert(target_path, id.clone());
        mapping.insert(record.info.id.clone(), id);
    }

    if unfiled_needed {
        let unfiled_path = match prefix {
            Some(prefix) => format!("{prefix}/unfiled"),
            None => "unfiled".to_string(),
        };
        let id = match by_path.get(&unfiled_path) {
            Some(id) => id.clone(),
            None => {
                create(
                    target,
                    "Unfiled",
                    prefix_id.as_deref(),
                    None,
                    &mut dry_run_counter,
                    summary,
                )
                .await?
            }
        };
        mapping.insert(UNFILED_ARCHIVE_ID.to_string(), id);
    }

    Ok(mapping)
}

/// Ensures dead code analysis sees the helper set used by integration tests.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::{MailEvent, MailboxInfo, MessageMeta};
    use crate::stores::git::GitMailStore;
    use crate::stores::{Checkpoint, MailStore, SnapshotKind};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    static NO_CANCEL: AtomicBool = AtomicBool::new(false);

    /// (name, parent id, role, assigned id) of a created mailbox.
    type CreatedMailbox = (String, Option<String>, Option<String>, String);
    /// (raw bytes, mailbox ids, keywords, received at) of an imported message.
    type ImportedMessage = (
        Vec<u8>,
        Vec<String>,
        Vec<String>,
        chrono::DateTime<chrono::Utc>,
    );

    /// An in-memory restore target which records everything it is asked to do.
    #[derive(Default)]
    struct MockRestoreTarget {
        existing_mailboxes: Vec<MailboxInfo>,
        existing_message_ids: Vec<String>,
        created: Mutex<Vec<CreatedMailbox>>,
        imported: Mutex<Vec<ImportedMessage>>,
        fail_imports: bool,
    }

    impl RestoreTarget for MockRestoreTarget {
        fn kind(&self) -> &'static str {
            "mock"
        }

        async fn connect(&mut self) -> Result<String, human_errors::Error> {
            Ok("acc-target".to_string())
        }

        async fn list_mailboxes(&self) -> Result<Vec<MailboxInfo>, human_errors::Error> {
            Ok(self.existing_mailboxes.clone())
        }

        async fn create_mailbox(
            &mut self,
            name: &str,
            parent_id: Option<&str>,
            role: Option<&str>,
        ) -> Result<String, human_errors::Error> {
            let mut created = self.created.lock().unwrap();
            let id = format!("tgt-{}", created.len());
            created.push((
                name.to_string(),
                parent_id.map(str::to_string),
                role.map(str::to_string),
                id.clone(),
            ));
            Ok(id)
        }

        async fn message_exists(&self, meta: &MessageMeta) -> Result<bool, human_errors::Error> {
            Ok(meta
                .message_id
                .first()
                .is_some_and(|id| self.existing_message_ids.contains(id)))
        }

        async fn import(
            &mut self,
            raw: Vec<u8>,
            mailbox_ids: Vec<String>,
            keywords: Vec<String>,
            received_at: chrono::DateTime<chrono::Utc>,
        ) -> Result<String, human_errors::Error> {
            if self.fail_imports {
                return Err(human_errors::system("import failed", &["test"]));
            }
            let mut imported = self.imported.lock().unwrap();
            imported.push((raw, mailbox_ids, keywords, received_at));
            Ok(format!("imported-{}", imported.len()))
        }
    }

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

    /// Builds a git archive with Inbox + Archive/Receipts and three messages,
    /// committed as two historical days.
    async fn build_archive(root: &std::path::Path) {
        let mut store = GitMailStore::new(root.to_path_buf(), None, None);
        store.open().await.unwrap();

        store
            .apply(vec![
                MailEvent::MailboxUpserted(mailbox("mb-inbox", "Inbox", Some("inbox"), None)),
                MailEvent::MailboxUpserted(mailbox("mb-archive", "Archive", Some("archive"), None)),
                MailEvent::MailboxUpserted(mailbox(
                    "mb-receipts",
                    "Receipts",
                    None,
                    Some("mb-archive"),
                )),
            ])
            .await
            .unwrap();

        for (id, boxes, keywords, received, raw) in [
            (
                "M1",
                &["mb-inbox"][..],
                &["$seen"][..],
                "2023-01-01T08:00:00Z",
                &b"message one"[..],
            ),
            (
                "M2",
                &["mb-inbox"][..],
                &[][..],
                "2023-01-01T15:00:00Z",
                &b"message two"[..],
            ),
            (
                "M3",
                &["mb-receipts"][..],
                &["$seen", "$flagged"][..],
                "2023-01-02T09:00:00Z",
                &b"message three"[..],
            ),
        ] {
            let m = meta(id, boxes, keywords, received);
            let event = MailEvent::MessageAdded {
                message: crate::entities::mail::MailMessage::new(m, store.mailboxes()),
                raw: raw.to_vec(),
            };
            store.apply(vec![event]).await.unwrap();
            store
                .checkpoint(&Checkpoint {
                    date: received[..10].parse().unwrap(),
                    kind: SnapshotKind::Backfill,
                    description: String::new(),
                })
                .await
                .unwrap();
        }
    }

    fn policy(root: &std::path::Path, prefix: Option<&str>) -> RestorePolicy {
        let prefix_line = prefix
            .map(|p| format!("mailbox_prefix: {p}\n"))
            .unwrap_or_default();
        serde_yaml::from_str(&format!(
            "from: !LocalGit {{ path: '{}' }}\nto: !Fastmail {{ token: t }}\n{prefix_line}",
            root.display().to_string().replace('\\', "/")
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn restore_recreates_tree_and_imports_with_fidelity() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget::default();
        let summary = run_restore(
            &mut target,
            &policy(dir.path(), None),
            &RestoreOptions::default(),
            &NO_CANCEL,
        )
        .await
        .unwrap();

        assert_eq!(summary.imported, 3);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.mailboxes_created, 3);

        // Parents created before children, with roles claimed on a fresh account.
        let created = target.created.lock().unwrap();
        let archive_pos = created.iter().position(|(n, ..)| n == "Archive").unwrap();
        let receipts = created.iter().find(|(n, ..)| n == "Receipts").unwrap();
        assert_eq!(
            receipts.1.as_deref(),
            Some(created[archive_pos].3.as_str()),
            "Receipts is created under Archive"
        );
        assert!(
            created
                .iter()
                .any(|(n, _, role, _)| n == "Inbox" && role.as_deref() == Some("inbox")),
        );

        // Full metadata fidelity on import.
        let imported = target.imported.lock().unwrap();
        let m3 = imported
            .iter()
            .find(|(raw, ..)| raw == b"message three")
            .expect("M3 imported");
        assert_eq!(
            m3.2.iter().collect::<BTreeSet<_>>(),
            ["$flagged".to_string(), "$seen".to_string()]
                .iter()
                .collect()
        );
        assert_eq!(m3.3.to_rfc3339(), "2023-01-02T09:00:00+00:00");
        assert_eq!(m3.1.len(), 1, "restored into its (mapped) mailbox");
    }

    #[tokio::test]
    async fn restore_skips_existing_and_respects_filter() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget {
            existing_message_ids: vec!["<M1@example.com>".to_string()],
            ..Default::default()
        };
        let options = RestoreOptions {
            filter: Some("message.mailbox == \"Inbox\"".to_string()),
            ..Default::default()
        };
        let summary = run_restore(&mut target, &policy(dir.path(), None), &options, &NO_CANCEL)
            .await
            .unwrap();

        assert_eq!(summary.skipped_filter, 1, "M3 filtered out");
        assert_eq!(summary.skipped_existing, 1, "M1 already on the server");
        assert_eq!(summary.imported, 1, "only M2 imported");
    }

    #[tokio::test]
    async fn restore_reuses_existing_mailboxes_by_path() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget {
            existing_mailboxes: vec![
                mailbox("tgt-inbox", "Inbox", Some("inbox"), None),
                mailbox("tgt-archive", "Archive", Some("archive"), None),
            ],
            ..Default::default()
        };
        let summary = run_restore(
            &mut target,
            &policy(dir.path(), None),
            &RestoreOptions::default(),
            &NO_CANCEL,
        )
        .await
        .unwrap();

        // Only Receipts is missing; it is created under the EXISTING Archive.
        assert_eq!(summary.mailboxes_created, 1);
        let created = target.created.lock().unwrap();
        assert_eq!(created[0].0, "Receipts");
        assert_eq!(created[0].1.as_deref(), Some("tgt-archive"));

        let imported = target.imported.lock().unwrap();
        let m1 = imported
            .iter()
            .find(|(raw, ..)| raw == b"message one")
            .unwrap();
        assert_eq!(m1.1, vec!["tgt-inbox".to_string()]);
    }

    #[tokio::test]
    async fn restore_with_prefix_roots_everything_under_it() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget::default();
        let summary = run_restore(
            &mut target,
            &policy(dir.path(), Some("Restored")),
            &RestoreOptions::default(),
            &NO_CANCEL,
        )
        .await
        .unwrap();

        assert_eq!(summary.mailboxes_created, 4, "prefix + 3 mailboxes");
        let created = target.created.lock().unwrap();
        assert_eq!(created[0].0, "Restored");
        let prefix_id = created[0].3.clone();
        let inbox = created.iter().find(|(n, ..)| n == "Inbox").unwrap();
        assert_eq!(inbox.1.as_deref(), Some(prefix_id.as_str()));
        assert_eq!(inbox.2, None, "no role claimed under a prefix");
    }

    #[tokio::test]
    async fn restore_at_date_reads_historical_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget::default();
        let options = RestoreOptions {
            at: Some("2023-01-01".to_string()),
            ..Default::default()
        };
        let summary = run_restore(&mut target, &policy(dir.path(), None), &options, &NO_CANCEL)
            .await
            .unwrap();

        // M3 arrived on 2023-01-02 and is not part of that snapshot.
        assert_eq!(summary.imported, 2);
        let imported = target.imported.lock().unwrap();
        assert!(!imported.iter().any(|(raw, ..)| raw == b"message three"));
    }

    #[tokio::test]
    async fn dry_run_imports_nothing() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget::default();
        let options = RestoreOptions {
            dry_run: true,
            ..Default::default()
        };
        let summary = run_restore(&mut target, &policy(dir.path(), None), &options, &NO_CANCEL)
            .await
            .unwrap();

        assert_eq!(summary.selected, 3);
        assert_eq!(summary.mailboxes_created, 3, "reported, not executed");
        assert!(target.created.lock().unwrap().is_empty());
        assert!(target.imported.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn failed_imports_are_reported_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        let mut target = MockRestoreTarget {
            fail_imports: true,
            ..Default::default()
        };
        let summary = run_restore(
            &mut target,
            &policy(dir.path(), None),
            &RestoreOptions::default(),
            &NO_CANCEL,
        )
        .await
        .unwrap();

        assert_eq!(summary.failed, 3);
        assert_eq!(summary.imported, 0);
    }

    #[tokio::test]
    async fn corrupted_archive_content_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        build_archive(dir.path()).await;

        // Tamper with a message file in a plain-directory copy of the layout.
        let plain = tempfile::tempdir().unwrap();
        copy_tree(dir.path(), plain.path());
        let eml = find_first_eml(plain.path());
        std::fs::write(&eml, b"tampered contents").unwrap();

        let policy: RestorePolicy = serde_yaml::from_str(&format!(
            "from: !LocalDir {{ path: '{}' }}\nto: !Fastmail {{ token: t }}",
            plain.path().display().to_string().replace('\\', "/")
        ))
        .unwrap();

        let mut target = MockRestoreTarget::default();
        let result =
            run_restore(&mut target, &policy, &RestoreOptions::default(), &NO_CANCEL).await;
        let error = result.expect_err("checksum mismatch fails the restore");
        assert!(error.to_string().contains("checksum"), "got: {error}");
    }

    fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            if name == ".git" {
                continue;
            }
            let dest = to.join(&name);
            if entry.file_type().unwrap().is_dir() {
                std::fs::create_dir_all(&dest).unwrap();
                copy_tree(&entry.path(), &dest);
            } else {
                std::fs::copy(entry.path(), &dest).unwrap();
            }
        }
    }

    fn find_first_eml(root: &std::path::Path) -> std::path::PathBuf {
        for entry in std::fs::read_dir(root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir()
                && let Some(found) = try_find_eml(&entry.path())
            {
                return found;
            }
        }
        panic!("no .eml file found");
    }

    fn try_find_eml(dir: &std::path::Path) -> Option<std::path::PathBuf> {
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            if entry.file_type().ok()?.is_dir() {
                if let Some(found) = try_find_eml(&entry.path()) {
                    return Some(found);
                }
            } else if entry.path().extension().is_some_and(|e| e == "eml") {
                return Some(entry.path());
            }
        }
        None
    }
}
