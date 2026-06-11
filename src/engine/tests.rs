//! End-to-end engine tests: a scripted in-memory server synced into real
//! stores (including git) in temporary directories.

use std::sync::atomic::AtomicBool;

use super::*;
use crate::entities::mail::{MailboxInfo, MessageMeta};
use crate::policy::{BackupPolicy, SourceConfig, StoreConfig};
use crate::sources::mock::MockMailSource;
use crate::stores::git::GitMailStore;
use crate::stores::{MailStore, SnapshotKind};

static NO_CANCEL: AtomicBool = AtomicBool::new(false);

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

fn policy(filter: Option<&str>) -> BackupPolicy {
    let filter_line = filter
        .map(|f| format!("filter: '{f}'\n"))
        .unwrap_or_default();
    serde_yaml::from_str(&format!(
        "from: !Jmap {{ url: 'http://mock', token: 'token' }}\nto: !LocalDir {{ path: '/unused' }}\n{filter_line}"
    ))
    .unwrap()
}

fn options() -> EngineOptions {
    EngineOptions::default()
}

/// A standard scripted server: Inbox + Archive, three messages across two
/// historical days.
fn scripted_source() -> MockMailSource {
    let source = MockMailSource::new("acc-1");
    source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox"), None));
    source.upsert_mailbox(mailbox("mb-archive", "Archive", Some("archive"), None));
    source.add_message(
        meta("M1", &["mb-inbox"], &["$seen"], "2023-01-01T08:00:00Z"),
        b"message one",
    );
    source.add_message(
        meta("M2", &["mb-inbox"], &[], "2023-01-01T15:00:00Z"),
        b"message two",
    );
    source.add_message(
        meta("M3", &["mb-archive"], &["$seen"], "2023-01-02T09:00:00Z"),
        b"message three",
    );
    source
}

fn git_log_messages(store: &GitMailStore) -> Vec<String> {
    // Walk first-parent ancestry via the stored state, oldest first.
    let repo = gix::open(store_root(store)).unwrap();
    let mut messages = Vec::new();
    let mut current = repo.head_id().ok().map(|id| id.detach());
    while let Some(id) = current {
        let commit = repo.find_commit(id).unwrap();
        messages.push(commit.message_raw().unwrap().to_string());
        current = commit.parent_ids().next().map(|p| p.detach());
    }
    messages.reverse();
    messages
}

fn store_root(store: &GitMailStore) -> std::path::PathBuf {
    store.root().to_path_buf()
}

#[tokio::test]
async fn backfill_creates_daily_commits_and_files() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();

    assert_eq!(summary.added, 3);
    assert!(!summary.interrupted);

    // Files filed under role-based primary mailboxes.
    assert!(store.lookup("M1").unwrap().path.starts_with("Inbox/"));
    assert!(store.lookup("M3").unwrap().path.starts_with("Archive/"));

    // History: init + 2 backdated day commits (no live commit — nothing
    // changed after the backfill anchor states were captured).
    let log = git_log_messages(&store);
    assert!(
        log.iter()
            .any(|m| m.starts_with("Mail backup for 2023-01-01"))
    );
    assert!(
        log.iter()
            .any(|m| m.starts_with("Mail backup for 2023-01-02"))
    );

    // Backfill is complete and the state cursor is set.
    assert!(store.state().backfill.is_none());
    assert!(store.state().source.email_state.is_some());
    assert_eq!(store.state().source.account_id, "acc-1");

    // A second run is a no-op.
    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    assert_eq!(
        summary.changes(),
        0,
        "second run changed nothing: {summary}"
    );
}

#[tokio::test]
async fn backup_filter_excludes_messages() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    let policy = policy(Some("message.mailbox == \"Inbox\""));
    let summary = run_backup(&mut source, &mut store, &policy, &options(), &NO_CANCEL)
        .await
        .unwrap();

    assert_eq!(summary.added, 2);
    assert_eq!(summary.skipped, 1);
    assert!(store.lookup("M3").is_none(), "filtered message not stored");
}

#[tokio::test]
async fn catch_up_applies_changes_day_by_day() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    let commits_before = git_log_messages(&store).len();

    // Server-side mutations while we were offline: an old message arrives
    // (e.g. an import), one gets read, one moves, one is deleted, and a new
    // mailbox shows up.
    source.upsert_mailbox(mailbox("mb-sub", "Receipts", None, Some("mb-archive")));
    source.add_message(
        meta("M4", &["mb-sub"], &[], "2023-01-05T10:00:00Z"),
        b"message four",
    );
    source.update_message(meta(
        "M2",
        &["mb-inbox"],
        &["$seen"],
        "2023-01-01T15:00:00Z",
    ));
    source.update_message(meta(
        "M1",
        &["mb-archive"],
        &["$seen"],
        "2023-01-01T08:00:00Z",
    ));
    source.delete_message("M3");

    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();

    assert_eq!(summary.added, 1, "{summary}");
    assert_eq!(summary.updated, 1, "{summary}");
    assert_eq!(summary.moved, 1, "{summary}");
    assert_eq!(summary.removed, 1, "{summary}");

    // The old-day arrival got its own backdated commit; the rest amended/created today's.
    let log = git_log_messages(&store);
    assert!(
        log.iter()
            .any(|m| m.starts_with("Mail backup for 2023-01-05"))
    );
    assert!(log.len() >= commits_before + 2);

    // File layout reflects the changes.
    assert!(store.lookup("M1").unwrap().path.starts_with("Archive/"));
    assert!(
        store
            .lookup("M4")
            .unwrap()
            .path
            .starts_with("Archive/Receipts/"),
        "got {}",
        store.lookup("M4").unwrap().path
    );
    assert!(store.lookup("M3").is_none());
    assert!(store.lookup("M2").unwrap().meta.keywords.contains("$seen"));
}

#[tokio::test]
async fn mailbox_deletion_removes_empty_directories() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    assert!(dir.path().join("Archive").exists());

    // The server deletes the only archived message and then the mailbox.
    source.delete_message("M3");
    source.delete_mailbox("mb-archive");

    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    assert_eq!(summary.removed, 1, "{summary}");
    assert!(store.mailboxes().get("mb-archive").is_none());
    assert!(!dir.path().join("Archive").exists());
}

#[tokio::test]
async fn update_out_of_filter_removes_message() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    let policy = policy(Some("!(message.keywords contains \"$flagged\")"));
    run_backup(&mut source, &mut store, &policy, &options(), &NO_CANCEL)
        .await
        .unwrap();
    assert!(store.lookup("M2").is_some());

    // Flagging M2 makes it fall outside the filter: it should be removed.
    source.update_message(meta(
        "M2",
        &["mb-inbox"],
        &["$flagged"],
        "2023-01-01T15:00:00Z",
    ));

    let summary = run_backup(&mut source, &mut store, &policy, &options(), &NO_CANCEL)
        .await
        .unwrap();
    assert_eq!(summary.removed, 1, "{summary}");
    assert!(store.lookup("M2").is_none());
}

#[tokio::test]
async fn state_too_old_triggers_full_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();

    // Mutate the server, then expire all old states so changes() cannot be
    // computed and the engine must reconcile from a full enumeration.
    source.add_message(
        meta("M5", &["mb-inbox"], &[], "2023-02-01T10:00:00Z"),
        b"message five",
    );
    source.update_message(meta("M1", &["mb-inbox"], &[], "2023-01-01T08:00:00Z"));
    source.delete_message("M2");
    source.expire_old_states();

    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();

    assert_eq!(summary.added, 1, "{summary}");
    assert_eq!(summary.updated, 1, "{summary}");
    assert_eq!(summary.removed, 1, "{summary}");
    assert!(store.lookup("M5").is_some());
    assert!(store.lookup("M2").is_none());
    assert!(!store.lookup("M1").unwrap().meta.keywords.contains("$seen"));

    // After reconciliation the regular changes flow works again.
    source.add_message(
        meta("M6", &["mb-inbox"], &[], "2023-02-02T10:00:00Z"),
        b"message six",
    );
    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    assert_eq!(summary.added, 1, "{summary}");
}

#[tokio::test]
async fn account_mismatch_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();

    let mut other = MockMailSource::new("acc-2");
    other.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox"), None));

    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
    let result = run_backup(
        &mut other,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await;
    let error = result.expect_err("a different account must be rejected");
    assert!(error.to_string().contains("acc-1"), "got: {error}");
}

#[tokio::test]
async fn dry_run_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    let options = EngineOptions {
        dry_run: true,
        ..Default::default()
    };
    let summary = run_backup(&mut source, &mut store, &policy(None), &options, &NO_CANCEL)
        .await
        .unwrap();
    assert_eq!(summary.added, 3, "the dry run reports what it would do");

    // Nothing was stored and no state was persisted.
    assert_eq!(store.list().count(), 0);
    let mut fresh = GitMailStore::new(dir.path().to_path_buf(), None, None);
    fresh.open().await.unwrap();
    assert!(fresh.state().needs_backfill());
    assert_eq!(fresh.state().source.account_id, "");
}

#[tokio::test]
async fn backfill_respects_start_date() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

    let mut policy = policy(None);
    policy.backfill_start = Some("2023-01-02".parse().unwrap());

    let summary = run_backup(&mut source, &mut store, &policy, &options(), &NO_CANCEL)
        .await
        .unwrap();

    // Only M3 (2023-01-02) is inside the window; M1/M2 are older.
    assert_eq!(summary.added, 1, "{summary}");
    assert!(store.lookup("M1").is_none());
    assert!(store.lookup("M3").is_some());
}

#[tokio::test]
async fn works_against_plain_directory_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = scripted_source();
    let mut store = crate::stores::AnyStore::from_config(&StoreConfig::LocalDir {
        path: dir.path().to_path_buf(),
    });

    let summary = run_backup(
        &mut source,
        &mut store,
        &policy(None),
        &options(),
        &NO_CANCEL,
    )
    .await
    .unwrap();
    assert_eq!(summary.added, 3);
    assert!(!dir.path().join(".git").exists());
    assert!(dir.path().join(".mail-backup/state.json").exists());

    // SourceConfig sugar resolves the Fastmail base URL.
    let config: SourceConfig = serde_yaml::from_str("!Fastmail { token: t }").unwrap();
    assert_eq!(config.session_url(), "https://api.fastmail.com");
    let _ = SnapshotKind::Live; // silence unused-import lint in cfg(test)
}
