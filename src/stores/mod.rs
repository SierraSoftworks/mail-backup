pub mod dir;
pub mod git;
pub mod index;
pub mod layout;
pub mod sidecar;

use serde::{Deserialize, Serialize};

use crate::entities::mail::{MailEvent, MailboxIndex, MessageMeta, SourceState};

/// A message as recorded in the local store: its full metadata, the relative
/// path of its `.eml` file, and the content hash of its raw bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoredMessage {
    pub meta: MessageMeta,
    /// Repository-relative, `/`-separated path of the raw message file.
    pub path: String,
    /// Full hex sha256 digest of the raw message bytes.
    pub sha256: String,
}

/// The result of applying a single [`MailEvent`] to a store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventOutcome {
    Added,
    Moved,
    Updated,
    Removed,
    Unchanged,
    /// The event could not be applied (e.g. deleting a non-empty mailbox, or
    /// updating a message the store has never seen). The caller may retry or
    /// trigger a reconciliation.
    Skipped,
}

/// Whether a checkpoint seals a historical (backfilled) day or extends the
/// current live day.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotKind {
    /// A backdated daily snapshot created during backfill or catch-up; always
    /// produces a new commit stamped with the historical day.
    Backfill,
    /// A live update: the first checkpoint of a UTC day creates that day's
    /// commit, and subsequent checkpoints amend it.
    Live,
}

/// A snapshot boundary: the store persists its state (and, for git stores,
/// commits) when one is reached.
#[derive(Clone, Debug)]
pub struct Checkpoint {
    pub date: chrono::NaiveDate,
    pub kind: SnapshotKind,
    /// A human-readable summary of what changed, included in commit messages.
    pub description: String,
}

/// The cursor tracking an in-progress backfill, allowing it to resume after
/// an interruption without repeating completed work.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackfillCursor {
    /// The server's Email state captured before enumeration began; a
    /// changes-based catch-up runs from here once backfill completes so that
    /// the finished archive reflects a consistent point in time.
    #[serde(default)]
    pub start_email_state: Option<String>,
    #[serde(default)]
    pub start_mailbox_state: Option<String>,
    /// The id of the last message of the last fully-committed day, used to
    /// anchor the next enumeration page.
    #[serde(default)]
    pub anchor_id: Option<String>,
    #[serde(default)]
    pub last_committed_day: Option<chrono::NaiveDate>,
    #[serde(default)]
    pub processed: u64,
}

/// The store's persisted synchronization state. This lives *outside* the
/// backed-up tree (it is account-specific and rebuildable), unlike the
/// sidecars which are the committed source of truth.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreState {
    #[serde(default)]
    pub source: SourceState,
    /// Present while a backfill is incomplete (including before it starts).
    #[serde(default)]
    pub backfill: Option<BackfillCursor>,
    /// The UTC day covered by the current live snapshot commit, if any.
    #[serde(default)]
    pub current_commit_day: Option<chrono::NaiveDate>,
    /// The commit id at the time the state was last saved (git stores only),
    /// used to detect crash windows on startup.
    #[serde(default)]
    pub head_at_save: Option<String>,
}

impl StoreState {
    /// Whether this store has never completed an initial backfill.
    pub fn needs_backfill(&self) -> bool {
        self.backfill.is_some() || self.source.email_state.is_none()
    }
}

/// A worktree mutation performed by a store, recorded so that layered stores
/// (the git store wraps the directory store) can mirror filesystem changes
/// into their own structures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PathEdit {
    /// The file at this relative path was created or rewritten.
    Upsert(String),
    /// The file at this relative path was removed.
    Remove(String),
}

/// A store of any configured kind, dispatching to the concrete
/// implementation chosen by the policy's `to:` configuration.
#[allow(clippy::large_enum_variant)] // a handful of instances exist at a time
pub enum AnyStore {
    Git(git::GitMailStore),
    Dir(dir::DirMailStore),
}

impl AnyStore {
    pub fn from_config(config: &crate::policy::StoreConfig) -> Self {
        match config {
            crate::policy::StoreConfig::LocalGit {
                path,
                commit_name,
                commit_email,
            } => AnyStore::Git(git::GitMailStore::new(
                path.clone(),
                commit_name.clone(),
                commit_email.clone(),
            )),
            crate::policy::StoreConfig::LocalDir { path } => {
                AnyStore::Dir(dir::DirMailStore::new(path.clone()))
            }
        }
    }
}

impl MailStore for AnyStore {
    fn kind(&self) -> &'static str {
        match self {
            AnyStore::Git(store) => store.kind(),
            AnyStore::Dir(store) => store.kind(),
        }
    }

    async fn open(&mut self) -> Result<(), human_errors::Error> {
        match self {
            AnyStore::Git(store) => store.open().await,
            AnyStore::Dir(store) => store.open().await,
        }
    }

    fn state(&self) -> &StoreState {
        match self {
            AnyStore::Git(store) => store.state(),
            AnyStore::Dir(store) => store.state(),
        }
    }

    fn state_mut(&mut self) -> &mut StoreState {
        match self {
            AnyStore::Git(store) => store.state_mut(),
            AnyStore::Dir(store) => store.state_mut(),
        }
    }

    fn mailboxes(&self) -> &crate::entities::mail::MailboxIndex {
        match self {
            AnyStore::Git(store) => store.mailboxes(),
            AnyStore::Dir(store) => store.mailboxes(),
        }
    }

    fn lookup(&self, message_id: &str) -> Option<&StoredMessage> {
        match self {
            AnyStore::Git(store) => store.lookup(message_id),
            AnyStore::Dir(store) => store.lookup(message_id),
        }
    }

    fn list(&self) -> impl Iterator<Item = &StoredMessage> {
        let iter: Box<dyn Iterator<Item = &StoredMessage>> = match self {
            AnyStore::Git(store) => Box::new(store.list()),
            AnyStore::Dir(store) => Box::new(store.list()),
        };
        iter
    }

    async fn apply(
        &mut self,
        events: Vec<MailEvent>,
    ) -> Result<Vec<EventOutcome>, human_errors::Error> {
        match self {
            AnyStore::Git(store) => store.apply(events).await,
            AnyStore::Dir(store) => store.apply(events).await,
        }
    }

    async fn checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<(), human_errors::Error> {
        match self {
            AnyStore::Git(store) => store.checkpoint(checkpoint).await,
            AnyStore::Dir(store) => store.checkpoint(checkpoint).await,
        }
    }

    async fn save_state(&mut self) -> Result<(), human_errors::Error> {
        match self {
            AnyStore::Git(store) => store.save_state().await,
            AnyStore::Dir(store) => store.save_state().await,
        }
    }
}

/// A destination which mail is backed up into.
///
/// Stores are stateful: they own the local copy of the mail account, the
/// index describing it, and the synchronization cursor. Implementations must
/// apply events idempotently — re-applying an already-applied event must be a
/// no-op — since crash recovery redelivers changes at least once.
pub trait MailStore: Send {
    fn kind(&self) -> &'static str;

    /// Opens (initializing if necessary) the store, loading or rebuilding its
    /// state and index.
    async fn open(&mut self) -> Result<(), human_errors::Error>;

    fn state(&self) -> &StoreState;
    fn state_mut(&mut self) -> &mut StoreState;

    fn mailboxes(&self) -> &MailboxIndex;

    fn lookup(&self, message_id: &str) -> Option<&StoredMessage>;

    fn list(&self) -> impl Iterator<Item = &StoredMessage>;

    /// Applies a batch of events to the local copy. Outcomes correspond 1:1
    /// with the provided events.
    async fn apply(
        &mut self,
        events: Vec<MailEvent>,
    ) -> Result<Vec<EventOutcome>, human_errors::Error>;

    /// Persists state at a snapshot boundary. Git stores commit (or amend)
    /// here; all stores save their state and index.
    async fn checkpoint(&mut self, checkpoint: &Checkpoint) -> Result<(), human_errors::Error>;

    /// Durably persists the current state and index without creating a
    /// snapshot commit. Used to record progress (such as backfill completion)
    /// that must survive a reopen even when no snapshot boundary follows it.
    async fn save_state(&mut self) -> Result<(), human_errors::Error>;
}
