pub mod jmap;
#[cfg(test)]
pub mod mock;

use std::sync::atomic::AtomicBool;

use tokio_stream::Stream;

use crate::entities::mail::{
    ChangeSet, DateRange, MailboxInfo, MessageMeta, SourceNotification, SourceState,
};

/// The result of asking a source for changes since a previous state.
#[derive(Clone, Debug)]
pub enum ChangesResult {
    /// One page of changes; request another while `has_more` is set.
    Changes(ChangeSet),
    /// The provided state is too old for the server to compute changes from
    /// (JMAP `cannotCalculateChanges`); the caller must run a full
    /// reconciliation instead.
    StateTooOld,
}

/// A mail service which messages are backed up from.
///
/// All reads must be strictly side-effect free on the server: a backup never
/// changes the state of the account it reads from.
pub trait MailSource: Send + Sync {
    fn kind(&self) -> &'static str;

    /// Establishes the connection and returns the account id along with the
    /// server's *current* state strings (used to anchor a backfill).
    async fn connect(&mut self) -> Result<SourceState, human_errors::Error>;

    async fn list_mailboxes(&self) -> Result<Vec<MailboxInfo>, human_errors::Error>;

    /// Enumerates every message in the given received-at range, ordered by
    /// receivedAt ascending. Yields metadata only; blobs are fetched
    /// separately.
    fn enumerate<'a>(
        &'a self,
        range: DateRange,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<MessageMeta, human_errors::Error>> + 'a;

    /// One page of changes since `since`.
    async fn changes(&self, since: &SourceState) -> Result<ChangesResult, human_errors::Error>;

    /// Resolves ids to message metadata. Ids the server no longer knows are
    /// silently omitted from the result.
    async fn get_messages(&self, ids: &[String]) -> Result<Vec<MessageMeta>, human_errors::Error>;

    /// Downloads the raw RFC5322 bytes of a message blob.
    async fn fetch_blob(
        &self,
        blob_id: &str,
        cancel: &AtomicBool,
    ) -> Result<Vec<u8>, human_errors::Error>;

    /// A long-lived stream of real-time change notifications. The stream ends
    /// when the connection drops (callers reconnect with backoff) or when
    /// `cancel` is set.
    fn events<'a>(
        &'a self,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<SourceNotification, human_errors::Error>> + 'a;
}
