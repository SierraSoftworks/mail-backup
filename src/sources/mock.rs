//! An in-memory, scriptable mail source used to test the sync engine and
//! streaming loop without any network involvement.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use tokio_stream::Stream;

use super::{ChangesResult, MailSource};
use crate::entities::mail::{
    ChangeSet, DateRange, MailboxInfo, MessageMeta, SourceNotification, SourceState,
};

#[derive(Clone, Debug)]
enum ChangeEntry {
    MessageCreated(String),
    MessageUpdated(String),
    MessageDestroyed(String),
    MailboxChanged,
}

/// The scripted state of a fake mail server. Tests mutate it between engine
/// runs via the `add_message`/`update_message`/`delete_message`/`upsert_mailbox`/
/// `delete_mailbox` helpers; every mutation advances the server state counter
/// and is journaled so that `changes()` replays it.
pub struct MockMailSource {
    pub account_id: String,
    inner: Mutex<MockState>,
    notifications: Mutex<Vec<SourceNotification>>,
}

#[derive(Default)]
struct MockState {
    seq: u64,
    /// The oldest state the server can compute changes from; anything older
    /// yields `ChangesResult::StateTooOld`.
    min_state: u64,
    mailboxes: Vec<MailboxInfo>,
    messages: HashMap<String, (MessageMeta, Vec<u8>)>,
    journal: Vec<(u64, ChangeEntry)>,
}

impl MockMailSource {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            inner: Mutex::new(MockState::default()),
            notifications: Mutex::new(Vec::new()),
        }
    }

    pub fn upsert_mailbox(&self, info: MailboxInfo) {
        let mut state = self.inner.lock().unwrap();
        state.seq += 1;
        let seq = state.seq;
        state.mailboxes.retain(|m| m.id != info.id);
        state.mailboxes.push(info);
        state.journal.push((seq, ChangeEntry::MailboxChanged));
    }

    pub fn delete_mailbox(&self, id: &str) {
        let mut state = self.inner.lock().unwrap();
        state.seq += 1;
        let seq = state.seq;
        state.mailboxes.retain(|m| m.id != id);
        state.journal.push((seq, ChangeEntry::MailboxChanged));
    }

    pub fn add_message(&self, meta: MessageMeta, raw: &[u8]) {
        let mut state = self.inner.lock().unwrap();
        state.seq += 1;
        let seq = state.seq;
        state
            .journal
            .push((seq, ChangeEntry::MessageCreated(meta.id.clone())));
        state.messages.insert(meta.id.clone(), (meta, raw.to_vec()));
    }

    /// Updates a message's mutable state (keywords/mailboxIds).
    pub fn update_message(&self, meta: MessageMeta) {
        let mut state = self.inner.lock().unwrap();
        state.seq += 1;
        let seq = state.seq;
        state
            .journal
            .push((seq, ChangeEntry::MessageUpdated(meta.id.clone())));
        let raw = state
            .messages
            .get(&meta.id)
            .map(|(_, raw)| raw.clone())
            .unwrap_or_default();
        state.messages.insert(meta.id.clone(), (meta, raw));
    }

    pub fn delete_message(&self, id: &str) {
        let mut state = self.inner.lock().unwrap();
        state.seq += 1;
        let seq = state.seq;
        state.messages.remove(id);
        state
            .journal
            .push((seq, ChangeEntry::MessageDestroyed(id.to_string())));
    }

    /// Makes every state older than the current one un-computable, forcing
    /// the engine down the full-reconciliation path.
    pub fn expire_old_states(&self) {
        let mut state = self.inner.lock().unwrap();
        state.min_state = state.seq;
    }

    /// Queues a notification for delivery on the events stream.
    pub fn push_notification(&self, notification: SourceNotification) {
        self.notifications.lock().unwrap().push(notification);
    }

    fn current_state(&self) -> SourceState {
        let state = self.inner.lock().unwrap();
        SourceState {
            account_id: self.account_id.clone(),
            email_state: Some(state.seq.to_string()),
            mailbox_state: Some(state.seq.to_string()),
        }
    }
}

impl MailSource for MockMailSource {
    fn kind(&self) -> &'static str {
        "mock"
    }

    async fn connect(&mut self) -> Result<SourceState, human_errors::Error> {
        Ok(self.current_state())
    }

    async fn list_mailboxes(&self) -> Result<Vec<MailboxInfo>, human_errors::Error> {
        Ok(self.inner.lock().unwrap().mailboxes.clone())
    }

    fn enumerate<'a>(
        &'a self,
        range: DateRange,
        _cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<MessageMeta, human_errors::Error>> + 'a {
        let mut metas: Vec<MessageMeta> = self
            .inner
            .lock()
            .unwrap()
            .messages
            .values()
            .map(|(meta, _)| meta.clone())
            .filter(|m| range.start.is_none_or(|s| m.received_at >= s))
            .filter(|m| range.end.is_none_or(|e| m.received_at <= e))
            .collect();
        metas.sort_by(|a, b| {
            a.received_at
                .cmp(&b.received_at)
                .then_with(|| a.id.cmp(&b.id))
        });

        tokio_stream::iter(metas.into_iter().map(Ok))
    }

    async fn changes(&self, since: &SourceState) -> Result<ChangesResult, human_errors::Error> {
        let state = self.inner.lock().unwrap();
        let since_seq: u64 = since
            .email_state
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        if since_seq < state.min_state {
            return Ok(ChangesResult::StateTooOld);
        }

        let mut changeset = ChangeSet {
            state: SourceState {
                account_id: self.account_id.clone(),
                email_state: Some(state.seq.to_string()),
                mailbox_state: Some(state.seq.to_string()),
            },
            ..Default::default()
        };

        for (seq, entry) in state.journal.iter() {
            if *seq <= since_seq {
                continue;
            }
            match entry {
                ChangeEntry::MessageCreated(id) => changeset.created.push(id.clone()),
                ChangeEntry::MessageUpdated(id) => changeset.updated.push(id.clone()),
                ChangeEntry::MessageDestroyed(id) => changeset.destroyed.push(id.clone()),
                ChangeEntry::MailboxChanged => changeset.mailboxes_changed = true,
            }
        }

        Ok(ChangesResult::Changes(changeset))
    }

    async fn get_messages(&self, ids: &[String]) -> Result<Vec<MessageMeta>, human_errors::Error> {
        let state = self.inner.lock().unwrap();
        Ok(ids
            .iter()
            .filter_map(|id| state.messages.get(id).map(|(meta, _)| meta.clone()))
            .collect())
    }

    async fn fetch_blob(
        &self,
        blob_id: &str,
        _cancel: &AtomicBool,
    ) -> Result<Vec<u8>, human_errors::Error> {
        let state = self.inner.lock().unwrap();
        state
            .messages
            .values()
            .find(|(meta, _)| meta.blob_id == blob_id)
            .map(|(_, raw)| raw.clone())
            .ok_or_else(|| {
                human_errors::system(
                    format!("The blob {blob_id} does not exist on the mock server."),
                    &["This indicates a bug in the test setup."],
                )
            })
    }

    fn events<'a>(
        &'a self,
        cancel: &'a AtomicBool,
    ) -> impl Stream<Item = Result<SourceNotification, human_errors::Error>> + 'a {
        async_stream::stream! {
            loop {
                if cancel.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let next = self.notifications.lock().unwrap().pop();
                match next {
                    Some(notification) => yield Ok(notification),
                    None => break,
                }
            }
        }
    }
}
