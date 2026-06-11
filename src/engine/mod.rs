//! The synchronization engine which pairs a [`MailSource`] with a
//! [`MailStore`] and keeps the store converged on the server's state.

pub mod backfill;
pub mod stream;
pub mod sync;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashSet};
use std::fmt::Display;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::{StreamExt, TryStreamExt};
use tracing_batteries::prelude::*;

use crate::BackupPolicy;
use crate::entities::mail::{MailEvent, MailMessage, MailboxInfo, MessageMeta};
use crate::sources::MailSource;
use crate::stores::{Checkpoint, EventOutcome, MailStore, SnapshotKind};

#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub dry_run: bool,
    /// Maximum concurrent blob downloads.
    pub concurrency: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            concurrency: 4,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackupSummary {
    pub added: usize,
    pub moved: usize,
    pub updated: usize,
    pub removed: usize,
    pub unchanged: usize,
    pub skipped: usize,
    /// Whether the run was interrupted by a shutdown signal before reaching a
    /// consistent end state (it will resume on the next run).
    pub interrupted: bool,
}

impl BackupSummary {
    pub fn record(&mut self, outcome: &EventOutcome) {
        match outcome {
            EventOutcome::Added => self.added += 1,
            EventOutcome::Moved => self.moved += 1,
            EventOutcome::Updated => self.updated += 1,
            EventOutcome::Removed => self.removed += 1,
            EventOutcome::Unchanged => self.unchanged += 1,
            EventOutcome::Skipped => self.skipped += 1,
        }
    }

    pub fn record_all(&mut self, outcomes: &[EventOutcome]) {
        for outcome in outcomes {
            self.record(outcome);
        }
    }

    /// Records only the outcomes of *message* events (per the mask computed
    /// by [`message_event_mask`]), so mailbox bookkeeping does not inflate
    /// the message counts reported to the user.
    pub fn record_masked(&mut self, mask: &[bool], outcomes: &[EventOutcome]) {
        for (is_message, outcome) in mask.iter().zip(outcomes.iter()) {
            if *is_message {
                self.record(outcome);
            }
        }
    }

    pub fn merge(&mut self, other: BackupSummary) {
        self.added += other.added;
        self.moved += other.moved;
        self.updated += other.updated;
        self.removed += other.removed;
        self.unchanged += other.unchanged;
        self.skipped += other.skipped;
        self.interrupted |= other.interrupted;
    }

    pub fn changes(&self) -> usize {
        self.added + self.moved + self.updated + self.removed
    }
}

impl Display for BackupSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} added, {} moved, {} updated, {} removed, {} unchanged, {} skipped",
            self.added, self.moved, self.updated, self.removed, self.unchanged, self.skipped
        )?;
        if self.interrupted {
            write!(f, " (interrupted)")?;
        }
        Ok(())
    }
}

/// Runs a complete backup pass for one policy: backfill if the store has
/// never finished one, then a changes-based catch-up (falling back to a full
/// reconciliation when the server can no longer compute changes).
pub async fn run_backup<S: MailSource, T: MailStore>(
    source: &mut S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
) -> Result<BackupSummary, human_errors::Error> {
    debug!(
        "Backing up from a {} source into a {} store",
        source.kind(),
        store.kind()
    );
    store.open().await?;
    let connected = source.connect().await?;

    let known_account = store.state().source.account_id.clone();
    if !known_account.is_empty() && known_account != connected.account_id {
        return Err(human_errors::user(
            format!(
                "The backup store at this location belongs to account '{}', but the configured source is account '{}'.",
                known_account, connected.account_id
            ),
            &[
                "Each backup store holds exactly one account. Configure a different backup path for this account, or remove the existing store if it is no longer needed.",
            ],
        ));
    }
    if !options.dry_run {
        store.state_mut().source.account_id = connected.account_id.clone();
    }

    let mut summary = BackupSummary::default();

    if store.state().needs_backfill() {
        info!("Starting backfill for {}", policy);
        summary.merge(backfill::run(source, store, policy, options, cancel, &connected).await?);
        if summary.interrupted {
            return Ok(summary);
        }
        if options.dry_run {
            // A dry-run backfill never advances the sync state, so the
            // follow-up catch-up would just re-report the same messages.
            return Ok(summary);
        }
    }

    let outcome = sync::run(source, store, policy, options, cancel).await?;
    let needs_reconcile = outcome.needs_reconcile;
    summary.merge(outcome.summary);

    if needs_reconcile && !summary.interrupted {
        warn!(
            "The server can no longer compute changes from our saved state; running a full reconciliation"
        );
        let fresh = source.connect().await?;
        summary.merge(sync::reconcile(source, store, policy, options, cancel, &fresh).await?);
    }

    Ok(summary)
}

pub(crate) fn cancelled(cancel: &AtomicBool) -> bool {
    cancel.load(Ordering::Relaxed)
}

/// Downloads the raw content for a set of messages with bounded concurrency.
pub(crate) async fn fetch_raw<S: MailSource>(
    source: &S,
    metas: Vec<MessageMeta>,
    concurrency: usize,
    cancel: &AtomicBool,
) -> Result<Vec<(MessageMeta, Vec<u8>)>, human_errors::Error> {
    futures::stream::iter(metas.into_iter().map(|meta| async move {
        let raw = source.fetch_blob(&meta.blob_id, cancel).await?;
        Ok::<_, human_errors::Error>((meta, raw))
    }))
    .buffer_unordered(concurrency.max(1))
    .try_collect()
    .await
}

/// Groups message metadata by the UTC day it was received, in chronological
/// order, for day-by-day snapshot commits.
pub(crate) fn group_by_day(
    metas: Vec<MessageMeta>,
) -> BTreeMap<chrono::NaiveDate, Vec<MessageMeta>> {
    let mut groups: BTreeMap<chrono::NaiveDate, Vec<MessageMeta>> = BTreeMap::new();
    for meta in metas {
        groups.entry(meta.received_day()).or_default().push(meta);
    }
    groups
}

/// Orders mailboxes so parents always precede their children.
pub(crate) fn topological_order(mut mailboxes: Vec<MailboxInfo>) -> Vec<MailboxInfo> {
    mailboxes.sort_by(|a, b| {
        a.sort_order
            .cmp(&b.sort_order)
            .then_with(|| a.id.cmp(&b.id))
    });

    let ids: HashSet<String> = mailboxes.iter().map(|m| m.id.clone()).collect();
    let mut placed: HashSet<String> = HashSet::new();
    let mut ordered = Vec::with_capacity(mailboxes.len());

    while ordered.len() < mailboxes.len() {
        let mut progressed = false;
        for mailbox in mailboxes.iter() {
            if placed.contains(&mailbox.id) {
                continue;
            }
            let parent_ready = match mailbox.parent_id.as_deref() {
                None => true,
                Some(parent) => !ids.contains(parent) || placed.contains(parent),
            };
            if parent_ready {
                placed.insert(mailbox.id.clone());
                ordered.push(mailbox.clone());
                progressed = true;
            }
        }
        if !progressed {
            // Parent cycle (should be impossible): emit the remainder as-is
            // rather than looping forever.
            for mailbox in mailboxes.iter() {
                if !placed.contains(&mailbox.id) {
                    ordered.push(mailbox.clone());
                }
            }
            break;
        }
    }

    ordered
}

/// Diffs the server's mailbox list against the store, producing upsert events
/// (parents first) and deferred deletion events.
pub(crate) async fn mailbox_events<S: MailSource, T: MailStore>(
    source: &S,
    store: &T,
) -> Result<(Vec<MailEvent>, Vec<MailEvent>), human_errors::Error> {
    let server = source.list_mailboxes().await?;
    let server_ids: HashSet<String> = server.iter().map(|m| m.id.clone()).collect();

    let upserts: Vec<MailEvent> = topological_order(server)
        .into_iter()
        .filter(|info| {
            store
                .mailboxes()
                .get(&info.id)
                .is_none_or(|record| record.info != *info)
        })
        .map(MailEvent::MailboxUpserted)
        .collect();

    let deletions: Vec<MailEvent> = store
        .mailboxes()
        .iter()
        .filter(|record| !server_ids.contains(&record.info.id))
        .map(|record| MailEvent::MailboxDeleted {
            id: record.info.id.clone(),
        })
        .collect();

    Ok((upserts, deletions))
}

/// The set of changes a sync pass intends to apply, before blobs are fetched.
#[derive(Default)]
pub(crate) struct PlannedChanges {
    pub mailbox_upserts: Vec<MailEvent>,
    pub mailbox_deletions: Vec<MailEvent>,
    pub adds: Vec<MessageMeta>,
    pub updates: Vec<MessageMeta>,
    pub deletes: Vec<String>,
}

impl PlannedChanges {
    pub fn is_empty(&self) -> bool {
        self.mailbox_upserts.is_empty()
            && self.mailbox_deletions.is_empty()
            && self.adds.is_empty()
            && self.updates.is_empty()
            && self.deletes.is_empty()
    }
}

/// Applies a set of planned changes: new mail is committed day-by-day
/// (backdated) for days before today, while today's additions, metadata
/// updates, deletions, and mailbox removals land in today's live snapshot.
///
/// `final_state` is only persisted with the concluding checkpoint — if the
/// process dies mid-way, the old state remains and the changes are
/// redelivered (idempotently) on the next run.
pub(crate) async fn apply_planned<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    plan: PlannedChanges,
    final_state: crate::entities::mail::SourceState,
    options: &EngineOptions,
    cancel: &AtomicBool,
) -> Result<BackupSummary, human_errors::Error> {
    let mut summary = BackupSummary::default();

    if options.dry_run {
        info!(
            "[dry-run] Would apply: {} mailbox upserts, {} mailbox deletions, {} new messages, {} updates, {} deletions",
            plan.mailbox_upserts.len(),
            plan.mailbox_deletions.len(),
            plan.adds.len(),
            plan.updates.len(),
            plan.deletes.len()
        );
        summary.added = plan.adds.len();
        summary.updated = plan.updates.len();
        summary.removed = plan.deletes.len();
        return Ok(summary);
    }

    if !plan.mailbox_upserts.is_empty() {
        store.apply(plan.mailbox_upserts).await?;
    }

    let today = chrono::Utc::now().date_naive();
    let mut groups = group_by_day(plan.adds);
    let todays_adds = groups.remove(&today).unwrap_or_default();

    for (day, metas) in groups {
        if cancelled(cancel) {
            summary.interrupted = true;
            return Ok(summary);
        }

        let count = metas.len();
        let fetched = fetch_raw(source, metas, options.concurrency, cancel).await?;
        let events = added_events(store, fetched);
        let outcomes = store.apply(events).await?;
        summary.record_all(&outcomes);
        store
            .checkpoint(&Checkpoint {
                date: day,
                kind: SnapshotKind::Backfill,
                description: format!("{count} messages received"),
            })
            .await?;
    }

    if cancelled(cancel) {
        summary.interrupted = true;
        return Ok(summary);
    }

    let fetched = fetch_raw(source, todays_adds, options.concurrency, cancel).await?;
    let mut events = added_events(store, fetched);
    events.extend(updated_events(store, plan.updates));
    events.extend(
        plan.deletes
            .into_iter()
            .map(|id| MailEvent::MessageDeleted { id }),
    );
    events.extend(plan.mailbox_deletions);

    if !events.is_empty() {
        let mask = message_event_mask(&events);
        let outcomes = store.apply(events).await?;
        summary.record_masked(&mask, &outcomes);
    }

    store.state_mut().source = final_state;
    store
        .checkpoint(&Checkpoint {
            date: today,
            kind: SnapshotKind::Live,
            description: format!("{summary}"),
        })
        .await?;

    Ok(summary)
}

pub(crate) fn message_event_mask(events: &[MailEvent]) -> Vec<bool> {
    events
        .iter()
        .map(|event| {
            matches!(
                event,
                MailEvent::MessageAdded { .. }
                    | MailEvent::MessageUpdated { .. }
                    | MailEvent::MessageDeleted { .. }
            )
        })
        .collect()
}

pub(crate) fn added_events<T: MailStore>(
    store: &T,
    fetched: Vec<(MessageMeta, Vec<u8>)>,
) -> Vec<MailEvent> {
    fetched
        .into_iter()
        .map(|(meta, raw)| MailEvent::MessageAdded {
            message: MailMessage::new(meta, store.mailboxes()),
            raw,
        })
        .collect()
}

pub(crate) fn updated_events<T: MailStore>(store: &T, metas: Vec<MessageMeta>) -> Vec<MailEvent> {
    metas
        .into_iter()
        .map(|meta| MailEvent::MessageUpdated {
            message: MailMessage::new(meta, store.mailboxes()),
        })
        .collect()
}

/// Deduplicates raw change lists: an id which was both created and destroyed
/// since our state never needs fetching, updates of destroyed or freshly
/// created ids are redundant, and repeated mentions collapse.
pub(crate) fn dedupe_changes(
    created: Vec<String>,
    updated: Vec<String>,
    destroyed: Vec<String>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let destroyed_set: HashSet<String> = destroyed.iter().cloned().collect();

    let mut seen = HashSet::new();
    let created: Vec<String> = created
        .into_iter()
        .filter(|id| !destroyed_set.contains(id) && seen.insert(id.clone()))
        .collect();
    let created_set: HashSet<String> = created.iter().cloned().collect();

    let mut seen = HashSet::new();
    let updated: Vec<String> = updated
        .into_iter()
        .filter(|id| {
            !destroyed_set.contains(id) && !created_set.contains(id) && seen.insert(id.clone())
        })
        .collect();

    let mut seen = HashSet::new();
    let destroyed: Vec<String> = destroyed
        .into_iter()
        .filter(|id| seen.insert(id.clone()))
        .collect();

    (created, updated, destroyed)
}

/// Evaluates the policy filter against a message.
pub(crate) fn matches_filter<T: MailStore>(
    policy: &BackupPolicy,
    store: &T,
    meta: &MessageMeta,
) -> Result<bool, human_errors::Error> {
    let message = MailMessage::new(meta.clone(), store.mailboxes());
    policy.filter.matches(&message)
}

/// Sorts planned message updates into adds (unknown to the store and matching
/// the filter), updates (known and matching), and deletes (known but no
/// longer matching the filter — the filter is a should-exist predicate).
pub(crate) fn plan_message_changes<T: MailStore>(
    policy: &BackupPolicy,
    store: &T,
    created: Vec<MessageMeta>,
    updated: Vec<MessageMeta>,
    plan: &mut PlannedChanges,
    summary: &mut BackupSummary,
) -> Result<(), human_errors::Error> {
    let mut sorted: HashSet<String> = HashSet::new();

    for meta in created.into_iter().chain(updated) {
        if !sorted.insert(meta.id.clone()) {
            continue;
        }

        let matches = matches_filter(policy, store, &meta)?;
        match (store.lookup(&meta.id).is_some(), matches) {
            (false, true) => plan.adds.push(meta),
            (false, false) => summary.skipped += 1,
            (true, true) => plan.updates.push(meta),
            (true, false) => plan.deletes.push(meta.id),
        }
    }

    Ok(())
}
