//! Changes-based catch-up synchronization, plus the full reconciliation
//! fallback for when the server can no longer compute changes from our saved
//! state.

use std::collections::HashSet;
use std::sync::atomic::AtomicBool;

use tokio_stream::StreamExt;
use tracing_batteries::prelude::*;

use super::{
    BackupSummary, EngineOptions, PlannedChanges, apply_planned, cancelled, dedupe_changes,
    matches_filter, plan_message_changes,
};
use crate::BackupPolicy;
use crate::entities::mail::{DateRange, SourceState};
use crate::sources::{ChangesResult, MailSource};
use crate::stores::MailStore;

pub struct SyncOutcome {
    pub summary: BackupSummary,
    /// Set when the server reported our state as too old; the caller must run
    /// [`reconcile`] to converge.
    pub needs_reconcile: bool,
}

/// Applies every change the server reports since our last synchronized state.
/// New mail received on earlier days is committed day-by-day (backdated);
/// everything else lands in today's live snapshot.
#[instrument(name = "sync.changes", skip_all, fields(
    source = %policy.from,
    store = %policy.to,
    created = EmptyField,
    updated = EmptyField,
    destroyed = EmptyField,
    mailboxes_changed = EmptyField,
    needs_reconcile = EmptyField,
    plan.adds = EmptyField,
    plan.updates = EmptyField,
    plan.deletes = EmptyField,
))]
pub async fn run<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
) -> Result<SyncOutcome, human_errors::Error> {
    let mut created = Vec::new();
    let mut updated = Vec::new();
    let mut destroyed = Vec::new();
    let mut mailboxes_changed = false;
    let mut state = store.state().source.clone();

    loop {
        if cancelled(cancel) {
            return Ok(SyncOutcome {
                summary: BackupSummary {
                    interrupted: true,
                    ..Default::default()
                },
                needs_reconcile: false,
            });
        }

        match source.changes(&state).await? {
            ChangesResult::StateTooOld => {
                Span::current().record("needs_reconcile", true);
                return Ok(SyncOutcome {
                    summary: BackupSummary::default(),
                    needs_reconcile: true,
                });
            }
            ChangesResult::Changes(changes) => {
                created.extend(changes.created);
                updated.extend(changes.updated);
                destroyed.extend(changes.destroyed);
                mailboxes_changed |= changes.mailboxes_changed;
                state = changes.state;
                if !changes.has_more {
                    break;
                }
            }
        }
    }

    let (created, updated, destroyed) = dedupe_changes(created, updated, destroyed);

    let span = Span::current();
    span.record("created", created.len());
    span.record("updated", updated.len());
    span.record("destroyed", destroyed.len());
    span.record("mailboxes_changed", mailboxes_changed);

    let mut summary = BackupSummary::default();
    let mut plan = PlannedChanges::default();

    if mailboxes_changed {
        let (upserts, deletions) = super::mailbox_events(source, store).await?;
        plan.mailbox_upserts = upserts;
        plan.mailbox_deletions = deletions;
    }

    let created_metas = source.get_messages(&created).await?;
    let updated_metas = source.get_messages(&updated).await?;
    plan_message_changes(
        policy,
        store,
        created_metas,
        updated_metas,
        &mut plan,
        &mut summary,
    )?;
    plan.deletes.extend(destroyed);

    span.record("plan.adds", plan.adds.len());
    span.record("plan.updates", plan.updates.len());
    span.record("plan.deletes", plan.deletes.len());

    if plan.is_empty() && state == store.state().source {
        debug!("No changes since the last synchronization");
        return Ok(SyncOutcome {
            summary,
            needs_reconcile: false,
        });
    }

    info!(
        "Applying changes: {} new, {} updated, {} deleted",
        plan.adds.len(),
        plan.updates.len(),
        plan.deletes.len()
    );

    summary.merge(apply_planned(source, store, plan, state, options, cancel).await?);
    Ok(SyncOutcome {
        summary,
        needs_reconcile: false,
    })
}

/// Fully re-enumerates the server and diffs against the store, without
/// re-downloading content for messages we already hold (a message id's
/// content is immutable; only keywords and mailbox membership change).
#[instrument(name = "sync.reconcile", skip_all, fields(
    source = %policy.from,
    store = %policy.to,
    enumerated = EmptyField,
    plan.adds = EmptyField,
    plan.updates = EmptyField,
    plan.deletes = EmptyField,
))]
pub async fn reconcile<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
    fresh_state: &SourceState,
) -> Result<BackupSummary, human_errors::Error> {
    let mut summary = BackupSummary::default();
    let mut plan = PlannedChanges::default();

    let (upserts, deletions) = super::mailbox_events(source, store).await?;
    plan.mailbox_upserts = upserts;
    plan.mailbox_deletions = deletions;

    // The store must know about new mailboxes before messages referencing
    // them are filed, and reconciliation decisions below read the (already
    // updated) mailbox index for filter evaluation.
    if !options.dry_run && !plan.mailbox_upserts.is_empty() {
        let upserts = std::mem::take(&mut plan.mailbox_upserts);
        store.apply(upserts).await?;
    }

    let mut seen: HashSet<String> = HashSet::new();

    {
        let stream = source.enumerate(DateRange::all(), cancel);
        tokio::pin!(stream);

        while let Some(meta) = stream.next().await {
            let meta = meta?;

            if cancelled(cancel) {
                summary.interrupted = true;
                return Ok(summary);
            }

            seen.insert(meta.id.clone());
            let matches = matches_filter(policy, store, &meta)?;

            match (store.lookup(&meta.id), matches) {
                (None, true) => plan.adds.push(meta),
                (None, false) => summary.skipped += 1,
                (Some(_), false) => plan.deletes.push(meta.id),
                (Some(stored), true) => {
                    // Compare only the mutable surface; receivedAt precision
                    // differences are normalized by the sidecar layer.
                    if stored.meta.keywords != meta.keywords
                        || stored.meta.mailbox_ids != meta.mailbox_ids
                    {
                        plan.updates.push(meta);
                    } else {
                        summary.unchanged += 1;
                    }
                }
            }
        }
    }

    // Anything we hold which the server no longer has was deleted remotely.
    plan.deletes.extend(
        store
            .list()
            .filter(|stored| !seen.contains(&stored.meta.id))
            .map(|stored| stored.meta.id.clone()),
    );

    let span = Span::current();
    span.record("enumerated", seen.len());
    span.record("plan.adds", plan.adds.len());
    span.record("plan.updates", plan.updates.len());
    span.record("plan.deletes", plan.deletes.len());

    info!(
        "Reconciliation plan: {} new, {} updated, {} deleted",
        plan.adds.len(),
        plan.updates.len(),
        plan.deletes.len()
    );

    summary.merge(apply_planned(source, store, plan, fresh_state.clone(), options, cancel).await?);
    Ok(summary)
}
