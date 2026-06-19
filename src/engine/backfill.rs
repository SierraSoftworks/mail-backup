//! The initial (or resumed) backfill: enumerate every message chronologically
//! and commit them one day at a time, with each day's snapshot backdated to
//! the day it covers.

use std::sync::atomic::AtomicBool;

use tokio_stream::StreamExt;
use tracing_batteries::prelude::*;

use super::{BackupSummary, EngineOptions, added_events, cancelled, fetch_raw, matches_filter};
use crate::BackupPolicy;
use crate::entities::mail::{DateRange, MessageMeta, SourceState};
use crate::sources::MailSource;
use crate::stores::{BackfillCursor, Checkpoint, MailStore, SnapshotKind};

#[instrument(name = "backfill", skip_all, fields(
    source = %policy.from,
    store = %policy.to,
    resumed = EmptyField,
    start_day = EmptyField,
    processed = EmptyField,
))]
pub async fn run<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
    connected: &SourceState,
) -> Result<BackupSummary, human_errors::Error> {
    let mut summary = BackupSummary::default();

    // Resume an interrupted backfill (keeping the state strings captured when
    // it originally started), or anchor a new one at the server's current
    // state. Once enumeration completes, a changes-based catch-up from these
    // states folds in everything that happened during the (possibly long)
    // backfill, producing a consistent point-in-time archive.
    let mut cursor = store
        .state()
        .backfill
        .clone()
        .unwrap_or_else(|| BackfillCursor {
            start_email_state: connected.email_state.clone(),
            start_mailbox_state: connected.mailbox_state.clone(),
            ..Default::default()
        });

    Span::current().record("resumed", cursor.last_committed_day.is_some());
    if let Some(day) = cursor.last_committed_day {
        info!(
            "Resuming backfill after {} ({} messages so far)",
            day, cursor.processed
        );
    }

    if !options.dry_run {
        store.state_mut().backfill = Some(cursor.clone());
    }

    // Mailbox structure first: message filing depends on it. These events
    // ride along with the first day's commit (no separate checkpoint), so an
    // interruption simply redoes this idempotent step.
    let (mailbox_upserts, _) = super::mailbox_events(source, store).await?;
    if options.dry_run {
        info!(
            "[dry-run] Would apply {} mailbox changes",
            mailbox_upserts.len()
        );
    } else if !mailbox_upserts.is_empty() {
        store.apply(mailbox_upserts).await?;
    }

    // Enumerate from the day after the last fully-committed one (or the
    // configured start). Messages of partially-written days are re-offered
    // and deduplicated by the store.
    let start_day = cursor
        .last_committed_day
        .and_then(|d| d.succ_opt())
        .or(policy.backfill_start);
    if let Some(day) = start_day {
        Span::current().record("start_day", display(day));
    }
    let range = DateRange {
        start: start_day.map(|d| {
            d.and_hms_opt(0, 0, 0)
                .expect("midnight is a valid time")
                .and_utc()
        }),
        end: None,
    };

    let mut pending: Vec<MessageMeta> = Vec::new();
    let mut current_day: Option<chrono::NaiveDate> = None;
    let mut interrupted = false;

    {
        use crate::telemetry::StreamExt as _;
        let span = info_span!("backfill.enumerate", start = range.start.map(display));
        let stream = source.enumerate(range, cancel).trace(span);
        tokio::pin!(stream);

        while let Some(meta) = stream.next().await {
            let meta = meta?;

            if cancelled(cancel) {
                interrupted = true;
                break;
            }

            let day = meta.received_day();
            if let Some(current) = current_day
                && day != current
            {
                finalize_day(
                    source,
                    store,
                    policy,
                    options,
                    cancel,
                    current,
                    std::mem::take(&mut pending),
                    &mut cursor,
                    &mut summary,
                )
                .await?;
            }
            current_day = Some(day);
            pending.push(meta);
        }
    }

    if !interrupted
        && let Some(day) = current_day
        && !pending.is_empty()
    {
        finalize_day(
            source,
            store,
            policy,
            options,
            cancel,
            day,
            pending,
            &mut cursor,
            &mut summary,
        )
        .await?;
    }

    Span::current().record("processed", cursor.processed);

    if interrupted || cancelled(cancel) {
        info!("Backfill interrupted; progress is saved and it will resume on the next run");
        summary.interrupted = true;
        return Ok(summary);
    }

    if options.dry_run {
        return Ok(summary);
    }

    // Enumeration is complete: switch the synchronization cursor to the state
    // captured when the backfill began. The regular catch-up which follows
    // every backup pass replays anything that changed while we were
    // backfilling.
    store.state_mut().source.email_state = cursor.start_email_state.clone();
    store.state_mut().source.mailbox_state = cursor.start_mailbox_state.clone();
    store.state_mut().backfill = None;

    // Persist completion now so it survives a reopen even if the catch-up that
    // follows finds nothing to checkpoint — otherwise a quiet mailbox would
    // re-run the backfill (and never reach reconciliation) on every restart.
    store.save_state().await?;

    info!("Backfill complete: {}", summary);
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
#[instrument(name = "backfill.day", skip_all, fields(
    day = %day,
    messages = metas.len(),
    stored = EmptyField,
    skipped = EmptyField,
    unchanged = EmptyField,
))]
async fn finalize_day<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
    day: chrono::NaiveDate,
    metas: Vec<MessageMeta>,
    cursor: &mut crate::stores::BackfillCursor,
    summary: &mut BackupSummary,
) -> Result<(), human_errors::Error> {
    let anchor = metas.last().map(|m| m.id.clone());
    let total = metas.len() as u64;

    let mut skipped = 0usize;
    let mut unchanged = 0usize;
    let mut to_fetch = Vec::new();
    for meta in metas {
        if !matches_filter(policy, store, &meta)? {
            skipped += 1;
            continue;
        }
        if store.lookup(&meta.id).is_some() {
            unchanged += 1;
            continue;
        }
        to_fetch.push(meta);
    }
    summary.skipped += skipped;
    summary.unchanged += unchanged;

    let span = Span::current();
    span.record("stored", to_fetch.len());
    span.record("skipped", skipped);
    span.record("unchanged", unchanged);

    if options.dry_run {
        info!(
            "[dry-run] Would back up {} messages for {}",
            to_fetch.len(),
            day
        );
        summary.added += to_fetch.len();
        return Ok(());
    }

    let count = to_fetch.len();
    let fetched = fetch_raw(source, to_fetch, options.concurrency, cancel).await?;
    let events = added_events(store, fetched);
    let outcomes = store.apply(events).await?;
    summary.record_all(&outcomes);

    cursor.anchor_id = anchor;
    cursor.last_committed_day = Some(day);
    cursor.processed += total;
    store.state_mut().backfill = Some(cursor.clone());

    store
        .checkpoint(&Checkpoint {
            date: day,
            kind: SnapshotKind::Backfill,
            description: format!("{count} messages received"),
        })
        .await?;

    debug!("Committed backfill day {} ({} messages)", day, count);
    Ok(())
}
