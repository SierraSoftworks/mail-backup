//! The long-running daemon loop: stream real-time change notifications from
//! the source and fold them into the store, amending the current day's
//! snapshot as mail arrives.
//!
//! Reliability model: notifications are only a *hint* to run a changes-based
//! sync — every sync starts from the persisted state cursor, so missed,
//! duplicated, or coalesced notifications can never lose data. On every
//! reconnect (and on a periodic safety poll) a sync runs regardless of
//! notifications. Because both the event stream and the changes-based sync
//! read from that same cursor, neither can recover a change the server failed
//! to record there; so on the configured cron `schedule` the daemon also runs
//! a full *snapshot refresh* (a complete re-enumeration reconciled against the
//! store), which does not depend on the cursor and is reported to the policy's
//! `ping` endpoints like any other scheduled run.
//!
//! Telemetry model: the daemon never holds a long-lived span. Each time-bound
//! operation — a backup pass or scheduled snapshot refresh (`daemon.backup`)
//! or an applied sync batch (`daemon.sync`) — records its own root trace, so
//! traces stay short and export promptly even though the process runs
//! indefinitely.

use std::sync::atomic::AtomicBool;
use std::time::Duration;

use tokio::time::Instant;
use tokio_stream::StreamExt;
use tracing_batteries::prelude::*;

use super::{BackupSummary, EngineOptions, cancelled, sync};
use crate::BackupPolicy;
use crate::entities::mail::SourceNotification;
use crate::ping::Pinger;
use crate::sources::MailSource;
use crate::stores::MailStore;

#[derive(Clone, Debug)]
pub struct StreamOptions {
    /// How long to wait after the last notification before applying a batch,
    /// coalescing rapid bursts of changes.
    pub quiet_period: Duration,
    /// The maximum time a batch may be delayed by continuous activity.
    pub max_batch_delay: Duration,
    /// How often to run a changes-based sync even without any notifications
    /// (belt and braces against missed events). Independent of the configured
    /// cron `schedule`, which drives a full snapshot refresh instead.
    pub safety_poll: Duration,
    /// Reconnection backoff bounds.
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            quiet_period: Duration::from_secs(3),
            max_batch_delay: Duration::from_secs(30),
            safety_poll: Duration::from_secs(6 * 60 * 60),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamEnd {
    /// A shutdown was requested.
    Cancelled,
    /// The event stream ended or failed; the caller should reconnect with
    /// backoff and run a catch-up sync.
    Disconnected,
    /// The server reported our state as too old; the caller must run a full
    /// reconciliation (which needs to re-connect the source).
    NeedsReconcile,
    /// The configured cron `schedule` came due; the caller should run a full
    /// snapshot refresh (reported to the `ping` endpoints) before resuming the
    /// stream.
    ScheduledRefresh,
}

/// Runs the daemon for a single policy: an initial backup pass (backfill
/// and/or catch-up), then the streaming loop with reconnection. When the
/// configured cron `schedule` comes due the stream yields, a full snapshot
/// refresh runs, and the stream resumes.
///
/// Each full backup pass and scheduled snapshot refresh is reported to the
/// policy's cron `ping` endpoints; the incremental live syncs in between are
/// intentionally not, as a cron monitor tracks scheduled runs rather than
/// every incremental change.
#[allow(clippy::too_many_arguments)]
pub async fn run<S: MailSource, T: MailStore>(
    name: &str,
    source: &mut S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    stream_options: &StreamOptions,
    schedule: Option<&croner::Cron>,
    cancel: &AtomicBool,
) -> Result<(), human_errors::Error> {
    let pinger = Pinger::new(policy.ping.clone());
    let mut backoff = stream_options.reconnect_min;
    // Set once the schedule comes due, so the next pass is a full snapshot
    // refresh (a complete re-enumeration) rather than a changes-based pass.
    let mut refresh = false;

    while !cancelled(cancel) {
        // Full pass (backfill if needed, then either a catch-up with a
        // reconcile fallback, or — when the schedule fired — an unconditional
        // snapshot refresh); this also re-connects the source after a
        // disconnect. Each pass is a root trace of its own.
        let do_refresh = std::mem::take(&mut refresh);
        let span = info_span!(
            parent: None,
            "daemon.backup",
            policy = %name,
            refresh = do_refresh,
            source = %policy.from,
            store = %policy.to,
            dry_run = options.dry_run,
            concurrency = options.concurrency,
            added = EmptyField,
            moved = EmptyField,
            updated = EmptyField,
            removed = EmptyField,
            unchanged = EmptyField,
            skipped = EmptyField,
            interrupted = EmptyField,
            error = EmptyField,
        );
        // The pass is wrapped in a `start`/`success`/`failure` ping; an
        // interrupted pass (clean shutdown) is reported as neither, since it
        // resumes on the next run. Instrumenting the whole `observe` (rather
        // than just the pass) means the pings run inside the `daemon.backup`
        // span and carry its trace context to the monitor.
        let pass = async {
            if do_refresh {
                info!("Running the scheduled snapshot refresh for {}", policy);
                super::run_refresh(source, store, policy, options, cancel).await
            } else {
                super::run_backup(source, store, policy, options, cancel).await
            }
        };
        let outcome = pinger
            .observe(pass, BackupSummary::completed)
            .instrument(span.clone())
            .await;
        match outcome {
            Ok(summary) => {
                summary.record_span(&span);
                if summary.changes() > 0 {
                    info!("Synchronized {}: {}", policy, summary);
                }
                if summary.interrupted {
                    break;
                }
                backoff = stream_options.reconnect_min;
            }
            Err(e) => {
                span.record("error", display(&e));
                warn!(
                    "Synchronization of {} failed: {}; retrying in {:?}",
                    policy, e, backoff
                );
                sleep_cancellable(backoff, cancel).await;
                backoff = (backoff * 2).min(stream_options.reconnect_max);
                continue;
            }
        }

        if cancelled(cancel) {
            break;
        }

        info!("Streaming live changes for {}", policy);
        match stream_phase(
            name,
            source,
            store,
            policy,
            options,
            stream_options,
            schedule,
            cancel,
        )
        .await
        {
            StreamEnd::Cancelled => break,
            StreamEnd::NeedsReconcile => continue,
            StreamEnd::ScheduledRefresh => {
                // The next pass becomes a full snapshot refresh, reported to
                // the cron monitor like any other scheduled run.
                refresh = true;
            }
            StreamEnd::Disconnected => {
                debug!(
                    "The event stream for {} ended; reconnecting in {:?}",
                    policy, backoff
                );
                sleep_cancellable(backoff, cancel).await;
                backoff = (backoff * 2).min(stream_options.reconnect_max);
            }
        }
    }

    info!("Daemon for {} shut down cleanly", policy);
    Ok(())
}

/// Consumes the source's event stream, debouncing notifications into batched
/// syncs, until the stream ends, a shutdown is requested, or a full
/// reconciliation becomes necessary.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn stream_phase<S: MailSource, T: MailStore>(
    name: &str,
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    stream_options: &StreamOptions,
    schedule: Option<&croner::Cron>,
    cancel: &AtomicBool,
) -> StreamEnd {
    let stream = source.events(cancel);
    tokio::pin!(stream);

    let mut pending = false;
    let mut quiet_deadline: Option<Instant> = None;
    let mut batch_deadline: Option<Instant> = None;
    let mut next_safety_poll = Instant::now() + stream_options.safety_poll;
    let next_scheduled = next_schedule_instant(schedule);
    let mut disconnected = false;

    loop {
        if cancelled(cancel) {
            return StreamEnd::Cancelled;
        }

        let now = Instant::now();

        // When the configured cron schedule comes due, hand control back to
        // the daemon loop so it can run (and ping) a full snapshot refresh,
        // which recovers anything missed at the change-feed level. Any pending
        // batch is left to be re-derived by the refresh's reconciliation.
        if next_scheduled.is_some_and(|at| now >= at) {
            debug!("The configured schedule for {} came due", policy);
            return StreamEnd::ScheduledRefresh;
        }

        // Apply a pending batch once it has gone quiet, been delayed too
        // long, or the stream has ended (nothing more to coalesce with); and
        // run the safety poll when it comes due.
        let due = pending
            && (disconnected
                || quiet_deadline.is_some_and(|d| now >= d)
                || batch_deadline.is_some_and(|d| now >= d));
        if due || now >= next_safety_poll {
            if now >= next_safety_poll {
                debug!("Running the safety synchronization for {}", policy);
                next_safety_poll = Instant::now() + stream_options.safety_poll;
            }

            // Each applied batch is a time-bound operation and gets its own
            // root trace, tagged with what prompted it.
            let trigger = if !due {
                "safety"
            } else if disconnected {
                "flush"
            } else {
                "notification"
            };
            let span = info_span!(
                parent: None,
                "daemon.sync",
                policy = %name,
                trigger,
                source = %policy.from,
                store = %policy.to,
                dry_run = options.dry_run,
                concurrency = options.concurrency,
                schedule = schedule.map(display),
                added = EmptyField,
                moved = EmptyField,
                updated = EmptyField,
                removed = EmptyField,
                unchanged = EmptyField,
                skipped = EmptyField,
                interrupted = EmptyField,
                error = EmptyField,
            );
            match run_sync(source, store, policy, options, cancel)
                .instrument(span.clone())
                .await
            {
                Ok(summary) => {
                    summary.record_span(&span);
                    pending = false;
                    quiet_deadline = None;
                    batch_deadline = None;
                }
                Err(SyncFailure::NeedsReconcile) => return StreamEnd::NeedsReconcile,
                Err(SyncFailure::Error(e)) => {
                    span.record("error", display(&e));
                    warn!("Applying live changes for {} failed: {}", policy, e);
                    return StreamEnd::Disconnected;
                }
            }
        }

        if disconnected {
            // The stream ended; any pending batch has been applied above.
            return StreamEnd::Disconnected;
        }

        // Wait for the next notification or the earliest deadline.
        let wake = [
            quiet_deadline,
            batch_deadline,
            Some(next_safety_poll),
            next_scheduled,
            Some(Instant::now() + Duration::from_secs(1)), // cancellation poll
        ]
        .into_iter()
        .flatten()
        .min()
        .expect("at least one deadline is always present");

        tokio::select! {
            event = stream.next() => match event {
                Some(Ok(SourceNotification::Changed { email, mailbox })) => {
                    debug!(
                        "Change notification received (email: {}, mailbox: {})",
                        email, mailbox
                    );
                    let now = Instant::now();
                    pending = true;
                    quiet_deadline = Some(now + stream_options.quiet_period);
                    batch_deadline
                        .get_or_insert(now + stream_options.max_batch_delay);
                }
                Some(Ok(SourceNotification::Ping)) => {}
                Some(Err(e)) => {
                    warn!("The event stream for {} failed: {}", policy, e);
                    disconnected = true;
                }
                None => {
                    disconnected = true;
                }
            },
            _ = tokio::time::sleep_until(wake) => {}
        }
    }
}

enum SyncFailure {
    NeedsReconcile,
    Error(human_errors::Error),
}

async fn run_sync<S: MailSource, T: MailStore>(
    source: &S,
    store: &mut T,
    policy: &BackupPolicy,
    options: &EngineOptions,
    cancel: &AtomicBool,
) -> Result<BackupSummary, SyncFailure> {
    match sync::run(source, store, policy, options, cancel).await {
        Ok(outcome) if outcome.needs_reconcile => Err(SyncFailure::NeedsReconcile),
        Ok(outcome) => {
            if outcome.summary.changes() > 0 {
                info!("Applied live changes: {}", outcome.summary);
            }
            Ok(outcome.summary)
        }
        Err(e) => Err(SyncFailure::Error(e)),
    }
}

/// The instant of the next occurrence of the cron `schedule`, if one is
/// configured and its next occurrence is computable. `None` leaves the
/// streaming loop relying solely on the safety poll, with no scheduled
/// snapshot refresh.
fn next_schedule_instant(schedule: Option<&croner::Cron>) -> Option<Instant> {
    schedule
        .and_then(|s| s.find_next_occurrence(&chrono::Utc::now(), false).ok())
        .and_then(|next| (next - chrono::Utc::now()).to_std().ok())
        .map(|delay| Instant::now() + delay)
}

async fn sleep_cancellable(duration: Duration, cancel: &AtomicBool) {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline && !cancelled(cancel) {
        tokio::time::sleep(Duration::from_millis(100).min(duration)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::{MailboxInfo, MessageMeta};
    use crate::sources::mock::MockMailSource;
    use crate::stores::MailStore;
    use crate::stores::git::GitMailStore;
    use std::sync::atomic::AtomicBool;

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
            message_id: vec![],
            subject: None,
            from: vec![],
        }
    }

    fn policy() -> crate::BackupPolicy {
        serde_yaml::from_str(
            "from: !Jmap { url: 'http://mock', token: 'token' }\nto: !LocalDir { path: '/unused' }",
        )
        .unwrap()
    }

    fn fast_stream_options() -> StreamOptions {
        StreamOptions {
            quiet_period: Duration::from_millis(10),
            max_batch_delay: Duration::from_millis(100),
            safety_poll: Duration::from_secs(3600),
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn streaming_applies_live_changes_and_amends() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let mut source = MockMailSource::new("acc-1");
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

        source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox")));
        let today = chrono::Utc::now().date_naive();
        source.add_message(
            meta("M1", &["mb-inbox"], &[], &format!("{today}T01:00:00Z")),
            b"initial message",
        );

        // Initial pass: backfill the existing message.
        super::super::run_backup(
            &mut source,
            &mut store,
            &policy(),
            &EngineOptions::default(),
            &cancel,
        )
        .await
        .unwrap();
        assert!(store.lookup("M1").is_some());

        // Live: a new message arrives and a notification is queued; the
        // stream then ends (mock yields queued items and stops), which must
        // flush the pending batch before reporting the disconnect.
        source.add_message(
            meta(
                "M2",
                &["mb-inbox"],
                &["$seen"],
                &format!("{today}T02:00:00Z"),
            ),
            b"live message",
        );
        source.push_notification(crate::entities::mail::SourceNotification::Changed {
            email: true,
            mailbox: false,
        });

        let end = stream_phase(
            "test",
            &source,
            &mut store,
            &policy(),
            &EngineOptions::default(),
            &fast_stream_options(),
            None,
            &cancel,
        )
        .await;

        assert_eq!(end, StreamEnd::Disconnected);
        assert!(store.lookup("M2").is_some(), "live change applied");
        assert!(store.lookup("M2").unwrap().meta.keywords.contains("$seen"));
    }

    #[tokio::test]
    async fn cancellation_stops_the_stream() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(true);
        let source = MockMailSource::new("acc-1");
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();

        let end = stream_phase(
            "test",
            &source,
            &mut store,
            &policy(),
            &EngineOptions::default(),
            &fast_stream_options(),
            None,
            &cancel,
        )
        .await;
        assert_eq!(end, StreamEnd::Cancelled);
    }

    #[tokio::test]
    async fn streaming_yields_for_a_scheduled_refresh() {
        use std::str::FromStr;

        // Auto-advancing virtual time fast-forwards through the streaming
        // loop's deadlines without waiting in real time.
        tokio::time::pause();
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let mut source = MockMailSource::new("acc-1");
        // A healthy, long-lived subscription with no notifications: only the
        // schedule should end the streaming phase.
        source.hold_events_open();
        source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox")));
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();

        // Fires every minute; the safety poll (an hour out in the fast
        // options) is far enough away that the schedule comes due first.
        let schedule = croner::Cron::from_str("* * * * *").unwrap();

        let end = stream_phase(
            "test",
            &source,
            &mut store,
            &policy(),
            &EngineOptions::default(),
            &fast_stream_options(),
            Some(&schedule),
            &cancel,
        )
        .await;

        assert_eq!(end, StreamEnd::ScheduledRefresh);
    }

    #[tokio::test]
    async fn streaming_does_not_yield_for_a_refresh_without_a_schedule() {
        // Without a schedule the held-open stream just keeps running on the
        // safety poll; cancellation is the only thing that ends it.
        tokio::time::pause();
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let mut source = MockMailSource::new("acc-1");
        source.hold_events_open();
        source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox")));
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);
        store.open().await.unwrap();

        let policy = policy();
        let options = EngineOptions::default();
        let stream_options = fast_stream_options();
        let phase = stream_phase(
            "test",
            &source,
            &mut store,
            &policy,
            &options,
            &stream_options,
            None,
            &cancel,
        );

        // The phase never yields a scheduled refresh on its own; only the
        // cancellation we trigger after letting it run a while ends it.
        let cancelling = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        };
        let (end, _) = tokio::join!(phase, cancelling);
        assert_eq!(end, StreamEnd::Cancelled);
    }

    #[tokio::test]
    async fn daemon_pings_each_scheduled_snapshot_refresh() {
        use std::str::FromStr;
        use std::sync::atomic::Ordering;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let mut source = MockMailSource::new("acc-1");
        // Healthy long-lived stream so only the schedule prompts a new pass.
        source.hold_events_open();
        source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox")));
        let today = chrono::Utc::now().date_naive();
        source.add_message(
            meta("M1", &["mb-inbox"], &[], &format!("{today}T01:00:00Z")),
            b"message",
        );
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

        // A policy that pings the mock monitor, and a per-second schedule so a
        // snapshot refresh comes due promptly.
        let policy: crate::BackupPolicy = serde_yaml::from_str(&format!(
            "from: !Jmap {{ url: 'http://mock', token: 'token' }}\n\
             to: !LocalDir {{ path: '/unused' }}\n\
             ping:\n  start: {base}/start\n  success: {base}/ok\n",
            base = server.uri()
        ))
        .unwrap();
        let schedule = croner::Cron::from_str("* * * * * *").unwrap();
        let options = EngineOptions::default();
        let stream_options = fast_stream_options();

        let daemon = run(
            "test",
            &mut source,
            &mut store,
            &policy,
            &options,
            &stream_options,
            Some(&schedule),
            &cancel,
        );
        // Stop once both the initial pass and at least one scheduled refresh
        // have completed and reported success to the monitor.
        let stopper = async {
            loop {
                let oks = server
                    .received_requests()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .filter(|r| r.url.path() == "/ok")
                    .count();
                if oks >= 2 {
                    cancel.store(true, Ordering::Relaxed);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        let (result, _) = tokio::join!(daemon, stopper);
        result.unwrap();

        let requests = server.received_requests().await.unwrap_or_default();
        let starts = requests.iter().filter(|r| r.url.path() == "/start").count();
        let oks = requests.iter().filter(|r| r.url.path() == "/ok").count();
        assert!(
            starts >= 2,
            "the initial pass and the scheduled refresh both ping start (got {starts})"
        );
        assert!(
            oks >= 2,
            "both passes report success to the monitor (got {oks})"
        );
    }

    #[tokio::test]
    async fn daemon_run_completes_when_cancelled_after_disconnect() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = AtomicBool::new(false);
        let mut source = MockMailSource::new("acc-1");
        let mut store = GitMailStore::new(dir.path().to_path_buf(), None, None);

        source.upsert_mailbox(mailbox("mb-inbox", "Inbox", Some("inbox")));
        source.add_message(
            meta("M1", &["mb-inbox"], &[], "2023-01-01T08:00:00Z"),
            b"message",
        );

        // The mock's event stream ends immediately (no notifications), so the
        // daemon cycles through reconnects; cancel after a short delay.
        let canceller = async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            cancel.store(true, std::sync::atomic::Ordering::Relaxed);
        };

        let policy = policy();
        let options = EngineOptions::default();
        let stream_options = fast_stream_options();
        let daemon = run(
            "test",
            &mut source,
            &mut store,
            &policy,
            &options,
            &stream_options,
            None,
            &cancel,
        );

        let (result, _) = tokio::join!(daemon, canceller);
        result.unwrap();
        assert!(store.lookup("M1").is_some(), "initial backfill ran");
    }
}
