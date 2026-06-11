# Running as a Daemon
While `mail-backup backup` performs a one-shot synchronization and exits, the recommended
way to run Mail Backup is as a long-lived daemon:

```bash
./mail-backup run --config config.yaml
```

In this mode the tool:

1. Performs (or resumes) the initial backfill if it hasn't completed yet.
2. Runs a changes-based catch-up to bring the archive up to date.
3. Opens a real-time event stream (JMAP EventSource) to the server and applies changes
   moments after they happen — new mail, moves between folders, flag changes, and
   deletions.

## Daily snapshots and amending
Each calendar day (UTC) gets exactly one commit. The first change of the day creates the
day's commit; every subsequent change *amends* it. When the day rolls over, the previous
day's commit is sealed exactly as it was, and the next change starts a new one. Mail
received on earlier days (e.g. during backfill or after an import) is committed as
backdated daily snapshots, keeping `git log` an accurate ledger of your mailbox history.

## Reliability
Notifications are only ever treated as a *hint* to synchronize — every synchronization
starts from the persisted server-state cursor, so missed or duplicated notifications can
never lose data. In addition:

- If the event stream drops, the daemon reconnects with exponential backoff and always
  runs a catch-up synchronization on reconnection.
- The cron expression in `schedule` acts as a safety net, running a full synchronization
  on that cadence even while the stream is healthy (a 6-hour fallback applies when no
  schedule is configured).
- If the server can no longer compute changes from our saved state (for example after a
  very long offline period), the daemon automatically falls back to a full reconciliation,
  which never re-downloads messages it already holds.
- All file writes are atomic, and interrupting the process at any point (including during
  the initial backfill) is safe: the next run resumes and converges on the same state.

## Shutdown
Press `Ctrl+C` (or send `SIGINT`) to shut down. The daemon finishes the batch it is
applying, commits, saves its state, and exits cleanly.

## Running under systemd

```ini
[Unit]
Description=Mail Backup
After=network-online.target
Wants=network-online.target

[Service]
ExecStart=/usr/local/bin/mail-backup run --config /etc/mail-backup/config.yaml
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

## Running in Docker

```bash
docker run -d \
  --name mail-backup \
  --restart unless-stopped \
  -v $(pwd)/config.yaml:/config.yaml \
  -v $(pwd)/backups:/backups \
  ghcr.io/sierrasoftworks/mail-backup:latest \
  run --config /config.yaml
```
