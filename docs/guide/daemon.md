# Running as a Daemon
While `mail-backup backup` performs a one-shot synchronization and exits, the recommended
way to run Mail Backup is as a long-lived daemon:

```bash
./mail-backup run --config config.yaml
```

In this mode the tool:

1. Performs (or resumes) the initial backfill if it hasn't completed yet.
2. Runs a changes-based catch-up to bring the archive up to date.
3. Opens a real-time event stream to the server and applies changes moments after they
   happen — new mail, moves between folders, flag changes, and deletions. The daemon
   prefers websocket push (RFC 8887) where the server advertises it, falls back to
   EventSource/SSE, and finally to periodic state polling, so any reachable JMAP server
   ends up with a working stream. A transport that keeps failing is set aside for a
   while in favour of the next one, and is tried again later.

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
- A changes-based catch-up also runs every 6 hours regardless of notifications, as a
  belt-and-braces against any missed events.
- On the cadence set by the cron expression in `schedule`, the daemon runs a full
  *snapshot refresh* — a complete re-enumeration of the server reconciled against the
  archive — even while the stream is healthy. Because the event stream and the
  changes-based syncs both read from the same server-state cursor, neither can recover a
  change the server failed to record there; the snapshot refresh does not depend on the
  cursor, so it catches anything they missed. It never re-downloads messages it already
  holds, and (like the initial pass) it is reported to the policy's [`ping`](../reference/config.md#cron-monitoring-ping)
  endpoints. With no schedule configured, only the 6-hour catch-up applies.
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
The recommended server setup is Docker Compose — the repository ships a ready-to-use
[`docker-compose.yaml`](https://github.com/SierraSoftworks/mail-backup/blob/main/docker-compose.yaml):

```yaml
services:
  mail-backup:
    image: ghcr.io/sierrasoftworks/mail-backup:latest
    restart: unless-stopped
    command: ["run", "--config", "/config.yaml"]
    volumes:
      - ./config.yaml:/config.yaml:ro
      - ./backups:/backups
    # Give the daemon time to checkpoint cleanly when stopping.
    stop_grace_period: 30s
```

Place your `config.yaml` next to it (with stores pointing at paths under `/backups`),
then:

```bash
docker compose up -d
docker compose logs -f mail-backup
```

Stopping the container sends `SIGTERM`, which the daemon treats exactly like `Ctrl+C`:
it finishes the in-flight batch, commits, and saves its state before exiting.

A plain `docker run` works too:

```bash
docker run -d \
  --name mail-backup \
  --restart unless-stopped \
  --stop-timeout 30 \
  -v $(pwd)/config.yaml:/config.yaml:ro \
  -v $(pwd)/backups:/backups \
  ghcr.io/sierrasoftworks/mail-backup:latest \
  run --config /config.yaml
```
