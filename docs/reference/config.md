# Configuration
Mail Backup reads a single YAML configuration file (default `config.yaml`, override with
`--config`). It contains an optional schedule plus any number of backup and restore
policies, each keyed by a name of your choosing — the name selects the policy on the
command line (`--policy personal`) and identifies it in log output.

```yaml
schedule: "0 6 * * *"

backups:
  personal:
    from: !Fastmail
      token: fmu1-xxxxxxxx-xxxxxxxxxxxxxxxx
      account: user@example.com
    to: !LocalGit
      path: /backups/mail/personal
      commit_name: mail-backup
      commit_email: mail-backup@example.com
    filter: '!(message.keywords contains "$junk")'
    backfill_start: 2008-01-01

restores:
  personal:
    from: !LocalGit
      path: /backups/mail/personal
    to: !Fastmail
      token: fmu1-yyyyyyyy-yyyyyyyyyyyyyyyy
    filter: message.received > "2026-01-01"
    dedupe: message-id
    mailbox_prefix: Restored
```

## `schedule`
A cron expression controlling how often the daemon (`run`) performs a full snapshot
refresh: a complete re-enumeration of the server, reconciled against the archive, on top
of the real-time stream and a 6-hour changes-based catch-up. Because the stream and the
catch-up both read from the same server-state cursor, the refresh is what recovers
anything the server failed to record there. Each scheduled refresh is reported to the
policy's [`ping`](#cron-monitoring-ping) endpoints. With no schedule configured the 6-hour
catch-up still runs, but no full refresh does.

## Sources (`from:` in backups, `to:` in restores)
Sources describe a mail account, written as YAML tagged values. Credentials are part of
the source itself.

### `!Fastmail`
| Field | Required | Description |
|---|---|---|
| `token` | yes | A Fastmail API token. Read-only scope suffices for backups; restores need write access. |
| `account` | no | Selects an account by id or email address when the token can access several. Defaults to the primary mail account. |

### `!Jmap`
Any other JMAP (RFC 8620/8621) provider.

| Field | Required | Description |
|---|---|---|
| `url` | yes | The server's base URL; the standard `/.well-known/jmap` session resource is resolved from it. |
| `token` | yes | A bearer token with mail access. |
| `account` | no | As for `!Fastmail`. |

## Stores (`to:` in backups, `from:` in restores)

### `!LocalGit`
A local git repository receiving one commit per day of mail. Created and initialized
automatically on first use.

| Field | Required | Description |
|---|---|---|
| `path` | yes | The repository's location on disk. |
| `commit_name` | no | Git author/committer name (default `mail-backup`). |
| `commit_email` | no | Git author/committer email. |

### `!LocalDir`
A plain directory tree with the same layout, but no version history.

| Field | Required | Description |
|---|---|---|
| `path` | yes | The directory's location on disk. |

## Backup policy fields

| Field | Required | Description |
|---|---|---|
| `from` | yes | The mail account to back up. |
| `to` | yes | The store to back it up into. Each store holds exactly one account. |
| `filter` | no | Which messages to archive (default: everything). See [Filters](../advanced/filters.md). |
| `backfill_start` | no | The earliest day of mail (by received date) the initial backfill reaches back to. |
| `ping` | no | HTTP cron-monitoring endpoints for this policy's scheduled runs. See [Cron monitoring](#cron-monitoring-ping). |

### Cron monitoring (`ping`)
Reports the lifecycle of each scheduled backup run to an external HTTP cron monitor —
for example [Sentry Crons](https://docs.sentry.io/product/crons/) or
[Healthchecks.io](https://healthchecks.io/). Each state has its own URL, fetched with a
plain HTTP `GET` as the run reaches it; any state you omit is simply not reported. Only
full backup runs are reported — a one-shot `backup`, and in the daemon the initial pass
and each scheduled [snapshot refresh](#schedule). The daemon's incremental live syncs are
not reported, and a run cut short by shutdown reports neither success nor failure.

Pings are best-effort: a ping that fails or times out is logged and otherwise ignored, so
an unreachable monitor can never take a backup down.

Each ping carries the W3C trace context (a `traceparent` header) of the backup run it
reports, so a monitor that understands trace context can correlate it with the same
OpenTelemetry trace as the run. Nothing is added when OpenTelemetry tracing is not
configured.

| Field | Required | Description |
|---|---|---|
| `start` | no | Pinged when a run begins. |
| `success` | no | Pinged when a run completes successfully. |
| `failure` | no | Pinged when a run fails. |

```yaml
backups:
  personal:
    from: !Fastmail { token: fmu1-xxxxxxxx-xxxxxxxxxxxxxxxx }
    to: !LocalGit { path: /backups/mail/personal }
    ping:
      # Sentry distinguishes states with a query string …
      start: https://sentry.io/api/0/monitors/personal/<key>/?status=in_progress
      success: https://sentry.io/api/0/monitors/personal/<key>/?status=ok
      failure: https://sentry.io/api/0/monitors/personal/<key>/?status=error
      # … while Healthchecks.io uses a path suffix:
      # start: https://hc-ping.com/<uuid>/start
      # success: https://hc-ping.com/<uuid>
      # failure: https://hc-ping.com/<uuid>/fail
```

## Restore policy fields

| Field | Required | Description |
|---|---|---|
| `from` | yes | The store to restore from. |
| `to` | yes | The mail account to restore into. |
| `filter` | no | Which archived messages to restore (default: everything). |
| `dedupe` | no | `message-id` (default) skips messages already on the target; `none` imports everything. |
| `mailbox_prefix` | no | Restore mailboxes underneath this folder rather than at the account's top level. |
