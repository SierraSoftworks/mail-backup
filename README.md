<div align="center">
  <img src="./docs/.vuepress/public/icon.svg" alt="Mail Backup" width="460">
</div>

**Automatically back up your JMAP mailboxes to a local git repository.**

Mail Backup continuously archives your [Fastmail](https://www.fastmail.com) (or any other
[JMAP](https://jmap.io)) mail into a local git repository, with one backdated commit per day
of mail — so you can browse, diff, and restore the exact state of your mailbox at any point
in the past.

```bash
mail-backup run --config config.yaml
```

```yaml
# config.yaml
schedule: "0 6 * * *"

backups:
  personal:
    from: !Fastmail
      token: fmu1-xxxxxxxx-xxxxxxxxxxxxxxxx
    to: !LocalGit
      path: /backups/mail
    filter: '!(message.keywords contains "$junk")'

restores:
  personal:
    from: !LocalGit
      path: /backups/mail
    to: !Fastmail
      token: fmu1-yyyyyyyy-yyyyyyyyyyyyyyyy
```

## Installation

Install with [Homebrew](https://brew.sh):

```sh
brew install sierrasoftworks/tap/mail-backup
```

## Features

 - **Daily snapshots, as git commits.** The initial backfill walks your mail history
   chronologically, committing one backdated snapshot per day; afterwards the daemon
   streams changes from the server in real time (JMAP websocket push, EventSource, or
   state polling — whichever the server supports, in that order) and amends the current
   day's commit as mail arrives. On the configured `schedule` it also runs a full snapshot
   refresh that re-enumerates the server, catching anything the live stream missed.
 - **Full fidelity.** Every message is stored as its raw RFC 5322 bytes (headers and
   attachments included) plus a metadata sidecar capturing all of its mailboxes, keywords
   (read/unread, flagged, …), and received date. Moves between folders become git renames;
   `git log --follow` traces a message's whole journey.
 - **Restore included.** `mail-backup restore` recreates your mailbox tree and re-imports
   messages — to the same account, a different one, or under a prefix folder — from the
   latest state or any historical snapshot (`--at 2026-03-01`). Re-runs skip everything
   already on the server.
 - **Strictly read-only backups.** The backup path never modifies the account it reads.
 - **Advanced filtering.** The same expression language as
   [github-backup](https://github.com/SierraSoftworks/github-backup), with mail-aware
   properties: `message.mailbox == "INBOX" && !(message.keywords contains "$junk")`.
 - **Built to survive crashes.** Atomic writes, idempotent change application, resumable
   backfills, and automatic full reconciliation when the server can no longer compute
   changes from the saved state.
 - **Observable.** OpenTelemetry tracing via
   [tracing-batteries](https://github.com/SierraSoftworks/tracing-batteries-rs).

## Getting started

1. Create a (read-only) Fastmail API token at *Settings → Privacy & Security → Manage API
   tokens*.
2. Write a `config.yaml` like the one above.
3. Validate and run:

```bash
mail-backup check --config config.yaml
mail-backup backup --config config.yaml   # one-shot
mail-backup run --config config.yaml      # daemon with real-time streaming
```

Full documentation lives at
**[mail-backup.sierrasoftworks.com](https://mail-backup.sierrasoftworks.com)**,
including the [configuration reference](docs/reference/config.md), the
[filter language](docs/advanced/filters.md), and the
[on-disk storage layout](docs/advanced/storage-layout.md).

## Development

```bash
cargo test          # the full suite runs offline (wiremock + temp repositories)
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The architecture mirrors github-backup: mail *sources* (`src/sources/`), *stores*
(`src/stores/`), and restore *targets* (`src/restore/`) are trait-based, so additional
protocols (IMAP), destinations (S3), and targets can be added without touching the sync
engine (`src/engine/`).
