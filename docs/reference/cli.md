# Command Line
```
mail-backup [OPTIONS] <COMMAND>
```

## Global options

| Option | Default | Description |
|---|---|---|
| `-c, --config <FILE>` | `config.yaml` | Path to the configuration file. |
| `-d, --dry-run` | off | Plan and log actions without writing anything (locally or remotely). |
| `--concurrency <N>` | `4` | Maximum concurrent message downloads/uploads. |

## `mail-backup run`
Runs as a daemon: backfills/catches up each backup policy, then streams live changes
from the server, amending the current day's snapshot as mail arrives. All configured
backup policies run concurrently. See [Running as a Daemon](../guide/daemon.md).

## `mail-backup backup`
Performs a one-shot backup — backfill (if incomplete) plus a changes-based catch-up —
then exits.

| Option | Description |
|---|---|
| `--policy <NAME>` | Only run the backup policy with this name (default: all). |

## `mail-backup restore`
Restores an archive to a mail server. See [Restoring Mail](../guide/restore.md).

| Option | Description |
|---|---|
| `--policy <NAME>` | The name of the restore policy to run. Defaults to the only configured policy; required when several exist. |
| `--at <DATE\|REV>` | Restore the archive as it was at the end of a day (`YYYY-MM-DD`) or at a git revision. |
| `--filter <EXPR>` | Override the policy's filter expression. |
| `--force` | Import messages even when they already exist on the target. |

## `mail-backup check`
Validates the configuration, connects to every configured mail source, and reports the
account and mailbox count for each — without touching any store.

## `mail-backup index`
Rebuilds a store's derived index from the metadata sidecars on disk. Useful after
hand-maintenance of the repository or when migrating an archive between machines.

| Option | Description |
|---|---|
| `--path <DIR>` | The store to reindex (default: every backup policy's store). |

## `mail-backup verify`
Checks that a store's working tree matches its committed state (for git stores) or its
index (for plain directories), reporting every inconsistency found. Exits non-zero when
inconsistencies exist.

| Option | Description |
|---|---|
| `--path <DIR>` | The store to verify (default: every backup policy's store). |

## Exit codes
`0` on success; `1` when any command fails (details are logged, and exported via
OpenTelemetry when configured — see [Telemetry](../guide/telemetry.md)).
