# Restoring Mail
A backup you can't restore is just a pile of files. Mail Backup ships a first-class
`restore` command which re-imports an archive — or any historical snapshot of it — to a
JMAP server, recreating the mailbox tree and preserving each message's folders, keywords
(including read/unread state), and original received date.

## Configuration

Restores are configured as policies in the same file as your backups:

```yaml
restores:
  personal:
    from: !LocalGit
      path: /backups/mail
    to: !Fastmail
      token: fmu1-yyyyyyyy-yyyyyyyyyyyyyyyy   # needs WRITE access
    filter: message.received > "2026-01-01"
    dedupe: message-id
    # mailbox_prefix: Restored
```

A restore policy is the mirror image of a backup policy: `from` is a local store and `to`
is a mail account. The token used for restoring needs write (mail) access, unlike the
read-only token used for backups.

## Running a restore

Always start with a dry run, which connects to the target read-only and prints exactly
what would happen:

```bash
./mail-backup restore --config config.yaml --dry-run
```

Then run it for real:

```bash
./mail-backup restore --config config.yaml
```

Useful options:

| Option | Effect |
|---|---|
| `--policy <name>` | Choose a restore policy by name (required when several are configured; the only policy is used by default otherwise). |
| `--at 2026-03-01` | Restore the archive as it was at the end of that day. |
| `--at <commit>` | Restore the archive as of a specific git commit. |
| `--filter EXPR` | Override the policy's filter (see [Filters](../advanced/filters.md)). |
| `--force` | Import even messages which already exist on the target. |
| `--dry-run` | Plan everything, change nothing. |

## How duplicates are avoided
With `dedupe: message-id` (the default), each selected message is checked against the
target server by its `Message-ID` header and skipped when it already exists. This makes
restores safely *re-runnable*: if a restore is interrupted or some imports fail, simply
run it again — everything that already made it through is skipped.

Messages without a `Message-ID` header fall back to a received-time and size comparison;
ambiguous cases are imported (favouring completeness over deduplication). Use
`dedupe: none` or `--force` to disable the checks entirely.

## How mailboxes are recreated
Existing mailboxes on the target are matched by their full name path (e.g.
`Archive/Receipts`, case-insensitive) and reused; missing ones are created, parents
first. Special-role mailboxes (Inbox, Archive, Sent, …) only claim their role on the
target when no existing mailbox holds it.

With `mailbox_prefix` set, everything is created beneath that folder instead of at the
top level — handy for restoring into a folder of an active account without mixing
restored mail into your live folders.

## Integrity
Every message's content is verified against the sha256 checksum recorded in its metadata
sidecar before being uploaded. A mismatch (e.g. a corrupted or hand-edited file) aborts
the restore with a pointer to the offending file rather than silently restoring modified
mail.
