# Storage Layout
The on-disk format is designed around three goals: every byte of your mail is preserved
exactly, the archive is fully self-describing (a clone of the repository is enough to
restore from), and git's history mechanics map naturally onto mailbox semantics.

## The tree

```
/backups/mail/
├── .gitattributes                              # "* -text" — protects raw mail from CRLF mangling
├── Inbox/
│   ├── .mailbox.yaml                           # mailbox metadata (id, true name, role, parent)
│   ├── 20260311-084512-d34db33fc4f3.eml        # raw RFC 5322 message, byte-identical to the server's copy
│   └── 20260311-084512-d34db33fc4f3.meta.yaml  # message metadata sidecar
└── Archive/
    ├── .mailbox.yaml
    └── Receipts/
        ├── .mailbox.yaml
        └── ...
```

- **One directory per mailbox**, mirroring your folder hierarchy. Directory names are
  sanitized to be safe on every platform (Windows included); the mailbox's *true* name
  lives in its `.mailbox.yaml`.
- **One `.eml` file per message** holding the complete raw RFC 5322 content — every
  header, every attachment, byte-for-byte as the server stores it.
- **One `.meta.yaml` sidecar per message** recording everything needed for a
  full-fidelity restore: the message's id, blob id, thread id, **all** of its mailbox
  memberships, its keywords (including `$seen` read state), received time, size,
  `Message-ID` headers, and a sha256 checksum of the raw content.

## File names
Messages are named `<received-at UTC>-<sha256 prefix>.eml`, e.g.
`20260311-084512-d34db33fc4f3.eml`. Both components are immutable properties of the
message, so the name is stable for the message's entire life — files sort
chronologically, and a message keeps its identity wherever it goes.

## Moves are renames
JMAP models a message's folder as mutable metadata, but on disk each message lives under
its *primary* mailbox (chosen by a stable role-based rule: Inbox over Archive over Sent…,
with Junk and Trash last). When a message moves between folders, its file moves to the
new directory with byte-identical content — which git's rename detection recognises as a
rename, giving you `git log --follow` across a message's whole journey:

```bash
git log --follow --oneline -- "Archive/20260311-084512-d34db33fc4f3.eml"
```

Membership of *additional* mailboxes (and keyword changes such as read/unread) only
touch the small `.meta.yaml` sidecar, producing tiny, readable diffs.

## Daily snapshot commits
Each UTC day of mail becomes one commit, authored *on that day*:

- During the initial backfill (and any catch-up covering older days), commits are
  backdated to the day they describe, so `git log` reads as a true daily ledger going
  back to your first email.
- During live streaming, the current day's commit is amended as changes arrive, then
  sealed when the day rolls over.

Deletions mirror the server: the file disappears from the current tree, while history
retains every byte for as long as you keep the repository.

## State and the index
The synchronization cursor (the JMAP state strings) and a derived message index live
under `.git/mail-backup/` — deliberately *outside* the committed tree, because they are
account-specific and entirely rebuildable. The committed sidecars are the source of
truth: `mail-backup index` rebuilds the index from them at any time, and a fresh clone
of the archive is fully restorable.

Two maintenance commands keep you honest:

```bash
# Rebuild the derived index from the sidecars
mail-backup index --path /backups/mail

# Verify the worktree matches the committed state
mail-backup verify --path /backups/mail
```

## A note on repository hygiene
The archive is a normal git repository — you can clone it, push it to a private remote,
or run `git gc` when you want to compact loose objects. Avoid *editing* files in the
worktree by hand, since the worktree is the staging area for the next snapshot;
`mail-backup verify` reports any divergence it finds.
