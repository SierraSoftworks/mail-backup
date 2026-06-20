# Introduction
Your email is often the most irreplaceable data you own: receipts, contracts, conversations,
and account recovery all live there. Mail providers are reliable, but accounts get locked,
sync clients misbehave, and a single errant filter rule can empty a folder before you notice.

Mail Backup gives you an independent, local, versioned copy of your mailbox. It speaks
[JMAP](https://jmap.io) (the protocol behind [Fastmail](https://www.fastmail.com)), stores
every message as a plain `.eml` file in a git repository, and commits your mailbox once per
day — so you can reproduce the state of your mail at any point in the past, and restore any
of it back to a server when you need to.

## Getting an API token
For Fastmail, create an API token at
[Settings → Privacy & Security → Manage API tokens](https://app.fastmail.com/settings/security/tokens):

1. Click **New API token**.
2. Grant it **read-only** mail access — backups never write to your account, so the token
   doesn't need more than that. (A restore needs a separate token with write access.)
3. Copy the token (it is only shown once) into your configuration file.

For other JMAP providers, any bearer token with mail access works; configure the provider's
base URL with the `!Jmap` source.

## Installation

Install with [Homebrew](https://brew.sh):

```sh
brew install sierrasoftworks/tap/mail-backup
```

## Your first backup

Create a `config.yaml`:

```yaml
backups:
  personal:
    from: !Fastmail
      token: fmu1-xxxxxxxx-xxxxxxxxxxxxxxxx
    to: !LocalGit
      path: /backups/mail
```

Each policy gets a name of your choosing (here `personal`) which identifies it in log
output and on the command line.

Then validate your configuration and connectivity:

```bash
./mail-backup check --config config.yaml
```

And run your first backup:

```bash
./mail-backup backup --config config.yaml
```

The first run *backfills* your mailbox: it enumerates your mail history in chronological
order and creates one git commit per day of mail, backdated to that day. Depending on the
size of your mailbox this can take a while — it is safe to interrupt at any point and will
resume where it left off.

Once the backfill completes, each subsequent `backup` run applies only what changed. To keep
the archive continuously up to date instead, run the daemon:

```bash
./mail-backup run --config config.yaml
```

See [Running as a Daemon](./daemon.md) for more.

## Browsing your archive

The archive is a normal git repository containing one directory per mailbox, one `.eml` file
per message, and a small `.meta.yaml` sidecar next to each message:

```bash
cd /backups/mail

# What did my mailbox look like on the 1st of March?
git log --until 2026-03-01 -1
git checkout <commit>

# When did this message arrive, and where has it been?
git log --follow -- "Inbox/20260311-084512-d34db33fc4f3.eml"
```

See [Storage Layout](../advanced/storage-layout.md) for the full details of the on-disk
format.
