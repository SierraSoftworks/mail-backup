---
home: true

heroImage: /logo.svg
heroText: Mail Backup

actions:
    - text: Get Started
      link: /guide/

features:
    - title: Daily Snapshots
      details: |
        Your mailbox is stored as a local git repository with one commit per day of mail,
        letting you browse, diff, and restore the exact state of your mailbox at any point
        in the past.

    - title: Real-Time Streaming
      details: |
        Runs as a long-lived daemon which streams changes from your mail server the moment
        they happen, folding new mail, moves, flag changes, and deletions into the current
        day's snapshot.

    - title: Full-Fidelity Restore
      details: |
        Every message is stored as its raw RFC 5322 content alongside a metadata sidecar
        capturing folders, keywords, and read state — enough to restore your entire mailbox
        (or any historical snapshot of it) back to a JMAP server.
---

Mail Backup continuously archives your [Fastmail](https://www.fastmail.com) (or any other
[JMAP](https://jmap.io)) mailboxes into a local git repository. It backfills your mail history
one day at a time, then keeps streaming new changes in real time — and when the unthinkable
happens, it restores everything (folders, flags, and read state included) right back to your
mail server.

Backups are strictly read-only: the tool never modifies the state of the account it backs up.

## Example

```bash
# Run the daemon directly
./mail-backup run --config config.yaml

# Or run it in a container
docker run \
  -v $(pwd)/config.yaml:/config.yaml \
  -v $(pwd)/backups:/backups \
  ghcr.io/sierrasoftworks/mail-backup:latest \
  run --config /config.yaml
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
```
