# Filters
Filters let you describe exactly which messages a backup (or restore) policy applies to,
using a small expression language with a rich understanding of your mail's metadata. The
same language — and the same properties — work in both directions: a backup filter decides
what gets archived, and a restore filter decides what gets re-imported.

```yaml
backups:
  personal:
    from: !Fastmail
      token: fmu1-xxxxxxxx
    to: !LocalGit
      path: /backups/mail
    filter: '!(message.keywords contains "$junk") && message.size < 26214400'
```

::: tip
A backup filter is a *should-exist* predicate: when a message changes such that it no
longer matches (e.g. it gains a keyword your filter excludes), it is removed from the
archive's current state — though, as ever with git, history retains it.
:::

## Properties

| Property | Type | Description |
|---|---|---|
| `message.id` | string | The server-assigned (JMAP) id of the message. |
| `message.thread` | string | The conversation thread id. |
| `message.blob` | string | The id of the message's raw content blob. |
| `message.mailbox` | string | The full name path of the message's primary mailbox, e.g. `"Archive/Receipts"`. |
| `message.mailboxes` | list | The name paths of *every* mailbox the message belongs to. |
| `message.keywords` | list | The message's keywords, e.g. `"$seen"`, `"$flagged"`, `"$draft"`, `"$answered"`, plus any custom keywords. (`message.keyword` is an alias.) |
| `message.received` | string | The time the message was received, RFC 3339 in UTC, e.g. `"2026-03-01T08:15:00Z"`. Lexicographic comparison is chronological. |
| `message.date` | string | The UTC day the message was received, e.g. `"2026-03-01"`. |
| `message.size` | number | The size of the raw message in bytes. |
| `message.subject` | string | The message's subject line. |
| `message.from` | list | The sender email addresses. |

## Syntax

| Feature | Syntax | Example |
|---|---|---|
| Equality | `==`, `!=` | `message.mailbox == "INBOX"` |
| Comparison | `>`, `<`, `>=`, `<=` | `message.size >= 1048576` |
| String matching | `startswith`, `endswith`, `contains` | `message.subject contains "invoice"` |
| Membership | `in` | `"$seen" in message.keywords` |
| Logic | `&&`, `\|\|`, `!` | `a && (b \|\| !c)` |
| Grouping | `( … )` | `(a \|\| b) && c` |
| Literals | numbers, strings, booleans, `null`, arrays | `["red", "blue"]` |

String comparisons are case-insensitive, so `message.mailbox == "inbox"` matches a
mailbox named `Inbox`.

## Examples

```yaml
# Everything except junk and trash
filter: '!(message.mailbox == "Spam" || message.mailbox == "Trash")'

# Only mail since 2024, excluding anything over 25 MiB
filter: 'message.received > "2024-01-01" && message.size < 26214400'

# Only the Inbox and everything under Archive
filter: 'message.mailbox == "INBOX" || message.mailbox startswith "Archive"'

# Only flagged messages from a particular sender
filter: 'message.keywords contains "$flagged" && message.from contains "alerts@example.com"'
```
