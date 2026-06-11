//! Conversions from jmap-client's wire types to the domain types used by the
//! rest of the application.

use crate::entities::mail::{MailboxInfo, MessageMeta};

/// Converts a JMAP Email object (fetched with the backup property set) into
/// our message metadata.
pub fn email_to_meta(
    email: &jmap_client::email::Email,
) -> Result<MessageMeta, human_errors::Error> {
    let id = email.id().unwrap_or_default().to_string();
    if id.is_empty() {
        return Err(human_errors::system(
            "The mail server returned a message without an id.",
            &["This is likely a bug in the mail server; please report it to us on GitHub."],
        ));
    }

    let received_at = email
        .received_at()
        .and_then(|secs| chrono::DateTime::from_timestamp(secs, 0))
        .ok_or_else(|| {
            human_errors::system(
                format!("The mail server returned message {id} without a valid receivedAt date."),
                &["This is likely a bug in the mail server; please report it to us on GitHub."],
            )
        })?;

    Ok(MessageMeta {
        blob_id: email.blob_id().unwrap_or_default().to_string(),
        thread_id: email.thread_id().unwrap_or_default().to_string(),
        mailbox_ids: email
            .mailbox_ids()
            .into_iter()
            .map(str::to_string)
            .collect(),
        keywords: email.keywords().into_iter().map(str::to_string).collect(),
        received_at,
        size: email.size() as u64,
        message_id: email
            .message_id()
            .map(|ids| ids.to_vec())
            .unwrap_or_default(),
        subject: email.subject().map(str::to_string),
        from: email
            .from()
            .map(|addresses| addresses.iter().map(|a| a.email().to_string()).collect())
            .unwrap_or_default(),
        id,
    })
}

/// Converts a JMAP Mailbox object into our mailbox metadata.
pub fn mailbox_to_info(mailbox: &jmap_client::mailbox::Mailbox) -> MailboxInfo {
    MailboxInfo {
        id: mailbox.id().unwrap_or_default().to_string(),
        name: mailbox.name().unwrap_or_default().to_string(),
        role: role_to_string(mailbox.role()),
        parent_id: mailbox.parent_id().map(str::to_string),
        sort_order: mailbox.sort_order(),
    }
}

fn role_to_string(role: jmap_client::mailbox::Role) -> Option<String> {
    use jmap_client::mailbox::Role;
    match role {
        Role::None => None,
        Role::Archive => Some("archive".to_string()),
        Role::Drafts => Some("drafts".to_string()),
        Role::Important => Some("important".to_string()),
        Role::Inbox => Some("inbox".to_string()),
        Role::Junk => Some("junk".to_string()),
        Role::Sent => Some("sent".to_string()),
        Role::Trash => Some("trash".to_string()),
        Role::Other(other) => Some(other.to_lowercase()),
    }
}
