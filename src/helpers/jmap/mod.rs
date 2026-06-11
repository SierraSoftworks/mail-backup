mod client;
mod entities;

pub use client::{
    MailClient, is_anchor_not_found, is_cannot_calculate_changes, is_keepalive_artifact, retry,
};
pub use entities::{email_to_meta, mailbox_to_info};
