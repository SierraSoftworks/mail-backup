mod client;
mod entities;

pub use client::{MailClient, is_anchor_not_found, is_cannot_calculate_changes, retry};
pub use entities::{email_to_meta, mailbox_to_info};
