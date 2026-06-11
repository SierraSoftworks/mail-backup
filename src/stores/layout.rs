//! Deterministic path rules for the on-disk mail store layout.
//!
//! Everything here is a pure function: re-processing the same server state
//! must always produce the same paths, since idempotent crash recovery and
//! git rename detection both depend on path stability.

use std::collections::BTreeSet;

use sha2::{Digest, Sha256};
use unicode_normalization::UnicodeNormalization;

use crate::entities::mail::{MailboxIndex, MailboxRecord, MessageMeta};

/// File name of the per-directory mailbox metadata file. The leading dot is
/// reserved for the store itself — sanitization prevents mailbox names from
/// ever colliding with it.
pub const MAILBOX_META_FILE: &str = ".mailbox.yaml";

/// Extension of message sidecar files, appended to the message basename.
pub const SIDECAR_SUFFIX: &str = ".meta.yaml";

/// Extension of raw message files.
pub const MESSAGE_SUFFIX: &str = ".eml";

/// The directory which receives messages whose mailboxes are not (yet) known
/// to the store. Messages move out of here as soon as an update reveals a
/// known mailbox.
pub const UNFILED_DIR: &str = "_unfiled";

/// Windows reserved device names which cannot be used as file or directory
/// names (case-insensitive, with or without an extension).
const RESERVED_NAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// The maximum byte length of a single sanitized path segment, leaving
/// headroom within Windows' path length limits.
const MAX_SEGMENT_BYTES: usize = 100;

fn short_hash(input: &str, len: usize) -> String {
    let digest = Sha256::digest(input.as_bytes());
    let mut out = String::with_capacity(len);
    for byte in digest.iter() {
        out.push_str(&format!("{byte:02x}"));
        if out.len() >= len {
            break;
        }
    }
    out.truncate(len);
    out
}

/// Sanitizes a single mailbox name into a Windows-safe directory name.
///
/// This is only used when a mailbox is first assigned a directory (or when it
/// is renamed on the server) — the result is persisted in the mailbox's
/// metadata file and never recomputed, so later changes to these rules cannot
/// move existing directories.
pub fn sanitize_segment(name: &str) -> String {
    let mut sanitized: String = name
        .nfc()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();

    // A leading dot is reserved for store metadata files (and would produce
    // hidden directories on Unix-like systems).
    if sanitized.starts_with('.') {
        sanitized.replace_range(..1, "_");
    }

    // Windows forbids trailing dots and spaces in file and directory names.
    while sanitized.ends_with('.') || sanitized.ends_with(' ') {
        sanitized.pop();
    }

    if sanitized.is_empty() {
        sanitized.push('_');
    }

    let stem = sanitized.split('.').next().unwrap_or(&sanitized);
    if RESERVED_NAMES
        .iter()
        .any(|r| r.eq_ignore_ascii_case(stem.trim_end()))
    {
        sanitized.insert(0, '_');
    }

    if sanitized.len() > MAX_SEGMENT_BYTES {
        let mut cutoff = MAX_SEGMENT_BYTES;
        while !sanitized.is_char_boundary(cutoff) {
            cutoff -= 1;
        }
        sanitized.truncate(cutoff);
        // Re-check the trailing character rule after truncation.
        while sanitized.ends_with('.') || sanitized.ends_with(' ') {
            sanitized.pop();
        }
        if sanitized.is_empty() {
            sanitized.push('_');
        }
    }

    sanitized
}

/// Assigns a directory name for a newly-seen mailbox, avoiding collisions
/// with sibling directories already assigned to other mailboxes.
///
/// `taken` holds the case-folded names of sibling directories which belong to
/// *other* mailboxes. When a collision occurs the name is suffixed with a
/// short hash of the mailbox's own (immutable) id, making the outcome
/// deterministic regardless of processing order for the colliding mailbox,
/// while the first arrival keeps the clean name.
pub fn assign_dir_name(name: &str, mailbox_id: &str, taken: &BTreeSet<String>) -> String {
    let sanitized = sanitize_segment(name);
    if taken.contains(&sanitized.to_lowercase()) {
        format!("{sanitized}~{}", short_hash(mailbox_id, 6))
    } else {
        sanitized
    }
}

/// The rank of a mailbox when selecting the primary mailbox a message's file
/// lives under. Lower ranks win; junk and trash rank last so that a message
/// which is also in a "real" mailbox never gets filed under them.
fn role_rank(role: Option<&str>) -> u8 {
    match role.map(|r| r.to_ascii_lowercase()).as_deref() {
        Some("inbox") => 0,
        Some("archive") => 1,
        Some("sent") => 2,
        Some("drafts") => 3,
        Some("junk") | Some("spam") => 6,
        Some("trash") => 7,
        Some(_) => 4,
        None => 5,
    }
}

/// Selects the primary mailbox for a message: the single directory its file
/// is stored under. This must be a pure, stable function of the message's
/// mailbox memberships and the mailbox index, so that re-processing a change
/// is idempotent.
pub fn primary_mailbox<'a>(
    mailbox_ids: &BTreeSet<String>,
    mailboxes: &'a MailboxIndex,
) -> Option<&'a MailboxRecord> {
    mailbox_ids
        .iter()
        .filter_map(|id| mailboxes.get(id))
        .min_by(|a, b| {
            role_rank(a.info.role.as_deref())
                .cmp(&role_rank(b.info.role.as_deref()))
                .then_with(|| a.dir_path.cmp(&b.dir_path))
                .then_with(|| a.info.id.cmp(&b.info.id))
        })
}

/// The basename (no directory, no extension) of a message file:
/// `<receivedAt UTC as yyyyMMdd-HHmmss>-<first 12 hex chars of sha256(raw)>`.
///
/// Both components are immutable properties of the message, so the basename
/// never changes when a message moves between mailboxes — which keeps the
/// file content byte-identical across moves and lets git detect them as
/// renames.
pub fn message_basename(meta: &MessageMeta, sha256_hex: &str) -> String {
    format!(
        "{}-{}",
        meta.received_at.format("%Y%m%d-%H%M%S"),
        &sha256_hex[..12.min(sha256_hex.len())]
    )
}

/// Resolves the repository-relative path of a message's `.eml` file, handling
/// the (rare) case where a *different* message already owns the natural name:
/// two distinct messages with identical content received in the same second
/// and stored in the same directory. The colliding message gets a suffix
/// derived from its own immutable id, so the outcome is deterministic.
pub fn message_path(
    dir_path: &str,
    meta: &MessageMeta,
    sha256_hex: &str,
    is_taken_by_other: impl Fn(&str) -> bool,
) -> String {
    let base = message_basename(meta, sha256_hex);
    let natural = format!("{dir_path}/{base}{MESSAGE_SUFFIX}");
    if is_taken_by_other(&natural) {
        format!(
            "{dir_path}/{base}~{}{MESSAGE_SUFFIX}",
            short_hash(&meta.id, 6)
        )
    } else {
        natural
    }
}

/// The sidecar path for a given message file path.
pub fn sidecar_path(message_path: &str) -> String {
    match message_path.strip_suffix(MESSAGE_SUFFIX) {
        Some(stem) => format!("{stem}{SIDECAR_SUFFIX}"),
        None => format!("{message_path}{SIDECAR_SUFFIX}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::mail::MailboxInfo;
    use rstest::rstest;

    #[rstest]
    #[case("INBOX", "INBOX")]
    #[case("Archive: 2023", "Archive_ 2023")]
    #[case("a/b\\c", "a_b_c")]
    #[case("what?*", "what__")]
    #[case("trailing.", "trailing")]
    #[case("trailing ", "trailing")]
    #[case("...", "_")]
    #[case("", "_")]
    #[case(".hidden", "_hidden")]
    #[case("CON", "_CON")]
    #[case("con", "_con")]
    #[case("COM7", "_COM7")]
    #[case("NUL.txt", "_NUL.txt")]
    #[case("console", "console")]
    #[case("Ünïcode 📬", "Ünïcode 📬")]
    fn sanitization(#[case] input: &str, #[case] expected: &str) {
        assert_eq!(sanitize_segment(input), expected);
    }

    #[test]
    fn sanitize_truncates_long_names() {
        let long = "x".repeat(500);
        let sanitized = sanitize_segment(&long);
        assert_eq!(sanitized.len(), MAX_SEGMENT_BYTES);
    }

    #[test]
    fn sanitize_truncates_on_char_boundary() {
        let long = "📬".repeat(100); // 4 bytes per char; 100 is not a boundary
        let sanitized = sanitize_segment(&long);
        assert!(sanitized.len() <= MAX_SEGMENT_BYTES);
        assert!(sanitized.chars().all(|c| c == '📬'));
    }

    #[test]
    fn sanitize_normalizes_unicode() {
        // "é" as combining sequence (U+0065 U+0301) vs precomposed (U+00E9)
        let decomposed = "Caf\u{0065}\u{0301}";
        let precomposed = "Caf\u{00E9}";
        assert_eq!(sanitize_segment(decomposed), sanitize_segment(precomposed));
    }

    #[test]
    fn assign_dir_name_avoids_collisions() {
        let mut taken = BTreeSet::new();
        assert_eq!(assign_dir_name("Inbox", "mb-1", &taken), "Inbox");

        taken.insert("inbox".to_string());
        let assigned = assign_dir_name("INBOX", "mb-2", &taken);
        assert!(assigned.starts_with("INBOX~"), "got: {assigned}");
        assert_eq!(assigned.len(), "INBOX~".len() + 6);

        // The suffix is a pure function of the mailbox's own id.
        assert_eq!(assigned, assign_dir_name("INBOX", "mb-2", &taken));
    }

    fn record(id: &str, role: Option<&str>, dir: &str) -> MailboxRecord {
        MailboxRecord {
            info: MailboxInfo {
                id: id.to_string(),
                name: dir.to_string(),
                role: role.map(str::to_string),
                parent_id: None,
                sort_order: 0,
            },
            dir_path: dir.to_string(),
        }
    }

    #[rstest]
    #[case(&[("mb-i", Some("inbox"), "Inbox"), ("mb-t", Some("trash"), "Trash")], "mb-i")]
    #[case(&[("mb-a", Some("archive"), "Archive"), ("mb-i", Some("inbox"), "Inbox")], "mb-i")]
    #[case(&[("mb-t", Some("trash"), "Trash"), ("mb-j", Some("junk"), "Junk")], "mb-j")]
    #[case(&[("mb-x", None, "Beta"), ("mb-y", None, "Alpha")], "mb-y")] // path tiebreak
    #[case(&[("mb-t", Some("trash"), "Trash")], "mb-t")] // only-trash still wins
    fn primary_mailbox_selection(
        #[case] boxes: &[(&str, Option<&str>, &str)],
        #[case] expected: &str,
    ) {
        let mut index = MailboxIndex::default();
        let mut ids = BTreeSet::new();
        for (id, role, dir) in boxes {
            index.insert(record(id, *role, dir));
            ids.insert(id.to_string());
        }
        let primary = primary_mailbox(&ids, &index).expect("a primary mailbox");
        assert_eq!(primary.info.id, expected);
    }

    #[test]
    fn primary_mailbox_ignores_unknown_ids() {
        let mut index = MailboxIndex::default();
        index.insert(record("mb-a", None, "Alpha"));
        let ids: BTreeSet<String> = ["mb-unknown", "mb-a"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(primary_mailbox(&ids, &index).unwrap().info.id, "mb-a");

        let only_unknown: BTreeSet<String> = ["mb-unknown".to_string()].into_iter().collect();
        assert!(primary_mailbox(&only_unknown, &index).is_none());
    }

    fn test_meta() -> MessageMeta {
        MessageMeta {
            id: "M123".to_string(),
            blob_id: "G1".to_string(),
            thread_id: "T1".to_string(),
            mailbox_ids: BTreeSet::new(),
            keywords: BTreeSet::new(),
            received_at: chrono::DateTime::parse_from_rfc3339("2023-06-15T10:30:05Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
            size: 1,
            message_id: vec![],
            subject: None,
            from: vec![],
        }
    }

    #[test]
    fn message_basename_format() {
        let sha = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert_eq!(
            message_basename(&test_meta(), sha),
            "20230615-103005-abcdef012345"
        );
    }

    #[test]
    fn message_path_collision_suffix_is_deterministic() {
        let sha = "abcdef0123456789";
        let clean = message_path("Inbox", &test_meta(), sha, |_| false);
        assert_eq!(clean, "Inbox/20230615-103005-abcdef012345.eml");

        let collided = message_path("Inbox", &test_meta(), sha, |p| p == clean);
        assert_ne!(collided, clean);
        assert!(collided.starts_with("Inbox/20230615-103005-abcdef012345~"));
        // Deterministic for the same message id.
        assert_eq!(
            collided,
            message_path("Inbox", &test_meta(), sha, |p| p == clean)
        );
    }

    #[test]
    fn sidecar_path_replaces_extension() {
        assert_eq!(
            sidecar_path("Inbox/20230615-103005-abcdef012345.eml"),
            "Inbox/20230615-103005-abcdef012345.meta.yaml"
        );
    }
}
