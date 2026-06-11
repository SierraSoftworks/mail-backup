use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;

use crate::Filter;

/// A mail service which messages can be backed up from, and restored to.
///
/// Sources are written in your configuration file as YAML tagged values,
/// for example `!Fastmail { token: "..." }`. Credentials are part of the
/// source definition itself.
#[derive(Deserialize, Clone, PartialEq, Eq)]
pub enum SourceConfig {
    /// A Fastmail account, accessed using an API token created at
    /// https://app.fastmail.com/settings/security/tokens
    Fastmail {
        token: String,
        #[serde(default)]
        account: Option<String>,
    },
    /// Any other JMAP provider, accessed using a bearer token and the
    /// provider's base URL (the standard `/.well-known/jmap` session resource
    /// is resolved from it).
    Jmap {
        url: String,
        token: String,
        #[serde(default)]
        account: Option<String>,
    },
}

impl SourceConfig {
    /// The JMAP server base URL for this source. The session resource lives
    /// at `<base>/.well-known/jmap`; a configured URL which already includes
    /// that suffix is normalized.
    pub fn session_url(&self) -> String {
        let base = match self {
            SourceConfig::Fastmail { .. } => "https://api.fastmail.com",
            SourceConfig::Jmap { url, .. } => url,
        };
        base.trim_end_matches('/')
            .trim_end_matches("/.well-known/jmap")
            .trim_end_matches('/')
            .to_string()
    }

    pub fn token(&self) -> &str {
        match self {
            SourceConfig::Fastmail { token, .. } => token,
            SourceConfig::Jmap { token, .. } => token,
        }
    }

    pub fn account(&self) -> Option<&str> {
        match self {
            SourceConfig::Fastmail { account, .. } => account.as_deref(),
            SourceConfig::Jmap { account, .. } => account.as_deref(),
        }
    }
}

impl Display for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceConfig::Fastmail { account, .. } => match account {
                Some(account) => write!(f, "Fastmail({account})"),
                None => write!(f, "Fastmail"),
            },
            SourceConfig::Jmap { url, account, .. } => match account {
                Some(account) => write!(f, "Jmap({account} at {url})"),
                None => write!(f, "Jmap({url})"),
            },
        }
    }
}

impl Debug for SourceConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // NOTE: deliberately does not include the token
        Display::fmt(self, f)
    }
}

/// A local store which mail is backed up into, and restored from.
///
/// Stores are written in your configuration file as YAML tagged values,
/// for example `!LocalGit { path: "/backup/mail" }`.
#[derive(Deserialize, Clone, PartialEq, Eq)]
pub enum StoreConfig {
    /// A local git repository which receives one commit per day of mail,
    /// with the active day's commit being amended as new mail arrives.
    LocalGit {
        path: PathBuf,
        #[serde(default)]
        commit_name: Option<String>,
        #[serde(default)]
        commit_email: Option<String>,
    },
    /// A plain directory tree with no version history.
    LocalDir { path: PathBuf },
}

impl StoreConfig {
    pub fn path(&self) -> &PathBuf {
        match self {
            StoreConfig::LocalGit { path, .. } => path,
            StoreConfig::LocalDir { path } => path,
        }
    }
}

impl Display for StoreConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreConfig::LocalGit { path, .. } => write!(f, "LocalGit({})", path.display()),
            StoreConfig::LocalDir { path } => write!(f, "LocalDir({})", path.display()),
        }
    }
}

impl Debug for StoreConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

/// A policy describing a mail account which should be backed up into a local store.
#[derive(Deserialize)]
pub struct BackupPolicy {
    pub from: SourceConfig,
    pub to: StoreConfig,
    #[serde(default)]
    pub filter: Filter,
    /// The earliest day of mail (by receivedAt) to include when backfilling.
    #[serde(default)]
    pub backfill_start: Option<chrono::NaiveDate>,
}

impl Display for BackupPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.from, self.to)
    }
}

impl Debug for BackupPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.from, self.to)
    }
}

/// The strategy used to avoid creating duplicate messages when restoring
/// mail to a server which may already contain some of it.
#[derive(Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum DedupeMode {
    /// Skip messages whose Message-ID header already exists on the target server.
    #[default]
    MessageId,
    /// Import every selected message, even if it already exists on the target.
    None,
}

/// A policy describing a local store which can be restored to a mail account.
#[derive(Deserialize)]
pub struct RestorePolicy {
    pub from: StoreConfig,
    pub to: SourceConfig,
    #[serde(default)]
    pub filter: Filter,
    #[serde(default)]
    pub dedupe: DedupeMode,
    /// When set, restored mailboxes are created underneath a folder with this name
    /// rather than at the top level of the target account.
    #[serde(default)]
    pub mailbox_prefix: Option<String>,
}

impl Display for RestorePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.from, self.to)
    }
}

impl Debug for RestorePolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.from, self.to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn deserialize_backup_policy_fastmail() {
        let policy = r#"
          from: !Fastmail
            token: fmu1-secret
            account: user@example.com
          to: !LocalGit
            path: /backup/mail
            commit_name: mail-backup
          filter: message.mailbox == "INBOX"
          backfill_start: 2008-01-01
        "#;
        let policy: BackupPolicy = serde_yaml::from_str(policy).unwrap();
        assert_eq!(
            policy.from,
            SourceConfig::Fastmail {
                token: "fmu1-secret".to_string(),
                account: Some("user@example.com".to_string()),
            }
        );
        assert_eq!(policy.from.session_url(), "https://api.fastmail.com");
        assert_eq!(
            policy.to,
            StoreConfig::LocalGit {
                path: PathBuf::from("/backup/mail"),
                commit_name: Some("mail-backup".to_string()),
                commit_email: None,
            }
        );
        assert_eq!(policy.filter.raw(), "message.mailbox == \"INBOX\"");
        assert_eq!(
            policy.backfill_start,
            Some(chrono::NaiveDate::from_ymd_opt(2008, 1, 1).unwrap())
        );
        assert_eq!(
            format!("{}", policy),
            "Fastmail(user@example.com) -> LocalGit(/backup/mail)"
        );
    }

    #[test]
    fn deserialize_backup_policy_jmap_dir() {
        let policy = r#"
          from: !Jmap
            url: https://jmap.example.com/.well-known/jmap
            token: secret
          to: !LocalDir
            path: /backup/mail
        "#;
        let policy: BackupPolicy = serde_yaml::from_str(policy).unwrap();
        assert_eq!(policy.from.session_url(), "https://jmap.example.com");
        assert_eq!(policy.from.token(), "secret");
        assert_eq!(policy.from.account(), None);
        assert_eq!(policy.filter.raw(), "true");
        assert!(policy.backfill_start.is_none());
    }

    #[test]
    fn deserialize_restore_policy() {
        let policy = r#"
          from: !LocalGit
            path: /backup/mail
          to: !Fastmail
            token: fmu1-secret
          filter: message.received > "2026-01-01"
          dedupe: message-id
          mailbox_prefix: Restored
        "#;
        let policy: RestorePolicy = serde_yaml::from_str(policy).unwrap();
        assert_eq!(policy.dedupe, DedupeMode::MessageId);
        assert_eq!(policy.mailbox_prefix.as_deref(), Some("Restored"));
        assert_eq!(format!("{}", policy), "LocalGit(/backup/mail) -> Fastmail");
    }

    #[rstest]
    #[case("dedupe: message-id", DedupeMode::MessageId)]
    #[case("dedupe: none", DedupeMode::None)]
    #[case("", DedupeMode::MessageId)]
    fn deserialize_dedupe_mode(#[case] yaml: &str, #[case] expected: DedupeMode) {
        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(default)]
            dedupe: DedupeMode,
        }
        let wrapper: Wrapper = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(wrapper.dedupe, expected);
    }

    #[test]
    fn missing_required_fields_fail() {
        let result =
            serde_yaml::from_str::<BackupPolicy>("from: !Fastmail {}\nto: !LocalDir { path: /x }");
        assert!(result.is_err(), "missing token should fail to deserialize");

        let result =
            serde_yaml::from_str::<BackupPolicy>("from: !Fastmail { token: x }\nto: !LocalGit {}");
        assert!(result.is_err(), "missing path should fail to deserialize");
    }

    #[test]
    fn debug_does_not_leak_token() {
        let source: SourceConfig =
            serde_yaml::from_str("!Fastmail { token: super-secret }").unwrap();
        let debug = format!("{:?}", source);
        assert!(!debug.contains("super-secret"));
    }
}
