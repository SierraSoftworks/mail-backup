use serde::Deserialize;
use std::fmt::{Debug, Display, Formatter};
use std::path::PathBuf;
use url::Url;

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

/// HTTP cron-monitoring endpoints, pinged as a scheduled backup run reaches
/// each lifecycle state. Designed for services such as [Sentry Crons] or
/// [Healthchecks.io].
///
/// Each state has its own URL, so the same shape works both for services that
/// distinguish states with a query string (Sentry uses `?status=in_progress`,
/// `?status=ok` and `?status=error`) and for those that use a path suffix
/// (Healthchecks uses `/start` and `/fail`). Any state left unset is simply not
/// reported. Pings are best-effort: a failed or slow ping is logged but never
/// affects the backup itself.
///
/// [Sentry Crons]: https://docs.sentry.io/product/crons/
/// [Healthchecks.io]: https://healthchecks.io/
#[derive(Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct PingConfig {
    /// Pinged when a backup run begins.
    #[serde(default)]
    pub start: Option<Url>,
    /// Pinged when a backup run completes successfully.
    #[serde(default)]
    pub success: Option<Url>,
    /// Pinged when a backup run fails.
    #[serde(default)]
    pub fail: Option<Url>,
}

impl PingConfig {
    /// Whether at least one state has a URL configured.
    pub fn is_enabled(&self) -> bool {
        self.start.is_some() || self.success.is_some() || self.fail.is_some()
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
    /// HTTP cron-monitoring endpoints pinged as each scheduled backup run for
    /// this policy starts, succeeds, or fails.
    #[serde(default)]
    pub ping: PingConfig,
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
    fn deserialize_backup_policy_with_ping() {
        let policy = r#"
          from: !Fastmail { token: fmu1-secret }
          to: !LocalDir { path: /backup/mail }
          ping:
            start: https://sentry.io/api/0/cron/personal/key/?status=in_progress
            success: https://sentry.io/api/0/cron/personal/key/?status=ok
            fail: https://sentry.io/api/0/cron/personal/key/?status=error
        "#;
        let policy: BackupPolicy = serde_yaml::from_str(policy).unwrap();
        assert!(policy.ping.is_enabled());
        assert_eq!(
            policy.ping.start.as_ref().unwrap().as_str(),
            "https://sentry.io/api/0/cron/personal/key/?status=in_progress"
        );
        assert_eq!(
            policy.ping.success.as_ref().unwrap().as_str(),
            "https://sentry.io/api/0/cron/personal/key/?status=ok"
        );
        assert_eq!(
            policy.ping.fail.as_ref().unwrap().as_str(),
            "https://sentry.io/api/0/cron/personal/key/?status=error"
        );
    }

    #[test]
    fn ping_defaults_to_disabled() {
        let policy: BackupPolicy = serde_yaml::from_str(
            "from: !Fastmail { token: x }\nto: !LocalDir { path: /backup/mail }",
        )
        .unwrap();
        assert!(!policy.ping.is_enabled());
        assert_eq!(policy.ping, PingConfig::default());
    }

    #[test]
    fn deserialize_partial_ping() {
        let policy: BackupPolicy = serde_yaml::from_str(
            "from: !Fastmail { token: x }\nto: !LocalDir { path: /x }\nping:\n  fail: https://example.com/fail",
        )
        .unwrap();
        assert!(policy.ping.is_enabled());
        assert!(policy.ping.start.is_none());
        assert!(policy.ping.success.is_none());
        assert_eq!(
            policy.ping.fail.as_ref().unwrap().as_str(),
            "https://example.com/fail"
        );
    }

    #[test]
    fn invalid_ping_url_fails_to_deserialize() {
        let result = serde_yaml::from_str::<BackupPolicy>(
            "from: !Fastmail { token: x }\nto: !LocalDir { path: /x }\nping:\n  start: \"not a url\"",
        );
        assert!(result.is_err(), "an unparseable ping URL should fail");
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
