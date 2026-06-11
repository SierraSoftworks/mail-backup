use human_errors::ResultExt;
use serde::{Deserialize, Deserializer};
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::policy::{BackupPolicy, RestorePolicy};

#[derive(Deserialize)]
pub struct Config {
    #[serde(default, deserialize_with = "deserialize_cron")]
    pub schedule: Option<croner::Cron>,

    /// Backup policies, keyed by a human-friendly name which is used to
    /// select them on the command line and in log output.
    #[serde(default)]
    pub backups: BTreeMap<String, BackupPolicy>,

    /// Restore policies, keyed by name like `backups`.
    #[serde(default)]
    pub restores: BTreeMap<String, RestorePolicy>,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, human_errors::Error> {
        let content = std::fs::read_to_string(path).wrap_user_err(
            format!("Failed to read the config file {}.", path),
            &["Make sure that the configuration file exists and can be read by the process."],
        )?;
        let config: Config = serde_yaml::from_str(&content).wrap_user_err(
            "Failed to parse your configuration file, as it is not recognized as valid YAML.",
            &["Make sure that your configuration file is formatted correctly."],
        )?;
        Ok(config)
    }
}

fn deserialize_cron<'de, D>(deserializer: D) -> Result<Option<croner::Cron>, D::Error>
where
    D: Deserializer<'de>,
{
    if let Some(s) = Deserialize::deserialize(deserializer)? {
        let s: String = s;
        return croner::Cron::from_str(&s)
            .map_err(serde::de::Error::custom)
            .map(Some);
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case("0 0 * * *")]
    #[case("0 */5 * * *")]
    fn deserialize_cron(#[case] format: &str) {
        let config: Config = serde_yaml::from_str(&format!("schedule: {}", format)).unwrap();
        assert!(config.schedule.is_some());
    }

    #[test]
    fn deserialize_cron_not_provided() {
        let config: Config = serde_yaml::from_str("").unwrap();
        assert!(config.schedule.is_none());
        assert!(config.backups.is_empty());
        assert!(config.restores.is_empty());
    }

    #[test]
    fn deserialize_example_config() {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("config.yaml");

        let config =
            Config::load(&path.display().to_string()).expect("the example config should be valid");
        assert!(config.schedule.is_some());
        assert!(!config.backups.is_empty());
        assert!(!config.restores.is_empty());
        assert!(
            config.backups.keys().all(|name| !name.is_empty()),
            "policies are keyed by name"
        );
    }

    #[test]
    fn deserialize_named_policies() {
        let config: Config = serde_yaml::from_str(
            r#"
            backups:
              personal:
                from: !Fastmail { token: a }
                to: !LocalGit { path: /backup/a }
              work:
                from: !Fastmail { token: b }
                to: !LocalGit { path: /backup/b }
            restores:
              personal:
                from: !LocalGit { path: /backup/a }
                to: !Fastmail { token: a }
            "#,
        )
        .unwrap();

        assert_eq!(
            config.backups.keys().collect::<Vec<_>>(),
            vec!["personal", "work"]
        );
        assert!(config.restores.contains_key("personal"));
    }
}
