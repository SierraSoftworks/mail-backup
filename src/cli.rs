use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// Backup your JMAP mailboxes automatically.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "config.yaml", global = true)]
    pub config: String,

    /// Plan and log actions without writing anything.
    #[arg(short, long, global = true)]
    pub dry_run: bool,

    /// The maximum number of concurrent message downloads/uploads.
    #[arg(long, default_value = "4", global = true)]
    pub concurrency: usize,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run as a daemon: backfill/catch-up, then stream live changes from the server.
    Run,

    /// Run a one-shot backup: backfill (if incomplete) and catch-up sync, then exit.
    Backup {
        /// Only run the backup policy with this name (default: all).
        #[arg(long)]
        policy: Option<String>,
    },

    /// Restore a local mail archive to a mail server.
    Restore {
        /// The name of the restore policy to run (default: the only configured one).
        #[arg(long)]
        policy: Option<String>,

        /// Restore the archive as it was at this commit or date (YYYY-MM-DD).
        #[arg(long)]
        at: Option<String>,

        /// Override the restore policy's filter expression.
        #[arg(long)]
        filter: Option<String>,

        /// Import messages even when they already exist on the target server.
        #[arg(long)]
        force: bool,
    },

    /// Validate the configuration and connectivity to each configured mail source.
    Check,

    /// Rebuild the local store index from the metadata sidecars on disk.
    Index {
        /// The path of the store to reindex (default: every backup policy's store).
        #[arg(long)]
        path: Option<PathBuf>,
    },

    /// Verify that the store's working tree matches its committed state.
    Verify {
        /// The path of the store to verify (default: every backup policy's store).
        #[arg(long)]
        path: Option<PathBuf>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_backup() {
        let cli = Cli::parse_from(["mail-backup", "backup", "--policy", "personal"]);
        assert!(
            matches!(cli.command, Command::Backup { policy: Some(name) } if name == "personal")
        );
        assert_eq!(cli.config, "config.yaml");
        assert!(!cli.dry_run);
    }

    #[test]
    fn parse_global_flags_after_subcommand() {
        let cli = Cli::parse_from(["mail-backup", "run", "--config", "custom.yaml", "--dry-run"]);
        assert!(matches!(cli.command, Command::Run));
        assert_eq!(cli.config, "custom.yaml");
        assert!(cli.dry_run);
    }

    #[test]
    fn parse_restore() {
        let cli = Cli::parse_from([
            "mail-backup",
            "restore",
            "--at",
            "2026-01-01",
            "--filter",
            "message.mailbox == \"INBOX\"",
            "--force",
        ]);
        match cli.command {
            Command::Restore {
                policy,
                at,
                filter,
                force,
            } => {
                assert_eq!(policy, None);
                assert_eq!(at.as_deref(), Some("2026-01-01"));
                assert_eq!(filter.as_deref(), Some("message.mailbox == \"INBOX\""));
                assert!(force);
            }
            _ => panic!("expected restore command"),
        }
    }
}
