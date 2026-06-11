use clap::Parser;
use human_errors::Error;
use std::sync::atomic::AtomicBool;
use tracing_batteries::prelude::*;
use tracing_batteries::{OpenTelemetry, Session};

#[macro_use]
mod macros;

mod cli;
mod config;
mod engine;
mod entities;
mod errors;
mod filter;
pub(crate) mod helpers;
mod policy;
mod restore;
mod sources;
mod stores;
mod telemetry;

pub use filter::{Filter, FilterValue, Filterable};
pub use policy::{BackupPolicy, RestorePolicy, SourceConfig, StoreConfig};

use cli::{Cli, Command};
use sources::MailSource;
use stores::MailStore;

static CANCEL: AtomicBool = AtomicBool::new(false);

/// The state directory for a store rooted at the given path: inside `.git`
/// when the store is a git repository, alongside the mail otherwise.
fn state_dir_for_root(root: &std::path::Path) -> std::path::PathBuf {
    if root.join(".git").is_dir() {
        root.join(".git/mail-backup")
    } else {
        root.join(".mail-backup")
    }
}

/// Formats the configured policy names for inclusion in error messages.
fn available_policies<'a>(names: impl Iterator<Item = &'a String>) -> String {
    let names: Vec<&str> = names.map(String::as_str).collect();
    if names.is_empty() {
        " No policies are configured.".to_string()
    } else {
        format!(" Available policies: {}.", names.join(", "))
    }
}

async fn run(cli: Cli, session: &Session) -> Result<(), Error> {
    let config = config::Config::load(&cli.config)?;

    match cli.command {
        Command::Run => {
            let _page = session.record_new_page("/run");

            if config.backups.is_empty() {
                return Err(human_errors::user(
                    "There are no backup policies to run.",
                    &["Add at least one backup policy to your configuration."],
                ));
            }

            let options = engine::EngineOptions {
                dry_run: cli.dry_run,
                concurrency: cli.concurrency,
            };
            let stream_options = engine::stream::StreamOptions::default();

            // Each policy gets its own daemon; they run concurrently on this
            // task and all shut down together on Ctrl+C.
            let daemons = config.backups.iter().map(|(name, policy)| {
                let options = options.clone();
                let stream_options = stream_options.clone();
                let schedule = config.schedule.as_ref();
                async move {
                    let _span = info_span!("daemon.policy", policy = %name).entered();
                    let mut source = sources::jmap::JmapMailSource::from_config(&policy.from);
                    let mut store = stores::AnyStore::from_config(&policy.to);
                    engine::stream::run(
                        &mut source,
                        &mut store,
                        policy,
                        &options,
                        &stream_options,
                        schedule,
                        &CANCEL,
                    )
                    .await
                    .map_err(|e| (name.clone(), e))
                }
            });

            let results = futures::future::join_all(daemons).await;
            let mut failed = false;
            for result in results {
                if let Err((name, e)) = result {
                    failed = true;
                    error!("The daemon for '{}' failed: {}", name, e);
                    session.record_error(&e);
                }
            }

            if failed {
                Err(human_errors::system(
                    "One or more backup daemons terminated with an error.",
                    &["Check the log output above for the underlying problem."],
                ))
            } else {
                Ok(())
            }
        }
        Command::Backup { policy } => {
            let _page = session.record_new_page("/backup");

            let options = engine::EngineOptions {
                dry_run: cli.dry_run,
                concurrency: cli.concurrency,
            };

            let selected: Vec<(&String, &BackupPolicy)> = config
                .backups
                .iter()
                .filter(|(name, _)| policy.as_deref().is_none_or(|p| p == name.as_str()))
                .collect();

            if selected.is_empty() {
                return Err(match &policy {
                    Some(name) => human_errors::user(
                        format!(
                            "There is no backup policy named '{}'.{}",
                            name,
                            available_policies(config.backups.keys())
                        ),
                        &["Check the --policy name against your configuration file."],
                    ),
                    None => human_errors::user(
                        "There are no backup policies to run.",
                        &["Add at least one backup policy to your configuration."],
                    ),
                });
            }

            for (name, policy) in selected {
                let _span = info_span!("backup.policy", policy = %name).entered();
                info!("Backing up '{}' ({})", name, policy);

                let mut source = sources::jmap::JmapMailSource::from_config(&policy.from);
                let mut store = stores::AnyStore::from_config(&policy.to);
                let summary =
                    engine::run_backup(&mut source, &mut store, policy, &options, &CANCEL).await?;
                info!("Backup of '{}' complete: {}", name, summary);

                if CANCEL.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
            }
            Ok(())
        }
        Command::Restore {
            policy,
            at,
            filter,
            force,
        } => {
            let _page = session.record_new_page("/restore");

            let name = match policy {
                Some(name) => name,
                // With exactly one restore policy configured, it is the
                // obvious default; with several, an explicit choice is
                // required before we write to a mail account.
                None if config.restores.len() == 1 => config
                    .restores
                    .keys()
                    .next()
                    .expect("exactly one restore policy exists")
                    .clone(),
                None => {
                    return Err(human_errors::user(
                        format!(
                            "Several restore policies are configured; pick one with --policy.{}",
                            available_policies(config.restores.keys())
                        ),
                        &["Run the command again with --policy <name>."],
                    ));
                }
            };

            let policy = config.restores.get(&name).ok_or_else(|| {
                human_errors::user(
                    format!(
                        "There is no restore policy named '{}'.{}",
                        name,
                        available_policies(config.restores.keys())
                    ),
                    &["Check the --policy name against your configuration file."],
                )
            })?;

            let _span = info_span!("restore.policy", policy = %name).entered();
            info!("Restoring '{}' ({})", name, policy);

            let options = restore::RestoreOptions {
                at,
                filter,
                force,
                dry_run: cli.dry_run,
            };

            let mut target = restore::jmap::JmapRestoreTarget::from_config(&policy.to);
            let summary = restore::run_restore(&mut target, policy, &options, &CANCEL).await?;
            info!("{}", summary);

            if summary.failed > 0 {
                Err(human_errors::system(
                    format!("{} messages failed to import.", summary.failed),
                    &[
                        "Re-run the restore to retry them; messages which were already imported are skipped automatically.",
                    ],
                ))
            } else {
                Ok(())
            }
        }
        Command::Check => {
            let _page = session.record_new_page("/check");
            info!(
                "Loaded configuration with {} backup and {} restore policies",
                config.backups.len(),
                config.restores.len()
            );

            for (name, policy) in config.backups.iter() {
                let mut source = sources::jmap::JmapMailSource::from_config(&policy.from);
                match source.connect().await {
                    Ok(state) => {
                        let mailboxes = source.list_mailboxes().await?;
                        info!(
                            " - backup '{}' ({}): OK (account {}, {} mailboxes)",
                            name,
                            policy,
                            state.account_id,
                            mailboxes.len()
                        );
                    }
                    Err(e) => {
                        error!(" - backup '{}' ({}): FAILED", name, policy);
                        return Err(e);
                    }
                }
            }
            for (name, policy) in config.restores.iter() {
                info!(" - restore '{}' ({}): configuration OK", name, policy);
            }
            Ok(())
        }
        Command::Index { path } => {
            let _page = session.record_new_page("/index");

            let targets: Vec<(std::path::PathBuf, std::path::PathBuf)> = match path {
                Some(path) => vec![(path.clone(), state_dir_for_root(&path))],
                None => config
                    .backups
                    .values()
                    .map(|p| {
                        let root = p.to.path().clone();
                        let state_dir = match &p.to {
                            StoreConfig::LocalGit { path, .. } => path.join(".git/mail-backup"),
                            StoreConfig::LocalDir { path } => path.join(".mail-backup"),
                        };
                        (root, state_dir)
                    })
                    .collect(),
            };

            if targets.is_empty() {
                return Err(human_errors::user(
                    "There are no stores to reindex.",
                    &["Add a backup policy to your configuration, or pass --path explicitly."],
                ));
            }

            for (root, state_dir) in targets {
                let _span = info_span!("index.rebuild", store = %root.display()).entered();
                let mut store = stores::dir::DirMailStore::with_state_dir(root.clone(), state_dir);
                store.open().await?;
                store.rebuild_index()?;
                info!(
                    "Rebuilt the index for {}: {} mailboxes, {} messages",
                    root.display(),
                    store.mailboxes().len(),
                    store.list().count()
                );
            }
            Ok(())
        }
        Command::Verify { path } => {
            let _page = session.record_new_page("/verify");

            let targets: Vec<StoreConfig> = match path {
                Some(path) => {
                    if path.join(".git").is_dir() {
                        vec![StoreConfig::LocalGit {
                            path,
                            commit_name: None,
                            commit_email: None,
                        }]
                    } else {
                        vec![StoreConfig::LocalDir { path }]
                    }
                }
                None => config.backups.values().map(|p| p.to.clone()).collect(),
            };

            if targets.is_empty() {
                return Err(human_errors::user(
                    "There are no stores to verify.",
                    &["Add a backup policy to your configuration, or pass --path explicitly."],
                ));
            }

            let mut total_issues = 0;
            for target in targets {
                let _span = info_span!("verify.store", store = %target).entered();
                let issues = match stores::AnyStore::from_config(&target) {
                    stores::AnyStore::Git(mut store) => {
                        store.open().await?;
                        store.verify()?
                    }
                    stores::AnyStore::Dir(mut store) => {
                        store.open().await?;
                        store.verify()?
                    }
                };

                if issues.is_empty() {
                    info!("{}: consistent", target);
                } else {
                    total_issues += issues.len();
                    warn!("{}: {} inconsistencies found", target, issues.len());
                    for issue in issues {
                        warn!(" - {}", issue);
                    }
                }
            }

            if total_issues > 0 {
                Err(human_errors::user(
                    format!("Verification found {total_issues} inconsistencies."),
                    &[
                        "Run `mail-backup index` to rebuild the store index, or `mail-backup backup` to reconcile against the server.",
                    ],
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[tokio::main]
async fn main() {
    ctrlc::set_handler(|| {
        CANCEL.store(true, std::sync::atomic::Ordering::Relaxed);
        warn!("Received SIGINT, shutting down...");
    })
    .unwrap_or_default();

    let cli = Cli::parse();

    let session = Session::new("mail-backup", version!()).with_battery(OpenTelemetry::new(""));

    let result = run(cli, &session).await;

    if let Err(e) = result {
        session.record_error(&e);
        error!("{}", human_errors::pretty(&e));
        session.shutdown();
        std::process::exit(1);
    } else {
        session.shutdown();
    }
}
