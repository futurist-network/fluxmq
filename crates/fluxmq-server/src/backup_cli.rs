//! `fluxmq backup …` / `fluxmq restore …`: offline backup tools (no running broker needed).
//!
//! The target comes from flags or environment: `--fs-path`, or `--s3-bucket` with
//! `--s3-endpoint --s3-region --s3-prefix --s3-path-style` and credentials from
//! `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (or the matching flags). Secrets are never
//! printed.

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context as _, bail};
use clap::{Args, Subcommand};
use fluxmq_backup::Target;
use fluxmq_backup::crypto::Key;
use fluxmq_backup::restore::{RestoreOptions, restore};
use fluxmq_backup::service::{self, RunCtx};
use fluxmq_proto::backup::BackupTarget;

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// Backups on a target: list them, or back up a stopped broker's data directory.
    Backup {
        #[command(subcommand)]
        action: BackupAction,
    },
    /// Restore a backup into an empty data directory (then start `fluxmq --data-dir …`).
    Restore(RestoreArgs),
    /// TLS tools (v0.4): `fluxmq tls gen-dev --out <dir> [--nodes n1,n2] [--hosts …]`.
    Tls {
        #[command(subcommand)]
        action: crate::tls::TlsAction,
    },
}

#[derive(Debug, Clone, Subcommand)]
pub enum BackupAction {
    /// List backups on the target, newest first.
    List(ListArgs),
    /// Back up a data directory. The broker using it must be stopped (for a running broker
    /// use the console or `POST /api/backups/run`).
    Run(RunArgs),
}

/// Backup target (filesystem or S3-compatible).
#[derive(Debug, Clone, Args)]
pub struct TargetArgs {
    /// Filesystem target directory (instead of S3).
    #[arg(long, env = "FLUXMQ_BACKUP_FS_PATH", conflicts_with = "s3_bucket")]
    pub fs_path: Option<PathBuf>,
    /// S3 bucket.
    #[arg(long, env = "FLUXMQ_BACKUP_S3_BUCKET")]
    pub s3_bucket: Option<String>,
    /// Custom S3 endpoint (MinIO, Cloudflare R2, Backblaze B2, …); omit for AWS.
    #[arg(long, env = "FLUXMQ_BACKUP_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,
    /// S3 region (`auto` for R2).
    #[arg(long, env = "FLUXMQ_BACKUP_S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,
    /// Key prefix inside the bucket.
    #[arg(long, env = "FLUXMQ_BACKUP_S3_PREFIX", default_value = "")]
    pub s3_prefix: String,
    /// Path-style addressing (MinIO).
    #[arg(long, env = "FLUXMQ_BACKUP_S3_PATH_STYLE")]
    pub s3_path_style: bool,
    /// Access key id (default: `AWS_ACCESS_KEY_ID`).
    #[arg(long, env = "AWS_ACCESS_KEY_ID", hide_env_values = true)]
    pub s3_access_key_id: Option<String>,
    /// Secret access key (default: `AWS_SECRET_ACCESS_KEY`).
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY", hide_env_values = true)]
    pub s3_secret_access_key: Option<String>,
}

impl TargetArgs {
    pub fn target(&self) -> anyhow::Result<BackupTarget> {
        match (&self.fs_path, &self.s3_bucket) {
            (Some(p), None) => Ok(BackupTarget::Filesystem {
                path: p.display().to_string(),
            }),
            (None, Some(bucket)) => Ok(BackupTarget::S3 {
                endpoint: self.s3_endpoint.clone().filter(|e| !e.is_empty()),
                region: self.s3_region.clone(),
                bucket: bucket.clone(),
                prefix: self.s3_prefix.clone(),
                access_key_id: self.s3_access_key_id.clone().unwrap_or_default(),
                secret_access_key: self.s3_secret_access_key.clone().unwrap_or_default(),
                path_style: self.s3_path_style,
            }),
            (None, None) => bail!("no backup target: pass --fs-path <dir> or --s3-bucket <name>"),
            (Some(_), Some(_)) => bail!("pass either --fs-path or --s3-bucket, not both"),
        }
    }

    fn open(&self) -> anyhow::Result<Target> {
        let t = self.target()?;
        fluxmq_backup::config::validate_target(&t)?;
        Ok(Target::open(&t)?)
    }
}

#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    #[command(flatten)]
    pub target: TargetArgs,
    /// Print JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    #[command(flatten)]
    pub target: TargetArgs,
    /// Data directory of the (stopped) broker to back up.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// Encrypt with this base64 32-byte key (`openssl rand -base64 32`).
    #[arg(long, env = "FLUXMQ_BACKUP_ENCRYPTION_KEY", hide_env_values = true)]
    pub encryption_key: Option<String>,
    /// Metadata, offsets and DLQ only (no messages).
    #[arg(long)]
    pub no_messages: bool,
}

#[derive(Debug, Clone, Args)]
pub struct RestoreArgs {
    #[command(flatten)]
    pub target: TargetArgs,
    /// Backup id, or `latest`.
    #[arg(long)]
    pub backup: String,
    /// Empty (or missing) directory to restore into.
    #[arg(long)]
    pub data_dir: PathBuf,
    /// Key of an encrypted backup.
    #[arg(long, env = "FLUXMQ_BACKUP_ENCRYPTION_KEY", hide_env_values = true)]
    pub encryption_key: Option<String>,
}

fn human(bytes: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i + 1 < U.len() {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

fn rate(bytes: u64, secs: f64) -> String {
    format!("{}/s", human((bytes as f64 / secs.max(1e-6)) as u64))
}

fn key(arg: &Option<String>) -> anyhow::Result<Option<Key>> {
    Ok(arg
        .as_deref()
        .filter(|k| !k.is_empty())
        .map(Key::parse)
        .transpose()?)
}

/// Runs a subcommand to completion.
pub async fn run(cmd: Command) -> anyhow::Result<()> {
    match cmd {
        Command::Backup {
            action: BackupAction::List(a),
        } => list(a).await,
        Command::Backup {
            action: BackupAction::Run(a),
        } => backup(a).await,
        Command::Restore(a) => restore_cmd(a).await,
        Command::Tls { action } => crate::tls::run(action),
    }
}

async fn list(a: ListArgs) -> anyhow::Result<()> {
    let target = a.target.open()?;
    let ids = service::list_manifest_ids(&target).await?;
    let mut manifests = Vec::with_capacity(ids.len());
    for id in ids.iter().rev() {
        manifests.push(service::load_manifest(&target, id).await?);
    }
    if a.json {
        let infos: Vec<_> = manifests
            .iter()
            .map(|m| {
                serde_json::json!({
                    "info": m.info,
                    "encrypted": m.encryption.is_some(),
                    "include_messages": m.include_messages,
                    "node_id": m.node_id,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&infos)?);
        return Ok(());
    }
    if manifests.is_empty() {
        println!("no backups on {}", target.describe);
        return Ok(());
    }
    println!(
        "{:<28} {:<10} {:>8} {:>10} {:>10} {:>9} {:>9}  ENCRYPTED",
        "ID", "TRIGGER", "CHANNELS", "MESSAGES", "SIZE", "UPLOADED", "REUSED"
    );
    for m in &manifests {
        let i = &m.info;
        println!(
            "{:<28} {:<10} {:>8} {:>10} {:>10} {:>9} {:>9}  {}",
            i.id,
            i.trigger,
            i.channels,
            i.messages,
            human(i.total_bytes),
            i.segments_uploaded,
            i.segments_reused,
            if m.encryption.is_some() { "yes" } else { "no" }
        );
    }
    Ok(())
}

async fn backup(a: RunArgs) -> anyhow::Result<()> {
    if !a.data_dir.join("channels").is_dir() {
        bail!(
            "{} is not a FluxMQ data directory (no channels/ subdirectory)",
            a.data_dir.display()
        );
    }
    let target = a.target.open()?;
    let key = key(&a.encryption_key)?;
    let meta_path = a.data_dir.join("meta.json");
    let meta: Option<fluxmq_meta::SharedMetaStore> = if meta_path.exists() {
        let m = tokio::task::spawn_blocking(move || fluxmq_meta::LocalMetaStore::open(meta_path))
            .await??;
        Some(std::sync::Arc::new(m))
    } else {
        None
    };
    let broker = fluxmq_core::Broker::open(fluxmq_core::BrokerConfig::new(&a.data_dir))
        .await
        .with_context(|| format!("opening {}", a.data_dir.display()))?;
    let mut ctx = RunCtx::new(broker.clone(), target.clone(), "cli");
    ctx.meta = meta;
    ctx.key = key;
    ctx.include_messages = !a.no_messages;
    let started = Instant::now();
    let res = service::run_backup(&ctx).await;
    broker.shutdown().await;
    let info = res?;
    let secs = started.elapsed().as_secs_f64();
    println!(
        "backup {} written to {}: {} channel(s), {} partition(s), {} message(s), {} total \
         ({} uploaded, {} segment(s) uploaded, {} reused) in {secs:.2}s ({})",
        info.id,
        target.describe,
        info.channels,
        info.partitions,
        info.messages,
        human(info.total_bytes),
        human(info.bytes_uploaded),
        info.segments_uploaded,
        info.segments_reused,
        rate(info.total_bytes, secs),
    );
    Ok(())
}

async fn restore_cmd(a: RestoreArgs) -> anyhow::Result<()> {
    let target = a.target.open()?;
    let key = key(&a.encryption_key)?;
    let started = Instant::now();
    let sum = restore(
        &target,
        &a.backup,
        &a.data_dir,
        key.as_ref(),
        &RestoreOptions::default(),
        &|line| println!("{line}"),
    )
    .await
    .with_context(|| {
        format!(
            "restore failed; remove {} before retrying",
            a.data_dir.display()
        )
    })?;
    let secs = started.elapsed().as_secs_f64();
    println!(
        "restored backup {} into {} in {secs:.2}s ({}): {} channel(s), {} group(s), {} DLQ entr(ies), {} metadata key(s)\nstart the broker with: fluxmq --data-dir {}",
        sum.id,
        a.data_dir.display(),
        rate(sum.bytes, secs),
        sum.channels,
        sum.groups,
        sum.dlq_entries,
        sum.meta_keys,
        a.data_dir.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::Cli;

    #[test]
    fn subcommands_parse_and_plain_run_is_unchanged() {
        let cli = Cli::try_parse_from(["fluxmq", "--data-dir", "/x"]).unwrap();
        assert!(cli.command.is_none());
        let cli = Cli::try_parse_from([
            "fluxmq",
            "restore",
            "--fs-path",
            "/b",
            "--backup",
            "latest",
            "--data-dir",
            "/d",
        ])
        .unwrap();
        let Some(Command::Restore(r)) = cli.command else {
            panic!("restore expected")
        };
        assert_eq!(r.backup, "latest");
        assert!(matches!(
            r.target.target().unwrap(),
            BackupTarget::Filesystem { .. }
        ));
        let cli = Cli::try_parse_from([
            "fluxmq",
            "backup",
            "list",
            "--s3-bucket",
            "b",
            "--s3-endpoint",
            "http://127.0.0.1:9000",
            "--s3-path-style",
            "--s3-access-key-id",
            "AK",
            "--s3-secret-access-key",
            "SK",
        ])
        .unwrap();
        let Some(Command::Backup {
            action: BackupAction::List(l),
        }) = cli.command
        else {
            panic!("backup list expected")
        };
        let BackupTarget::S3 {
            path_style,
            secret_access_key,
            ..
        } = l.target.target().unwrap()
        else {
            panic!("s3 expected")
        };
        assert!(path_style);
        assert_eq!(secret_access_key, "SK");
        assert!(Cli::try_parse_from(["fluxmq", "restore", "--backup", "x"]).is_err());
    }
}
