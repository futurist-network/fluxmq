//! [`BackupService`]: configuration, scheduler, manual runs, progress, history, retention and
//! garbage collection. The backup procedure itself is [`run_backup`] (also used by the CLI).
//!
//! * Config lives in the meta store at [`crate::CONFIG_KEY`] (secrets stored as given,
//!   redacted on read — see [`crate::config`]).
//! * One run at a time (manual runs while one is in progress get `Conflict`).
//! * Scheduler: every `interval_secs` after the previous run started (first run
//!   `startup_delay` after start when no earlier backup exists). In a cluster only the
//!   controller schedules and runs backups ([`crate::cluster`]: every node uploads the
//!   partitions it leads, the controller writes the manifest); a cluster with a node older
//!   than 0.3.0 refuses them ([`BackupError::UnsupportedInCluster`]).
//! * After a successful run: retention (`keep_last`, `keep_days`; the newest backup is always
//!   kept) and garbage collection of objects no remaining manifest references.
//! * History = manifests on the target (newest first), plus the in-memory running / last
//!   failed run.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fluxmq_core::Broker;
use fluxmq_core::backup::ChannelCut;
use fluxmq_meta::{MetaStoreExt, SharedMetaStore};
use fluxmq_proto::backup::{BackupConfig, BackupInfo, BackupOverview, BackupStatus, BackupTarget};
use fluxmq_proto::notify::TestResult;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStoreExt;
use parking_lot::Mutex;
use tokio::sync::{Notify, broadcast};
use tracing::{info, warn};

use crate::cluster::{ClusterHook, Partial, PartialChannel, PartialRequest};
use crate::config;
use crate::crypto::Key;
use crate::manifest::{
    self, ChannelEntry, ChannelState, FORMAT, Manifest, MetaSnapshot, ObjectRef, PartitionEntry,
    SegmentEntry,
};
use crate::objects::{self, Source};
use crate::store::Target;
use crate::{BackupError, CONFIG_KEY, Result, join_err, now_ms};

/// Segments uploaded concurrently.
const SEGMENT_CONCURRENCY: usize = 3;

#[derive(Clone)]
pub struct ServiceOptions {
    /// Clustered node: backups are coordinated by the controller through this hook.
    pub cluster: Option<Arc<dyn ClusterHook>>,
    pub cluster_name: String,
    /// Delay of the first scheduled run when the target holds no backup yet.
    pub startup_delay: Duration,
    /// Scheduler tick.
    pub tick: Duration,
    /// Run the scheduler task (false: manual runs only — tests, `AppState::new`).
    pub scheduler: bool,
}

impl Default for ServiceOptions {
    fn default() -> Self {
        Self {
            cluster: None,
            cluster_name: "local".into(),
            startup_delay: Duration::from_secs(60),
            tick: Duration::from_secs(5),
            scheduler: true,
        }
    }
}

/// Lifecycle notifications (the server records them in the audit log).
#[derive(Debug, Clone, PartialEq)]
pub enum BackupEvent {
    Started(BackupInfo),
    Succeeded(BackupInfo),
    Failed(BackupInfo),
    /// Removed by retention (`retention = true`) or deleted through the API.
    Deleted {
        id: String,
        retention: bool,
    },
}

#[derive(Default)]
struct State {
    running: Option<Arc<Mutex<BackupInfo>>>,
    last_success: Option<BackupInfo>,
    last_failure: Option<BackupInfo>,
    next_run_ms: Option<u64>,
    last_started_ms: Option<u64>,
    /// Whether `last_started_ms` was initialized from the target's newest manifest.
    history_probed: bool,
}

/// Cache key of a segment prefix CRC: (channel, channel created_at, partition, base, len).
type CrcKey = (String, u64, u32, u64, u64);

struct Inner {
    broker: Broker,
    meta: SharedMetaStore,
    opts: ServiceOptions,
    started_ms: u64,
    state: Mutex<State>,
    events: broadcast::Sender<BackupEvent>,
    crc_cache: Arc<Mutex<HashMap<CrcKey, u32>>>,
    /// Serializes runs, retention and deletes (async mutex: held across awaits).
    run_lock: Arc<tokio::sync::Mutex<()>>,
    /// Manifests are immutable: cached per (target, id).
    manifests: Mutex<HashMap<(String, String), Arc<Manifest>>>,
    stop: Arc<Notify>,
    stopped: AtomicBool,
}

/// Cheap to clone.
#[derive(Clone)]
pub struct BackupService {
    inner: Arc<Inner>,
}

impl BackupService {
    /// Starts the service (and its scheduler unless `opts.scheduler` is false). Must be called
    /// inside a Tokio runtime.
    pub fn start(broker: Broker, meta: SharedMetaStore, opts: ServiceOptions) -> BackupService {
        let svc = BackupService {
            inner: Arc::new(Inner {
                broker,
                meta,
                started_ms: now_ms(),
                state: Mutex::new(State::default()),
                events: broadcast::channel(256).0,
                crc_cache: Arc::new(Mutex::new(HashMap::new())),
                run_lock: Arc::new(tokio::sync::Mutex::new(())),
                manifests: Mutex::new(HashMap::new()),
                stop: Arc::new(Notify::new()),
                stopped: AtomicBool::new(false),
                opts,
            }),
        };
        if svc.inner.opts.scheduler {
            let weak = Arc::downgrade(&svc.inner);
            let tick = svc.inner.opts.tick;
            tokio::spawn(async move {
                loop {
                    let Some(inner) = weak.upgrade() else { break };
                    if inner.stopped.load(Ordering::Acquire) {
                        break;
                    }
                    let notify = inner.stop.clone();
                    let stop = notify.notified();
                    tokio::pin!(stop);
                    stop.as_mut().enable();
                    BackupService { inner }.scheduler_tick().await;
                    tokio::select! {
                        () = tokio::time::sleep(tick) => {}
                        () = &mut stop => break,
                    }
                }
            });
        }
        svc
    }

    /// Memory-only service with no scheduler (`AppState::new` defaults, tests).
    pub fn disabled(broker: Broker) -> BackupService {
        Self::start(
            broker,
            Arc::new(fluxmq_meta::LocalMetaStore::in_memory()),
            ServiceOptions {
                scheduler: false,
                ..ServiceOptions::default()
            },
        )
    }

    /// Stops the scheduler (a running backup finishes in the background).
    pub fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::Release);
        self.inner.stop.notify_waiters();
    }

    pub fn events(&self) -> broadcast::Receiver<BackupEvent> {
        self.inner.events.subscribe()
    }

    /// Stored configuration with secrets (internal use only — never return it from the API).
    pub fn config(&self) -> Result<Option<BackupConfig>> {
        Ok(self.inner.meta.get_as::<BackupConfig>(CONFIG_KEY)?)
    }

    /// Configuration safe to show (`None` = not configured).
    pub fn config_redacted(&self) -> Result<Option<BackupConfig>> {
        Ok(self.config()?.map(|c| config::redact(&c)))
    }

    /// Validates and stores `new` (secret rule applied); returns the redacted result.
    pub async fn put_config(&self, new: BackupConfig) -> Result<BackupConfig> {
        let stored = self.config()?;
        let merged = config::merge(new, stored.as_ref())?;
        config::validate(&merged)?;
        let rev = self.inner.meta.put_as(CONFIG_KEY, &merged).await?;
        self.inner.meta.wait_for(rev).await?;
        {
            let mut st = self.inner.state.lock();
            // Recompute the schedule against the (possibly new) target and interval.
            st.history_probed = false;
            st.next_run_ms = None;
        }
        Ok(config::redact(&merged))
    }

    /// Write + read + delete probe against `target` (redacted secrets resolve to the stored
    /// configuration's).
    pub async fn test_target(&self, target: BackupTarget) -> TestResult {
        let resolved = self.config().map_err(|e| e.to_string()).and_then(|stored| {
            config::merge_target(target, stored.as_ref().map(|c| &c.target))
                .and_then(|t| config::validate_target(&t).map(|()| t))
                .map_err(|e| e.to_string())
        });
        match resolved.and_then(|t| Target::open(&t).map_err(|e| e.to_string())) {
            Ok(t) => t.probe().await,
            Err(e) => TestResult {
                ok: false,
                latency_ms: 0,
                error: Some(e),
                error_detail: None,
            },
        }
    }

    pub fn overview(&self) -> BackupOverview {
        let cfg = self.config().ok().flatten();
        let st = self.inner.state.lock();
        BackupOverview {
            configured: cfg.is_some(),
            enabled: cfg.as_ref().is_some_and(|c| c.enabled),
            running: st.running.as_ref().map(|r| r.lock().clone()),
            last_success: st.last_success.clone(),
            last_failure: st.last_failure.clone(),
            next_run_ms: st.next_run_ms,
        }
    }

    fn configured(&self) -> Result<(BackupConfig, Target, Option<Key>)> {
        let cfg = self.config()?.ok_or(BackupError::NotConfigured)?;
        let target = Target::open(&cfg.target)?;
        let key = cfg.encryption_key.as_deref().map(Key::parse).transpose()?;
        Ok((cfg, target, key))
    }

    /// Starts a backup in the background; returns its initial info. `Conflict` while another
    /// run is in progress.
    pub fn run(&self, trigger: &str) -> Result<BackupInfo> {
        let (cell, task) = self.begin(trigger)?;
        tokio::spawn(task);
        let info = cell.lock().clone();
        Ok(info)
    }

    /// Runs a backup to completion (tests, embedded use). Returns the final info; a failed
    /// run is `Ok` with `status = Failed`.
    pub async fn run_and_wait(&self, trigger: &str) -> Result<BackupInfo> {
        let (_, task) = self.begin(trigger)?;
        tokio::spawn(task).await.map_err(join_err)
    }

    fn begin(
        &self,
        trigger: &str,
    ) -> Result<(
        Arc<Mutex<BackupInfo>>,
        impl std::future::Future<Output = BackupInfo> + Send + 'static,
    )> {
        if let Some(hook) = &self.inner.opts.cluster {
            if let Some(reason) = hook.unsupported() {
                return Err(BackupError::UnsupportedInCluster(reason));
            }
            if !hook.is_controller() {
                return Err(BackupError::NotController(
                    "backups of a cluster run on the controller".into(),
                ));
            }
        }
        let (cfg, target, key) = self.configured()?;
        let guard = self
            .inner
            .run_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| BackupError::Conflict("a backup is already running".into()))?;
        let started = now_ms();
        let info = fresh_info(manifest::new_backup_id(started), trigger, started);
        let cell = Arc::new(Mutex::new(info.clone()));
        {
            let mut st = self.inner.state.lock();
            st.running = Some(cell.clone());
            st.last_started_ms = Some(started);
            st.next_run_ms = Some(started + u64::from(cfg.interval_secs) * 1000);
        }
        let _ = self.inner.events.send(BackupEvent::Started(info.clone()));
        info!(id = %info.id, trigger, target = %target.describe, "backup started");
        let svc = self.clone();
        let cell2 = cell.clone();
        let task = async move {
            let _guard = guard;
            let ctx = RunCtx {
                broker: svc.inner.broker.clone(),
                meta: Some(svc.inner.meta.clone()),
                target: target.clone(),
                key,
                include_messages: cfg.include_messages,
                cluster_name: svc.inner.opts.cluster_name.clone(),
                info: cell2.clone(),
                crc_cache: svc.inner.crc_cache.clone(),
            };
            let res = match svc.inner.opts.cluster.clone() {
                None => run_backup(&ctx).await,
                Some(hook) => run_cluster_backup(&ctx, hook.as_ref()).await,
            };
            let mut fin = cell2.lock().clone();
            fin.finished_at_ms = Some(now_ms());
            match res {
                Ok(done) => {
                    fin = done;
                    info!(
                        id = %fin.id,
                        uploaded = fin.bytes_uploaded,
                        reused = fin.segments_reused,
                        "backup succeeded"
                    );
                    let _ = svc.inner.events.send(BackupEvent::Succeeded(fin.clone()));
                    {
                        let mut st = svc.inner.state.lock();
                        st.running = None;
                        st.last_success = Some(fin.clone());
                    }
                    if let Err(e) = svc
                        .retention_locked(&target, cfg.keep_last, cfg.keep_days)
                        .await
                    {
                        warn!(error = %e, "backup retention / garbage collection failed");
                    }
                }
                Err(e) => {
                    fin.status = BackupStatus::Failed;
                    fin.error = Some(e.to_string());
                    warn!(id = %fin.id, error = %e, "backup failed");
                    let _ = svc.inner.events.send(BackupEvent::Failed(fin.clone()));
                    let mut st = svc.inner.state.lock();
                    st.running = None;
                    st.last_failure = Some(fin.clone());
                }
            }
            fin
        };
        Ok((cell, task))
    }

    async fn scheduler_tick(&self) {
        if let Some(hook) = &self.inner.opts.cluster
            && !hook.is_controller()
        {
            // Another node schedules; re-read the history if this node becomes controller.
            let mut st = self.inner.state.lock();
            st.next_run_ms = None;
            st.history_probed = false;
            return;
        }
        let cfg = match self.config() {
            Ok(Some(c)) if c.enabled => c,
            _ => {
                self.inner.state.lock().next_run_ms = None;
                return;
            }
        };
        let probe = !self.inner.state.lock().history_probed;
        if probe {
            // Continue the schedule of an earlier process from the newest backup.
            let newest = match Target::open(&cfg.target) {
                Ok(t) => list_manifest_ids(&t)
                    .await
                    .ok()
                    .and_then(|ids| ids.last().and_then(|id| started_ms_from_id(id))),
                Err(_) => None,
            };
            let mut st = self.inner.state.lock();
            st.history_probed = true;
            if st.last_started_ms.is_none() {
                st.last_started_ms = newest;
            }
        }
        let now = now_ms();
        let due = {
            let mut st = self.inner.state.lock();
            let next = match st.last_started_ms {
                Some(t) => t + u64::from(cfg.interval_secs) * 1000,
                None => self.inner.started_ms + self.inner.opts.startup_delay.as_millis() as u64,
            };
            st.next_run_ms = Some(next);
            now >= next && st.running.is_none()
        };
        if due {
            match self.run("scheduled") {
                Ok(_) | Err(BackupError::Conflict(_)) => {}
                Err(e) => {
                    warn!(error = %e, "scheduled backup could not start");
                    // Don't retry every tick: wait one interval.
                    self.inner.state.lock().last_started_ms = Some(now);
                }
            }
        }
    }

    async fn manifest(&self, target: &Target, id: &str) -> Result<Arc<Manifest>> {
        let ck = (target.describe.clone(), id.to_string());
        if let Some(m) = self.inner.manifests.lock().get(&ck) {
            return Ok(m.clone());
        }
        let m = Arc::new(load_manifest(target, id).await?);
        self.inner.manifests.lock().insert(ck, m.clone());
        Ok(m)
    }

    /// History, newest first: the running backup and the last failure (if newer than the
    /// newest manifest), then every manifest on the target.
    pub async fn list(&self) -> Result<Vec<BackupInfo>> {
        let (_, target, _) = self.configured()?;
        let ids = list_manifest_ids(&target).await?;
        let mut out = Vec::with_capacity(ids.len() + 2);
        {
            let st = self.inner.state.lock();
            if let Some(r) = &st.running {
                out.push(r.lock().clone());
            }
            if let Some(f) = &st.last_failure
                && ids
                    .last()
                    .is_none_or(|newest| f.id.as_str() > newest.as_str())
            {
                out.push(f.clone());
            }
        }
        let futs: Vec<_> = ids
            .iter()
            .rev()
            .cloned()
            .map(|id| {
                let (svc, t) = (self.clone(), target.clone());
                async move { svc.manifest(&t, &id).await }
            })
            .collect();
        let manifests = futures::stream::iter(futs)
            .buffered(8)
            .collect::<Vec<_>>()
            .await;
        for m in manifests {
            match m {
                Ok(m) => out.push(m.info.clone()),
                Err(e) => warn!(error = %e, "unreadable backup manifest skipped"),
            }
        }
        Ok(out)
    }

    /// The manifest of backup `id` (`latest` = newest).
    pub async fn get(&self, id: &str) -> Result<Arc<Manifest>> {
        let (_, target, _) = self.configured()?;
        let id = resolve_id(&target, id).await?;
        self.manifest(&target, &id).await
    }

    /// Deletes backup `id` and garbage-collects objects no other backup references.
    pub async fn delete(&self, id: &str) -> Result<()> {
        if !manifest::valid_backup_id(id) {
            return Err(BackupError::NotFound(format!("backup '{id}'")));
        }
        let (_, target, _) = self.configured()?;
        let _guard = self
            .inner
            .run_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| BackupError::Conflict("a backup is running; retry later".into()))?;
        let ids = list_manifest_ids(&target).await?;
        if !ids.iter().any(|i| i == id) {
            return Err(BackupError::NotFound(format!("backup '{id}'")));
        }
        target
            .store
            .delete(&target.path(&manifest::manifest_key(id)))
            .await?;
        self.inner
            .manifests
            .lock()
            .remove(&(target.describe.clone(), id.to_string()));
        let _ = self.inner.events.send(BackupEvent::Deleted {
            id: id.to_string(),
            retention: false,
        });
        gc(&target).await?;
        Ok(())
    }

    /// A node's share of a cluster backup (`POST /internal/v1/backup/partial`): cuts and
    /// uploads the partitions this node leads. Refuses when this node's configuration doesn't
    /// match the controller's (target / key fingerprint not replicated yet).
    pub async fn partial(&self, req: PartialRequest) -> Result<Partial> {
        if !manifest::valid_backup_id(&req.id) {
            return Err(BackupError::Invalid(format!("backup id '{}'", req.id)));
        }
        let (_, target, key) = self.configured()?;
        let fp = key.as_ref().map(|k| k.fingerprint().to_string());
        if target.describe != req.target || fp != req.fingerprint {
            return Err(BackupError::Conflict(format!(
                "backup configuration on node {} differs from the controller's (not replicated \
                 yet?); retry",
                self.inner.broker.node_id()
            )));
        }
        let ctx = RunCtx {
            broker: self.inner.broker.clone(),
            meta: None,
            target,
            key,
            include_messages: req.include_messages,
            cluster_name: self.inner.opts.cluster_name.clone(),
            info: Arc::new(Mutex::new(fresh_info(req.id.clone(), "cluster", now_ms()))),
            crc_cache: self.inner.crc_cache.clone(),
        };
        info!(id = %req.id, "uploading led partitions for a cluster backup");
        cut_and_upload(&ctx).await
    }

    /// Retention + GC; the caller holds the run lock.
    async fn retention_locked(
        &self,
        target: &Target,
        keep_last: u32,
        keep_days: u32,
    ) -> Result<()> {
        let deleted = apply_retention(target, keep_last, keep_days, now_ms()).await?;
        for id in &deleted {
            self.inner
                .manifests
                .lock()
                .remove(&(target.describe.clone(), id.clone()));
            let _ = self.inner.events.send(BackupEvent::Deleted {
                id: id.clone(),
                retention: true,
            });
        }
        let (n, bytes) = gc(target).await?;
        if !deleted.is_empty() || n > 0 {
            info!(
                backups = deleted.len(),
                objects = n,
                bytes,
                "backup retention removed old backups"
            );
        }
        Ok(())
    }
}

/// Everything one backup run needs.
pub struct RunCtx {
    pub broker: Broker,
    /// Meta store to snapshot (`None`: no metadata, e.g. a bare broker in tests).
    pub meta: Option<SharedMetaStore>,
    pub target: Target,
    pub key: Option<Key>,
    pub include_messages: bool,
    pub cluster_name: String,
    /// Live progress (id, trigger and start time preset by the caller).
    pub info: Arc<Mutex<BackupInfo>>,
    /// Segment-prefix CRC cache shared across runs (avoids re-reading sealed segments).
    pub crc_cache: Arc<Mutex<HashMap<CrcKey, u32>>>,
}

impl RunCtx {
    /// A context for one-off runs (CLI, tests).
    pub fn new(broker: Broker, target: Target, trigger: &str) -> RunCtx {
        let started = now_ms();
        RunCtx {
            broker,
            meta: None,
            target,
            key: None,
            include_messages: true,
            cluster_name: "local".into(),
            info: Arc::new(Mutex::new(fresh_info(
                manifest::new_backup_id(started),
                trigger,
                started,
            ))),
            crc_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn update(&self, f: impl FnOnce(&mut BackupInfo)) {
        f(&mut self.info.lock());
    }
}

/// Info of a run that just started.
fn fresh_info(id: String, trigger: &str, started: u64) -> BackupInfo {
    BackupInfo {
        id,
        status: BackupStatus::Running,
        started_at_ms: started,
        finished_at_ms: None,
        trigger: trigger.to_string(),
        channels: 0,
        partitions: 0,
        messages: 0,
        bytes_uploaded: 0,
        segments_uploaded: 0,
        segments_reused: 0,
        total_bytes: 0,
        progress_bytes: 0,
        error: None,
    }
}

struct SegJob {
    channel: String,
    created_at_ms: u64,
    partition: u32,
    cut: Arc<fluxmq_core::backup::SegmentCut>,
    /// Position in the manifest: (channel index, partition index, segment index).
    slot: (usize, usize, usize),
}

/// Takes the cuts, uploads metadata, state and segments, writes the manifest. Returns the
/// finished info (`Succeeded`). Objects of a failed run are left for the next GC.
pub async fn run_backup(ctx: &RunCtx) -> Result<BackupInfo> {
    let partial = cut_and_upload(ctx).await?;
    finish(ctx, None, vec![partial]).await
}

/// Cuts every local channel (in replicated mode only the partitions this node leads — see
/// `Broker::backup_cut`) and uploads their segments. Group offsets and DLQ are returned, not
/// uploaded: [`finish`] merges them (cluster: from every node) into the state objects.
/// Progress and upload counters are added to `ctx.info` as it goes.
pub async fn cut_and_upload(ctx: &RunCtx) -> Result<Partial> {
    let fp = ctx.key.as_ref().map(|k| k.fingerprint().to_string());

    // 1. Cuts of every channel (pins the segments until uploaded).
    let mut cuts: Vec<ChannelCut> = Vec::new();
    for c in ctx.broker.channels() {
        let b = ctx.broker.clone();
        let name = c.name.clone();
        match tokio::task::spawn_blocking(move || b.backup_cut(&name))
            .await
            .map_err(join_err)?
        {
            Ok(cut) => cuts.push(cut),
            Err(fluxmq_core::CoreError::ChannelNotFound(_)) => {} // deleted meanwhile
            Err(e) => return Err(e.into()),
        }
    }

    // 2. Plan.
    let mut channels: Vec<PartialChannel> = Vec::with_capacity(cuts.len());
    let mut jobs: Vec<SegJob> = Vec::new();
    let mut seg_bytes = 0u64;
    let mut partitions = 0u32;
    let mut messages = 0u64;
    for (ci, cut) in cuts.into_iter().enumerate() {
        let ChannelCut {
            config,
            partitions: pcuts,
            group_offsets,
            dlq,
            ..
        } = cut;
        let mut pentries = Vec::with_capacity(pcuts.len());
        for (pi, pc) in pcuts.into_iter().enumerate() {
            partitions += 1;
            let mut pe = PartitionEntry {
                partition: pc.partition,
                low: pc.end,
                end: pc.end,
                segments: Vec::new(),
            };
            if ctx.include_messages {
                pe.low = pc.low;
                messages += pc.messages();
                for (si, seg) in pc.segments.into_iter().enumerate() {
                    seg_bytes += seg.len + seg.index.len() as u64;
                    jobs.push(SegJob {
                        channel: config.name.clone(),
                        created_at_ms: config.created_at_ms,
                        partition: pc.partition,
                        cut: Arc::new(seg),
                        slot: (ci, pi, si),
                    });
                }
            }
            pentries.push(pe);
        }
        channels.push(PartialChannel {
            config,
            partitions: pentries,
            group_offsets,
            dlq,
        });
    }
    ctx.update(|i| {
        i.partitions += partitions;
        i.messages += messages;
        i.total_bytes += seg_bytes;
    });
    let before = ctx.info.lock().clone();

    // 3. Segments, a few at a time; each pin is released as soon as its upload is done.
    let progress = |n: u64| ctx.update(|i| i.progress_bytes += n);
    let results: Vec<(usize, usize, usize, SegmentEntry)> = futures::stream::iter(jobs)
        .map(|job| upload_segment(ctx, job, fp.as_deref(), &progress))
        .buffer_unordered(SEGMENT_CONCURRENCY)
        .try_collect()
        .await?;
    let mut results = results;
    results.sort_by_key(|r| (r.0, r.1, r.2));
    for (ci, pi, _, seg) in results {
        channels[ci].partitions[pi].segments.push(seg);
    }
    let after = ctx.info.lock().clone();
    Ok(Partial {
        node_id: ctx.broker.node_id().to_string(),
        channels,
        messages,
        segment_bytes: seg_bytes,
        bytes_uploaded: after.bytes_uploaded - before.bytes_uploaded,
        segments_uploaded: after.segments_uploaded - before.segments_uploaded,
        segments_reused: after.segments_reused - before.segments_reused,
    })
}

/// Controller side of a cluster backup (see [`crate::cluster`]): local and remote partials
/// concurrently, then [`finish`] with the coverage check.
pub async fn run_cluster_backup(ctx: &RunCtx, hook: &dyn ClusterHook) -> Result<BackupInfo> {
    let expected = hook.expected();
    let req = PartialRequest {
        id: ctx.info.lock().id.clone(),
        include_messages: ctx.include_messages,
        target: ctx.target.describe.clone(),
        fingerprint: ctx.key.as_ref().map(|k| k.fingerprint().to_string()),
    };
    let remote = hook.remote_partials(req);
    let (local, remote) = tokio::join!(cut_and_upload(ctx), remote);
    let (local, remote) = (local?, remote?);
    for r in &remote {
        ctx.update(|i| {
            i.total_bytes += r.segment_bytes;
            i.progress_bytes += r.segment_bytes;
            i.bytes_uploaded += r.bytes_uploaded;
            i.segments_uploaded += r.segments_uploaded;
            i.segments_reused += r.segments_reused;
        });
    }
    let mut all = Vec::with_capacity(remote.len() + 1);
    all.push(local);
    all.extend(remote);
    finish(ctx, Some(&expected), all).await
}

/// Merges the partials (one per node; a single one on a single node), checks partition
/// coverage against `expected` (channel → partition count; `None` = no check), uploads the
/// metadata snapshot and per-channel state objects, and writes the manifest last.
pub async fn finish(
    ctx: &RunCtx,
    expected: Option<&BTreeMap<String, u32>>,
    partials: Vec<Partial>,
) -> Result<BackupInfo> {
    let id = ctx.info.lock().id.clone();
    let fp = ctx.key.as_ref().map(|k| k.fingerprint().to_string());
    let key = ctx.key.as_ref();
    let mut nodes: Vec<String> = partials.iter().map(|p| p.node_id.clone()).collect();
    nodes.sort();
    nodes.dedup();
    let merged = crate::cluster::merge(partials, expected)?;

    // Metadata snapshot (secrets of the backup config itself are never included).
    let meta_snap = match &ctx.meta {
        Some(m) => MetaSnapshot {
            revision: m.revision(),
            entries: m
                .list("")
                .into_iter()
                .filter(|(k, _)| !k.starts_with("backup/"))
                .map(|(k, v)| (k, v.value))
                .collect(),
        },
        None => MetaSnapshot::default(),
    };
    let meta_bytes = Bytes::from(serde_json::to_vec(&meta_snap)?);
    let mut states: Vec<Bytes> = Vec::with_capacity(merged.len());
    let mut entries: Vec<ChannelEntry> = Vec::with_capacity(merged.len());
    for c in merged {
        let groups = c.group_offsets.len() as u32;
        let dlq_entries = c.dlq.len() as u64;
        let state = Bytes::from(serde_json::to_vec(&ChannelState {
            group_offsets: c.group_offsets,
            dlq: c.dlq,
        })?);
        entries.push(ChannelEntry {
            state: ObjectRef {
                key: manifest::channel_state_key(&id, &c.config.name),
                len: state.len() as u64,
                crc: crc32fast::hash(&state),
            },
            config: c.config,
            partitions: c.partitions,
            groups,
            dlq_entries,
        });
        states.push(state);
    }
    let extra = meta_bytes.len() as u64 + states.iter().map(|s| s.len() as u64).sum::<u64>();
    let include = ctx.include_messages;
    ctx.update(|i| {
        i.channels = entries.len() as u32;
        i.partitions = entries.iter().map(|e| e.partitions.len() as u32).sum();
        i.messages = if include {
            entries
                .iter()
                .flat_map(|e| &e.partitions)
                .map(|p| p.end - p.low)
                .sum()
        } else {
            0
        };
        i.total_bytes += extra;
    });
    let progress = |n: u64| ctx.update(|i| i.progress_bytes += n);

    let meta_ref = ObjectRef {
        key: manifest::meta_key(&id),
        len: meta_bytes.len() as u64,
        crc: crc32fast::hash(&meta_bytes),
    };
    let stored = objects::put_object(
        &ctx.target,
        &meta_ref.key,
        &Source::Bytes(meta_bytes),
        key,
        &progress,
    )
    .await?;
    ctx.update(|i| i.bytes_uploaded += stored);
    for (e, state) in entries.iter().zip(states) {
        let stored = objects::put_object(
            &ctx.target,
            &e.state.key,
            &Source::Bytes(state),
            key,
            &progress,
        )
        .await?;
        ctx.update(|i| i.bytes_uploaded += stored);
    }

    // Manifest last: a backup exists only once its manifest does.
    let mut info = ctx.info.lock().clone();
    info.status = BackupStatus::Succeeded;
    info.finished_at_ms = Some(now_ms());
    info.progress_bytes = info.total_bytes;
    let manifest = Manifest {
        format: FORMAT,
        info: info.clone(),
        node_id: ctx.broker.node_id().to_string(),
        cluster_name: ctx.cluster_name.clone(),
        fluxmq_version: fluxmq_proto::VERSION.to_string(),
        include_messages: ctx.include_messages,
        encryption: fp,
        meta: meta_ref,
        channels: entries,
        nodes,
    };
    let body = serde_json::to_vec_pretty(&manifest)?;
    ctx.target
        .store
        .put(
            &ctx.target.path(&manifest::manifest_key(&id)),
            object_store::PutPayload::from(body),
        )
        .await?;
    *ctx.info.lock() = info.clone();
    Ok(info)
}

async fn upload_segment(
    ctx: &RunCtx,
    job: SegJob,
    fp: Option<&str>,
    progress: &(dyn Fn(u64) + Send + Sync),
) -> Result<(usize, usize, usize, SegmentEntry)> {
    let src = Source::Segment(job.cut.clone());
    let len = job.cut.len;
    let ck: CrcKey = (
        job.channel.clone(),
        job.created_at_ms,
        job.partition,
        job.cut.base_offset,
        len,
    );
    let cached = ctx.crc_cache.lock().get(&ck).copied();
    let crc = match cached {
        Some(c) => c,
        None => {
            let c = objects::crc_of(&src).await?;
            ctx.crc_cache.lock().insert(ck, c);
            c
        }
    };
    let (log_key, idx_key) =
        manifest::segment_keys(&job.channel, job.partition, job.cut.base_offset, crc, fp);
    let index = Bytes::from(job.cut.index.clone());
    let key = ctx.key.as_ref();
    let log_stored = objects::stored_len(len, key);
    let log_there = objects::exists_with_len(&ctx.target, &log_key, log_stored).await?;
    let idx_there = match ctx.target.store.head(&ctx.target.path(&idx_key)).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => return Err(e.into()),
    };
    let mut uploaded = 0u64;
    // Index bytes of the same log may differ between processes (rebuilt on recovery). The
    // index is derived data: restore uses it only if it verifies, else recovery rebuilds it.
    let index_ref = ObjectRef {
        key: idx_key.clone(),
        len: index.len() as u64,
        crc: crc32fast::hash(&index),
    };
    if !log_there {
        uploaded += objects::put_object(&ctx.target, &log_key, &src, key, progress).await?;
    }
    if !idx_there || !log_there {
        uploaded += objects::put_object(
            &ctx.target,
            &idx_key,
            &Source::Bytes(index.clone()),
            key,
            &|_| {},
        )
        .await?;
    }
    progress(if log_there { len } else { 0 } + index.len() as u64);
    let reused = log_there;
    ctx.update(|i| {
        i.bytes_uploaded += uploaded;
        if reused {
            i.segments_reused += 1;
        } else {
            i.segments_uploaded += 1;
        }
    });
    let entry = SegmentEntry {
        base_offset: job.cut.base_offset,
        end_offset: job.cut.end_offset,
        log: ObjectRef {
            key: log_key,
            len,
            crc,
        },
        index: index_ref,
    };
    drop(src);
    drop(job.cut);
    Ok((job.slot.0, job.slot.1, job.slot.2, entry))
}

/// Backup ids on the target, oldest first.
pub async fn list_manifest_ids(target: &Target) -> Result<Vec<String>> {
    let prefix = target.path(manifest::MANIFESTS_DIR);
    let metas: Vec<object_store::ObjectMeta> =
        target.store.list(Some(&prefix)).try_collect().await?;
    let mut ids: Vec<String> = metas
        .iter()
        .filter_map(|m| manifest::id_from_manifest_key(m.location.as_ref()))
        .filter(|id| manifest::valid_backup_id(id))
        .map(str::to_string)
        .collect();
    ids.sort();
    Ok(ids)
}

/// Start time encoded in a backup id (`yyyymmddThhmmssZ-…`).
pub fn started_ms_from_id(id: &str) -> Option<u64> {
    let ts = id.split('-').next()?;
    chrono::NaiveDateTime::parse_from_str(ts, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|t| t.and_utc().timestamp_millis().max(0) as u64)
}

pub async fn load_manifest(target: &Target, id: &str) -> Result<Manifest> {
    if !manifest::valid_backup_id(id) {
        return Err(BackupError::NotFound(format!("backup '{id}'")));
    }
    let bytes = match target
        .store
        .get(&target.path(&manifest::manifest_key(id)))
        .await
    {
        Ok(r) => r.bytes().await?,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(BackupError::NotFound(format!("backup '{id}'")));
        }
        Err(e) => return Err(e.into()),
    };
    let m: Manifest = serde_json::from_slice(&bytes)?;
    if m.format != FORMAT {
        return Err(BackupError::Unsupported(format!(
            "backup '{id}' uses manifest format {} (this version reads {FORMAT})",
            m.format
        )));
    }
    Ok(m)
}

/// `latest` → newest id; otherwise validates that `id` exists.
pub async fn resolve_id(target: &Target, id: &str) -> Result<String> {
    let ids = list_manifest_ids(target).await?;
    if id == "latest" {
        return ids
            .last()
            .cloned()
            .ok_or_else(|| BackupError::NotFound("no backups on the target".into()));
    }
    if ids.iter().any(|i| i == id) {
        Ok(id.to_string())
    } else {
        Err(BackupError::NotFound(format!("backup '{id}'")))
    }
}

/// Deletes manifests beyond `keep_last` and older than `keep_days` (0 = no age limit); the
/// newest backup is always kept. Returns the deleted ids.
pub async fn apply_retention(
    target: &Target,
    keep_last: u32,
    keep_days: u32,
    now: u64,
) -> Result<Vec<String>> {
    let ids = list_manifest_ids(target).await?;
    let keep_last = keep_last.max(1) as usize;
    let cutoff = (keep_days > 0).then(|| now.saturating_sub(u64::from(keep_days) * 86_400_000));
    let n = ids.len();
    let mut deleted = Vec::new();
    for (i, id) in ids.iter().enumerate() {
        let newest = i + 1 == n;
        let beyond_count = n - i > keep_last;
        let too_old = cutoff.is_some_and(|c| started_ms_from_id(id).is_some_and(|t| t < c));
        if !newest && (beyond_count || too_old) {
            target
                .store
                .delete(&target.path(&manifest::manifest_key(id)))
                .await?;
            deleted.push(id.clone());
        }
    }
    Ok(deleted)
}

/// Deletes `segments/` and `state/` objects no manifest references. Returns
/// `(objects, bytes)` deleted. Must not run concurrently with a backup writing to the same
/// target (the service serializes them; don't point two writers at one target).
pub async fn gc(target: &Target) -> Result<(u32, u64)> {
    let ids = list_manifest_ids(target).await?;
    let mut referenced: BTreeSet<String> = BTreeSet::new();
    for id in &ids {
        let m = load_manifest(target, id).await?;
        referenced.extend(m.referenced().map(|k| target.path(k).to_string()));
    }
    let mut deleted = 0u32;
    let mut bytes = 0u64;
    for dir in [manifest::SEGMENTS_DIR, manifest::STATE_DIR] {
        let prefix = target.path(dir);
        let metas: Vec<object_store::ObjectMeta> =
            target.store.list(Some(&prefix)).try_collect().await?;
        for m in metas {
            if !referenced.contains(m.location.as_ref()) {
                match target.store.delete(&m.location).await {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                        deleted += 1;
                        bytes += m.size;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    Ok((deleted, bytes))
}
