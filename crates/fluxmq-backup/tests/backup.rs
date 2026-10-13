//! End-to-end backup → restore through `object_store` (InMemory and LocalFileSystem), plus an
//! opt-in MinIO run (`#[ignore]`, see `minio_roundtrip`).

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use bytes::Bytes;
use fluxmq_backup::crypto::Key;
use fluxmq_backup::restore::{RestoreOptions, restore};
use fluxmq_backup::service::{self, RunCtx};
use fluxmq_backup::{BackupService, ServiceOptions, Target};
use fluxmq_core::{Broker, BrokerConfig, SubscriberInfo};
use fluxmq_meta::{LocalMetaStore, MetaStore};
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::backup::{BackupConfig, BackupStatus, BackupTarget};
use fluxmq_proto::model::{ChannelConfig, ChannelMode, PublishMessage, StartFrom};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;

fn cfg(dir: &Path) -> BrokerConfig {
    let mut c = BrokerConfig::new(dir);
    c.node_id = Some("n1".into());
    c.segment_bytes = 4096;
    c.offset_checkpoint_interval = Duration::from_millis(20);
    c
}

async fn open(dir: &Path) -> Broker {
    Broker::open(cfg(dir)).await.unwrap()
}

fn msg(key: &str, payload: String) -> PublishMessage {
    PublishMessage {
        key: Some(key.into()),
        headers: Default::default(),
        payload: Bytes::from(payload),
    }
}

async fn publish(b: &Broker, ch: &str, from: usize, n: usize) {
    let msgs = (from..from + n)
        .map(|i| {
            msg(
                &format!("k{}", i % 5),
                format!("payload-{i:06}-{}", "x".repeat(40)),
            )
        })
        .collect();
    b.publish(ch, msgs).await.unwrap();
}

fn sub_info() -> SubscriberInfo {
    SubscriberInfo {
        client_name: "t".into(),
        remote_addr: "127.0.0.1:1".into(),
        connection_id: 1,
    }
}

fn all_messages(b: &Broker, ch: &str) -> Vec<fluxmq_proto::model::Message> {
    let (mut m, _, _) = b
        .read_messages(
            ch,
            &MessageQuery {
                limit: Some(100_000),
                ..Default::default()
            },
        )
        .unwrap();
    m.sort_by_key(|m| (m.partition, m.offset));
    m
}

/// A broker with two channels, a consumer group with committed progress and a DLQ entry.
async fn populated(dir: &Path) -> Broker {
    let b = open(dir).await;
    let mut orders = ChannelConfig::new("orders", ChannelMode::Stream);
    orders.partitions = 3;
    b.create_channel(orders).unwrap();
    b.create_channel(ChannelConfig::new("jobs", ChannelMode::Queue))
        .unwrap();
    publish(&b, "orders", 0, 400).await;
    publish(&b, "jobs", 0, 10).await;
    let mut sub = b
        .subscribe("jobs", "workers", sub_info(), 100, StartFrom::Earliest)
        .unwrap();
    for i in 0..4 {
        let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(d.message.offset, i);
        if i == 3 {
            sub.nack(d.delivery_id, "poison", false).unwrap();
        } else {
            sub.ack(&[d.delivery_id]).unwrap();
        }
    }
    drop(sub);
    b
}

async fn meta_with_keys() -> Arc<LocalMetaStore> {
    let m = Arc::new(LocalMetaStore::in_memory());
    m.put("routes/r1", serde_json::json!({"name": "r1"}), None)
        .await
        .unwrap();
    m.put("auth/tokens/t1", serde_json::json!({"hash": "abc"}), None)
        .await
        .unwrap();
    m.put("cluster/channels/orders", serde_json::json!({}), None)
        .await
        .unwrap();
    m
}

fn key_b64(b: u8) -> String {
    base64::engine::general_purpose::STANDARD.encode([b; 32])
}

async fn object_keys(t: &Target) -> BTreeSet<String> {
    t.store
        .list(None)
        .map_ok(|m| m.location.to_string())
        .try_collect()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn service_backup_restore_roundtrip_filesystem() {
    let dir = tempfile::tempdir().unwrap();
    let b = populated(&dir.path().join("src")).await;
    let meta = meta_with_keys().await;
    let svc = BackupService::start(
        b.clone(),
        meta.clone(),
        ServiceOptions {
            scheduler: false,
            ..ServiceOptions::default()
        },
    );
    let target_dir = dir.path().join("target");
    let cfg = BackupConfig {
        enabled: false,
        target: BackupTarget::Filesystem {
            path: target_dir.display().to_string(),
        },
        interval_secs: 3600,
        keep_last: 5,
        keep_days: 0,
        include_messages: true,
        encryption_key: None,
    };
    svc.put_config(cfg).await.unwrap();
    // Keep publishing during the backup: the restore must hold an exact prefix.
    let pb = b.clone();
    let live = tokio::spawn(async move {
        for i in 0..50 {
            publish(&pb, "orders", 1000 + i, 1).await;
        }
    });
    let info = svc.run_and_wait("manual").await.unwrap();
    live.await.unwrap();
    assert_eq!(info.status, BackupStatus::Succeeded, "{:?}", info.error);
    assert_eq!(info.channels, 2);
    assert_eq!(info.partitions, 4);
    assert!(info.segments_uploaded > 3);
    assert_eq!(info.progress_bytes, info.total_bytes);
    // The backup config itself is never in the snapshot.
    assert!(meta.get("backup/config").is_some());
    let hist = svc.list().await.unwrap();
    assert_eq!(hist.len(), 1);
    assert_eq!(hist[0].id, info.id);
    let manifest = svc.get("latest").await.unwrap();
    // Group offsets were taken after the cut; the source keeps progressing.
    b.shutdown().await;

    let target = Target::open(&BackupTarget::Filesystem {
        path: target_dir.display().to_string(),
    })
    .unwrap();
    let restored = dir.path().join("restored");
    let sum = restore(
        &target,
        "latest",
        &restored,
        None,
        &RestoreOptions { fsync: false },
        &|_| {},
    )
    .await
    .unwrap();
    assert_eq!(sum.channels, 2);
    assert_eq!(sum.dlq_entries, 1);
    // Meta: routes/tokens restored; cluster/* dropped; backup/* never included.
    let m = LocalMetaStore::open(restored.join("meta.json")).unwrap();
    assert!(m.get("routes/r1").is_some());
    assert!(m.get("auth/tokens/t1").is_some());
    assert!(m.get("cluster/channels/orders").is_none());
    assert!(m.get("backup/config").is_none());

    let r = open(&restored).await;
    let msgs = all_messages(&r, "orders");
    let expected: u64 = manifest
        .channels
        .iter()
        .find(|c| c.config.name == "orders")
        .unwrap()
        .partitions
        .iter()
        .map(|p| p.end)
        .sum();
    assert!(expected >= 400);
    assert_eq!(msgs.len() as u64, expected);
    // Every partition holds 0..end contiguously.
    for p in &manifest
        .channels
        .iter()
        .find(|c| c.config.name == "orders")
        .unwrap()
        .partitions
    {
        let offs: Vec<u64> = msgs
            .iter()
            .filter(|m| m.partition == p.partition)
            .map(|m| m.offset)
            .collect();
        assert_eq!(offs, (0..p.end).collect::<Vec<_>>());
    }
    // Consumer group resumes from its committed offset (3: the dead-lettered message was
    // finished, so 4 is next... committed = lowest unfinished).
    let mut sub = r
        .subscribe("jobs", "workers", sub_info(), 10, StartFrom::Latest)
        .unwrap();
    let d = tokio::time::timeout(Duration::from_secs(5), sub.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(d.message.offset, 4);
    drop(sub);
    // DLQ restored.
    let (dl, total) = r.dlq_list(Some("jobs"), None, None, 10);
    assert_eq!(total, 1);
    assert_eq!(dl[0].reason, "poison");
    assert_eq!(dl[0].message.offset, 3);
    // Restore refuses a non-empty directory.
    let err = restore(
        &target,
        "latest",
        &restored,
        None,
        &RestoreOptions::default(),
        &|_| {},
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("not empty"), "{err}");
    r.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_backup_reuses_sealed_segments() {
    let dir = tempfile::tempdir().unwrap();
    let b = populated(dir.path()).await;
    let target = Target::in_memory();
    let ctx = RunCtx::new(b.clone(), target.clone(), "cli");
    let first = service::run_backup(&ctx).await.unwrap();
    assert!(first.segments_uploaded > 5);
    assert_eq!(first.segments_reused, 0);
    publish(&b, "orders", 400, 5).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    let mut ctx2 = RunCtx::new(b.clone(), target.clone(), "cli");
    ctx2.crc_cache = ctx.crc_cache.clone();
    let second = service::run_backup(&ctx2).await.unwrap();
    // Only partitions that grew re-upload their (partial) active segment.
    assert!(
        second.segments_reused >= first.segments_uploaded - 3,
        "first {first:?}\nsecond {second:?}"
    );
    assert!(second.segments_uploaded <= 3);
    assert!(second.bytes_uploaded < first.bytes_uploaded / 2);
    // Both restore.
    for id in [&first.id, &second.id] {
        let out = dir.path().join(format!("r-{id}"));
        restore(
            &target,
            id,
            &out,
            None,
            &RestoreOptions { fsync: false },
            &|_| {},
        )
        .await
        .unwrap();
    }
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_keeps_newest_and_gc_removes_unreferenced_objects() {
    let dir = tempfile::tempdir().unwrap();
    let b = populated(dir.path()).await;
    let target = Target::in_memory();
    let mut ids = Vec::new();
    for round in 0..4 {
        publish(&b, "orders", 1000 + round * 100, 100).await;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let info = service::run_backup(&RunCtx::new(b.clone(), target.clone(), "cli"))
            .await
            .unwrap();
        ids.push(info.id);
    }
    // A stray object (e.g. from a failed run) is collected too.
    target
        .store
        .put(
            &target.path("segments/orders/0/stray.log"),
            Bytes::from_static(b"x").into(),
        )
        .await
        .unwrap();
    let deleted = service::apply_retention(
        &target,
        2,
        0,
        fluxmq_backup::service::started_ms_from_id(&ids[3]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(deleted, ids[..2].to_vec());
    let (n, _) = service::gc(&target).await.unwrap();
    assert!(n > 0);
    let left = service::list_manifest_ids(&target).await.unwrap();
    assert_eq!(left, ids[2..].to_vec());
    // Exactly the referenced objects remain (+ manifests).
    let mut want: BTreeSet<String> = BTreeSet::new();
    for id in &left {
        let m = service::load_manifest(&target, id).await.unwrap();
        want.extend(m.referenced().map(|k| target.path(k).to_string()));
        want.insert(target.path(&format!("manifests/{id}.json")).to_string());
    }
    assert_eq!(object_keys(&target).await, want);
    // keep_days: everything older than a day is gone except the newest backup.
    let far_future = fluxmq_backup::service::started_ms_from_id(&ids[3]).unwrap() + 30 * 86_400_000;
    let deleted = service::apply_retention(&target, 100, 1, far_future)
        .await
        .unwrap();
    assert_eq!(deleted, vec![ids[2].clone()]);
    service::gc(&target).await.unwrap();
    restore(
        &target,
        "latest",
        &dir.path().join("r"),
        None,
        &RestoreOptions { fsync: false },
        &|_| {},
    )
    .await
    .unwrap();
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encrypted_backup_roundtrip_and_wrong_key_fails() {
    let dir = tempfile::tempdir().unwrap();
    let b = populated(dir.path()).await;
    let target = Target::in_memory();
    let key = Key::parse(&key_b64(9)).unwrap();
    let mut ctx = RunCtx::new(b.clone(), target.clone(), "cli");
    ctx.key = Some(key.clone());
    let info = service::run_backup(&ctx).await.unwrap();
    // No object body contains plaintext payloads.
    for k in object_keys(&target).await {
        if k.starts_with("manifests/") {
            continue;
        }
        let body = target
            .store
            .get(&k.as_str().into())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            !body.windows(14).any(|w| w == b"payload-000001"),
            "{k} leaks plaintext"
        );
        assert!(body.starts_with(b"FXBE"), "{k}");
    }
    let opts = RestoreOptions { fsync: false };
    let wrong = Key::parse(&key_b64(10)).unwrap();
    let e = restore(
        &target,
        &info.id,
        &dir.path().join("a"),
        Some(&wrong),
        &opts,
        &|_| {},
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("wrong encryption key"), "{e}");
    let e = restore(
        &target,
        &info.id,
        &dir.path().join("b"),
        None,
        &opts,
        &|_| {},
    )
    .await
    .unwrap_err();
    assert!(e.to_string().contains("encrypted"), "{e}");
    let out = dir.path().join("c");
    restore(&target, &info.id, &out, Some(&key), &opts, &|_| {})
        .await
        .unwrap();
    let r = open(&out).await;
    assert_eq!(all_messages(&r, "orders"), all_messages(&b, "orders"));
    r.shutdown().await;
    b.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_only_backup_keeps_offsets_continuing() {
    let dir = tempfile::tempdir().unwrap();
    let b = populated(dir.path()).await;
    let target = Target::in_memory();
    let mut ctx = RunCtx::new(b.clone(), target.clone(), "cli");
    ctx.include_messages = false;
    let info = service::run_backup(&ctx).await.unwrap();
    assert_eq!(info.segments_uploaded, 0);
    let out = dir.path().join("r");
    restore(
        &target,
        &info.id,
        &out,
        None,
        &RestoreOptions { fsync: false },
        &|_| {},
    )
    .await
    .unwrap();
    let r = open(&out).await;
    assert!(all_messages(&r, "orders").is_empty());
    let high = b
        .stats()
        .channels
        .iter()
        .find(|c| c.config.name == "jobs")
        .unwrap()
        .partitions[0]
        .high_offset;
    let ids = r
        .publish("jobs", vec![msg("k", "new".into())])
        .await
        .unwrap();
    assert_eq!(
        ids[0].offset, high,
        "offsets continue after the backup's end"
    );
    r.shutdown().await;
    b.shutdown().await;
}

/// Manual run against MinIO:
/// `docker run -d --name fluxmq-minio -p 9000:9000 minio/minio server /data`, create bucket
/// `fluxmq` (`docker exec fluxmq-minio mc mb /data/fluxmq` or the console), then
/// `FLUXMQ_MINIO=http://127.0.0.1:9000 cargo test -p fluxmq-backup --test backup -- --ignored`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a MinIO server (see doc comment)"]
async fn minio_roundtrip() {
    let endpoint = std::env::var("FLUXMQ_MINIO").unwrap_or("http://127.0.0.1:9000".into());
    let t = BackupTarget::S3 {
        endpoint: Some(endpoint),
        region: "us-east-1".into(),
        bucket: std::env::var("FLUXMQ_MINIO_BUCKET").unwrap_or("fluxmq".into()),
        prefix: format!("test-{}", std::process::id()),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").unwrap_or("minioadmin".into()),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or("minioadmin".into()),
        path_style: true,
    };
    let target = Target::open(&t).unwrap();
    let probe = target.probe().await;
    assert!(probe.ok, "{probe:?}");
    let info = big_roundtrip(&target).await;
    // Clean up the prefix.
    target
        .store
        .delete(&target.path(&format!("manifests/{}.json", info.id)))
        .await
        .unwrap();
    service::gc(&target).await.unwrap();
}

/// Backs up a channel with one ~7.7 MiB segment (multipart, encrypted), restores, compares.
async fn big_roundtrip(target: &Target) -> fluxmq_proto::backup::BackupInfo {
    let dir = tempfile::tempdir().unwrap();
    // Large segments: one segment above the multipart threshold exercises streaming
    // multipart uploads.
    let mut c = cfg(&dir.path().join("src"));
    c.segment_bytes = 64 << 20;
    let b = Broker::open(c).await.unwrap();
    let mut big = ChannelConfig::new("big", ChannelMode::Stream);
    big.partitions = 1;
    b.create_channel(big).unwrap();
    let payload = "y".repeat(64 * 1024);
    for _ in 0..12 {
        let msgs = (0..10).map(|_| msg("k", payload.clone())).collect();
        b.publish("big", msgs).await.unwrap();
    }
    let mut ctx = RunCtx::new(b.clone(), target.clone(), "cli");
    ctx.key = Some(Key::parse(&key_b64(3)).unwrap());
    let info = service::run_backup(&ctx).await.unwrap();
    let out = dir.path().join("r");
    restore(
        target,
        &info.id,
        &out,
        ctx.key.as_ref(),
        &RestoreOptions { fsync: false },
        &|_| {},
    )
    .await
    .unwrap();
    let r = open(&out).await;
    assert_eq!(all_messages(&r, "big"), all_messages(&b, "big"));
    assert_eq!(all_messages(&r, "big").len(), 120);
    r.shutdown().await;
    b.shutdown().await;
    info
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_segment_multipart_roundtrip_on_filesystem_and_memory() {
    let dir = tempfile::tempdir().unwrap();
    let fs = Target::open(&BackupTarget::Filesystem {
        path: dir.path().join("t").display().to_string(),
    })
    .unwrap();
    let info = big_roundtrip(&fs).await;
    assert!(info.bytes_uploaded > objects_threshold());
    big_roundtrip(&Target::in_memory()).await;
}

fn objects_threshold() -> u64 {
    fluxmq_backup::objects::MULTIPART_THRESHOLD
}

/// Fake cluster: this node is not the controller / an old node is present.
struct FakeHook {
    controller: bool,
    old_node: bool,
}

impl fluxmq_backup::cluster::ClusterHook for FakeHook {
    fn is_controller(&self) -> bool {
        self.controller
    }
    fn unsupported(&self) -> Option<String> {
        self.old_node
            .then(|| "node n3 (0.2.0) predates 0.3.0".to_string())
    }
    fn expected(&self) -> std::collections::BTreeMap<String, u32> {
        Default::default()
    }
    fn remote_partials(
        &self,
        _req: fluxmq_backup::cluster::PartialRequest,
    ) -> futures::future::BoxFuture<
        'static,
        fluxmq_backup::Result<Vec<fluxmq_backup::cluster::Partial>>,
    > {
        Box::pin(async { Ok(vec![]) })
    }
}

#[tokio::test]
async fn clustered_node_refusals_are_typed() {
    let dir = tempfile::tempdir().unwrap();
    let b = open(&dir.path().join("d")).await;
    for (controller, old_node) in [(false, false), (true, true)] {
        let svc = BackupService::start(
            b.clone(),
            Arc::new(LocalMetaStore::in_memory()),
            ServiceOptions {
                cluster: Some(Arc::new(FakeHook {
                    controller,
                    old_node,
                })),
                scheduler: false,
                ..ServiceOptions::default()
            },
        );
        svc.put_config(BackupConfig {
            enabled: true,
            target: BackupTarget::Filesystem {
                path: dir.path().join("t").display().to_string(),
            },
            interval_secs: 300,
            keep_last: 1,
            keep_days: 0,
            include_messages: true,
            encryption_key: None,
        })
        .await
        .unwrap();
        let e = svc.run("manual").unwrap_err();
        if old_node {
            assert!(
                matches!(e, fluxmq_backup::BackupError::UnsupportedInCluster(_)),
                "{e}"
            );
        } else {
            assert!(
                matches!(e, fluxmq_backup::BackupError::NotController(_)),
                "{e}"
            );
        }
        assert!(svc.overview().enabled);
    }
    b.shutdown().await;
}
