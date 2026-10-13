//! Rolling upgrade v0.6 → v0.7 with real binaries (ignored by default):
//!
//! ```text
//! FLUXMQ_OLD_BIN=/path/to/fluxmq-v0.6 FLUXMQ_NEW_BIN=target/release/fluxmq \
//!   cargo test -p fluxmq-server --test rolling_upgrade_bin -- --ignored --nocapture
//! ```
//!
//! Three processes start on the old binary (group commit, RF=3, min_isr=2). Under continuous
//! load (SDK `publish_batch`, lz4), each node is killed with SIGKILL and restarted on the new
//! binary, one at a time; afterwards the leader of a partition is killed -9 three more times.
//! Checks: zero acknowledged messages lost (every acked id is stored with its payload), no
//! batch envelope on disk while any node runs the old binary, envelopes written once the last
//! node is upgraded.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fluxmq_client::{Client, ClientOptions, Compression, PublishMessage};
use fluxmq_proto::batch;
use fluxmq_proto::cluster::{ClusterMetadata, NodeStatus};
use serde_json::json;

const CH: &str = "upgrade";
const P: u32 = 6;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Proc {
    id: String,
    data: PathBuf,
    ports: [u16; 4], // client, http, raft, replication
    child: Option<Child>,
    bin: PathBuf,
}

impl Proc {
    fn spawn(&mut self, bin: &Path, peers: &str) {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.data.with_extension("log"))
            .unwrap();
        let child = Command::new(bin)
            .env("FLUXMQ_NODE_ID", &self.id)
            .env("FLUXMQ_DATA_DIR", &self.data)
            .env("FLUXMQ_CLIENT_ADDR", format!("127.0.0.1:{}", self.ports[0]))
            .env("FLUXMQ_HTTP_ADDR", format!("127.0.0.1:{}", self.ports[1]))
            .env("FLUXMQ_RAFT_ADDR", format!("127.0.0.1:{}", self.ports[2]))
            .env(
                "FLUXMQ_REPLICATION_ADDR",
                format!("127.0.0.1:{}", self.ports[3]),
            )
            .env("FLUXMQ_PEERS", peers)
            .env("FLUXMQ_CLUSTER_SECRET", "upgrade-secret")
            .env("FLUXMQ_MIN_ISR", "2")
            .env("FLUXMQ_REPLICATION_FACTOR", "3")
            .env("FLUXMQ_AUTO_REBALANCE", "off")
            .env("FLUXMQ_UI_DIR", "/nonexistent")
            .env(
                "FLUXMQ_LOG",
                std::env::var("FLUXMQ_TEST_NODE_LOG").unwrap_or_else(|_| {
                    "warn,fluxmq_cluster::reconcile=info,fluxmq_core::replica=info".into()
                }),
            )
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn node");
        self.child = Some(child);
        self.bin = bin.to_path_buf();
    }

    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill(); // SIGKILL
            let _ = c.wait();
        }
    }

    fn http(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.ports[1])
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill9();
    }
}

async fn metadata(p: &Proc) -> Option<ClusterMetadata> {
    reqwest::get(p.http("/api/cluster/metadata"))
        .await
        .ok()?
        .json()
        .await
        .ok()
}

async fn wait_for<T, F, Fut>(what: &str, timeout: Duration, mut f: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(v) = f().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Every node Up, one controller, the channel's partitions all led with a full ISR.
///
/// `old`: the v0.6 binary while some node still runs it. v0.6 leaders have two ISR bugs
/// fixed in v0.7: a lost ISR report is never re-sent (the returned follower stays out of the
/// committed ISR until that leader restarts), and the ISR can flap (stale committed ISRs evict
/// a follower that rejoins on the next fetch). So in the mixed phase a partition led by a
/// v0.6 node needs only `min_isr` (2) members, and one led by a v0.7 node counts once it was
/// *seen* with a full ISR since this wait began. With only new binaries the strict check
/// applies.
async fn wait_healthy(procs: &[Proc], channel: Option<&str>, old: Option<&Path>) {
    let last = std::sync::Mutex::new(String::new());
    let seen_full = std::sync::Mutex::new(BTreeSet::<u32>::new());
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let ok = healthy(procs, channel, &last, old.map(|o| (o, &seen_full))).await;
        if ok.is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster not healthy; last state: {}",
            last.lock().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn healthy(
    procs: &[Proc],
    channel: Option<&str>,
    last: &std::sync::Mutex<String>,
    mixed: Option<(&Path, &std::sync::Mutex<BTreeSet<u32>>)>,
) -> Option<()> {
    {
        let mut ctl = BTreeSet::new();
        for p in procs {
            let Some(md) = metadata(p).await else {
                *last.lock().unwrap() = format!("{}: no metadata", p.id);
                return None;
            };
            *last.lock().unwrap() = format!(
                "via {}: nodes {:?} controller {:?} parts {:?}",
                p.id,
                md.nodes
                    .iter()
                    .map(|n| (&n.id, n.status))
                    .collect::<Vec<_>>(),
                md.controller,
                channel.and_then(|c| md.channels.get(c)).map(|ps| ps
                    .iter()
                    .map(|x| (x.partition, x.leader.clone(), x.isr.clone()))
                    .collect::<Vec<_>>())
            );
            let up = md
                .nodes
                .iter()
                .filter(|n| n.status == NodeStatus::Up)
                .count();
            if up != procs.len() || md.controller.is_none() {
                return None;
            }
            ctl.insert(md.controller.clone());
            if let Some(ch) = channel {
                let parts = md.channels.get(ch)?;
                if parts.len() != P as usize || parts.iter().any(|x| x.leader.is_none()) {
                    return None;
                }
                match mixed {
                    Some((old, seen)) => {
                        let led_by_old = |leader: &Option<String>| {
                            procs
                                .iter()
                                .any(|p| Some(&p.id) == leader.as_ref() && p.bin == old)
                        };
                        let mut seen = seen.lock().unwrap();
                        for x in parts {
                            if x.isr.len() == 3 || (led_by_old(&x.leader) && x.isr.len() >= 2) {
                                seen.insert(x.partition);
                            }
                        }
                        if seen.len() != P as usize {
                            return None;
                        }
                    }
                    None if parts.iter().any(|x| x.isr.len() != 3) => return None,
                    None => {}
                }
            }
        }
        (ctl.len() == 1).then_some(())
    }
}

fn segment_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let path = e.path();
        if path.is_dir() {
            segment_files(&path, out);
        } else if path.extension().is_some_and(|x| x == "log")
            && path.components().any(|c| c.as_os_str() == CH)
        {
            out.push(path);
        }
    }
}

/// Batch envelopes stored for the channel on this node.
fn envelopes(p: &Proc) -> usize {
    let mut files = Vec::new();
    segment_files(&p.data, &mut files);
    let mut n = 0;
    for f in files {
        let b = std::fs::read(&f).unwrap_or_default();
        let mut at = 0usize;
        while at + 4 <= b.len() {
            let w = u32::from_le_bytes(b[at..at + 4].try_into().unwrap());
            if w == 0 {
                break;
            }
            if batch::is_batch(w) {
                n += 1;
            }
            at += 4 + (w & !batch::BATCH_FLAG) as usize;
        }
    }
    n
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn rolling_upgrade_v06_to_v07_with_kill9() {
    let (Ok(old), Ok(new)) = (
        std::env::var("FLUXMQ_OLD_BIN"),
        std::env::var("FLUXMQ_NEW_BIN"),
    ) else {
        eprintln!("set FLUXMQ_OLD_BIN and FLUXMQ_NEW_BIN");
        return;
    };
    let (old, new) = (PathBuf::from(old), PathBuf::from(new));
    // Kept for post-mortem (node logs are `<data>/nN.log`); removed at the end on success.
    let dir = tempfile::tempdir().unwrap().keep();
    eprintln!("data: {}", dir.display());
    let mut procs: Vec<Proc> = (1..=3)
        .map(|i| Proc {
            id: format!("n{i}"),
            data: dir.join(format!("n{i}")),
            ports: [free_port(), free_port(), free_port(), free_port()],
            child: None,
            bin: old.clone(),
        })
        .collect();
    let peers = procs
        .iter()
        .map(|p| format!("{}@127.0.0.1:{}", p.id, p.ports[2]))
        .collect::<Vec<_>>()
        .join(",");
    for p in &mut procs {
        p.spawn(&old, &peers);
    }
    wait_healthy(&procs, None, None).await;
    let resp = reqwest::Client::new()
        .post(procs[0].http("/api/channels"))
        .json(&json!({"name": CH, "mode": "stream", "partitions": P}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        201,
        "{}",
        resp.text().await.unwrap_or_default()
    );
    wait_healthy(&procs, Some(CH), None).await;

    // Load: acked (partition, offset) → payload.
    let seeds: Vec<String> = procs
        .iter()
        .map(|p| format!("127.0.0.1:{}", p.ports[0]))
        .collect();
    let stop = Arc::new(AtomicBool::new(false));
    let acked = Arc::new(std::sync::Mutex::new(BTreeMap::<(u32, u64), String>::new()));
    let errors = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut load = Vec::new();
    for t in 0..3 {
        let (stop, acked, errors) = (stop.clone(), acked.clone(), errors.clone());
        let mut opts = ClientOptions::with_seeds(seeds.clone(), format!("upgrade-load-{t}"));
        opts.compression = if t == 0 {
            Compression::None
        } else {
            Compression::Lz4
        };
        opts.publish_timeout = Duration::from_secs(20);
        load.push(tokio::spawn(async move {
            let client = Client::connect(opts).await.expect("client connects");
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let payloads: Vec<String> = (0..20)
                    .map(|k| format!("t{t}-{i}-{k}-{}", "z".repeat(64)))
                    .collect();
                i += 1;
                let msgs = payloads
                    .iter()
                    .map(|p| PublishMessage {
                        key: None,
                        headers: Default::default(),
                        payload: p.clone().into(),
                    })
                    .collect();
                match client.publish_batch(CH, msgs).await {
                    Ok(ids) => {
                        let mut a = acked.lock().unwrap();
                        for (id, p) in ids.iter().zip(payloads) {
                            a.insert((id.partition, id.offset), p);
                        }
                    }
                    Err(_) => {
                        errors.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            client.close();
        }));
    }
    let acked_len = || acked.lock().unwrap().len();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(acked_len() > 0, "no load on the old cluster");

    // Rolling upgrade, each node crashing (SIGKILL) rather than stopping.
    for i in 0..3 {
        let before = acked_len();
        procs[i].kill9();
        tokio::time::sleep(Duration::from_secs(1)).await;
        procs[i].spawn(&new.clone(), &peers);
        // Old leaders remain until the last node is replaced.
        wait_healthy(&procs, Some(CH), (i < 2).then_some(old.as_path())).await;
        wait_for("load continues", Duration::from_secs(60), || async {
            (acked_len() > before + 200).then_some(())
        })
        .await;
        let any_old = procs.iter().any(|p| p.bin == old);
        if any_old {
            for p in &procs {
                assert_eq!(
                    envelopes(p),
                    0,
                    "{}: envelope written in a mixed cluster",
                    p.id
                );
            }
        }
        eprintln!("upgraded {} (acked so far: {})", procs[i].id, acked_len());
    }
    wait_for(
        "batches enabled after the upgrade",
        Duration::from_secs(60),
        || async { procs.iter().any(|p| envelopes(p) > 0).then_some(()) },
    )
    .await;

    // kill -9 loop on partition 0's leader (new binaries).
    for round in 0..3 {
        let before = acked_len();
        let md = metadata(&procs[0]).await.unwrap();
        let leader = md.channels[CH][0].leader.clone().unwrap();
        let idx = procs.iter().position(|p| p.id == leader).unwrap();
        procs[idx].kill9();
        tokio::time::sleep(Duration::from_secs(1)).await;
        procs[idx].spawn(&new.clone(), &peers);
        wait_healthy(&procs, Some(CH), None).await;
        wait_for("load continues", Duration::from_secs(60), || async {
            (acked_len() > before + 200).then_some(())
        })
        .await;
        eprintln!(
            "kill -9 round {round}: {leader} (acked so far: {})",
            acked_len()
        );
    }
    stop.store(true, Ordering::Relaxed);
    for l in load {
        l.await.unwrap();
    }

    // Zero acked loss: every acked id is stored with its payload, read through every node.
    let acked = acked.lock().unwrap().clone();
    eprintln!(
        "acked {} messages, {} publish errors (retried)",
        acked.len(),
        errors.load(Ordering::Relaxed)
    );
    for p in &procs {
        let mut stored: BTreeMap<(u32, u64), String> = BTreeMap::new();
        for part in 0..P {
            let mut offset = 0u64;
            loop {
                let page: serde_json::Value = reqwest::get(p.http(&format!(
                    "/api/channels/{CH}/messages?partition={part}&offset={offset}&limit=1000"
                )))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
                let msgs = page["messages"].as_array().unwrap();
                for m in msgs {
                    let off = m["offset"].as_u64().unwrap();
                    let payload = m["payload"].to_string();
                    stored.insert((part, off), payload);
                }
                match msgs.last() {
                    Some(m) if msgs.len() == 1000 => offset = m["offset"].as_u64().unwrap() + 1,
                    _ => break,
                }
            }
        }
        let mut lost = 0;
        let mut wrong = 0;
        for (id, payload) in &acked {
            match stored.get(id) {
                None => lost += 1,
                Some(s) if !s.contains(payload.as_str()) => wrong += 1,
                Some(_) => {}
            }
        }
        assert_eq!(
            (lost, wrong),
            (0, 0),
            "via {}: acked messages lost / changed",
            p.id
        );
    }
    eprintln!(
        "envelopes stored: {:?}",
        procs
            .iter()
            .map(|p| (p.id.clone(), envelopes(p)))
            .collect::<Vec<_>>()
    );
    for p in &mut procs {
        p.kill9();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
