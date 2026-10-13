//! Property-style test of v0.4 replica moves under load. Six nodes; every partition of the
//! channel starts on {n1, n2, n3} and is moved, one partition at a time, to the disjoint set
//! {n4, n5, n6} with the controller's protocol (the test plays the controller, driving roles
//! through `set_role` like `fluxmq-cluster` does):
//!
//! 1. add the new replicas (`replicas = old ∪ new`, epoch bump), catch-up throttled;
//! 2. wait until every new replica is in the leader's ISR;
//! 3. commit the final replica list (epoch bump; leadership moves to a new replica if the
//!    leader is removed), removed replicas go `None` and delete their copy
//!    (`remove_replica`).
//!
//! Producers (acks = ISR, `min_isr` 2) and consumers (group `g` on every node, dead-lettering
//! "poison" messages) run throughout. Depending on the seed, the leader of the partition
//! being moved is killed abruptly (kill -9 style: its replication stops without any handoff,
//! its clients vanish) in the middle of the move, and the controller fails over from the
//! committed ISR.
//!
//! Invariants: no acked message is lost (final logs identical on the new replicas and
//! holding every acked message); every acked message is consumed or dead-lettered at least
//! once; group offsets replicated before a leadership change are in force on the new
//! leader; DLQ entries survive every leadership change; removed nodes hold nothing (their
//! channel directory is gone) while partitions they still replicate are never touched.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use fluxmq_core::FsyncPolicy;
use fluxmq_proto::model::StartFrom;
use parking_lot::Mutex;
use tokio::task::JoinHandle;

const P: u32 = 3;
const OLD: [usize; 3] = [0, 1, 2];
const NEW: [usize; 3] = [3, 4, 5];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn is_poison(payload: &str) -> bool {
    // "p{p}-{i}": every 23rd message is poison.
    payload
        .rsplit('-')
        .next()
        .and_then(|i| i.parse::<u64>().ok())
        .is_some_and(|i| i % 23 == 5)
}

/// What the simulated controller knows about one partition.
#[derive(Clone)]
struct Assign {
    epoch: u64,
    leader: usize,
    replicas: Vec<usize>,
}

#[derive(Default)]
struct Observed {
    /// Payloads whose publish was acknowledged.
    acked: BTreeSet<String>,
    /// Payloads a consumer acked (ack succeeded).
    consumed: BTreeSet<String>,
    /// Payloads a consumer dead-lettered (nack succeeded).
    dead: BTreeSet<String>,
}

struct World {
    n: Vec<Node>,
    killed: BTreeSet<usize>,
    assign: Vec<Assign>,
    /// Current leader per partition, read by the producers.
    leaders: Arc<Mutex<Vec<usize>>>,
    obs: Arc<Mutex<Observed>>,
    producers: Vec<JoinHandle<()>>,
    consumers: Vec<Option<JoinHandle<()>>>,
}

fn ids(n: &[Node], idx: &[usize]) -> Vec<String> {
    idx.iter().map(|&i| n[i].id.clone()).collect()
}

impl World {
    fn lead(&self, p: u32) {
        let a = &self.assign[p as usize];
        let replicas: Vec<&Node> = a.replicas.iter().map(|&i| &self.n[i]).collect();
        self.n[a.leader].lead(p, a.epoch, &replicas, 2);
    }

    fn follow_all(&self, p: u32) {
        let a = &self.assign[p as usize];
        for &r in &a.replicas {
            if r != a.leader && !self.killed.contains(&r) {
                self.n[r].follow(p, a.epoch, &self.n[a.leader]);
            }
        }
    }

    fn leader_node(&self, p: u32) -> &Node {
        &self.n[self.assign[p as usize].leader]
    }

    fn group_committed(&self, i: usize, p: u32) -> Option<u64> {
        self.n[i]
            .broker
            .replica(CH, p)
            .unwrap()
            .group_offsets()
            .into_iter()
            .find(|(g, _)| g == "g")
            .map(|(_, o)| o)
    }

    fn dlq_ids(&self, i: usize, p: u32) -> BTreeSet<u64> {
        self.n[i]
            .broker
            .dlq_list(Some(CH), None, None, 1_000_000)
            .0
            .into_iter()
            .filter(|d| d.message.partition == p)
            .map(|d| d.id)
            .collect()
    }

    /// Leadership of `p` moves from its current leader to `to` (epoch bump) — after making
    /// sure `to` has applied the leader's current group offsets and DLQ, which must then
    /// be in force on `to`. `committed_isr`: the ISR the controller commits.
    async fn move_leadership(&mut self, p: u32, to: usize, replicas: Vec<usize>, isr: Vec<String>) {
        let from = self.assign[p as usize].leader;
        let alive = !self.killed.contains(&from);
        let (c0, d0) = if alive {
            (self.group_committed(from, p), self.dlq_ids(from, p))
        } else {
            (None, BTreeSet::new())
        };
        if alive {
            wait_sync("new leader has the group offsets and DLQ", || {
                self.group_committed(to, p) >= c0 && self.dlq_ids(to, p).is_superset(&d0)
            })
            .await;
        }
        let (c1, d1) = (self.group_committed(to, p), self.dlq_ids(to, p));
        let a = &mut self.assign[p as usize];
        a.epoch += 1;
        a.leader = to;
        a.replicas = replicas;
        self.lead(p);
        self.n[to]
            .mgr
            .isr_committed(CH, p, self.assign[p as usize].epoch, isr);
        self.leaders.lock()[p as usize] = to;
        self.follow_all(p);
        // In force right after the change (the new leader resumes from them).
        assert!(
            self.group_committed(to, p) >= c1.max(c0),
            "p{p}: group offset went back on the new leader ({:?} < {:?})",
            self.group_committed(to, p),
            c1.max(c0)
        );
        assert!(
            self.dlq_ids(to, p).is_superset(&d1),
            "p{p}: DLQ entries lost on the new leader"
        );
    }

    /// kill -9 of node `k`: replication stops without handoff, its clients disappear. The
    /// controller fails over every partition `k` leads, from the committed ISR.
    async fn kill(&mut self, k: usize, rng: &mut Rng) {
        // Committed ISRs as the controller knows them (status survives the kill).
        let committed: Vec<Vec<String>> =
            (0..P).map(|p| self.n[k].status(p).committed_isr).collect();
        // Group offsets the leader has committed so far; once every electable replica has
        // applied them (replication runs continuously), they must survive the kill.
        let mut floors = BTreeMap::new();
        for p in 0..P {
            if self.assign[p as usize].leader != k {
                continue;
            }
            let c0 = self.group_committed(k, p);
            let electable: Vec<usize> = (0..self.n.len())
                .filter(|&i| i != k && committed[p as usize].contains(&self.n[i].id))
                .collect();
            wait_sync("electable replicas applied the group offsets", || {
                electable.iter().all(|&i| self.group_committed(i, p) >= c0)
            })
            .await;
            floors.insert(p, c0);
        }
        self.n[k].mgr.shutdown().await;
        if let Some(c) = self.consumers[k].take() {
            c.abort();
        }
        self.killed.insert(k);
        for p in 0..P {
            if self.assign[p as usize].leader != k {
                continue;
            }
            let cands: Vec<usize> = (0..self.n.len())
                .filter(|&i| i != k && committed[p as usize].contains(&self.n[i].id))
                .filter(|i| self.assign[p as usize].replicas.contains(i))
                .collect();
            assert!(!cands.is_empty(), "p{p}: no electable replica");
            // Prefer a new replica (as a preferred-leader election would later).
            let new: Vec<usize> = cands.iter().copied().filter(|i| NEW.contains(i)).collect();
            let pool = if new.is_empty() { &cands } else { &new };
            let to = pool[rng.below(pool.len() as u64) as usize];
            let isr: Vec<String> = committed[p as usize]
                .iter()
                .filter(|id| **id != self.n[k].id)
                .cloned()
                .collect();
            let replicas = self.assign[p as usize].replicas.clone();
            self.move_leadership(p, to, replicas, isr).await;
            let floor = floors.get(&p).copied().flatten();
            assert!(
                self.group_committed(to, p) >= floor,
                "p{p}: group offsets lost in the failover after the kill ({:?} < {floor:?})",
                self.group_committed(to, p)
            );
        }
    }
}

fn spawn_producer(p: u32, w: &World) -> JoinHandle<()> {
    let brokers: Vec<_> = w.n.iter().map(|n| n.broker.clone()).collect();
    let leaders = w.leaders.clone();
    let obs = w.obs.clone();
    tokio::spawn(async move {
        let mut i = 0u64;
        loop {
            let batch: Vec<String> = (i..i + 3).map(|j| format!("p{p}-{j}")).collect();
            let msgs = batch
                .iter()
                .map(|s| fluxmq_proto::model::PublishMessage {
                    key: None,
                    headers: Default::default(),
                    payload: bytes::Bytes::from(s.clone()),
                })
                .collect();
            let l = leaders.lock()[p as usize];
            // Clients give up after a while (e.g. on a frozen, killed leader).
            let res = tokio::time::timeout(
                Duration::from_secs(2),
                brokers[l].publish_to_partition(CH, p, msgs),
            )
            .await;
            match res {
                Ok(Ok(_)) => {
                    obs.lock().acked.extend(batch);
                    i += 3;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    })
}

fn spawn_consumer(node: &Node, obs: Arc<Mutex<Observed>>) -> JoinHandle<()> {
    let broker = node.broker.clone();
    let name = node.id.clone();
    tokio::spawn(async move {
        loop {
            let Ok(mut sub) = broker.subscribe(CH, "g", info(&name), 50, StartFrom::Earliest)
            else {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            };
            loop {
                let d = match tokio::time::timeout(Duration::from_millis(100), sub.next()).await {
                    Ok(Some(d)) => d,
                    Ok(None) => break,
                    Err(_) => continue,
                };
                let payload = String::from_utf8(d.message.payload.to_vec()).unwrap();
                if is_poison(&payload) {
                    if sub.nack(d.delivery_id, "poison", false).is_ok() {
                        obs.lock().dead.insert(payload);
                    }
                } else if sub.ack(&[d.delivery_id]).is_ok() {
                    obs.lock().consumed.insert(payload);
                }
            }
        }
    })
}

async fn run(seed: u64) {
    let mut rng = Rng(seed | 1);
    let n = cluster(6, P, FsyncPolicy::OsBuffered).await;
    let _ctl: Vec<_> = n.iter().map(auto_commit).collect();
    let assign: Vec<Assign> = (0..P)
        .map(|p| {
            // Rotate the old set so leaders are spread over n1..n3.
            let mut r = OLD.to_vec();
            r.rotate_left(p as usize % 3);
            Assign {
                epoch: 1,
                leader: r[0],
                replicas: r,
            }
        })
        .collect();
    let leaders = Arc::new(Mutex::new(assign.iter().map(|a| a.leader).collect()));
    let mut w = World {
        n,
        killed: BTreeSet::new(),
        assign,
        leaders,
        obs: Arc::default(),
        producers: Vec::new(),
        consumers: Vec::new(),
    };
    for p in 0..P {
        w.lead(p);
        let isr = ids(&w.n, &w.assign[p as usize].replicas);
        w.leader_node(p).mgr.isr_committed(CH, p, 1, isr);
        w.follow_all(p);
    }
    w.producers = (0..P).map(|p| spawn_producer(p, &w)).collect();
    w.consumers =
        w.n.iter()
            .map(|n| Some(spawn_consumer(n, w.obs.clone())))
            .collect();
    tokio::time::sleep(Duration::from_millis(300 + rng.below(300))).await;

    // Which partition's move gets its leader killed, and when (0: during catch-up,
    // 1: after catch-up, before the final replica list).
    let kill_at = (rng.below(4) != 0).then(|| (rng.below(u64::from(P)) as u32, rng.below(2)));
    let throttle = rng.below(2) == 0;
    let mut order: Vec<u32> = (0..P).collect();
    for i in (1..order.len()).rev() {
        order.swap(i, rng.below(i as u64 + 1) as usize);
    }
    for &p in &order {
        // (1) Add the new replicas.
        let old: Vec<usize> = w.assign[p as usize].replicas.clone();
        let mut all = old.clone();
        all.extend(NEW);
        let leader = w.assign[p as usize].leader;
        let isr: Vec<String> = w.n[leader].status(p).committed_isr;
        if throttle {
            for node in &w.n {
                node.mgr.set_catch_up_throttle(CH, p, 64 * 1024);
            }
        }
        {
            let a = &mut w.assign[p as usize];
            a.epoch += 1;
            a.replicas = all;
        }
        w.lead(p);
        w.leader_node(p)
            .mgr
            .isr_committed(CH, p, w.assign[p as usize].epoch, isr);
        w.follow_all(p);
        let kill_now = kill_at.filter(|&(kp, _)| kp == p).map(|(_, when)| when);
        if kill_now == Some(0) {
            tokio::time::sleep(Duration::from_millis(rng.below(60))).await;
            let k = w.assign[p as usize].leader;
            w.kill(k, &mut rng).await;
        }
        // (2) Wait until every new replica is in the ISR.
        let new_ids = ids(&w.n, &NEW);
        eventually(
            &format!("seed {seed:#x}: p{p} new replicas in ISR"),
            Duration::from_secs(20),
            || {
                let st = w.leader_node(p).status(p);
                let ok = new_ids.iter().all(|id| st.isr.contains(id));
                async move { ok }
            },
        )
        .await;
        if throttle {
            for node in &w.n {
                node.mgr.set_catch_up_throttle(CH, p, 0);
            }
        }
        tokio::time::sleep(Duration::from_millis(rng.below(150))).await;
        if kill_now == Some(1) {
            let k = w.assign[p as usize].leader;
            w.kill(k, &mut rng).await;
        }
        // (3) Final replica list; leadership moves to a new replica if needed.
        let cur = w.assign[p as usize].leader;
        let isr: Vec<String> = w
            .leader_node(p)
            .status(p)
            .isr
            .into_iter()
            .filter(|id| new_ids.contains(id))
            .collect();
        if NEW.contains(&cur) {
            let a = &mut w.assign[p as usize];
            a.epoch += 1;
            a.replicas = NEW.to_vec();
            w.lead(p);
            w.leader_node(p)
                .mgr
                .isr_committed(CH, p, w.assign[p as usize].epoch, isr);
            w.follow_all(p);
        } else {
            let to = NEW[rng.below(3) as usize];
            w.move_leadership(p, to, NEW.to_vec(), isr).await;
        }
        let epoch = w.assign[p as usize].epoch;
        // Removed replicas leave and delete their copy; their other partitions are untouched.
        for &o in &old {
            if w.killed.contains(&o) {
                continue;
            }
            let node = &w.n[o];
            let before: Vec<(u32, u64)> = (0..P)
                .filter(|&q| q != p && w.assign[q as usize].replicas.contains(&o))
                .map(|q| (q, node.leo(q)))
                .collect();
            node.offline(p);
            node.mgr.remove_replica(CH, p, epoch).await.unwrap();
            assert_eq!(
                node.leo(p),
                0,
                "seed {seed:#x}: {} still holds p{p}",
                node.id
            );
            for (q, leo) in before {
                assert!(
                    node.leo(q) >= leo,
                    "seed {seed:#x}: {} lost data of p{q}",
                    node.id
                );
            }
        }
    }

    // Stop producing; let consumers drain everything.
    for t in w.producers.drain(..) {
        t.abort();
        let _ = t.await;
    }
    // A publish whose client was aborted may still be queued in a writer: wait until the
    // logs are quiet.
    let leos = |w: &World| -> Vec<u64> { (0..P).map(|p| w.leader_node(p).leo(p)).collect() };
    let mut last = leos(&w);
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let now = leos(&w);
        if now == last {
            break;
        }
        last = now;
    }
    for p in 0..P {
        let l = w.assign[p as usize].leader;
        let leo = w.n[l].leo(p);
        eventually(
            &format!("seed {seed:#x}: p{p} drained"),
            Duration::from_secs(20),
            || {
                let ok = w.n[l].hw(p) == leo && w.group_committed(l, p) == Some(leo);
                async move { ok }
            },
        )
        .await;
    }
    // Consumers are done (everything committed): stop them and take the observations.
    for c in w.consumers.iter_mut().filter_map(Option::take) {
        c.abort();
        let _ = c.await;
    }
    let obs = std::mem::take(&mut *w.obs.lock());
    let mut total = 0;
    for p in 0..P {
        let l = w.assign[p as usize].leader;
        let leo = w.n[l].leo(p);
        wait_sync("new replicas converge", || {
            NEW.iter().all(|&i| w.n[i].leo(p) == leo)
        })
        .await;
        let want = w.n[l].log(p);
        for &i in &NEW {
            let got = w.n[i].log(p);
            if got != want {
                let first = got.iter().zip(&want).position(|(a, b)| a != b);
                for &j in &NEW {
                    eprintln!(
                        "{}: {:?} epochs {:?}",
                        w.n[j].id,
                        w.n[j].status(p),
                        w.n[j].broker.replica(CH, p).unwrap().last_epoch()
                    );
                }
                panic!(
                    "seed {seed:#x}: p{p} differs on {} (len {} vs leader {} len {}, first diff {first:?}; kill {kill_at:?}; assign epoch {} leader {})",
                    w.n[i].id,
                    got.len(),
                    w.n[l].id,
                    want.len(),
                    w.assign[p as usize].epoch,
                    w.n[w.assign[p as usize].leader].id
                );
            }
        }
        let present: BTreeSet<String> = want
            .iter()
            .map(|(_, _, pl, _)| String::from_utf8(pl.to_vec()).unwrap())
            .collect();
        let prefix = format!("p{p}-");
        let acked: Vec<&String> = obs
            .acked
            .iter()
            .filter(|s| s.starts_with(&prefix))
            .collect();
        total += acked.len();
        let lost: Vec<&&String> = acked.iter().filter(|s| !present.contains(**s)).collect();
        assert!(
            lost.is_empty(),
            "seed {seed:#x}: p{p}: {} acked messages lost, e.g. {:?}",
            lost.len(),
            &lost[..lost.len().min(5)]
        );
        let dlq: BTreeSet<String> = w.n[l]
            .broker
            .dlq_list(Some(CH), None, None, 1_000_000)
            .0
            .into_iter()
            .filter(|d| d.message.partition == p)
            .map(|d| String::from_utf8(d.message.payload.to_vec()).unwrap())
            .collect();
        for s in &acked {
            if is_poison(s) {
                assert!(
                    dlq.contains(*s),
                    "seed {seed:#x}: poison {s} missing from the DLQ"
                );
            } else {
                assert!(
                    obs.consumed.contains(*s),
                    "seed {seed:#x}: {s} never consumed"
                );
            }
        }
        // Every entry a consumer dead-lettered is in the final DLQ (DLQ entries survive).
        for s in obs.dead.iter().filter(|s| s.starts_with(&prefix)) {
            if present.contains(s) {
                assert!(dlq.contains(s), "seed {seed:#x}: DLQ entry {s} lost");
            }
        }
    }
    // Removed (alive) nodes hold nothing: their channel directory is gone.
    for &o in &OLD {
        if !w.killed.contains(&o) {
            let dir = w.n[o].dir.path().join("channels").join(CH);
            assert!(
                !dir.exists(),
                "seed {seed:#x}: {} kept {}",
                w.n[o].id,
                dir.display()
            );
        }
    }
    assert!(
        total > 100,
        "seed {seed:#x}: too few acked messages ({total})"
    );
    eprintln!(
        "seed {seed:#x}: moved {P} partitions, kill {kill_at:?}, throttle {throttle}, {total} acked, {} consumed, {} dead-lettered",
        obs.consumed.len(),
        obs.dead.len()
    );
    for node in w.n {
        node.mgr.shutdown().await;
        node.broker.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn moving_every_partition_to_disjoint_replicas_keeps_acked_data_offsets_and_dlq() {
    let seeds: Vec<u64> = match std::env::var("MOVE_SEED") {
        Ok(s) => vec![s.parse().unwrap()],
        // Covers: no kill; kill of the moving partition's leader during catch-up (0xc0ffee,
        // 42) and after catch-up before the final replica list (1, 8, 0x20); with and
        // without the catch-up throttle.
        Err(_) => vec![0x5eed, 0xc0ffee, 42, 1, 8, 0x20],
    };
    for seed in seeds {
        run(seed).await;
    }
}
