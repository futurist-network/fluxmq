//! Property-style test: random leader failovers (from the committed ISR, as the controller
//! does) and random follower stalls while producers run. Invariants checked at the end:
//! every replica holds an identical log, and every acknowledged message is in it.

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use fluxmq_core::FsyncPolicy;
use parking_lot::Mutex;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

async fn run(seed: u64, rounds: usize) {
    let mut rng = Rng(seed | 1);
    let n = cluster(3, 1, FsyncPolicy::OsBuffered).await;
    let _ctl: Vec<_> = n.iter().map(auto_commit).collect();
    let mut leader = 0usize;
    let mut epoch = 1u64;
    {
        let all: Vec<&Node> = n.iter().collect();
        n[0].lead(0, epoch, &all, 2);
        n[1].follow(0, epoch, &n[0]);
        n[2].follow(0, epoch, &n[0]);
    }
    let acked: Arc<Mutex<BTreeSet<String>>> = Arc::default();
    let mut next_id = 0usize;
    let (mut failovers, mut divergent) = (0, 0);
    for round in 0..rounds {
        // Producer against the current leader.
        let broker = n[leader].broker.clone();
        let acked_p = acked.clone();
        let base = next_id;
        next_id += 1_000_000;
        let producer = tokio::spawn(async move {
            let mut i = base;
            loop {
                let batch = msgs(i..i + 3);
                let ids: Vec<String> = batch
                    .iter()
                    .map(|m| String::from_utf8(m.payload.to_vec()).unwrap())
                    .collect();
                i += 3;
                if broker.publish_to_partition(CH, 0, batch).await.is_ok() {
                    acked_p.lock().extend(ids);
                } else {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        });
        // Sometimes stall a follower long enough to fall out of the ISR.
        let followers: Vec<usize> = (0..3).filter(|&i| i != leader).collect();
        let stalled = (rng.below(3) == 0).then(|| followers[rng.below(2) as usize]);
        if let Some(s) = stalled {
            n[s].offline(0);
        }
        tokio::time::sleep(Duration::from_millis(30 + rng.below(150))).await;
        if let Some(s) = stalled
            && rng.below(2) == 0
        {
            n[s].follow(0, epoch, &n[leader]);
        }
        tokio::time::sleep(Duration::from_millis(rng.below(500))).await;
        // Failover: the controller elects from the committed ISR of the current leader.
        let st = n[leader].status(0);
        let candidates: Vec<usize> = (0..3)
            .filter(|&i| i != leader && st.committed_isr.contains(&n[i].id))
            .collect();
        if candidates.is_empty() {
            producer.abort();
            continue;
        }
        let new_leader = candidates[rng.below(candidates.len() as u64) as usize];
        failovers += 1;
        epoch += 1;
        let all: Vec<&Node> = n.iter().collect();
        let committed: Vec<String> = st.committed_isr.clone();
        n[new_leader].lead(0, epoch, &all, 2);
        n[new_leader].mgr.isr_committed(CH, 0, epoch, committed);
        for i in 0..3 {
            if i != new_leader {
                n[i].follow(0, epoch, &n[new_leader]);
            }
        }
        producer.abort();
        let _ = producer.await;
        // The demoted leader may hold an unacked suffix the new leader lacks.
        if n[leader].leo(0) > n[new_leader].leo(0) {
            divergent += 1;
        }
        leader = new_leader;
        let _ = round;
    }
    // Let everything converge, then publish one final acked batch through the last leader.
    n[leader]
        .broker
        .publish_to_partition(CH, 0, msgs(next_id..next_id + 3))
        .await
        .unwrap();
    let leo = n[leader].leo(0);
    wait_sync("replicas converge", || (0..3).all(|i| n[i].leo(0) == leo)).await;
    let want = n[leader].log(0);
    for node in &n {
        assert_eq!(node.log(0), want, "seed {seed}: log of {} differs", node.id);
    }
    let present: BTreeSet<String> = want
        .iter()
        .map(|(_, _, p, _)| String::from_utf8(p.to_vec()).unwrap())
        .collect();
    let acked: BTreeSet<String> = acked.lock().clone();
    let missing: Vec<&String> = acked.iter().filter(|id| !present.contains(*id)).collect();
    assert!(
        missing.is_empty(),
        "seed {seed}: {} acked messages lost, e.g. {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    assert!(!acked.is_empty());
    eprintln!(
        "seed {seed:#x}: {failovers} failovers ({divergent} with a divergent former leader), {} acked, final log {}",
        acked.len(),
        want.len()
    );
    for node in n {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_failovers_keep_logs_identical_and_acked_messages() {
    let seeds: Vec<u64> = match std::env::var("FAILOVER_SEED") {
        Ok(s) => vec![s.parse().unwrap()],
        Err(_) => vec![0x5eed, 0xc0ffee, 42],
    };
    for seed in seeds {
        run(seed, 8).await;
    }
}
