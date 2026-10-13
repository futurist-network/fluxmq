//! Reconnect, publish retry and cluster-aware routing against the fake broker.

mod common;

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use common::{Behavior, FakeBroker, FakeCluster, eventually};
use fluxmq_client::{
    BatchingOptions, Client, ClientError, ClientOptions, ConnectionState, ErrorCode,
    ReconnectPolicy, StartFrom, SubscribeOptions, WireError, message,
};
use fluxmq_proto::{ChannelConfig, ChannelMode};

const WAIT: Duration = Duration::from_secs(10);

fn fast_reconnect() -> ReconnectPolicy {
    ReconnectPolicy::default().backoff(Duration::from_millis(20), Duration::from_millis(200))
}

fn opts(addr: impl Into<String>) -> ClientOptions {
    ClientOptions::new(addr, "rc-test").reconnect(fast_reconnect())
}

fn earliest(prefetch: u32) -> SubscribeOptions {
    SubscribeOptions::default()
        .prefetch(prefetch)
        .start(StartFrom::Earliest)
}

async fn single() -> (FakeBroker, Client) {
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(opts(broker.addr())).await.unwrap();
    client
        .create_channel(ChannelConfig::new("ch", ChannelMode::Stream))
        .await
        .unwrap();
    (broker, client)
}

async fn wait_state(client: &Client, want: ConnectionState) {
    let mut rx = client.state();
    tokio::time::timeout(WAIT, rx.wait_for(|s| *s == want))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "state {want:?} not reached, now {:?}",
                client.connection_state()
            )
        })
        .expect("state watch closed");
}

fn payload_str(d: &fluxmq_client::Delivery) -> String {
    String::from_utf8(d.payload().to_vec()).unwrap()
}

// ---- single node -----------------------------------------------------------------------------

#[tokio::test]
async fn single_node_fallback_when_metadata_unsupported() {
    let (broker, client) = single().await;
    assert!(client.metadata().is_none());
    assert_eq!(client.connection_state(), ConnectionState::Connected);
    client
        .publish("ch", message("x").key("k").build())
        .await
        .unwrap();
    let pubs = broker.rec(|r| r.publishes.clone());
    assert_eq!(
        pubs,
        vec![("ch".to_string(), None, 1)],
        "v1 behaviour: partition None"
    );
    assert!(broker.rec(|r| r.metadata_requests) >= 1);
}

#[tokio::test]
async fn state_transitions_and_reconnect_after_restart() {
    let (broker, client) = single().await;
    let mut states = client.state();
    let seen = tokio::spawn(async move {
        let mut v = vec![*states.borrow_and_update()];
        while states.changed().await.is_ok() {
            let s = *states.borrow_and_update();
            v.push(s);
            if s == ConnectionState::Closed {
                break;
            }
        }
        v
    });
    broker.restart(Duration::from_millis(300)).await;
    wait_state(&client, ConnectionState::Connected).await;
    eventually(WAIT, || (client.reconnects() == 1).then_some(())).await;
    client.ping().await.unwrap();
    assert_eq!(broker.rec(|r| r.hellos.len()), 2, "re-handshake");
    client.close();
    let seen = seen.await.unwrap();
    assert_eq!(
        seen,
        vec![
            ConnectionState::Connected,
            ConnectionState::Reconnecting,
            ConnectionState::Connected,
            ConnectionState::Closed
        ]
    );
}

#[tokio::test]
async fn seeds_skip_unreachable_and_initial_retry() {
    let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let dead_addr = dead.local_addr().unwrap().to_string();
    drop(dead);
    let broker = FakeBroker::start(Behavior::default()).await;
    let client = Client::connect(
        ClientOptions::with_seeds([dead_addr.clone(), broker.addr()], "s")
            .reconnect(fast_reconnect()),
    )
    .await
    .unwrap();
    client.ping().await.unwrap();

    // retry_initial_connect: the broker comes up later on the dead address.
    let addr = dead_addr.clone();
    let late = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        FakeBroker::start_on(&addr, Behavior::default()).await
    });
    let client = Client::connect(
        ClientOptions::new(dead_addr, "late")
            .reconnect(fast_reconnect().retry_initial_connect(true)),
    )
    .await
    .unwrap();
    client.ping().await.unwrap();
    drop(late.await.unwrap());
}

#[tokio::test]
async fn reconnect_gives_up_after_max_attempts() {
    let (broker, client) = single().await;
    let client = {
        drop(client);
        Client::connect(opts(broker.addr()).reconnect(fast_reconnect().max_attempts(Some(2))))
            .await
            .unwrap()
    };
    let mut consumer = client.subscribe("ch", "g", earliest(10)).await.unwrap();
    broker.stop();
    wait_state(&client, ConnectionState::Closed).await;
    let item = tokio::time::timeout(WAIT, consumer.next()).await.unwrap();
    assert!(
        matches!(item, Some(Err(ClientError::Closed(_)))),
        "{item:?}"
    );
    assert!(consumer.next().await.is_none());
    assert!(matches!(client.ping().await, Err(ClientError::Closed(_))));
}

#[tokio::test]
async fn publish_retries_on_connection_drop() {
    let (broker, client) = single().await;
    broker.drop_next_publishes(1);
    let id = client.publish("ch", message("a").build()).await.unwrap();
    assert_eq!(id.offset, 0);
    assert_eq!(broker.messages("ch").len(), 1);
    assert_eq!(broker.rec(|r| r.publish_frames), 2);
    assert!(client.reconnects() >= 1);
}

#[tokio::test]
async fn publish_retry_can_duplicate() {
    // Documented at-least-once behaviour: the broker stored the batch but the answer was lost.
    let (broker, client) = single().await;
    broker.store_then_drop_next_publishes(1);
    client.publish("ch", message("dup").build()).await.unwrap();
    let stored = broker.messages("ch");
    assert_eq!(stored.len(), 2);
    assert!(stored.iter().all(|m| m.payload.as_ref() == b"dup"));
}

#[tokio::test]
async fn publish_waits_for_restart_and_respects_timeout() {
    let (broker, client) = single().await;
    broker.stop();
    let c = client.clone();
    let pending = tokio::spawn(async move { c.publish("ch", message("late").build()).await });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(!pending.is_finished());
    broker.start_again().await;
    tokio::time::timeout(WAIT, pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(broker.messages("ch").len(), 1);

    // Down for longer than publish_timeout → retryable connection error.
    let client = Client::connect(opts(broker.addr()).publish_timeout(Duration::from_millis(300)))
        .await
        .unwrap();
    broker.stop();
    let t = std::time::Instant::now();
    let err = client
        .publish("ch", message("x").build())
        .await
        .unwrap_err();
    assert!(err.is_connection_error(), "{err}");
    assert!(t.elapsed() < Duration::from_secs(3), "{:?}", t.elapsed());
}

#[tokio::test]
async fn overloaded_is_retried_but_forbidden_and_bad_request_are_not() {
    let (broker, client) = single().await;
    broker.inject_publish_error(WireError::new(ErrorCode::Overloaded, "not enough replicas"));
    broker.inject_publish_error(WireError::new(ErrorCode::Overloaded, "queue full"));
    client.publish("ch", message("a").build()).await.unwrap();
    assert_eq!(broker.rec(|r| r.publish_frames), 3);

    broker.inject_publish_error(WireError::new(ErrorCode::Forbidden, "no publish"));
    let err = client
        .publish("ch", message("b").build())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::Forbidden));
    assert_eq!(broker.rec(|r| r.publish_frames), 4, "Forbidden not retried");

    broker.inject_publish_error(WireError::new(ErrorCode::BadRequest, "bad"));
    let err = client
        .publish("ch", message("c").build())
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(ErrorCode::BadRequest));
    assert_eq!(
        broker.rec(|r| r.publish_frames),
        5,
        "BadRequest not retried"
    );
}

#[tokio::test]
async fn consumer_resubscribes_after_restart_and_drops_stale_deliveries() {
    let (broker, client) = single().await;
    client
        .publish_batch(
            "ch",
            (0..10).map(|i| message(format!("{i}")).build()).collect(),
        )
        .await
        .unwrap();
    let mut consumer = client.subscribe("ch", "g", earliest(4)).await.unwrap();
    let first_sub = consumer.subscription_id();
    // Take 4 (prefetch), ack 2, hold 2 across the restart.
    let mut held = Vec::new();
    for i in 0..4 {
        let d = consumer.next().await.unwrap().unwrap();
        if i < 2 {
            d.ack().await.unwrap();
        } else {
            held.push(d);
        }
    }
    tokio::time::sleep(Duration::from_millis(30)).await; // acks flushed
    broker.restart(Duration::from_millis(200)).await;
    wait_state(&client, ConnectionState::Connected).await;

    // Acks for deliveries of the dead subscription are no-ops.
    for d in held {
        assert!(d.is_stale());
        d.ack().await.unwrap();
    }
    let mut got: Vec<String> = Vec::new();
    while got.len() < 8 {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .expect("delivery after resubscribe")
            .unwrap()
            .unwrap();
        got.push(payload_str(&d));
        d.ack().await.unwrap();
    }
    // 2..=3 were unacked at the restart → redelivered; nothing lost.
    let set: HashSet<_> = got.iter().cloned().collect();
    let expected: HashSet<_> = (2..10).map(|i| i.to_string()).collect();
    assert_eq!(set, expected, "{got:?}");
    assert_ne!(consumer.subscription_id(), first_sub);
    let subs = broker.rec(|r| r.subscribes.clone());
    assert_eq!(subs.len(), 2);
    assert!(
        subs.iter().all(|(c, g, _)| c == "ch" && g == "g"),
        "same group"
    );
    // No ack was ever sent for the new subscription with an id of the old one.
    let acks = broker.rec(|r| r.acks_by_sub.clone());
    assert!(
        acks.iter()
            .all(|(s, _)| *s == first_sub || *s == consumer.subscription_id())
    );
}

#[tokio::test]
async fn consumer_survives_drop_while_idle_and_receives_new_messages() {
    let (broker, client) = single().await;
    let mut consumer = client.subscribe("ch", "g", earliest(10)).await.unwrap();
    broker.kill_connections();
    wait_state(&client, ConnectionState::Connected).await;
    eventually(WAIT, || (broker.live_subs() == 1).then_some(())).await;
    client
        .publish("ch", message("after").build())
        .await
        .unwrap();
    let d = tokio::time::timeout(WAIT, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(payload_str(&d), "after");
    d.ack().await.unwrap();
}

#[tokio::test]
async fn forbidden_on_resubscribe_is_terminal() {
    let (broker, client) = single().await;
    let mut consumer = client.subscribe("ch", "g", earliest(10)).await.unwrap();
    broker.inject_subscribe_error(WireError::new(ErrorCode::Forbidden, "token revoked"));
    broker.kill_connections();
    let item = tokio::time::timeout(WAIT, consumer.next()).await.unwrap();
    match item {
        Some(Err(e)) => assert_eq!(e.code(), Some(ErrorCode::Forbidden), "{e}"),
        other => panic!("expected Forbidden, got {other:?}"),
    }
    assert!(consumer.next().await.is_none());
    // The client itself keeps working.
    client.ping().await.unwrap();
}

#[tokio::test]
async fn deleted_channel_is_terminal_for_the_consumer() {
    let (broker, client) = single().await;
    let mut consumer = client.subscribe("ch", "g", earliest(10)).await.unwrap();
    broker.delete_channel_local("ch");
    broker.kill_connections();
    let item = tokio::time::timeout(WAIT, consumer.next()).await.unwrap();
    match item {
        Some(Err(e)) => assert_eq!(e.code(), Some(ErrorCode::NotFound), "{e}"),
        other => panic!("expected NotFound, got {other:?}"),
    }
}

#[tokio::test]
async fn ordered_batching_producer_keeps_per_key_order_across_drops() {
    let (broker, client) = single().await;
    let bp = client.batching_producer(
        "ch",
        BatchingOptions {
            linger: Duration::from_millis(2),
            max_batch: 20,
            ordered: true,
            ..Default::default()
        },
    );
    let mut futs = Vec::new();
    for i in 0..300 {
        if i == 100 {
            broker.store_then_drop_next_publishes(1);
        }
        if i == 200 {
            broker.drop_next_publishes(2);
        }
        futs.push(
            bp.send(message(format!("{i}")).key(format!("k{}", i % 3)))
                .await
                .unwrap(),
        );
        if i % 50 == 0 {
            tokio::task::yield_now().await;
        }
    }
    for f in futs {
        f.await.unwrap();
    }
    bp.close().await;
    let stored = broker.messages("ch");
    // Per key, the sequence of first occurrences is strictly increasing (duplicates aside).
    let mut last: HashMap<String, i64> = HashMap::new();
    let mut seen = HashSet::new();
    for m in &stored {
        let v: i64 = std::str::from_utf8(&m.payload).unwrap().parse().unwrap();
        if !seen.insert(v) {
            continue;
        }
        let k = m.key.clone().unwrap();
        let prev = last.insert(k.clone(), v).unwrap_or(-1);
        assert!(v > prev, "key {k}: {v} after {prev}");
    }
    assert_eq!(seen.len(), 300, "nothing lost");
}

// ---- cluster ---------------------------------------------------------------------------------

async fn cluster_client(c: &FakeCluster) -> Client {
    Client::connect(ClientOptions::with_seeds(c.seeds(), "cl").reconnect(fast_reconnect()))
        .await
        .unwrap()
}

#[tokio::test]
async fn publish_routes_to_partition_leaders() {
    let cluster = FakeCluster::start(3).await;
    cluster.create_channel("orders", &["n1", "n2", "n3", "n1"]);
    let client = cluster_client(&cluster).await;
    let meta = client.metadata().expect("cluster metadata");
    assert_eq!(meta.nodes.len(), 3);
    let keys: Vec<String> = (0..40).map(|i| format!("order-{i}")).collect();
    let ids = client
        .publish_batch(
            "orders",
            keys.iter()
                .map(|k| message(k.clone()).key(k.clone()).build())
                .collect(),
        )
        .await
        .unwrap();
    let leaders = ["n1", "n2", "n3", "n1"];
    for (k, id) in keys.iter().zip(&ids) {
        let p = crc32fast::hash(k.as_bytes()) % 4;
        assert_eq!(
            id.partition, p,
            "client-side partitioning matches core for {k}"
        );
        let node = cluster.node(leaders[p as usize]);
        assert!(
            node.messages("orders")
                .iter()
                .any(|m| m.key.as_deref() == Some(k) && m.partition == p),
            "{k} not on {}",
            node.node_id
        );
    }
    for n in &cluster.nodes {
        assert!(n.rec(|r| r.publishes.iter().all(|(_, p, _)| p.is_some())));
    }
    assert_eq!(cluster.all_messages("orders").len(), 40);
    // Unkeyed messages of one call stick to one partition (one batch); successive calls move
    // round-robin over all partitions.
    let mut parts = HashSet::new();
    for c in 0..4 {
        let ids = client
            .publish_batch(
                "orders",
                (0..8)
                    .map(|i| message(format!("{c}-{i}")).build())
                    .collect(),
            )
            .await
            .unwrap();
        let call: HashSet<u32> = ids.iter().map(|i| i.partition).collect();
        assert_eq!(call.len(), 1, "one partition per call");
        parts.extend(call);
    }
    assert_eq!(parts.len(), 4);
}

#[tokio::test]
async fn not_leader_refreshes_metadata_and_retries_on_new_leader() {
    let cluster = FakeCluster::start(3).await;
    cluster.create_channel("orders", &["n1", "n1"]);
    let client = cluster_client(&cluster).await;
    let key = "8842";
    let p = crc32fast::hash(key.as_bytes()) % 2;
    // Leadership moves without a MetadataChanged push: the client still thinks n1 leads.
    cluster.set_leader("orders", p, "n2");
    let id = client
        .publish("orders", message("x").key(key).build())
        .await
        .unwrap();
    assert_eq!(id.partition, p);
    assert_eq!(cluster.node("n2").messages("orders").len(), 1);
    assert!(cluster.node("n1").messages("orders").is_empty());
    assert_eq!(
        cluster.node("n1").rec(|r| r.publish_frames),
        1,
        "one NotLeader"
    );
}

#[tokio::test]
async fn consumer_merges_nodes_and_routes_acks_to_origin() {
    let cluster = FakeCluster::start(3).await;
    cluster.create_channel("orders", &["n1", "n2", "n3"]);
    let client = cluster_client(&cluster).await;
    let mut consumer = client.subscribe("orders", "g", earliest(64)).await.unwrap();
    assert_eq!(consumer.subscription_ids().len(), 3);
    client
        .publish_batch(
            "orders",
            (0..60)
                .map(|i| message(format!("{i}")).key(format!("k{i}")).build())
                .collect(),
        )
        .await
        .unwrap();
    let mut per_node: HashMap<String, Vec<u64>> = HashMap::new();
    let mut got = HashSet::new();
    while got.len() < 60 {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        got.insert(payload_str(&d));
        per_node
            .entry(d.node_id().to_string())
            .or_default()
            .push(d.delivery_id());
        d.ack().await.unwrap();
    }
    assert_eq!(per_node.len(), 3, "deliveries from all three nodes");
    tokio::time::sleep(Duration::from_millis(50)).await;
    for n in &cluster.nodes {
        let mut acked = n.rec(|r| r.acked_ids());
        let mut expected = per_node[&n.node_id].clone();
        acked.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            acked, expected,
            "acks for {} went to {}",
            n.node_id, n.node_id
        );
        assert_eq!(n.messages("orders").len(), expected.len());
    }
}

#[tokio::test]
async fn metadata_changed_moves_subscriptions() {
    let cluster = FakeCluster::start(3).await;
    cluster.create_channel("orders", &["n1", "n2"]);
    let client = cluster_client(&cluster).await;
    let mut consumer = client.subscribe("orders", "g", earliest(16)).await.unwrap();
    let nodes: Vec<String> = consumer
        .subscription_ids()
        .into_iter()
        .map(|(n, _)| n)
        .collect();
    assert_eq!(nodes, vec!["n1", "n2"]);

    // Partition 1 moves n2 → n3; n2 announces it.
    let epoch = cluster.set_leader("orders", 1, "n3");
    cluster.node("n2").push_metadata_changed(epoch);
    eventually(WAIT, || {
        let ids: Vec<String> = consumer
            .subscription_ids()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        (ids == vec!["n1", "n3"]).then_some(())
    })
    .await;
    eventually(WAIT, || {
        (cluster.node("n2").rec(|r| r.unsubscribes.len()) == 1).then_some(())
    })
    .await;
    assert_eq!(client.metadata().unwrap().epoch, epoch);

    // New messages for partition 1 go to n3 and arrive through the new subscription.
    let key = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| crc32fast::hash(k.as_bytes()) % 2 == 1)
        .unwrap();
    client
        .publish("orders", message("moved").key(key).build())
        .await
        .unwrap();
    let d = tokio::time::timeout(WAIT, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(payload_str(&d), "moved");
    assert_eq!(d.node_id(), "n3");
    d.ack().await.unwrap();
}

#[tokio::test]
async fn cluster_node_restart_resubscribes_only_that_node() {
    let cluster = FakeCluster::start(2).await;
    cluster.create_channel("orders", &["n1", "n2"]);
    let client = cluster_client(&cluster).await;
    let mut consumer = client.subscribe("orders", "g", earliest(16)).await.unwrap();
    let before = consumer.subscription_ids();
    cluster.node("n2").restart(Duration::from_millis(200)).await;
    eventually(WAIT, || {
        let now = consumer.subscription_ids();
        (now.len() == 2 && now[1].1 != before[1].1).then_some(())
    })
    .await;
    assert_eq!(
        consumer.subscription_ids()[0],
        before[0],
        "n1 subscription untouched"
    );
    let key = (0..)
        .map(|i| format!("k{i}"))
        .find(|k| crc32fast::hash(k.as_bytes()) % 2 == 1)
        .unwrap();
    client
        .publish("orders", message("p1").key(key).build())
        .await
        .unwrap();
    let d = tokio::time::timeout(WAIT, consumer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(d.node_id(), "n2");
}
