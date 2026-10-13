//! A tiny in-process broker speaking the FluxMQ wire protocol, for SDK tests.
//!
//! Supports consumer groups (shared cursor, outstanding deliveries go back to the group when a
//! subscription/connection ends), connection drops, restarts on the same port, error injection,
//! `MetadataChanged` pushes and multi-node "clusters" sharing one `ClusterMetadata`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use fluxmq_proto::batch::{self, Compression, Records, codec};
use fluxmq_proto::cluster::{ClusterMetadata, NodeInfo, NodeStatus, PartitionAssignment};
use fluxmq_proto::wire::{
    self, AckMode, ClientFrame, ClientRequest, ClientStats, Delivery, DeliveryRange, ErrorCode,
    ServerFrame, ServerResponse, WireError,
};
use fluxmq_proto::{ChannelConfig, ChannelMode, Message, MessageId, StartFrom};
use futures::{SinkExt, StreamExt};
use parking_lot::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite, LengthDelimitedCodec};

pub type SharedMeta = Arc<Mutex<ClusterMetadata>>;

#[derive(Default, Clone)]
pub struct Behavior {
    pub token: Option<String>,
    /// Never answer `Publish` requests (timeout tests).
    pub swallow_publish: bool,
    /// Answer requests after a random 0..N ms delay, i.e. out of order.
    pub random_delay_ms: u64,
    /// Node id reported in `HelloOk` (default "n1").
    pub node_id: Option<String>,
    /// Serve this metadata (cluster mode); `None` = answer `Metadata` with `BadRequest`.
    pub cluster: Option<SharedMeta>,
    /// Newest client protocol version accepted (`None` = 4); newer `Hello`s are refused
    /// like real brokers do (`ProtocolViolation` "... (broker speaks 2..=N)", then the
    /// connection closes). v4 requests on a connection that negotiated less are refused.
    pub max_protocol: Option<u16>,
    /// Refuse newer `Hello`s without naming the supported versions.
    pub terse_refusal: bool,
    /// Compress raw (`DeliverRecords`) batches like this.
    pub deliver_compression: Compression,
    /// Corrupt the CRC of the first raw batch sent.
    pub corrupt_first_crc: bool,
    /// Raw batches: envelopes cover aligned offset blocks of this size (records of the block
    /// outside the delivered range are included, so the client must skip them). 0 = envelopes
    /// cover exactly the delivered records.
    pub envelope_align: u64,
}

#[derive(Default, Clone)]
pub struct Recorded {
    pub hellos: Vec<String>,
    /// Protocol version of every `Hello` received (refused ones included).
    pub hello_versions: Vec<u16>,
    pub ack_frames: Vec<Vec<u64>>,
    pub ack_frame_req_ids: Vec<u64>,
    /// (subscription id, delivery ids) per Ack frame.
    pub acks_by_sub: Vec<(u64, Vec<u64>)>,
    pub nacks: Vec<(u64, String, bool)>,
    pub stats: Vec<ClientStats>,
    pub publish_frames: usize,
    /// (channel, partition, message count) per Publish frame that was answered.
    pub publishes: Vec<(String, Option<u32>, usize)>,
    pub unsubscribes: Vec<u64>,
    pub subscribes: Vec<(String, String, u64)>,
    pub metadata_requests: usize,
    /// (channel, partition, record count, compression) per `PublishBatch` that was answered.
    pub publish_batches: Vec<(String, Option<u32>, u32, Compression)>,
    /// Ranges per `AckRanges` frame (their ids are also in `ack_frames`).
    pub ack_range_frames: Vec<Vec<DeliveryRange>>,
    /// (subscription, partition, offset, req_id) per `Commit`.
    pub commits: Vec<(u64, u32, u64, u64)>,
    /// `SubscribeWith` options (`None` = plain `Subscribe`), in order.
    pub subscribe_options: Vec<Option<wire::SubscribeOptions>>,
    /// v4 requests received on connections that negotiated v2/v3 (must stay 0).
    pub v4_on_old_protocol: usize,
    /// `DeliverRecords` frames sent.
    pub raw_frames: usize,
}

impl Recorded {
    pub fn acked_ids(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.ack_frames.iter().flatten().copied().collect();
        v.sort_unstable();
        v
    }
}

struct SubState {
    channel: String,
    group: String,
    prefetch: u32,
    outstanding: HashMap<u64, (usize, u32)>, // delivery_id -> (log index, attempt)
    notify: Arc<Notify>,
    /// Send fresh messages as `DeliverRecords`.
    raw: bool,
    cumulative: bool,
}

#[derive(Default)]
struct GroupState {
    cursor: usize,
    redeliver: VecDeque<(usize, u32)>,
}

#[derive(Default)]
struct State {
    channels: HashMap<String, ChannelConfig>,
    logs: HashMap<String, Vec<Message>>,
    groups: HashMap<(String, String), GroupState>,
    subs: HashMap<u64, SubState>,
    rec: Recorded,
    publish_errors: VecDeque<WireError>,
    subscribe_errors: VecDeque<WireError>,
    /// Drop the connection when the next N publishes arrive (without storing them).
    drop_publishes: usize,
    /// Store the next N publishes, then drop the connection without answering.
    store_then_drop: usize,
    crc_corrupted: bool,
    corrupt_next_crc: bool,
    behavior_align: u64,
    behavior_compression: Compression,
}

impl State {
    fn notify_group(&self, channel: &str, group: &str) {
        for s in self
            .subs
            .values()
            .filter(|s| s.channel == channel && s.group == group)
        {
            s.notify.notify_one();
        }
    }

    fn remove_sub(&mut self, id: u64) {
        if let Some(s) = self.subs.remove(&id) {
            let g = self
                .groups
                .entry((s.channel.clone(), s.group.clone()))
                .or_default();
            let mut back: Vec<_> = s.outstanding.into_values().collect();
            back.sort_unstable();
            for (idx, attempt) in back {
                g.redeliver.push_back((idx, attempt + 1));
            }
            self.notify_group(&s.channel, &s.group);
        }
    }
}

pub struct FakeBroker {
    pub addr: SocketAddr,
    pub node_id: String,
    state: Arc<Mutex<State>>,
    kill: Arc<watch::Sender<Option<String>>>, // Some("") = drop sockets; Some(reason) = Goodbye
    push: broadcast::Sender<ServerFrame>,
    behavior: Behavior,
    accept: Mutex<Option<JoinHandle<()>>>,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

impl FakeBroker {
    pub async fn start(behavior: Behavior) -> Self {
        Self::start_on("127.0.0.1:0", behavior).await
    }

    pub async fn start_on(addr: &str, behavior: Behavior) -> Self {
        let listener = TcpListener::bind(addr).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let (kill, _) = watch::channel(None);
        let (push, _) = broadcast::channel(64);
        let b = FakeBroker {
            addr,
            node_id: behavior.node_id.clone().unwrap_or_else(|| "n1".into()),
            state: Arc::new(Mutex::new(State {
                corrupt_next_crc: behavior.corrupt_first_crc,
                behavior_align: behavior.envelope_align,
                behavior_compression: behavior.deliver_compression,
                ..State::default()
            })),
            kill: Arc::new(kill),
            push,
            behavior,
            accept: Mutex::new(None),
        };
        b.spawn_accept(listener);
        b
    }

    fn spawn_accept(&self, listener: TcpListener) {
        let st = self.state.clone();
        let b = self.behavior.clone();
        let kill = self.kill.clone();
        let push = self.push.clone();
        let node_id = self.node_id.clone();
        let h = tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(handle_conn(
                    sock,
                    st.clone(),
                    b.clone(),
                    node_id.clone(),
                    kill.subscribe(),
                    push.subscribe(),
                ));
            }
        });
        *self.accept.lock() = Some(h);
    }

    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    pub fn rec<R>(&self, f: impl FnOnce(&Recorded) -> R) -> R {
        f(&self.state.lock().rec)
    }

    pub fn messages(&self, channel: &str) -> Vec<Message> {
        self.state
            .lock()
            .logs
            .get(channel)
            .cloned()
            .unwrap_or_default()
    }

    /// Creates a channel directly in the broker state.
    pub fn create_channel_local(&self, cfg: ChannelConfig) {
        let mut st = self.state.lock();
        st.logs.entry(cfg.name.clone()).or_default();
        st.channels.insert(cfg.name.clone(), cfg);
    }

    /// Deletes a channel (subscriptions on it are dropped silently, like a deleted channel).
    pub fn delete_channel_local(&self, name: &str) {
        let mut st = self.state.lock();
        st.channels.remove(name);
        st.logs.remove(name);
        st.subs.retain(|_, s| s.channel != name);
    }

    pub fn inject_publish_error(&self, e: WireError) {
        self.state.lock().publish_errors.push_back(e);
    }

    pub fn inject_subscribe_error(&self, e: WireError) {
        self.state.lock().subscribe_errors.push_back(e);
    }

    /// The next `n` publishes make the broker drop the connection without storing them.
    pub fn drop_next_publishes(&self, n: usize) {
        self.state.lock().drop_publishes = n;
    }

    /// The next `n` publishes are stored, then the connection drops before the answer.
    pub fn store_then_drop_next_publishes(&self, n: usize) {
        self.state.lock().store_then_drop = n;
    }

    /// Number of live subscriptions.
    pub fn live_subs(&self) -> usize {
        self.state.lock().subs.len()
    }

    /// Drops every connection without a goodbye.
    pub fn kill_connections(&self) {
        let _ = self.kill.send(Some(String::new()));
    }

    pub fn goodbye(&self, reason: &str) {
        let _ = self.kill.send(Some(reason.to_string()));
    }

    pub fn push_metadata_changed(&self, epoch: u64) {
        let _ = self.push.send(ServerFrame::MetadataChanged { epoch });
    }

    /// Stops listening and drops every connection ("process killed").
    pub fn stop(&self) {
        if let Some(h) = self.accept.lock().take() {
            h.abort();
        }
        self.kill_connections();
    }

    /// Listens again on the same address; the stored data and group offsets survive.
    pub async fn start_again(&self) {
        let mut tries = 0;
        let listener = loop {
            match TcpListener::bind(self.addr).await {
                Ok(l) => break l,
                Err(e) if tries < 50 => {
                    tries += 1;
                    let _ = e;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("rebind {}: {e}", self.addr),
            }
        };
        self.spawn_accept(listener);
    }

    pub async fn restart(&self, downtime: Duration) {
        self.stop();
        tokio::time::sleep(downtime).await;
        self.start_again().await;
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.stop();
    }
}

/// N fake nodes sharing one `ClusterMetadata`.
pub struct FakeCluster {
    pub nodes: Vec<FakeBroker>,
    pub meta: SharedMeta,
}

impl FakeCluster {
    pub async fn start(n: usize) -> Self {
        Self::start_with(n, Behavior::default()).await
    }

    /// Like [`FakeCluster::start`], every node with `behavior` (node id and metadata are set).
    pub async fn start_with(n: usize, behavior: Behavior) -> Self {
        let meta: SharedMeta = Arc::new(Mutex::new(ClusterMetadata {
            cluster_name: "fake".into(),
            epoch: 1,
            controller: Some("n1".into()),
            nodes: Vec::new(),
            channels: BTreeMap::new(),
            priority_channels: Vec::new(),
        }));
        let mut nodes = Vec::new();
        for i in 1..=n {
            let b = FakeBroker::start(Behavior {
                node_id: Some(format!("n{i}")),
                cluster: Some(meta.clone()),
                ..behavior.clone()
            })
            .await;
            meta.lock().nodes.push(NodeInfo {
                id: b.node_id.clone(),
                client_addr: b.addr(),
                http_addr: String::new(),
                status: NodeStatus::Up,
                is_controller: i == 1,
                is_voter: true,
                version: "fake".into(),
            });
            nodes.push(b);
        }
        FakeCluster { nodes, meta }
    }

    pub fn seeds(&self) -> Vec<String> {
        self.nodes.iter().map(FakeBroker::addr).collect()
    }

    pub fn node(&self, id: &str) -> &FakeBroker {
        self.nodes.iter().find(|n| n.node_id == id).expect("node")
    }

    /// Creates `name` on every node with `leaders[p]` leading partition `p`.
    pub fn create_channel(&self, name: &str, leaders: &[&str]) {
        let mut cfg = ChannelConfig::new(name, ChannelMode::Stream);
        cfg.partitions = leaders.len() as u32;
        for n in &self.nodes {
            n.create_channel_local(cfg.clone());
        }
        let mut m = self.meta.lock();
        m.channels.insert(
            name.to_string(),
            leaders
                .iter()
                .enumerate()
                .map(|(p, l)| PartitionAssignment {
                    partition: p as u32,
                    leader: Some(l.to_string()),
                    replicas: vec![l.to_string()],
                    isr: vec![l.to_string()],
                    epoch: 1,
                })
                .collect(),
        );
        m.epoch += 1;
    }

    /// Moves leadership of `channel`/`partition` to `leader`, bumps the epoch and returns it.
    pub fn set_leader(&self, channel: &str, partition: u32, leader: &str) -> u64 {
        let mut m = self.meta.lock();
        let a = &mut m.channels.get_mut(channel).expect("channel")[partition as usize];
        a.leader = Some(leader.to_string());
        a.epoch += 1;
        m.epoch += 1;
        m.epoch
    }

    /// Every message stored on any node for `channel`.
    pub fn all_messages(&self, channel: &str) -> Vec<Message> {
        self.nodes
            .iter()
            .flat_map(|n| n.messages(channel))
            .collect()
    }
}

fn response(req_id: u64, result: Result<ServerResponse, WireError>) -> ServerFrame {
    ServerFrame::Response { req_id, result }
}

enum Reply {
    Now(Result<ServerResponse, WireError>),
    None,
    /// Drop the connection without answering.
    Drop,
}

async fn handle_conn(
    sock: TcpStream,
    state: Arc<Mutex<State>>,
    behavior: Behavior,
    node_id: String,
    mut kill: watch::Receiver<Option<String>>,
    mut push: broadcast::Receiver<ServerFrame>,
) {
    let (rd, wr) = sock.into_split();
    let codec = LengthDelimitedCodec::builder()
        .big_endian()
        .length_field_length(4)
        .max_frame_length(wire::MAX_FRAME_LEN)
        .new_codec();
    let mut reader = FramedRead::new(rd, codec);
    let mut writer = FramedWrite::new(wr, BytesCodec::new());
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<ServerFrame>();
    let mut my_subs: Vec<u64> = Vec::new();
    let mut tasks = Vec::new();
    let mut greeted = false;
    let mut proto = 0u16;
    loop {
        tokio::select! {
            _ = kill.changed() => {
                let reason = kill.borrow().clone();
                if let Some(r) = reason && !r.is_empty() {
                    let f = wire::encode_frame(&ServerFrame::Goodbye { reason: r }).expect("enc");
                    let _ = writer.send(f).await;
                }
                break;
            }
            Ok(frame) = push.recv(), if greeted => {
                let f = wire::encode_frame(&frame).expect("enc");
                if writer.send(f).await.is_err() { break; }
            }
            Some(frame) = out_rx.recv() => {
                let f = wire::encode_frame(&frame).expect("enc");
                if writer.send(f).await.is_err() { break; }
            }
            next = reader.next() => {
                let Some(Ok(body)) = next else { break };
                let frame: ClientFrame = wire::decode_body(&body).expect("decode client frame");
                let req_id = frame.req_id;
                if !greeted {
                    match frame.body {
                        ClientRequest::Hello { client_name, auth_token, protocol_version, .. } => {
                            state.lock().rec.hello_versions.push(protocol_version);
                            let max = behavior.max_protocol.unwrap_or(4);
                            if protocol_version > max || protocol_version < 2 {
                                let msg = if behavior.terse_refusal {
                                    format!("unsupported protocol version {protocol_version}")
                                } else {
                                    format!("unsupported protocol version {protocol_version} (broker speaks 2..={max})")
                                };
                                let f = wire::encode_frame(&response(req_id, Err(WireError::new(ErrorCode::ProtocolViolation, msg)))).expect("enc");
                                let _ = writer.send(f).await;
                                break;
                            }
                            state.lock().rec.hellos.push(client_name);
                            if behavior.token.is_some() && behavior.token != auth_token {
                                let _ = out_tx.send(response(req_id, Err(WireError::new(ErrorCode::Unauthorized, "bad token"))));
                            } else {
                                greeted = true;
                                proto = protocol_version;
                                let _ = out_tx.send(response(req_id, Ok(ServerResponse::HelloOk {
                                    broker_version: "fake".into(), node_id: node_id.clone(), connection_id: 1,
                                })));
                            }
                        }
                        _ => break,
                    }
                    continue;
                }
                let reply = handle_request(&state, &behavior, &node_id, proto, req_id, frame.body, &out_tx, &mut my_subs, &mut tasks);
                let result = match reply {
                    Reply::Drop => break,
                    Reply::None => continue,
                    Reply::Now(r) => r,
                };
                if req_id == 0 { continue; }
                let frame = response(req_id, result);
                if behavior.random_delay_ms > 0 {
                    let d = (req_id.wrapping_mul(2654435761) >> 3) % behavior.random_delay_ms;
                    let tx = out_tx.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(d)).await;
                        let _ = tx.send(frame);
                    });
                } else {
                    let _ = out_tx.send(frame);
                }
            }
        }
    }
    for t in tasks {
        t.abort();
    }
    let mut st = state.lock();
    for id in my_subs {
        st.remove_sub(id);
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_request(
    state: &Arc<Mutex<State>>,
    behavior: &Behavior,
    node_id: &str,
    proto: u16,
    req_id: u64,
    body: ClientRequest,
    out: &mpsc::UnboundedSender<ServerFrame>,
    my_subs: &mut Vec<u64>,
    tasks: &mut Vec<JoinHandle<()>>,
) -> Reply {
    let mut st = state.lock();
    let is_v4 = matches!(
        body,
        ClientRequest::PublishBatch { .. }
            | ClientRequest::SubscribeWith { .. }
            | ClientRequest::AckRanges { .. }
            | ClientRequest::Commit { .. }
    );
    if is_v4 && proto < 4 {
        st.rec.v4_on_old_protocol += 1;
        return Reply::Now(Err(WireError::new(
            ErrorCode::ProtocolViolation,
            "v4 request on an older protocol",
        )));
    }
    Reply::Now(match body {
        ClientRequest::Hello { .. } => {
            Err(WireError::new(ErrorCode::ProtocolViolation, "dup hello"))
        }
        ClientRequest::PublishBatch {
            channel,
            partition,
            records,
        } => {
            st.rec.publish_frames += 1;
            if behavior.swallow_publish {
                return Reply::None;
            }
            if st.drop_publishes > 0 {
                st.drop_publishes -= 1;
                return Reply::Drop;
            }
            if let Some(e) = st.publish_errors.pop_front() {
                return Reply::Now(Err(e));
            }
            if let Some(e) = check_leader(behavior, node_id, &channel, partition) {
                return Reply::Now(Err(e));
            }
            let plain = match records.compression {
                Compression::None => records.body.to_vec(),
                c => match codec::decompress(c, &records.body, batch::MAX_BATCH_LEN) {
                    Ok(p) => p,
                    Err(e) => {
                        return Reply::Now(Err(WireError::new(
                            ErrorCode::BadRequest,
                            e.to_string(),
                        )));
                    }
                },
            };
            let plain = Bytes::from(plain);
            let decoded: Result<Vec<_>, _> = Records::new(&plain, records.record_count)
                .map(|r| {
                    r.map(|r| {
                        (
                            r.key.map(str::to_owned),
                            r.headers_map(),
                            plain.slice_ref(r.payload),
                        )
                    })
                })
                .collect();
            let decoded = match decoded {
                Ok(d) => d,
                Err(e) => {
                    return Reply::Now(Err(WireError::new(ErrorCode::BadRequest, e.to_string())));
                }
            };
            if partition.is_none() && decoded.iter().any(|(k, _, _)| k.is_some()) {
                return Reply::Now(Err(WireError::new(
                    ErrorCode::BadRequest,
                    "keyed batch spans partitions; send partition: Some(p)",
                )));
            }
            let Some(log) = st.logs.get_mut(&channel) else {
                return Reply::Now(Err(WireError::new(ErrorCode::NotFound, "no channel")));
            };
            let part = partition.unwrap_or(0);
            let base = log.len() as u64;
            let now = fluxmq_proto::now_ms();
            for (key, headers, payload) in decoded {
                let offset = log.len() as u64;
                log.push(Message {
                    partition: part,
                    offset,
                    key,
                    headers,
                    timestamp_ms: now,
                    payload,
                });
            }
            for s in st.subs.values().filter(|s| s.channel == channel) {
                s.notify.notify_one();
            }
            if st.store_then_drop > 0 {
                st.store_then_drop -= 1;
                return Reply::Drop;
            }
            let n = records.record_count;
            st.rec
                .publishes
                .push((channel.clone(), partition, n as usize));
            st.rec
                .publish_batches
                .push((channel, partition, n, records.compression));
            Ok(ServerResponse::PublishBatchOk {
                partition: part,
                base_offset: base,
                count: n,
            })
        }
        ClientRequest::AckRanges {
            subscription_id,
            ranges,
        } => {
            let ids: Vec<u64> = ranges
                .iter()
                .flat_map(|r| r.first..r.first + u64::from(r.count))
                .collect();
            st.rec.ack_range_frames.push(ranges);
            ack_ids(&mut st, req_id, subscription_id, ids);
            Ok(ServerResponse::Ok)
        }
        ClientRequest::Commit {
            subscription_id,
            partition,
            offset,
        } => {
            st.rec
                .commits
                .push((subscription_id, partition, offset, req_id));
            let st = &mut *st;
            if let Some(s) = st.subs.get_mut(&subscription_id) {
                if !s.cumulative {
                    return Reply::Now(Err(WireError::new(
                        ErrorCode::BadRequest,
                        "commit on an individual-ack subscription",
                    )));
                }
                let log = st.logs.get(&s.channel).map(Vec::as_slice).unwrap_or(&[]);
                s.outstanding.retain(|_, (idx, _)| {
                    let m = &log[*idx];
                    !(m.partition == partition && m.offset < offset)
                });
                s.notify.notify_one();
            }
            Ok(ServerResponse::Ok)
        }
        ClientRequest::CreateChannel(cfg) => {
            if st.channels.contains_key(&cfg.name) {
                Err(WireError::new(ErrorCode::AlreadyExists, "exists"))
            } else {
                st.logs.insert(cfg.name.clone(), Vec::new());
                st.channels.insert(cfg.name.clone(), cfg);
                Ok(ServerResponse::ChannelCreated)
            }
        }
        ClientRequest::Publish {
            channel,
            partition,
            messages,
        } => {
            st.rec.publish_frames += 1;
            if behavior.swallow_publish {
                return Reply::None;
            }
            if st.drop_publishes > 0 {
                st.drop_publishes -= 1;
                return Reply::Drop;
            }
            if let Some(e) = st.publish_errors.pop_front() {
                return Reply::Now(Err(e));
            }
            if let (Some(meta), Some(p)) = (&behavior.cluster, partition) {
                let leader = meta
                    .lock()
                    .channels
                    .get(&channel)
                    .and_then(|a| a.get(p as usize))
                    .and_then(|a| a.leader.clone());
                if leader.as_deref() != Some(node_id) {
                    return Reply::Now(Err(WireError::new(
                        ErrorCode::NotLeader,
                        leader.unwrap_or_default(),
                    )));
                }
            }
            let n = messages.len();
            let Some(log) = st.logs.get_mut(&channel) else {
                return Reply::Now(Err(WireError::new(ErrorCode::NotFound, "no channel")));
            };
            let mut ids = Vec::new();
            for m in messages {
                let offset = log.len() as u64;
                let part = partition.unwrap_or(0);
                log.push(Message {
                    partition: part,
                    offset,
                    key: m.key,
                    headers: m.headers,
                    timestamp_ms: fluxmq_proto::now_ms(),
                    payload: m.payload,
                });
                ids.push(MessageId {
                    partition: part,
                    offset,
                });
            }
            for s in st.subs.values().filter(|s| s.channel == channel) {
                s.notify.notify_one();
            }
            if st.store_then_drop > 0 {
                st.store_then_drop -= 1;
                return Reply::Drop;
            }
            st.rec.publishes.push((channel, partition, n));
            Ok(ServerResponse::PublishOk { ids })
        }
        ClientRequest::Subscribe {
            channel,
            group,
            prefetch,
            start,
        } => subscribe(
            state, &mut st, channel, group, prefetch, start, None, out, my_subs, tasks,
        ),
        ClientRequest::SubscribeWith {
            channel,
            group,
            prefetch,
            start,
            options,
        } => subscribe(
            state,
            &mut st,
            channel,
            group,
            prefetch,
            start,
            Some(options),
            out,
            my_subs,
            tasks,
        ),
        ClientRequest::Unsubscribe { subscription_id } => {
            st.rec.unsubscribes.push(subscription_id);
            st.remove_sub(subscription_id);
            Ok(ServerResponse::Ok)
        }
        ClientRequest::Ack {
            subscription_id,
            delivery_ids,
        } => {
            ack_ids(&mut st, req_id, subscription_id, delivery_ids);
            Ok(ServerResponse::Ok)
        }
        ClientRequest::Nack {
            subscription_id,
            delivery_id,
            reason,
            retry,
        } => {
            st.rec.nacks.push((delivery_id, reason, retry));
            let st = &mut *st;
            if let Some(s) = st.subs.get_mut(&subscription_id) {
                if let Some((idx, attempt)) = s.outstanding.remove(&delivery_id)
                    && retry
                {
                    st.groups
                        .entry((s.channel.clone(), s.group.clone()))
                        .or_default()
                        .redeliver
                        .push_back((idx, attempt + 1));
                }
                s.notify.notify_one();
            }
            Ok(ServerResponse::Ok)
        }
        ClientRequest::ReportStats(s) => {
            st.rec.stats.push(s);
            Ok(ServerResponse::Ok)
        }
        ClientRequest::Ping => Ok(ServerResponse::Pong),
        ClientRequest::Metadata { .. } => {
            st.rec.metadata_requests += 1;
            match &behavior.cluster {
                Some(m) => Ok(ServerResponse::Metadata {
                    json: serde_json::to_string(&*m.lock()).expect("json"),
                }),
                None => Err(WireError::new(ErrorCode::BadRequest, "no metadata")),
            }
        }
    })
}

fn check_leader(
    behavior: &Behavior,
    node_id: &str,
    channel: &str,
    partition: Option<u32>,
) -> Option<WireError> {
    let (Some(meta), Some(p)) = (&behavior.cluster, partition) else {
        return None;
    };
    let leader = meta
        .lock()
        .channels
        .get(channel)
        .and_then(|a| a.get(p as usize))
        .and_then(|a| a.leader.clone());
    (leader.as_deref() != Some(node_id))
        .then(|| WireError::new(ErrorCode::NotLeader, leader.unwrap_or_default()))
}

fn ack_ids(st: &mut State, req_id: u64, subscription_id: u64, delivery_ids: Vec<u64>) {
    st.rec.ack_frames.push(delivery_ids.clone());
    st.rec.ack_frame_req_ids.push(req_id);
    st.rec
        .acks_by_sub
        .push((subscription_id, delivery_ids.clone()));
    if let Some(s) = st.subs.get_mut(&subscription_id) {
        for d in delivery_ids {
            s.outstanding.remove(&d);
        }
        s.notify.notify_one();
    }
}

#[allow(clippy::too_many_arguments)]
fn subscribe(
    state: &Arc<Mutex<State>>,
    st: &mut State,
    channel: String,
    group: String,
    prefetch: u32,
    start: StartFrom,
    options: Option<wire::SubscribeOptions>,
    out: &mpsc::UnboundedSender<ServerFrame>,
    my_subs: &mut Vec<u64>,
    tasks: &mut Vec<JoinHandle<()>>,
) -> Result<ServerResponse, WireError> {
    {
        {
            if let Some(e) = st.subscribe_errors.pop_front() {
                return Err(e);
            }
            let Some(log_len) = st.logs.get(&channel).map(Vec::len) else {
                return Err(WireError::new(ErrorCode::NotFound, "no channel"));
            };
            let key = (channel.clone(), group.clone());
            st.groups.entry(key).or_insert_with(|| GroupState {
                cursor: match start {
                    StartFrom::Earliest | StartFrom::TimestampMs(_) => 0,
                    StartFrom::Latest => log_len,
                },
                ..Default::default()
            });
            let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            let notify = Arc::new(Notify::new());
            st.rec.subscribes.push((channel.clone(), group.clone(), id));
            st.rec.subscribe_options.push(options);
            let opts = options.unwrap_or_default();
            st.subs.insert(
                id,
                SubState {
                    channel,
                    group,
                    prefetch,
                    outstanding: HashMap::new(),
                    notify: notify.clone(),
                    raw: options.is_some() && opts.raw_records,
                    cumulative: opts.ack_mode == AckMode::Cumulative,
                },
            );
            my_subs.push(id);
            tasks.push(tokio::spawn(deliver_task(
                state.clone(),
                id,
                notify,
                out.clone(),
            )));
            Ok(ServerResponse::Subscribed {
                subscription_id: id,
            })
        }
    }
}

async fn deliver_task(
    state: Arc<Mutex<State>>,
    sub_id: u64,
    notify: Arc<Notify>,
    out: mpsc::UnboundedSender<ServerFrame>,
) {
    static DELIVERY: AtomicU64 = AtomicU64::new(1);
    loop {
        let notified = notify.notified();
        let frames = {
            let mut st = state.lock();
            let st = &mut *st;
            let Some(s) = st.subs.get_mut(&sub_id) else {
                return;
            };
            let log = st.logs.get(&s.channel).map(Vec::as_slice).unwrap_or(&[]);
            let g = st
                .groups
                .entry((s.channel.clone(), s.group.clone()))
                .or_default();
            let mut frames = Vec::new();
            // Fresh (first-attempt) log indexes collected for a raw frame.
            let mut run: Vec<usize> = Vec::new();
            while (s.outstanding.len() as u32 + run.len() as u32) < s.prefetch {
                if let Some((idx, attempt)) = g.redeliver.pop_front() {
                    let delivery_id = DELIVERY.fetch_add(1, Ordering::Relaxed);
                    s.outstanding.insert(delivery_id, (idx, attempt));
                    frames.push(ServerFrame::Deliver {
                        subscription_id: sub_id,
                        delivery: Delivery {
                            delivery_id,
                            attempt,
                            message: log[idx].clone(),
                        },
                    });
                } else if g.cursor < log.len() {
                    g.cursor += 1;
                    let idx = g.cursor - 1;
                    if s.raw {
                        run.push(idx);
                        continue;
                    }
                    let delivery_id = DELIVERY.fetch_add(1, Ordering::Relaxed);
                    s.outstanding.insert(delivery_id, (idx, 1));
                    frames.push(ServerFrame::Deliver {
                        subscription_id: sub_id,
                        delivery: Delivery {
                            delivery_id,
                            attempt: 1,
                            message: log[idx].clone(),
                        },
                    });
                } else {
                    break;
                }
            }
            // Raw frames: one per partition run of consecutive offsets.
            let mut start = 0;
            while start < run.len() {
                let mut end = start + 1;
                while end < run.len()
                    && run[end] == run[end - 1] + 1
                    && log[run[end]].partition == log[run[start]].partition
                {
                    end += 1;
                }
                let (first, last) = (run[start], run[end - 1]);
                let count = (end - start) as u32;
                let first_id = DELIVERY.fetch_add(u64::from(count), Ordering::Relaxed);
                for (i, idx) in (first..=last).enumerate() {
                    s.outstanding.insert(first_id + i as u64, (idx, 1));
                }
                let envelopes = raw_envelopes(
                    log,
                    first,
                    last + 1,
                    st.behavior_align,
                    st.behavior_compression,
                    st.corrupt_next_crc && !st.crc_corrupted,
                );
                if st.corrupt_next_crc {
                    st.crc_corrupted = true;
                }
                st.rec.raw_frames += 1;
                frames.push(ServerFrame::DeliverRecords {
                    subscription_id: sub_id,
                    partition: log[first].partition,
                    first_delivery_id: first_id,
                    from_offset: log[first].offset,
                    count,
                    envelopes,
                });
                start = end;
            }
            frames
        };
        for f in frames {
            if out.send(f).is_err() {
                return;
            }
        }
        tokio::select! {
            _ = notified => {}
            _ = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
    }
}

/// Stored-format envelopes holding log records `[from, to)`: with `align > 0` the envelopes
/// cover whole aligned blocks (same partition only), so they include records outside the range.
fn raw_envelopes(
    log: &[Message],
    from: usize,
    to: usize,
    align: u64,
    compression: Compression,
    corrupt: bool,
) -> Bytes {
    let part = log[from].partition;
    let same = |i: usize| log[i].partition == part;
    let (mut lo, mut hi) = (from, to);
    if align > 0 {
        let a = align as usize;
        while lo > 0 && lo % a != 0 && same(lo - 1) {
            lo -= 1;
        }
        while hi < log.len() && hi % a != 0 && same(hi) {
            hi += 1;
        }
    }
    let block = if align > 0 {
        align as usize
    } else {
        usize::MAX
    };
    let mut out = Vec::new();
    let mut i = lo;
    while i < hi {
        let j = if block == usize::MAX {
            hi
        } else {
            ((i / block + 1) * block).min(hi)
        };
        let mut sec = Vec::new();
        for (k, m) in log[i..j].iter().enumerate() {
            batch::encode_record(&mut sec, k as u32, m.key.as_deref(), &m.headers, &m.payload);
        }
        let (c, body) = match codec::compress(compression, &sec) {
            Some(z) => (compression, z),
            None => (Compression::None, sec),
        };
        let env =
            codec::build_envelope(log[i].offset, log[i].timestamp_ms, (j - i) as u32, c, &body)
                .expect("envelope");
        out.extend_from_slice(&env);
        i = j;
    }
    if corrupt {
        out[6] ^= 0xff; // inside the CRC field
    }
    Bytes::from(out)
}

pub fn payload(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

/// Polls `f` until it returns `Some` (panics after `timeout`).
pub async fn eventually<T>(timeout: Duration, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
