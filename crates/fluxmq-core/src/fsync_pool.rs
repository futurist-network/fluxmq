//! Broker-wide pool of long-lived fsync threads (v0.6).
//!
//! A channel writer turns one commit into a list of [`SyncOp`]s (data files written since the
//! last commit, directories with new entries, sealed indexes), groups them into chains with
//! [`chains_for`] and either runs a single chain inline ([`run_chain`]) or hands the chains to
//! the pool ([`FsyncPool::submit`]) and gets a [`Ticket`] back. Chains run concurrently on the
//! pool threads; the ops of one chain run in order on one thread.
//!
//! The pool replaces v0.5's per-commit `std::thread::scope` spawns (up to 8 new OS threads on
//! every commit). Threads are started lazily, up to the configured bound, and exit when the
//! pool is dropped.
//!
//! v0.9: a commit's ops are grouped (one group per partition, [`chains_for_groups`]) and a
//! [`Ticket`] reports each group as soon as every chain it depends on finished
//! ([`Ticket::take_ready`]), so a partition's durable mark does not wait for the slowest
//! partition of the commit.

use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// One durability step.
#[derive(Debug, Clone)]
pub enum SyncOp {
    /// Flush a file's data. `timed` ops are reported in the broker fsync histogram.
    /// Elsewhere: `fdatasync`. Apple: plain `fsync(2)` (made durable by the chain's barrier).
    File { file: Arc<File>, timed: bool },
    /// Make a directory's entries durable (a segment was created in it).
    Dir(PathBuf),
    /// Full device barrier on Apple (`F_FULLFSYNC`, which also covers every earlier plain
    /// fsync on the device); `fdatasync` elsewhere. Always timed.
    Barrier(Arc<File>),
    /// Fault injection (tests): sleeps, then fails with `EIO`.
    #[doc(hidden)]
    Fail(Duration),
    /// Fault injection (tests): sleeps, then succeeds (a slow device for one partition).
    #[doc(hidden)]
    Delay(Duration),
}

impl SyncOp {
    fn run(&self) -> io::Result<Option<Duration>> {
        let t = Instant::now();
        match self {
            SyncOp::File { file, timed } => {
                light_sync(file)?;
                Ok(timed.then(|| t.elapsed()))
            }
            SyncOp::Dir(path) => {
                light_sync(&File::open(path)?)?;
                Ok(None)
            }
            SyncOp::Barrier(file) => {
                file.sync_data()?;
                Ok(Some(t.elapsed()))
            }
            SyncOp::Fail(delay) => {
                std::thread::sleep(*delay);
                Err(io::Error::from_raw_os_error(libc::EIO))
            }
            SyncOp::Delay(delay) => {
                std::thread::sleep(*delay);
                Ok(None)
            }
        }
    }

    /// Injected test op (not a durability step).
    fn is_injected(&self) -> bool {
        matches!(self, SyncOp::Fail(_) | SyncOp::Delay(_))
    }
}

#[cfg(target_vendor = "apple")]
fn light_sync(f: &File) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    // SAFETY: the descriptor is owned by `f` and open for the whole call.
    if unsafe { libc::fsync(f.as_raw_fd()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(target_vendor = "apple"))]
fn light_sync(f: &File) -> io::Result<()> {
    f.sync_data()
}

/// One chain of a grouped commit: its ops run in order on one thread; every group listed in
/// `groups` depends on it.
#[derive(Debug)]
pub struct Chain {
    pub ops: Vec<SyncOp>,
    pub groups: Vec<usize>,
    /// Ends in a whole-device barrier shared by the commit (Apple); see
    /// [`Ticket::barrier_running`].
    pub barrier: bool,
}

/// Groups one commit's ops into chains:
/// * elsewhere: one chain per op (every `fdatasync` may run concurrently);
/// * Apple: a single chain of plain fsyncs ending in one `F_FULLFSYNC` barrier on the last
///   data file (one whole-device flush per commit instead of one per file).
pub fn chains_for(ops: Vec<SyncOp>) -> Vec<Vec<SyncOp>> {
    chains_for_groups(vec![ops])
        .into_iter()
        .map(|c| c.ops)
        .collect()
}

/// [`chains_for`] over the ops of several groups (one per partition): each resulting chain
/// lists the groups that depend on it. Elsewhere every op is its own chain of its own group,
/// so a group completes as soon as its own fsyncs did. On Apple the real ops of all groups
/// share one chain ending in a single barrier (nothing is durable before it, so they could
/// not complete earlier anyway); injected test ops (`Fail`, `Delay`) are separate chains of
/// their own group only, so a test can slow down or fail one partition on every platform.
pub fn chains_for_groups(groups: Vec<Vec<SyncOp>>) -> Vec<Chain> {
    let mut out = Vec::new();
    if cfg!(target_vendor = "apple") {
        let mut shared = Vec::new();
        let mut members = Vec::new();
        for (g, ops) in groups.into_iter().enumerate() {
            let mut real = false;
            for op in ops {
                if op.is_injected() {
                    out.push(Chain {
                        ops: vec![op],
                        groups: vec![g],
                        barrier: false,
                    });
                } else {
                    real = true;
                    shared.push(op);
                }
            }
            if real {
                members.push(g);
            }
        }
        if shared.is_empty() {
            return out;
        }
        // The last data file becomes the barrier; without one, a directory is fully synced.
        match shared
            .iter()
            .rposition(|o| matches!(o, SyncOp::File { .. } | SyncOp::Barrier(_)))
        {
            Some(i) => {
                let last = shared.remove(i);
                let f = match last {
                    SyncOp::File { file, .. } | SyncOp::Barrier(file) => file,
                    _ => unreachable!("position matched a file op"),
                };
                shared.push(SyncOp::Barrier(f));
            }
            None => {
                if let Some(SyncOp::Dir(path)) = shared.pop() {
                    match File::open(&path) {
                        Ok(f) => shared.push(SyncOp::Barrier(Arc::new(f))),
                        // Surface the error when the chain runs.
                        Err(_) => shared.push(SyncOp::Dir(path)),
                    }
                }
            }
        }
        out.push(Chain {
            ops: shared,
            groups: members,
            barrier: true,
        });
    } else {
        for (g, ops) in groups.into_iter().enumerate() {
            for op in ops {
                out.push(Chain {
                    ops: vec![op],
                    groups: vec![g],
                    barrier: false,
                });
            }
        }
    }
    out
}

/// Runs one chain on the calling thread. Stops at the first error. Returns the durations of
/// the timed ops.
pub fn run_chain(chain: &[SyncOp]) -> io::Result<Vec<Duration>> {
    let mut times = Vec::new();
    for op in chain {
        if let Some(d) = op.run()? {
            times.push(d);
        }
    }
    Ok(times)
}

/// Outcome of one group: the timed fsync durations of its chains, or the first error.
pub type GroupResult = io::Result<Vec<Duration>>;

struct GroupState {
    remaining: usize,
    err: Option<io::Error>,
    times: Vec<Duration>,
    /// Handed out by [`Ticket::take_ready`] (or consumed by [`Ticket::wait`]).
    taken: bool,
}

struct TicketState {
    /// Chains not yet finished.
    remaining: usize,
    /// Barrier chains not yet finished.
    barriers: usize,
    groups: Vec<GroupState>,
    /// Groups finished but not yet taken, in completion order.
    ready: Vec<usize>,
    waker: Option<Waker>,
}

struct TicketInner {
    st: Mutex<TicketState>,
    cv: Condvar,
}

fn copy_err(e: &io::Error) -> io::Error {
    match e.raw_os_error() {
        Some(code) => io::Error::from_raw_os_error(code),
        None => io::Error::new(e.kind(), e.to_string()),
    }
}

impl TicketInner {
    /// One chain finished. Its timed durations are attributed to its first group only (so a
    /// shared Apple barrier is recorded once); an error is reported to every dependent group.
    fn complete_one(&self, groups: &[usize], barrier: bool, res: GroupResult) {
        let waker = {
            let mut st = self.st.lock();
            if barrier {
                st.barriers -= 1;
            }
            let mut times = match &res {
                Ok(t) => Some(t.clone()),
                Err(_) => None,
            };
            let mut newly_ready = false;
            for &g in groups {
                let gs = &mut st.groups[g];
                match &res {
                    Ok(_) => gs.times.extend(times.take().unwrap_or_default()),
                    Err(e) => {
                        if gs.err.is_none() {
                            gs.err = Some(copy_err(e));
                        }
                    }
                }
                gs.remaining -= 1;
                if gs.remaining == 0 {
                    st.ready.push(g);
                    newly_ready = true;
                }
            }
            st.remaining -= 1;
            // The writer also waits for the last barrier to finish (serial-barrier rule), so
            // wake it then even if no group became ready.
            let barrier_done = barrier && st.barriers == 0;
            if !newly_ready && !barrier_done && st.remaining > 0 {
                return;
            }
            self.cv.notify_all();
            st.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }
}

/// Completion handle of one submitted commit.
pub struct Ticket(Arc<TicketInner>);

impl Ticket {
    /// `deps[g]` = number of chains group `g` depends on.
    fn new(remaining: usize, barriers: usize, deps: &[usize]) -> Self {
        let mut ready = Vec::new();
        let groups = deps
            .iter()
            .enumerate()
            .map(|(g, &n)| {
                if n == 0 {
                    ready.push(g);
                }
                GroupState {
                    remaining: n,
                    err: None,
                    times: Vec::new(),
                    taken: false,
                }
            })
            .collect();
        Ticket(Arc::new(TicketInner {
            st: Mutex::new(TicketState {
                remaining,
                barriers,
                groups,
                ready,
                waker: None,
            }),
            cv: Condvar::new(),
        }))
    }

    /// A whole-device barrier chain of this ticket is still running (Apple).
    pub fn barrier_running(&self) -> bool {
        self.0.st.lock().barriers > 0
    }

    /// Every chain finished.
    pub fn is_done(&self) -> bool {
        self.0.st.lock().remaining == 0
    }

    /// Blocks until every chain finished; the first error wins. Returns the timed durations
    /// of the groups not taken yet.
    pub fn wait(&self) -> GroupResult {
        let mut st = self.0.st.lock();
        while st.remaining > 0 {
            self.0.cv.wait(&mut st);
        }
        st.ready.clear();
        let mut times = Vec::new();
        let mut err = None;
        for gs in st.groups.iter_mut().filter(|g| !g.taken) {
            gs.taken = true;
            times.append(&mut gs.times);
            if err.is_none() {
                err = gs.err.take();
            }
        }
        match err {
            Some(e) => Err(e),
            None => Ok(times),
        }
    }

    /// Moves every finished, not yet taken group into `out` as `(group, result)`.
    pub fn take_ready(&self, out: &mut Vec<(usize, GroupResult)>) {
        let mut tagged = Vec::new();
        self.take_ready_into(0, &mut tagged);
        out.extend(tagged.into_iter().map(|(_, g, r)| (g, r)));
    }

    /// [`take_ready`](Self::take_ready), tagging each entry with `tag` (no allocation).
    pub fn take_ready_into(&self, tag: u64, out: &mut Vec<(u64, usize, GroupResult)>) {
        let mut st = self.0.st.lock();
        let st = &mut *st;
        for g in st.ready.drain(..) {
            let gs = &mut st.groups[g];
            gs.taken = true;
            let res = match gs.err.take() {
                Some(e) => Err(e),
                None => Ok(std::mem::take(&mut gs.times)),
            };
            out.push((tag, g, res));
        }
    }

    /// Blocks until a group is ready to take, or every group was taken.
    pub fn wait_ready(&self) {
        let mut st = self.0.st.lock();
        while st.ready.is_empty() && st.groups.iter().any(|g| !g.taken) {
            self.0.cv.wait(&mut st);
        }
    }

    /// Ready once a group can be taken or every chain finished.
    pub fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut st = self.0.st.lock();
        if !st.ready.is_empty() || st.remaining == 0 {
            return Poll::Ready(());
        }
        match &st.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => st.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// Ready once every chain finished (does not consume the result; call [`wait`](Self::wait)).
    pub fn poll_done(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut st = self.0.st.lock();
        if st.remaining == 0 {
            return Poll::Ready(());
        }
        match &st.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => st.waker = Some(cx.waker().clone()),
        }
        Poll::Pending
    }

    /// Future form of [`poll_done`](Self::poll_done).
    pub fn done(&self) -> impl Future<Output = ()> + '_ {
        struct Done<'a>(&'a Ticket);
        impl Future for Done<'_> {
            type Output = ();
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                self.0.poll_done(cx)
            }
        }
        Done(self)
    }
}

type Task = (Vec<SyncOp>, Vec<usize>, bool, Arc<TicketInner>);

struct PoolState {
    queue: VecDeque<Task>,
    threads: usize,
    idle: usize,
}

struct PoolInner {
    st: Mutex<PoolState>,
    cv: Condvar,
    stop: AtomicBool,
}

/// Bounded pool of fsync threads, shared by every channel of a broker.
pub struct FsyncPool {
    inner: Arc<PoolInner>,
    max_threads: usize,
}

impl std::fmt::Debug for FsyncPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsyncPool")
            .field("max_threads", &self.max_threads)
            .finish()
    }
}

/// Default thread bound: twice the CPUs (fsync threads mostly sleep in the kernel), 4..=32.
pub fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(8, |n| n.get() * 2)
        .clamp(4, 32)
}

impl FsyncPool {
    /// `max_threads == 0` picks [`default_threads`].
    pub fn new(max_threads: usize) -> Self {
        let max_threads = if max_threads == 0 {
            default_threads()
        } else {
            max_threads
        };
        Self {
            inner: Arc::new(PoolInner {
                st: Mutex::new(PoolState {
                    queue: VecDeque::new(),
                    threads: 0,
                    idle: 0,
                }),
                cv: Condvar::new(),
                stop: AtomicBool::new(false),
            }),
            max_threads,
        }
    }

    pub fn max_threads(&self) -> usize {
        self.max_threads
    }

    /// Threads started so far (test hook).
    pub fn threads(&self) -> usize {
        self.inner.st.lock().threads
    }

    /// Queues `chains`; the ticket completes once all of them ran. An empty list completes
    /// immediately.
    pub fn submit(&self, chains: Vec<Vec<SyncOp>>) -> Ticket {
        let chains = chains
            .into_iter()
            .map(|ops| Chain {
                ops,
                groups: vec![0],
                barrier: false,
            })
            .collect();
        self.submit_groups(chains, 1)
    }

    /// Queues grouped `chains` (see [`chains_for_groups`]) of `n_groups` groups; each group
    /// becomes ready on the ticket once every chain listing it ran. A group no chain lists
    /// is ready immediately.
    pub fn submit_groups(&self, chains: Vec<Chain>, n_groups: usize) -> Ticket {
        let mut deps = vec![0usize; n_groups];
        for c in &chains {
            for &g in &c.groups {
                deps[g] += 1;
            }
        }
        let barriers = chains.iter().filter(|c| c.barrier).count();
        let ticket = Ticket::new(chains.len(), barriers, &deps);
        if chains.is_empty() {
            return ticket;
        }
        let spawn = {
            let mut st = self.inner.st.lock();
            let n = chains.len();
            for c in chains {
                st.queue
                    .push_back((c.ops, c.groups, c.barrier, ticket.0.clone()));
            }
            // Start threads for the work idle threads can't pick up right away.
            let want = n.saturating_sub(st.idle);
            let room = self.max_threads.saturating_sub(st.threads);
            let spawn = want.min(room);
            st.threads += spawn;
            spawn
        };
        for _ in 0..spawn {
            let inner = self.inner.clone();
            let res = std::thread::Builder::new()
                .name("fluxmq-fsync".into())
                .spawn(move || worker(inner));
            if res.is_err() {
                // Could not start a thread: the existing ones (or, with none, the caller's
                // wait) still drain the queue — fall back to running inline if nobody can.
                let mut st = self.inner.st.lock();
                st.threads -= 1;
                if st.threads == 0 {
                    let tasks: Vec<Task> = st.queue.drain(..).collect();
                    drop(st);
                    for (chain, groups, barrier, t) in tasks {
                        t.complete_one(&groups, barrier, run_chain(&chain));
                    }
                    return ticket;
                }
            }
        }
        self.inner.cv.notify_all();
        ticket
    }

    /// Runs grouped `chains` on the calling thread (a commit with nothing to overlap) and
    /// returns an already complete ticket.
    pub fn run_inline(chains: Vec<Chain>, n_groups: usize) -> Ticket {
        let mut deps = vec![0usize; n_groups];
        for c in &chains {
            for &g in &c.groups {
                deps[g] += 1;
            }
        }
        let barriers = chains.iter().filter(|c| c.barrier).count();
        let ticket = Ticket::new(chains.len(), barriers, &deps);
        for c in chains {
            ticket
                .0
                .complete_one(&c.groups, c.barrier, run_chain(&c.ops));
        }
        ticket
    }
}

impl Drop for FsyncPool {
    fn drop(&mut self) {
        self.inner.stop.store(true, Ordering::Release);
        let _st = self.inner.st.lock();
        self.inner.cv.notify_all();
    }
}

fn worker(inner: Arc<PoolInner>) {
    loop {
        let task = {
            let mut st = inner.st.lock();
            loop {
                if let Some(t) = st.queue.pop_front() {
                    break Some(t);
                }
                if inner.stop.load(Ordering::Acquire) {
                    break None;
                }
                st.idle += 1;
                inner.cv.wait(&mut st);
                st.idle -= 1;
            }
        };
        let Some((chain, groups, barrier, ticket)) = task else {
            inner.st.lock().threads -= 1;
            return;
        };
        let res = catch_unwind(AssertUnwindSafe(|| run_chain(&chain)))
            .unwrap_or_else(|_| Err(io::Error::other("fsync thread panicked")));
        ticket.complete_one(&groups, barrier, res);
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn file_op(dir: &std::path::Path, name: &str) -> SyncOp {
        let mut f = File::create(dir.join(name)).unwrap();
        f.write_all(b"data").unwrap();
        SyncOp::File {
            file: Arc::new(f),
            timed: true,
        }
    }

    #[test]
    fn pool_runs_chains_and_reuses_threads() {
        let dir = tempfile::tempdir().unwrap();
        let pool = FsyncPool::new(3);
        for round in 0..20 {
            let ops: Vec<SyncOp> = (0..5)
                .map(|i| file_op(dir.path(), &format!("f{round}-{i}")))
                .chain([SyncOp::Dir(dir.path().to_path_buf())])
                .collect();
            let t = pool.submit(chains_for(ops));
            let times = t.wait().unwrap();
            assert!(t.is_done());
            // Every timed op is reported (on Apple the last file op became the barrier).
            assert_eq!(times.len(), 5);
        }
        assert!(pool.threads() <= 3, "bounded: {} threads", pool.threads());
        assert!(pool.threads() >= 1);
    }

    #[test]
    fn errors_are_reported_and_empty_tickets_complete() {
        let pool = FsyncPool::new(2);
        assert!(pool.submit(Vec::new()).wait().unwrap().is_empty());
        let t = pool.submit(vec![vec![SyncOp::Dir("/nonexistent/fluxmq-test".into())]]);
        assert!(t.wait().is_err());
    }

    #[test]
    fn ticket_future_wakes() {
        let dir = tempfile::tempdir().unwrap();
        let pool = FsyncPool::new(1);
        let t = pool.submit(chains_for(vec![file_op(dir.path(), "a")]));
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut cx = Context::from_waker(&waker);
        while t.poll_done(&mut cx).is_pending() {
            std::thread::park_timeout(Duration::from_millis(50));
        }
        t.wait().unwrap();
    }

    /// v0.9: each group is reported as soon as its own chains finished; a slow group does not
    /// hold back a fast one, and an error reaches every group depending on the failed chain.
    #[test]
    fn groups_complete_independently() {
        let dir = tempfile::tempdir().unwrap();
        let pool = FsyncPool::new(4);
        let groups = vec![
            vec![
                SyncOp::Delay(Duration::from_millis(300)),
                file_op(dir.path(), "slow"),
            ],
            vec![file_op(dir.path(), "fast")],
            vec![SyncOp::Fail(Duration::from_millis(50))],
        ];
        let chains = chains_for_groups(groups);
        let t0 = Instant::now();
        let ticket = pool.submit_groups(chains, 3);
        let mut got = Vec::new();
        while got.len() < 3 {
            ticket.wait_ready();
            let before = got.len();
            ticket.take_ready(&mut got);
            for (g, r) in &got[before..] {
                let at = t0.elapsed();
                match g {
                    0 => {
                        assert!(at >= Duration::from_millis(300));
                        assert!(r.is_ok());
                    }
                    1 => {
                        assert!(at < Duration::from_millis(250), "fast group waited {at:?}");
                        assert!(r.is_ok());
                    }
                    2 => assert_eq!(r.as_ref().unwrap_err().raw_os_error(), Some(libc::EIO)),
                    _ => unreachable!(),
                }
            }
        }
        let order: Vec<usize> = got.iter().map(|(g, _)| *g).collect();
        assert_eq!(order.last(), Some(&0), "slow group last: {order:?}");
        assert!(ticket.is_done());
        // Every group taken: nothing more to wait for.
        ticket.wait_ready();
        // A group with no chain is ready at once; inline tickets complete before returning.
        let t = FsyncPool::run_inline(
            chains_for_groups(vec![vec![], vec![file_op(dir.path(), "i")]]),
            2,
        );
        let mut got = Vec::new();
        t.take_ready(&mut got);
        assert_eq!(got.len(), 2);
        assert!(got.iter().all(|(_, r)| r.is_ok()));
    }

    #[test]
    fn shared_apple_chain_reports_every_member() {
        let dir = tempfile::tempdir().unwrap();
        let chains = chains_for_groups(vec![
            vec![file_op(dir.path(), "a")],
            vec![
                file_op(dir.path(), "b"),
                SyncOp::Dir(dir.path().to_path_buf()),
            ],
        ]);
        if cfg!(target_vendor = "apple") {
            assert_eq!(chains.len(), 1);
            assert_eq!(chains[0].groups, vec![0, 1]);
            assert!(matches!(chains[0].ops.last(), Some(SyncOp::Barrier(_))));
        } else {
            assert_eq!(chains.len(), 3);
        }
        let t = FsyncPool::new(2).submit_groups(chains, 2);
        t.wait_ready();
        let mut got = Vec::new();
        while got.len() < 2 {
            t.wait_ready();
            t.take_ready(&mut got);
        }
        // The barrier's time is recorded once (first member only).
        let timed: usize = got.iter().map(|(_, r)| r.as_ref().unwrap().len()).sum();
        assert_eq!(timed, 2);
    }

    struct Unpark(std::thread::Thread);
    impl std::task::Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    #[test]
    fn dropping_the_pool_stops_threads() {
        let dir = tempfile::tempdir().unwrap();
        let pool = FsyncPool::new(2);
        pool.submit(chains_for(vec![
            file_op(dir.path(), "a"),
            file_op(dir.path(), "b"),
        ]))
        .wait()
        .unwrap();
        let inner = pool.inner.clone();
        drop(pool);
        let deadline = Instant::now() + Duration::from_secs(5);
        while inner.st.lock().threads > 0 {
            assert!(Instant::now() < deadline, "fsync threads did not exit");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}
