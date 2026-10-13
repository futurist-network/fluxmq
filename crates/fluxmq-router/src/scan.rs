//! Scan-based filter evaluation over stored messages: message search with `filter=` and the
//! route backtest. Built only on `Broker::read_messages` pages (≤ 1000 records each) so it
//! inherits the core's read path (positional reads, committed data only, never blocks the
//! writer). Blocking: call from `spawn_blocking`.
//!
//! Partitions are scanned with one cursor each and merged by timestamp at page granularity
//! (the cursor whose frontier is newest — or oldest, for forward scans — is advanced next), so
//! "newest N matches" is exact across partitions, while the total work is bounded by
//! [`SCAN_CAP`] records.

use fluxmq_core::{Broker, CoreError};
use fluxmq_proto::Message;
use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::routing::Condition;

use crate::filter::{MatchContext, matches_in};

/// Upper bound of records visited by one search/backtest (same as the core's read cap).
pub const SCAN_CAP: u64 = 200_000;
/// Records per `read_messages` page (the core's max `limit`).
const PAGE: u64 = 1000;
pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dir {
    /// Oldest first (from an offset or a timestamp).
    Forward,
    /// Newest first within [since, until].
    Backward,
}

#[derive(Debug, Clone, Copy)]
enum State {
    FromOffset(u64),
    FromTime(u64),
    BackwardInit,
    /// Next backward page ends before this offset (exclusive).
    BackwardBefore(u64),
    Done,
}

struct Cursor {
    partition: u32,
    state: State,
    /// Timestamp of the last message handed out (merge key).
    frontier: Option<u64>,
}

struct Scanner<'a> {
    broker: &'a Broker,
    channel: &'a str,
    dir: Dir,
    since: Option<u64>,
    until: Option<u64>,
    cursors: Vec<Cursor>,
    scanned: u64,
    cap: u64,
}

impl Scanner<'_> {
    fn active(&self) -> impl Iterator<Item = &Cursor> {
        self.cursors
            .iter()
            .filter(|c| !matches!(c.state, State::Done))
    }

    fn exhausted(&self) -> bool {
        self.active().next().is_none()
    }

    fn capped(&self) -> bool {
        self.scanned >= self.cap
    }

    /// True when no unscanned message can sort before `boundary` (a timestamp) in scan order.
    fn all_past(&self, boundary: u64) -> bool {
        self.active().all(|c| match (self.dir, c.frontier) {
            (_, None) => false,
            (Dir::Backward, Some(f)) => f < boundary,
            (Dir::Forward, Some(f)) => f > boundary,
        })
    }

    /// Next page in scan order (newest first for backward scans), or `None` when every cursor
    /// is exhausted or the scan cap is reached.
    fn next_page(&mut self) -> Result<Option<Vec<Message>>, CoreError> {
        if self.capped() {
            return Ok(None);
        }
        let dir = self.dir;
        let pick = self
            .cursors
            .iter()
            .enumerate()
            .filter(|(_, c)| !matches!(c.state, State::Done))
            .max_by_key(|(_, c)| match (dir, c.frontier) {
                (_, None) => (1u8, 0u64),
                (Dir::Backward, Some(f)) => (0, f),
                (Dir::Forward, Some(f)) => (0, u64::MAX - f),
            })
            .map(|(i, _)| i);
        let Some(i) = pick else {
            return Ok(None);
        };
        let page = self.fetch(i)?;
        if let Some(last) = page.last() {
            self.cursors[i].frontier = Some(last.timestamp_ms);
        }
        Ok(Some(page))
    }

    fn read(&mut self, q: &MessageQuery) -> Result<(Vec<Message>, bool), CoreError> {
        let (msgs, scanned, more) = self.broker.read_messages(self.channel, q)?;
        self.scanned += scanned.max(msgs.len() as u64);
        Ok((msgs, more))
    }

    /// A tiny positioning read (≤ 2 records, re-read by the first page): not counted.
    fn probe(&self, q: &MessageQuery) -> Result<Vec<Message>, CoreError> {
        Ok(self.broker.read_messages(self.channel, q)?.0)
    }

    fn in_window(&self, m: &Message) -> bool {
        self.since.is_none_or(|s| m.timestamp_ms >= s)
            && self.until.is_none_or(|u| m.timestamp_ms <= u)
    }

    fn fetch(&mut self, i: usize) -> Result<Vec<Message>, CoreError> {
        let partition = self.cursors[i].partition;
        let base = MessageQuery {
            partition: Some(partition),
            until_ms: self.until,
            limit: Some(PAGE as u32),
            ..MessageQuery::default()
        };
        match self.cursors[i].state {
            State::Done => Ok(Vec::new()),
            State::FromOffset(_) | State::FromTime(_) => {
                let q = match self.cursors[i].state {
                    State::FromOffset(o) => MessageQuery {
                        offset: Some(o),
                        ..base
                    },
                    State::FromTime(t) => MessageQuery {
                        since_ms: Some(t),
                        ..base
                    },
                    _ => unreachable!(),
                };
                let (mut msgs, more) = self.read(&q)?;
                self.cursors[i].state = match msgs.last() {
                    Some(last) if more => State::FromOffset(last.offset + 1),
                    _ => State::Done,
                };
                msgs.retain(|m| self.in_window(m));
                Ok(msgs)
            }
            State::BackwardInit => {
                // Locate the newest message ≤ until (exact, via the time index).
                let q = MessageQuery {
                    since_ms: self.since,
                    newest_first: Some(true),
                    limit: Some(1),
                    ..base
                };
                let msgs = self.probe(&q)?;
                self.cursors[i].state = match msgs.first() {
                    Some(m) => State::BackwardBefore(m.offset + 1),
                    None => State::Done,
                };
                self.fetch(i)
            }
            State::BackwardBefore(end) => {
                let start = end.saturating_sub(PAGE);
                let q = MessageQuery {
                    offset: Some(start),
                    until_ms: None,
                    limit: Some((end - start) as u32),
                    ..base
                };
                let (mut msgs, _) = self.read(&q)?;
                msgs.retain(|m| m.offset < end);
                let hit_low = start == 0 || msgs.first().is_none_or(|m| m.offset > start);
                let below_since = self
                    .since
                    .is_some_and(|s| msgs.first().is_some_and(|m| m.timestamp_ms < s));
                self.cursors[i].state = if hit_low || below_since {
                    State::Done
                } else {
                    State::BackwardBefore(start)
                };
                msgs.retain(|m| self.in_window(m));
                msgs.reverse();
                Ok(msgs)
            }
        }
    }
}

/// Result of [`search`]: same shape as `Broker::read_messages`.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    /// Sorted by timestamp ascending (ties by partition, offset), like the core.
    pub messages: Vec<Message>,
    pub scanned: u64,
    pub has_more: bool,
}

fn sort_key(m: &Message) -> (u64, u32, u64) {
    (m.timestamp_ms, m.partition, m.offset)
}

/// Message search with a filter, honouring the [`MessageQuery`] precedence of
/// `Broker::read_messages` (`offset` → forward; `since_ms` without `newest_first` → forward
/// from that time; otherwise the newest `limit` matches in [`since_ms`, `until_ms`]), `key`
/// and `partition`. Collects up to `limit` matches, visiting at most [`SCAN_CAP`] records.
pub fn search(
    broker: &Broker,
    channel: &str,
    q: &MessageQuery,
    conditions: &[Condition],
) -> Result<SearchResult, CoreError> {
    search_capped(broker, channel, q, conditions, SCAN_CAP)
}

#[doc(hidden)]
pub fn search_capped(
    broker: &Broker,
    channel: &str,
    q: &MessageQuery,
    conditions: &[Condition],
    cap: u64,
) -> Result<SearchResult, CoreError> {
    let cfg = broker
        .channel(channel)
        .ok_or_else(|| CoreError::ChannelNotFound(channel.to_string()))?;
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize;
    let parts: Vec<u32> = match (q.partition, q.key.as_deref()) {
        (Some(p), _) if p < cfg.partitions => vec![p],
        (Some(p), _) => {
            return Err(CoreError::InvalidArgument(format!(
                "partition {p} out of range"
            )));
        }
        // Keyed messages always live in `crc32(key) % partitions` (core's placement).
        (None, Some(k)) => vec![crc32fast::hash(k.as_bytes()) % cfg.partitions.max(1)],
        (None, None) => (0..cfg.partitions).collect(),
    };
    let newest_first = q.newest_first == Some(true);
    let (dir, cursors): (Dir, Vec<Cursor>) = match (q.offset, q.since_ms) {
        (Some(o), _) => (
            Dir::Forward,
            vec![Cursor {
                partition: parts[0],
                state: State::FromOffset(o),
                frontier: None,
            }],
        ),
        (None, Some(t)) if !newest_first => (
            Dir::Forward,
            parts
                .iter()
                .map(|&partition| Cursor {
                    partition,
                    state: State::FromTime(t),
                    frontier: None,
                })
                .collect(),
        ),
        _ => (
            Dir::Backward,
            parts
                .iter()
                .map(|&partition| Cursor {
                    partition,
                    state: State::BackwardInit,
                    frontier: None,
                })
                .collect(),
        ),
    };
    let mut scan = Scanner {
        broker,
        channel,
        dir,
        // Forward-from-offset ignores `since_ms`, like the core.
        since: if q.offset.is_some() { None } else { q.since_ms },
        until: q.until_ms,
        cursors,
        scanned: 0,
        cap,
    };
    let order = |v: &mut Vec<Message>| match dir {
        Dir::Forward => v.sort_by_key(sort_key),
        Dir::Backward => v.sort_by_key(|m| std::cmp::Reverse(sort_key(m))),
    };
    let mut found: Vec<Message> = Vec::new();
    let mut truncated = false;
    while let Some(page) = scan.next_page()? {
        for m in page {
            if q.key
                .as_deref()
                .is_some_and(|k| m.key.as_deref() != Some(k))
            {
                continue;
            }
            if matches_in(conditions, &MatchContext::new(&m)) {
                found.push(m);
            }
        }
        if found.len() >= limit {
            order(&mut found);
            if found.len() > limit {
                found.truncate(limit);
                truncated = true;
            }
            if scan.all_past(found[limit - 1].timestamp_ms) {
                break;
            }
        }
    }
    order(&mut found);
    if found.len() > limit {
        found.truncate(limit);
        truncated = true;
    }
    found.sort_by_key(sort_key);
    Ok(SearchResult {
        messages: found,
        scanned: scan.scanned,
        has_more: truncated || !scan.exhausted(),
    })
}

/// Result of [`backtest`].
#[derive(Debug, Clone, PartialEq)]
pub struct BacktestResult {
    pub scanned: u64,
    pub matched: u64,
    /// The scan cap was hit before covering the window.
    pub truncated: bool,
    /// Newest matches first.
    pub samples: Vec<Message>,
}

/// Counts messages with `timestamp_ms >= since_ms` matching `conditions`, newest first,
/// visiting at most [`SCAN_CAP`] records.
pub fn backtest(
    broker: &Broker,
    channel: &str,
    conditions: &[Condition],
    since_ms: u64,
    sample_limit: usize,
) -> Result<BacktestResult, CoreError> {
    backtest_capped(
        broker,
        channel,
        conditions,
        since_ms,
        sample_limit,
        SCAN_CAP,
    )
}

#[doc(hidden)]
pub fn backtest_capped(
    broker: &Broker,
    channel: &str,
    conditions: &[Condition],
    since_ms: u64,
    sample_limit: usize,
    cap: u64,
) -> Result<BacktestResult, CoreError> {
    let cfg = broker
        .channel(channel)
        .ok_or_else(|| CoreError::ChannelNotFound(channel.to_string()))?;
    let mut scan = Scanner {
        broker,
        channel,
        dir: Dir::Backward,
        since: Some(since_ms),
        until: None,
        cursors: (0..cfg.partitions)
            .map(|partition| Cursor {
                partition,
                state: State::BackwardInit,
                frontier: None,
            })
            .collect(),
        scanned: 0,
        cap,
    };
    let newest = |v: &mut Vec<Message>| v.sort_by_key(|m| std::cmp::Reverse(sort_key(m)));
    let mut matched = 0u64;
    let mut samples: Vec<Message> = Vec::new();
    while let Some(page) = scan.next_page()? {
        for m in page {
            if matches_in(conditions, &MatchContext::new(&m)) {
                matched += 1;
                if sample_limit > 0 {
                    samples.push(m);
                    if samples.len() >= sample_limit * 2 {
                        newest(&mut samples);
                        samples.truncate(sample_limit);
                    }
                }
            }
        }
    }
    newest(&mut samples);
    samples.truncate(sample_limit);
    Ok(BacktestResult {
        scanned: scan.scanned,
        matched,
        truncated: !scan.exhausted(),
        samples,
    })
}
