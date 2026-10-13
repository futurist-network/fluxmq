//! Message browser reads (`Broker::read_messages`).
//!
//! Implements the [`MessageQuery`] precedence: `offset` → forward from (partition, offset);
//! else, with `newest_first`, the newest `limit` messages within [`since_ms`, `until_ms`];
//! else `since_ms` → forward from that time on the selected partitions; else the newest
//! `limit` messages (before `until_ms` when given). `key` filters by exact key (and, without
//! an explicit partition, restricts the scan to the key's partition). Results are
//! sorted by timestamp ascending (ties by partition, offset).
//!
//! Reads use positional I/O on dedicated read handles and only see committed data; they
//! never take a lock the writer needs for more than a segment-list clone.

use std::ops::ControlFlow;

use fluxmq_proto::api::MessageQuery;
use fluxmq_proto::model::Message;

use crate::channel::Channel;
use crate::error::{CoreError, Result};
use crate::storage::PartitionLog;
use crate::storage::record::Record;

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
/// Upper bound of records visited by one query (cost guard); hitting it sets `has_more`.
pub const MAX_SCAN: u64 = 200_000;

struct Filter<'a> {
    key: Option<&'a str>,
    since_ms: Option<u64>,
    until_ms: Option<u64>,
}

impl Filter<'_> {
    fn key_ok(&self, r: &Record) -> bool {
        self.key.is_none_or(|k| r.key.as_deref() == Some(k))
    }
    fn past_until(&self, r: &Record) -> bool {
        self.until_ms.is_some_and(|u| r.timestamp_ms > u)
    }
    fn in_window(&self, r: &Record) -> bool {
        self.since_ms.is_none_or(|s| r.timestamp_ms >= s) && !self.past_until(r)
    }
}

fn sort_ts(v: &mut [Message]) {
    v.sort_by_key(|m| (m.timestamp_ms, m.partition, m.offset));
}

pub fn read_messages(ch: &Channel, q: &MessageQuery) -> Result<(Vec<Message>, u64, bool)> {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize;
    let parts: Vec<usize> = match q.partition {
        Some(p) if p < ch.partitions() => vec![p as usize],
        Some(p) => {
            return Err(CoreError::InvalidArgument(format!(
                "partition {p} out of range"
            )));
        }
        // Every publish path places keyed messages with `partition_for` (partition counts are
        // immutable), so a key can only live in one partition: scan just that one.
        None => match q.key.as_deref() {
            Some(k) => vec![ch.partition_for(Some(k)) as usize],
            None => (0..ch.logs.len()).collect(),
        },
    };
    let f = Filter {
        key: q.key.as_deref(),
        since_ms: q.since_ms,
        until_ms: q.until_ms,
    };
    if let Some(offset) = q.offset {
        let p = parts[0];
        return forward(
            &ch.logs[p],
            ch.read_limit(p),
            Start::Offset(offset),
            &f,
            limit,
        )
        .map_err(Into::into);
    }
    // Newest-N applies without `since_ms`, or within [since, until] when `newest_first`.
    let newest_first = q.newest_first == Some(true);
    let mut all = Vec::new();
    let mut scanned = 0u64;
    let mut has_more = false;
    for &p in &parts {
        let budget = MAX_SCAN.saturating_sub(scanned);
        let (msgs, s, more) = match q.since_ms {
            Some(since) if !newest_first => {
                forward(&ch.logs[p], ch.read_limit(p), Start::Time(since), &f, limit)?
            }
            _ => newest(&ch.logs[p], ch.read_limit(p), &f, limit, budget)?,
        };
        scanned += s;
        has_more |= more;
        all.extend(msgs);
    }
    sort_ts(&mut all);
    if all.len() > limit {
        has_more = true;
        if q.since_ms.is_some() && !newest_first {
            all.truncate(limit);
        } else {
            all.drain(..all.len() - limit);
        }
    }
    Ok((all, scanned, has_more))
}

enum Start {
    Offset(u64),
    Time(u64),
}

/// Forward scan collecting up to `limit` matches.
fn forward(
    log: &PartitionLog,
    end: u64,
    start: Start,
    f: &Filter<'_>,
    limit: usize,
) -> std::io::Result<(Vec<Message>, u64, bool)> {
    let mut out = Vec::new();
    let mut stopped_early = false;
    let mut last = None;
    let visit = |r: Record| {
        last = Some(r.offset);
        if f.past_until(&r) {
            return ControlFlow::Break(());
        }
        if f.key_ok(&r) {
            out.push(r.into_message(log.partition));
            if out.len() >= limit {
                stopped_early = true;
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    };
    let scanned = match start {
        Start::Offset(o) => log.scan_until(o, end, scan_capped(visit))?,
        Start::Time(t) => log.scan_from_time_until(t, end, scan_capped(visit))?,
    };
    let capped = scanned >= MAX_SCAN;
    let more_exists = last.is_some_and(|o| o + 1 < end.min(log.high()));
    Ok((out, scanned, (stopped_early || capped) && more_exists))
}

/// Wraps a visitor so that it stops after [`MAX_SCAN`] records.
fn scan_capped(
    mut visit: impl FnMut(Record) -> ControlFlow<()>,
) -> impl FnMut(Record) -> ControlFlow<()> {
    let mut n = 0u64;
    move |r| {
        n += 1;
        let flow = visit(r);
        if n >= MAX_SCAN {
            ControlFlow::Break(())
        } else {
            flow
        }
    }
}

/// Newest `limit` matches of one partition within [`since_ms`, `until_ms`], scanning
/// backwards in doubling windows. The upper bound is located exactly via the time index
/// (one short forward scan); the lower bound comes from the in-memory index only, so the
/// scan never reads messages older than the window's first index entry. Looks for one match
/// beyond `limit` so `has_more` is exact (unless the scan budget runs out first, in which
/// case it is conservatively true). Returns the matches in offset order.
fn newest(
    log: &PartitionLog,
    end_limit: u64,
    f: &Filter<'_>,
    limit: usize,
    budget: u64,
) -> std::io::Result<(Vec<Message>, u64, bool)> {
    let floor = match f.since_ms {
        Some(s) => log.offset_for_time_approx(s).max(log.low()),
        None => log.low(),
    };
    let mut end = match f.until_ms {
        Some(u) => log.offset_for_time(u.saturating_add(1))?,
        None => log.high(),
    }
    .min(end_limit);
    let mut window = limit as u64 + 1;
    let mut found: Vec<Message> = Vec::new();
    let mut scanned = 0u64;
    while end > floor && found.len() <= limit && scanned < budget {
        let start = end.saturating_sub(window).max(floor);
        let mut chunk = Vec::new();
        scanned += log.scan(start, |r| {
            if r.offset >= end {
                return ControlFlow::Break(());
            }
            if f.key_ok(&r) && f.in_window(&r) {
                chunk.push(r.into_message(log.partition));
            }
            ControlFlow::Continue(())
        })?;
        chunk.append(&mut found);
        found = chunk;
        end = start;
        window = window.saturating_mul(2);
    }
    let has_more = found.len() > limit || (end > floor && scanned >= budget);
    if found.len() > limit {
        found.drain(..found.len() - limit);
    }
    Ok((found, scanned, has_more))
}
