//! Delayed messages (v0.12): "deliver this message at 09:00" or "in 10 minutes".
//!
//! # Publishing
//! A message carrying [`DELIVER_AT_HEADER`] (unix ms) or [`DELAY_HEADER`] (milliseconds from
//! now, by the broker's clock) is not appended to its channel right away. It is stored durably
//! in the system channel [`SCHEDULED_CHANNEL`] (replicated like any channel) and published to
//! its channel when due. A due time in the past (or within [`IMMEDIATE_WITHIN_MS`]) publishes
//! at once. Over [`MAX_DELAY_MS`] → `InvalidArgument`. REST: `deliver_at_ms` / `delay_ms` on a
//! publish item. SDK: `MessageBuilder::deliver_at` / `delay`. Delayed messages travel in
//! `Publish` requests (never `PublishBatch`, whose records the broker does not decode).
//!
//! The id returned for a delayed message is its **schedule id**: its position in
//! [`SCHEDULED_CHANNEL`] (cancel or release it early with it). When released, the message gets
//! a new id in its channel and the headers [`SCHEDULED_ID_HEADER`] and
//! [`SCHEDULED_FOR_HEADER`]; the delay headers are removed. It is partitioned at release time
//! (key hash, or round-robin without a key), so per-key order is the order of release.
//!
//! # Guarantees
//! * Durable before the publisher's ack, like any publish (fsync, ISR).
//! * At least once: released, then marked done with a control record in the same partition of
//!   [`SCHEDULED_CHANNEL`]. A crash or a leader change between the two can release a message
//!   twice; the copies carry the same [`SCHEDULED_ID_HEADER`].
//! * Timing: released within about 100 ms of the due time while the partition's leader runs;
//!   overdue messages (broker down at the due time) are released as soon as it is back.
//! * Retention of [`SCHEDULED_CHANNEL`] is [`MAX_DELAY_MS`] + 7 days, so an entry always
//!   outlives its due time.
//!
//! # Control records
//! Appended to the entry's partition (empty payload): [`OP_HEADER`] = `done` (released),
//! `cancel` (cancelled), `drop` (its channel no longer exists), `now` (release early), and
//! [`REF_HEADER`] = the entry's offset. Only the partition's leader acts on them.
//!
//! REST (`/api/scheduled`): list upcoming (read), cancel and release now (operate).

use serde::{Deserialize, Serialize};

use crate::model::MessageId;

/// System channel holding delayed messages until they are due.
pub const SCHEDULED_CHANNEL: &str = "$scheduled";
/// Partitions of [`SCHEDULED_CHANNEL`] (spread over the cluster's nodes).
pub const SCHEDULED_PARTITIONS: u32 = 4;
/// Longest accepted delay: 30 days.
pub const MAX_DELAY_MS: u64 = 30 * 24 * 3600 * 1000;
/// Retention of [`SCHEDULED_CHANNEL`]: the longest delay plus a week of slack (a broker that
/// was down still finds its overdue entries).
pub const SCHEDULED_RETENTION_SECS: u64 = MAX_DELAY_MS / 1000 + 7 * 24 * 3600;
/// A due time this close to now publishes immediately.
pub const IMMEDIATE_WITHIN_MS: u64 = 50;

/// Publish header: deliver at this unix time (ms).
pub const DELIVER_AT_HEADER: &str = "flux-deliver-at";
/// Publish header: deliver this many milliseconds from now (the broker's clock).
pub const DELAY_HEADER: &str = "flux-delay-ms";
/// On a released message: `<partition>/<offset>` of its entry in [`SCHEDULED_CHANNEL`].
pub const SCHEDULED_ID_HEADER: &str = "flux-scheduled-id";
/// On a released message: the due time it was scheduled for (unix ms).
pub const SCHEDULED_FOR_HEADER: &str = "flux-scheduled-for";

/// Entry header: the channel to publish to.
pub const TARGET_HEADER: &str = "flux-sched-target";
/// Entry header: due time (unix ms).
pub const DUE_HEADER: &str = "flux-sched-due";
/// Control record header: `done` | `cancel` | `drop` | `now`.
pub const OP_HEADER: &str = "flux-sched-op";
/// Control record header: offset of the entry it refers to.
pub const REF_HEADER: &str = "flux-sched-ref";

/// When a publish item asks to be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timing {
    Now,
    /// Unix ms.
    At(u64),
}

/// Reads the delay headers of a message (`now_ms` = the broker's clock). `Err` = bad value or
/// over [`MAX_DELAY_MS`].
pub fn timing(
    headers: &std::collections::BTreeMap<String, String>,
    now_ms: u64,
) -> Result<Timing, String> {
    let at = match (headers.get(DELIVER_AT_HEADER), headers.get(DELAY_HEADER)) {
        (None, None) => return Ok(Timing::Now),
        (Some(_), Some(_)) => {
            return Err(format!(
                "{DELIVER_AT_HEADER} and {DELAY_HEADER} are mutually exclusive"
            ));
        }
        (Some(v), None) => v.trim().parse::<u64>().map_err(|_| {
            format!("{DELIVER_AT_HEADER} must be a unix time in milliseconds, got '{v}'")
        })?,
        (None, Some(v)) => {
            let d = v.trim().parse::<u64>().map_err(|_| {
                format!("{DELAY_HEADER} must be milliseconds (a whole number), got '{v}'")
            })?;
            now_ms.saturating_add(d)
        }
    };
    if at > now_ms.saturating_add(MAX_DELAY_MS) {
        return Err(format!(
            "delivery time is more than {} days ahead (the maximum)",
            MAX_DELAY_MS / 86_400_000
        ));
    }
    Ok(if at <= now_ms.saturating_add(IMMEDIATE_WITHIN_MS) {
        Timing::Now
    } else {
        Timing::At(at)
    })
}

/// Whether a message asks for delayed delivery (cheap check before decoding anything).
pub fn is_delayed(headers: &std::collections::BTreeMap<String, String>) -> bool {
    headers.contains_key(DELIVER_AT_HEADER) || headers.contains_key(DELAY_HEADER)
}

/// One pending delayed message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledMessage {
    /// Schedule id: the entry's position in [`SCHEDULED_CHANNEL`].
    pub id: MessageId,
    pub channel: String,
    pub key: Option<String>,
    /// When it will be published (unix ms).
    pub due_ms: u64,
    /// When it was scheduled (unix ms).
    pub scheduled_at_ms: u64,
    /// Payload bytes.
    pub size: u32,
    /// `flux-type` header, when present.
    pub event_type: Option<String>,
    /// Node holding it (the partition's leader).
    pub node: String,
}

/// Pending delayed messages of one channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledChannel {
    pub channel: String,
    pub count: u64,
    pub next_due_ms: u64,
}

/// `GET /api/scheduled?channel=&limit=`: the soonest pending messages (all nodes) and counts
/// per channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledList {
    /// Soonest first, at most `limit` (default 100, max 1000).
    pub items: Vec<ScheduledMessage>,
    /// Pending in total (all channels, or the requested one).
    pub total: u64,
    /// Every channel with pending messages, soonest due first.
    pub channels: Vec<ScheduledChannel>,
    /// Released since the nodes started (summed).
    pub released: u64,
    /// Cancelled since the nodes started (summed).
    pub cancelled: u64,
}

/// What one node reports for [`ScheduledList`] (merged by the API).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeScheduled {
    pub items: Vec<ScheduledMessage>,
    pub channels: Vec<ScheduledChannel>,
    pub released: u64,
    pub cancelled: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn h(k: &str, v: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(k.to_string(), v.to_string())])
    }

    #[test]
    fn timing_reads_both_headers_and_bounds() {
        let now = 1_000_000;
        assert_eq!(timing(&BTreeMap::new(), now), Ok(Timing::Now));
        assert_eq!(
            timing(&h(DELAY_HEADER, "60000"), now),
            Ok(Timing::At(now + 60_000))
        );
        assert_eq!(
            timing(&h(DELIVER_AT_HEADER, "2000000"), now),
            Ok(Timing::At(2_000_000))
        );
        // Past or nearly now: immediate.
        assert_eq!(timing(&h(DELIVER_AT_HEADER, "5"), now), Ok(Timing::Now));
        assert_eq!(timing(&h(DELAY_HEADER, "10"), now), Ok(Timing::Now));
        assert!(timing(&h(DELAY_HEADER, "-1"), now).is_err());
        assert!(timing(&h(DELAY_HEADER, &(MAX_DELAY_MS + 1).to_string()), now).is_err());
        let mut both = h(DELAY_HEADER, "1000");
        both.insert(DELIVER_AT_HEADER.into(), "2000000".into());
        assert!(timing(&both, now).is_err());
        assert!(is_delayed(&h(DELAY_HEADER, "1")));
        assert!(!is_delayed(&h("x", "1")));
    }
}
