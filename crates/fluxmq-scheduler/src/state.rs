//! Pending delayed messages of one `$scheduled` partition, in memory: only (offset, due time,
//! channel) per message — about 100 bytes — never the payload (read back when due).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

#[derive(Debug, Clone)]
struct Entry {
    due: u64,
    channel: Arc<str>,
    /// Not before this (unix ms) after a failed publish.
    retry_at: u64,
}

#[derive(Debug, Default)]
pub struct PartitionState {
    entries: BTreeMap<u64, Entry>,
    by_due: BTreeSet<(u64, u64)>,
    per_channel: HashMap<Arc<str>, u64>,
}

impl PartitionState {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Lowest pending offset: everything below it is settled.
    pub fn low_offset(&self) -> Option<u64> {
        self.entries.keys().next().copied()
    }

    pub fn next_due(&self) -> Option<u64> {
        self.by_due.iter().next().map(|&(d, _)| d)
    }

    pub fn insert(&mut self, offset: u64, due: u64, channel: &str) {
        if self.entries.contains_key(&offset) {
            return;
        }
        let channel = match self.per_channel.get_key_value(channel) {
            Some((k, _)) => k.clone(),
            None => Arc::from(channel),
        };
        *self.per_channel.entry(channel.clone()).or_default() += 1;
        self.by_due.insert((due, offset));
        self.entries.insert(
            offset,
            Entry {
                due,
                channel,
                retry_at: 0,
            },
        );
    }

    /// Forgets an entry (released, cancelled or dropped). `true` when it was pending.
    pub fn remove(&mut self, offset: u64) -> bool {
        let Some(e) = self.entries.remove(&offset) else {
            return false;
        };
        self.by_due.remove(&(e.due, offset));
        if let Some(n) = self.per_channel.get_mut(&e.channel) {
            *n -= 1;
            if *n == 0 {
                self.per_channel.remove(&e.channel);
            }
        }
        true
    }

    /// Makes an entry due now ("release now").
    pub fn release_now(&mut self, offset: u64) -> bool {
        let Some(e) = self.entries.get_mut(&offset) else {
            return false;
        };
        self.by_due.remove(&(e.due, offset));
        e.due = 0;
        e.retry_at = 0;
        self.by_due.insert((0, offset));
        true
    }

    /// Postpones an entry's next try (its publish failed) without changing its due time.
    pub fn retry_later(&mut self, offset: u64, at_ms: u64) {
        if let Some(e) = self.entries.get_mut(&offset) {
            e.retry_at = at_ms;
        }
    }

    /// Up to `max` entries due at `now_ms` (soonest first, skipping ones waiting for a retry).
    pub fn due(&self, now_ms: u64, max: usize) -> Vec<(u64, u64, Arc<str>)> {
        let mut out = Vec::new();
        for &(due, offset) in &self.by_due {
            if due > now_ms || out.len() >= max {
                break;
            }
            let e = &self.entries[&offset];
            if e.retry_at <= now_ms {
                out.push((offset, due, e.channel.clone()));
            }
        }
        out
    }

    /// When the next attempt can happen: the soonest due time, or a retry time after it
    /// (looks at the 64 soonest entries; the caller also wakes periodically).
    pub fn next_wake(&self) -> Option<u64> {
        self.by_due
            .iter()
            .map(|&(due, off)| due.max(self.entries[&off].retry_at))
            .take(64)
            .min()
    }

    /// Pending per channel: (channel, count, soonest due).
    pub fn channels(&self) -> Vec<(Arc<str>, u64, u64)> {
        let mut next: HashMap<&str, u64> = HashMap::new();
        for &(due, off) in &self.by_due {
            if next.len() == self.per_channel.len() {
                break;
            }
            next.entry(&self.entries[&off].channel).or_insert(due);
        }
        self.per_channel
            .iter()
            .map(|(c, &n)| (c.clone(), n, next.get(&**c).copied().unwrap_or(0)))
            .collect()
    }

    /// The soonest `limit` entries (of `channel` when given): (offset, due, channel).
    pub fn upcoming(&self, channel: Option<&str>, limit: usize) -> Vec<(u64, u64, Arc<str>)> {
        self.by_due
            .iter()
            .map(|&(due, off)| (off, due, self.entries[&off].channel.clone()))
            .filter(|(_, _, c)| channel.is_none_or(|w| &**c == w))
            .take(limit)
            .collect()
    }

    pub fn contains(&self, offset: u64) -> bool {
        self.entries.contains_key(&offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn due_order_counts_and_removal() {
        let mut s = PartitionState::default();
        s.insert(10, 300, "orders");
        s.insert(11, 100, "email");
        s.insert(12, 200, "orders");
        s.insert(12, 999, "orders"); // duplicate offset ignored
        assert_eq!(s.len(), 3);
        assert_eq!(s.low_offset(), Some(10));
        assert_eq!(s.next_due(), Some(100));
        let due: Vec<u64> = s.due(250, 10).iter().map(|d| d.0).collect();
        assert_eq!(due, vec![11, 12]);
        let mut ch = s.channels();
        ch.sort();
        assert_eq!(
            ch,
            vec![(Arc::from("email"), 1, 100), (Arc::from("orders"), 2, 200)]
        );
        assert!(s.remove(11));
        assert!(!s.remove(11));
        assert_eq!(s.channels().len(), 1);
        assert_eq!(s.upcoming(Some("orders"), 1)[0].0, 12);
    }

    #[test]
    fn release_now_and_retry_later() {
        let mut s = PartitionState::default();
        s.insert(1, 1_000, "a");
        s.insert(2, 2_000, "a");
        assert!(s.due(10, 10).is_empty());
        assert!(s.release_now(2));
        assert_eq!(s.due(10, 10)[0].0, 2);
        s.retry_later(2, 500);
        assert!(s.due(10, 10).is_empty());
        assert_eq!(s.next_wake(), Some(500));
        assert_eq!(s.due(600, 10)[0].0, 2);
        assert!(!s.release_now(99));
    }
}
