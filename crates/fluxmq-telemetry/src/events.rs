//! Events feed: broker lifecycle events + alert transitions, newest first, at most 100.
//!
//! `DeadLettered` events (core batches them per second) are coalesced further: within
//! [`DLQ_COALESCE_MS`] of a channel's first one, later ones update that entry (count, time)
//! and move it to the top, so a steady trickle of dead letters can't flush the feed.

use std::collections::{HashMap, VecDeque};

use fluxmq_core::BrokerEvent;
use fluxmq_proto::telemetry::{Event, EventKind, EventsFeed, Health};

use crate::alerts::{Params, Transition, kind_str, params};

pub const MAX_EVENTS: usize = 100;
/// Window in which a channel's dead-letter events merge into one feed entry.
pub const DLQ_COALESCE_MS: u64 = 60_000;

#[derive(Debug, Default)]
pub struct EventLog {
    next_id: u64,
    events: VecDeque<Event>,
    changed: bool,
    /// channel → (event id, accumulated count, window start).
    dlq_runs: HashMap<String, (u64, u64, u64)>,
    /// Events added since the last [`EventLog::take_fresh`] (oldest first). Coalesced
    /// dead-letter updates are not repeated here.
    fresh: Vec<Event>,
}

fn plural(n: u64) -> &'static str {
    if n == 1 { "message" } else { "messages" }
}

/// `email-service#12` → `email-service`.
fn client_name(instance: &str) -> &str {
    instance.rsplit_once('#').map_or(instance, |(name, _)| name)
}

/// Maps a broker event to `(kind, title, subject, severity)`.
pub fn describe(event: &BrokerEvent) -> (EventKind, String, String, Health) {
    match event {
        BrokerEvent::ChannelCreated { channel } => (
            EventKind::ChannelCreated,
            "Channel created".into(),
            channel.clone(),
            Health::Healthy,
        ),
        BrokerEvent::ChannelDeleted { channel } => (
            EventKind::ChannelDeleted,
            "Channel deleted".into(),
            channel.clone(),
            Health::Warning,
        ),
        BrokerEvent::ChannelUpdated { channel } => (
            EventKind::ChannelUpdated,
            "Channel updated".into(),
            channel.clone(),
            Health::Healthy,
        ),
        BrokerEvent::ConsumerConnected { instance, .. } => (
            EventKind::ConsumerConnected,
            "Consumer connected".into(),
            client_name(instance).to_string(),
            Health::Healthy,
        ),
        BrokerEvent::ConsumerDisconnected { instance, .. } => (
            EventKind::ConsumerDisconnected,
            "Consumer disconnected".into(),
            client_name(instance).to_string(),
            Health::Healthy,
        ),
        BrokerEvent::DeadLettered { channel, count, .. } => (
            EventKind::DeadLettered,
            "Messages rerouted to DLQ".into(),
            format!("{channel} ({count} {})", plural(*count)),
            Health::Warning,
        ),
        BrokerEvent::DlqRetried { count } => (
            EventKind::DlqRetried,
            "Dead letters retried".into(),
            format!("{count} {}", plural(*count)),
            Health::Healthy,
        ),
        BrokerEvent::DlqDiscarded { count } => (
            EventKind::DlqDiscarded,
            "Dead letters discarded".into(),
            format!("{count} {}", plural(*count)),
            Health::Healthy,
        ),
        BrokerEvent::MessagesReplayed { channel, count } => (
            EventKind::MessagesReplayed,
            "Messages replayed".into(),
            format!("{channel} ({count} {})", plural(*count)),
            Health::Healthy,
        ),
    }
}

/// Structured params of a broker event (see [`Event::params`]): `dead_lettered` and
/// `messages_replayed` → `{channel, count}`, `dlq_retried`/`dlq_discarded` → `{count}`.
pub fn params_of(event: &BrokerEvent) -> Params {
    match event {
        BrokerEvent::DeadLettered { channel, count, .. }
        | BrokerEvent::MessagesReplayed { channel, count } => params([
            ("channel", channel.as_str().into()),
            ("count", (*count).into()),
        ]),
        BrokerEvent::DlqRetried { count } | BrokerEvent::DlqDiscarded { count } => {
            params([("count", (*count).into())])
        }
        _ => Params::new(),
    }
}

/// `{alert_kind, alert_id, channel?, group?, instance?}` of an alert transition event.
fn alert_event_params(a: &fluxmq_proto::telemetry::Alert) -> Params {
    let mut p = params([
        ("alert_kind", kind_str(a.kind).into()),
        ("alert_id", a.id.as_str().into()),
    ]);
    if let Some(c) = &a.channel {
        p.insert("channel".into(), c.as_str().into());
    }
    if let Some(g) = &a.group {
        p.insert("group".into(), g.as_str().into());
    }
    if let Some(i) = a.params.get("instance") {
        p.insert("instance".into(), i.clone());
    }
    p
}

impl EventLog {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            ..Default::default()
        }
    }

    pub fn push(
        &mut self,
        ts: u64,
        kind: EventKind,
        title: String,
        subject: String,
        severity: Health,
    ) {
        self.push_with(ts, kind, title, subject, severity, Params::new());
    }

    /// [`EventLog::push`] with structured params for client-side localisation.
    pub fn push_with(
        &mut self,
        ts: u64,
        kind: EventKind,
        title: String,
        subject: String,
        severity: Health,
        params: Params,
    ) {
        let id = self.next_id;
        self.next_id += 1;
        let event = Event {
            id,
            ts,
            kind,
            title,
            subject,
            severity,
            params,
        };
        if self.fresh.len() < MAX_EVENTS {
            self.fresh.push(event.clone());
        }
        self.events.push_front(event);
        self.events.truncate(MAX_EVENTS);
        self.changed = true;
    }

    pub fn push_broker(&mut self, ts: u64, event: &BrokerEvent) {
        if let BrokerEvent::DeadLettered { channel, count, .. } = event {
            self.push_dead_lettered(ts, channel, *count);
            return;
        }
        let (kind, title, subject, severity) = describe(event);
        self.push_with(ts, kind, title, subject, severity, params_of(event));
    }

    fn push_dead_lettered(&mut self, ts: u64, channel: &str, count: u64) {
        if let Some(&(id, total, start)) = self.dlq_runs.get(channel)
            && ts.saturating_sub(start) < DLQ_COALESCE_MS
            && let Some(pos) = self.events.iter().position(|e| e.id == id)
            && let Some(mut ev) = self.events.remove(pos)
        {
            let total = total + count;
            ev.ts = ts;
            ev.subject = format!("{channel} ({total} {})", plural(total));
            ev.params.insert("count".into(), total.into());
            self.events.push_front(ev);
            self.dlq_runs
                .insert(channel.to_string(), (id, total, start));
            self.changed = true;
            return;
        }
        let event = BrokerEvent::DeadLettered {
            channel: channel.to_string(),
            group: String::new(),
            reason: String::new(),
            count,
        };
        let (kind, title, subject, severity) = describe(&event);
        let id = self.next_id;
        self.push_with(ts, kind, title, subject, severity, params_of(&event));
        self.dlq_runs.insert(channel.to_string(), (id, count, ts));
    }

    pub fn push_transition(&mut self, ts: u64, t: &Transition) {
        match t {
            Transition::Opened(a) => {
                let subject = alert_subject(a);
                self.push_with(
                    ts,
                    EventKind::AlertOpened,
                    a.title.clone(),
                    subject,
                    a.severity,
                    alert_event_params(a),
                );
            }
            Transition::Resolved(a) => {
                let subject = alert_subject(a);
                self.push_with(
                    ts,
                    EventKind::AlertResolved,
                    format!("Resolved: {}", a.title),
                    subject,
                    Health::Healthy,
                    alert_event_params(a),
                );
            }
        }
    }

    /// Returns whether anything was added since the last call.
    pub fn take_changed(&mut self) -> bool {
        std::mem::take(&mut self.changed)
    }

    /// Drains the events added since the last call (oldest first, at most [`MAX_EVENTS`]).
    pub fn take_fresh(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.fresh)
    }

    pub fn feed(&self) -> EventsFeed {
        EventsFeed {
            events: self.events.iter().cloned().collect(),
        }
    }
}

fn alert_subject(a: &fluxmq_proto::telemetry::Alert) -> String {
    match (&a.channel, &a.group) {
        (Some(c), Some(g)) => format!("{c} / {g}"),
        (Some(c), None) => c.clone(),
        _ => {
            a.id.split_once(':')
                .map_or_else(|| a.id.clone(), |(_, s)| s.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::telemetry::{Alert, AlertKind};

    #[test]
    fn titles_and_subjects() {
        let (_, t, s, _) = describe(&BrokerEvent::ChannelCreated {
            channel: "audit.logs".into(),
        });
        assert_eq!((t.as_str(), s.as_str()), ("Channel created", "audit.logs"));
        let (_, t, s, _) = describe(&BrokerEvent::ConsumerConnected {
            channel: "email".into(),
            group: "g".into(),
            instance: "email-service#12".into(),
        });
        assert_eq!(
            (t.as_str(), s.as_str()),
            ("Consumer connected", "email-service")
        );
        let (k, t, s, sev) = describe(&BrokerEvent::DeadLettered {
            channel: "payments.events".into(),
            group: "g".into(),
            reason: "boom".into(),
            count: 24,
        });
        assert_eq!(k, EventKind::DeadLettered);
        assert_eq!(
            (t.as_str(), s.as_str()),
            ("Messages rerouted to DLQ", "payments.events (24 messages)")
        );
        assert_eq!(sev, Health::Warning);
        let (_, _, s, _) = describe(&BrokerEvent::DlqRetried { count: 1 });
        assert_eq!(s, "1 message");
    }

    #[test]
    fn newest_first_bounded() {
        let mut log = EventLog::new();
        for i in 0..150 {
            log.push_broker(
                i,
                &BrokerEvent::ChannelCreated {
                    channel: format!("c{i}"),
                },
            );
        }
        assert!(log.take_changed());
        assert!(!log.take_changed());
        let feed = log.feed();
        assert_eq!(feed.events.len(), MAX_EVENTS);
        assert_eq!(feed.events[0].subject, "c149");
        assert_eq!(feed.events[0].id, 150);
        assert_eq!(feed.events[99].subject, "c50");
    }

    #[test]
    fn dead_letters_coalesce_per_channel_window() {
        let mut log = EventLog::new();
        let dl = |channel: &str, count| BrokerEvent::DeadLettered {
            channel: channel.into(),
            group: "g".into(),
            reason: "r".into(),
            count,
        };
        log.push_broker(1_000, &dl("payments", 1));
        log.push_broker(2_000, &dl("orders", 2));
        log.push_broker(3_000, &dl("payments", 2));
        let f = log.feed();
        assert_eq!(f.events.len(), 2);
        assert_eq!(f.events[0].subject, "payments (3 messages)");
        assert_eq!(f.events[0].params["count"], 3);
        assert_eq!(f.events[0].params["channel"], "payments");
        assert_eq!(f.events[0].ts, 3_000);
        assert_eq!(f.events[0].id, 1, "entry keeps its id");
        assert_eq!(f.events[1].subject, "orders (2 messages)");
        // A new window starts a new entry.
        log.push_broker(1_000 + DLQ_COALESCE_MS, &dl("payments", 1));
        let f = log.feed();
        assert_eq!(f.events.len(), 3);
        assert_eq!(f.events[0].subject, "payments (1 message)");
    }

    #[test]
    fn alert_transitions() {
        let mut log = EventLog::new();
        let a = Alert {
            id: "lag_growth:email/workers".into(),
            severity: Health::Critical,
            kind: AlertKind::LagGrowth,
            title: "Email queue is growing rapidly".into(),
            detail: String::new(),
            possible_reason: None,
            link: None,
            channel: Some("email".into()),
            group: Some("workers".into()),
            since_ms: 0,
            params: Default::default(),
        };
        log.push_transition(1, &Transition::Opened(a.clone()));
        log.push_transition(2, &Transition::Resolved(a));
        let f = log.feed();
        assert_eq!(f.events[0].kind, EventKind::AlertResolved);
        assert_eq!(
            f.events[0].title,
            "Resolved: Email queue is growing rapidly"
        );
        assert_eq!(f.events[1].kind, EventKind::AlertOpened);
        assert_eq!(f.events[1].severity, Health::Critical);
        assert_eq!(f.events[1].subject, "email / workers");
        assert_eq!(f.events[1].params["alert_kind"], "lag_growth");
        assert_eq!(f.events[1].params["alert_id"], "lag_growth:email/workers");
        assert_eq!(f.events[1].params["group"], "workers");
        assert_eq!(f.events[0].params["channel"], "email");
    }
}
