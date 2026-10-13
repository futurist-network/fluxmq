//! Rule matching: which alerts / events a [`NotificationRule`] selects.
//!
//! * Alerts: `severity ≥ min_severity`, kind in `alert_kinds` (empty = any), channel matches
//!   one of the `channels` globs (empty = any; an alert without a channel matches only when
//!   `channels` is empty).
//! * Events: kind in `event_kinds` (empty = **none** — events are opt-in), channel (derived
//!   from the event subject for channel-scoped kinds) matches `channels` like alerts.
//!
//! Globs: `*` = any run of characters (dots included), `?` = exactly one character, anything
//! else literal. `orders.*` matches `orders.eu` and `orders.eu.retry`, not `orders`.

use fluxmq_proto::notify::{NotificationRule, RuleMatch};
use fluxmq_proto::telemetry::{Alert, Event, EventKind};

/// Whether `rule` (enabled or not — callers check `enabled`) selects `alert`.
pub fn matches_alert(m: &RuleMatch, alert: &Alert) -> bool {
    alert.severity >= m.min_severity
        && (m.alert_kinds.is_empty() || m.alert_kinds.contains(&alert.kind))
        && channel_ok(&m.channels, alert.channel.as_deref())
}

/// Whether `rule` selects `event`.
pub fn matches_event(m: &RuleMatch, event: &Event) -> bool {
    m.event_kinds.contains(&event.kind) && channel_ok(&m.channels, event_channel(event))
}

/// Enabled rules matching `alert`.
pub fn rules_for_alert<'a>(
    rules: impl IntoIterator<Item = &'a NotificationRule>,
    alert: &Alert,
) -> Vec<&'a NotificationRule> {
    rules
        .into_iter()
        .filter(|r| r.enabled && matches_alert(&r.matcher, alert))
        .collect()
}

fn channel_ok(globs: &[String], channel: Option<&str>) -> bool {
    if globs.is_empty() {
        return true;
    }
    channel.is_some_and(|c| globs.iter().any(|g| glob_match(g, c)))
}

/// The channel an event is about, for channel-scoped kinds (subjects are `orders` or
/// `orders (3 messages)`).
pub fn event_channel(event: &Event) -> Option<&str> {
    match event.kind {
        EventKind::ChannelCreated | EventKind::ChannelDeleted | EventKind::ChannelUpdated => {
            Some(event.subject.as_str())
        }
        EventKind::DeadLettered | EventKind::MessagesReplayed => Some(
            event
                .subject
                .split_once(" (")
                .map_or(event.subject.as_str(), |(c, _)| c),
        ),
        EventKind::AlertOpened | EventKind::AlertResolved => event
            .subject
            .split_once(" / ")
            .map(|(c, _)| c)
            .or(Some(event.subject.as_str())),
        _ => None,
    }
}

/// `*` / `?` glob over chars (iterative, linear backtracking).
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxmq_proto::telemetry::{AlertKind, Health};

    fn alert(kind: AlertKind, sev: Health, channel: Option<&str>) -> Alert {
        Alert {
            id: "x".into(),
            severity: sev,
            kind,
            title: "t".into(),
            detail: "d".into(),
            possible_reason: None,
            link: None,
            channel: channel.map(Into::into),
            group: None,
            since_ms: 0,
            params: Default::default(),
        }
    }

    fn m(min: Health, kinds: &[AlertKind], events: &[EventKind], ch: &[&str]) -> RuleMatch {
        RuleMatch {
            min_severity: min,
            alert_kinds: kinds.to_vec(),
            event_kinds: events.to_vec(),
            channels: ch.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn globs() {
        assert!(glob_match("orders.*", "orders.eu"));
        assert!(glob_match("orders.*", "orders.eu.retry"));
        assert!(!glob_match("orders.*", "orders"));
        assert!(glob_match("*", ""));
        assert!(glob_match("o?ders", "orders"));
        assert!(!glob_match("o?ders", "oders"));
        assert!(glob_match("*.dlq", "a.b.dlq"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXbYY"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exact2"));
        assert!(glob_match("$system.*", "$system.events"));
    }

    #[test]
    fn severity_kinds_channels() {
        let crit = alert(AlertKind::LagGrowth, Health::Critical, Some("orders.eu"));
        let warn = alert(AlertKind::LagGrowth, Health::Warning, Some("orders.eu"));
        let node = alert(AlertKind::NodeDown, Health::Critical, None);

        let any_warn = m(Health::Warning, &[], &[], &[]);
        assert!(matches_alert(&any_warn, &crit));
        assert!(matches_alert(&any_warn, &warn));
        assert!(matches_alert(&any_warn, &node));

        let only_crit = m(Health::Critical, &[], &[], &[]);
        assert!(matches_alert(&only_crit, &crit));
        assert!(!matches_alert(&only_crit, &warn));

        let kinds = m(Health::Warning, &[AlertKind::NodeDown], &[], &[]);
        assert!(!matches_alert(&kinds, &crit));
        assert!(matches_alert(&kinds, &node));

        let chans = m(Health::Warning, &[], &[], &["orders.*"]);
        assert!(matches_alert(&chans, &crit));
        assert!(
            !matches_alert(&chans, &node),
            "no channel → only when channels empty"
        );
        let other = alert(AlertKind::LagGrowth, Health::Critical, Some("billing"));
        assert!(!matches_alert(&chans, &other));
    }

    #[test]
    fn events_are_opt_in() {
        let ev = Event {
            id: 1,
            ts: 0,
            kind: EventKind::DeadLettered,
            title: "Messages rerouted to DLQ".into(),
            subject: "orders.eu (3 messages)".into(),
            severity: Health::Warning,
            params: Default::default(),
        };
        assert!(!matches_event(&m(Health::Warning, &[], &[], &[]), &ev));
        let r = m(Health::Critical, &[], &[EventKind::DeadLettered], &[]);
        assert!(
            matches_event(&r, &ev),
            "min_severity does not filter events"
        );
        let r = m(
            Health::Warning,
            &[],
            &[EventKind::DeadLettered],
            &["orders.*"],
        );
        assert!(matches_event(&r, &ev));
        let r = m(
            Health::Warning,
            &[],
            &[EventKind::DeadLettered],
            &["billing"],
        );
        assert!(!matches_event(&r, &ev));
        let conn = Event {
            kind: EventKind::ConsumerConnected,
            subject: "svc".into(),
            ..ev
        };
        let r = m(
            Health::Warning,
            &[],
            &[EventKind::ConsumerConnected],
            &["svc"],
        );
        assert!(
            !matches_event(&r, &conn),
            "consumer events carry no channel"
        );
        let r = m(Health::Warning, &[], &[EventKind::ConsumerConnected], &[]);
        assert!(matches_event(&r, &conn));
    }
}
