//! Message copy shared by every sender: a [`NotificationPayload`] → a small document model
//! (headline + items + footer) that each channel formats in its own markup.
//!
//! Copy rules: concise, one severity marker per item, title, detail, possible reason,
//! channel / group, a console link when `--public-url` is configured.

use fluxmq_proto::notify::NotificationPayload;
use fluxmq_proto::telemetry::{Alert, Event, Health};

/// Max chars of free text taken from one alert field.
pub const MAX_FIELD: usize = 600;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tone {
    Critical,
    Warning,
    Resolved,
    Info,
}

impl Tone {
    pub fn marker(self) -> &'static str {
        match self {
            Tone::Critical => "🔴",
            Tone::Warning => "🟠",
            Tone::Resolved => "✅",
            Tone::Info => "ℹ️",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Tone::Critical => "CRITICAL",
            Tone::Warning => "WARNING",
            Tone::Resolved => "RESOLVED",
            Tone::Info => "INFO",
        }
    }

    /// Discord embed / accent colour.
    pub fn color(self) -> u32 {
        match self {
            Tone::Critical => 0xE5484D,
            Tone::Warning => 0xF5A524,
            Tone::Resolved => 0x30A46C,
            Tone::Info => 0x5B8DEF,
        }
    }

    fn of(sev: Health, resolved: bool) -> Tone {
        match (resolved, sev) {
            (true, _) => Tone::Resolved,
            (false, Health::Critical) => Tone::Critical,
            (false, Health::Warning) => Tone::Warning,
            (false, Health::Healthy) => Tone::Info,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub tone: Tone,
    pub title: String,
    pub detail: Option<String>,
    pub reason: Option<String>,
    /// `("Channel", "orders")`, `("Group", "billing")`, `("Open for", "6m")`.
    pub scope: Vec<(&'static str, String)>,
    /// Absolute console link (only with a public URL).
    pub link: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Doc {
    /// Overall tone (worst item).
    pub tone: Tone,
    pub headline: String,
    pub items: Vec<Item>,
    pub cluster: String,
    pub console_url: Option<String>,
}

pub fn truncate(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_chars.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn link(public_url: Option<&str>, path: Option<&str>) -> Option<String> {
    let base = public_url?.trim_end_matches('/');
    let path = path?;
    Some(if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    })
}

pub fn fmt_age(ms: u64) -> String {
    let s = ms / 1000;
    match s {
        0..=59 => format!("{s}s"),
        60..=3599 => format!("{}m", s / 60),
        3600..=86_399 => format!("{}h {}m", s / 3600, (s % 3600) / 60),
        _ => format!("{}d {}h", s / 86_400, (s % 86_400) / 3600),
    }
}

fn alert_item(a: &Alert, resolved: bool, now_ms: u64, public_url: Option<&str>) -> Item {
    let mut scope = Vec::new();
    if let Some(c) = &a.channel {
        scope.push(("Channel", c.clone()));
    }
    if let Some(g) = &a.group {
        scope.push(("Group", g.clone()));
    }
    if a.since_ms > 0 && now_ms > a.since_ms {
        let label = if resolved { "Lasted" } else { "Open for" };
        scope.push((label, fmt_age(now_ms - a.since_ms)));
    }
    Item {
        tone: Tone::of(a.severity, resolved),
        title: truncate(&a.title, 200),
        detail: (!a.detail.is_empty()).then(|| truncate(&a.detail, MAX_FIELD)),
        reason: if resolved {
            None
        } else {
            a.possible_reason
                .as_deref()
                .filter(|r| !r.is_empty())
                .map(|r| truncate(r, MAX_FIELD))
        },
        scope,
        link: link(public_url, a.link.as_deref()),
    }
}

fn event_item(e: &Event, public_url: Option<&str>) -> Item {
    let channel = crate::matcher::event_channel(e);
    Item {
        tone: Tone::of(e.severity, false),
        title: truncate(&e.title, 200),
        detail: (!e.subject.is_empty()).then(|| truncate(&e.subject, MAX_FIELD)),
        reason: None,
        scope: Vec::new(),
        link: channel.and_then(|c| link(public_url, Some(&format!("/channels/{c}")))),
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {many}")
    }
}

/// Builds the document for `payload` (`public_url` = `--public-url`, for item links).
pub fn doc(payload: &NotificationPayload, public_url: Option<&str>) -> Doc {
    let now = payload.sent_at_ms;
    let (items, headline): (Vec<Item>, String) = match payload.status.as_str() {
        "resolved" => {
            let items: Vec<Item> = payload
                .alerts
                .iter()
                .map(|a| alert_item(a, true, now, public_url))
                .collect();
            let h = match payload.alerts.as_slice() {
                [a] => format!("Resolved: {}", truncate(&a.title, 200)),
                v => format!("{} resolved", plural(v.len(), "issue", "issues")),
            };
            (items, h)
        }
        "event" => {
            let items: Vec<Item> = payload
                .events
                .iter()
                .map(|e| event_item(e, public_url))
                .collect();
            let h = match payload.events.as_slice() {
                [e] => truncate(&e.title, 200),
                v => plural(v.len(), "event", "events"),
            };
            (items, h)
        }
        "test" => (
            vec![Item {
                tone: Tone::Info,
                title: "Test notification".into(),
                detail: Some("If you can read this, FluxMQ can deliver notifications here.".into()),
                reason: None,
                scope: Vec::new(),
                link: None,
            }],
            "FluxMQ test notification".into(),
        ),
        _ => {
            let items: Vec<Item> = payload
                .alerts
                .iter()
                .map(|a| alert_item(a, false, now, public_url))
                .collect();
            let h = match payload.alerts.as_slice() {
                [a] => truncate(&a.title, 200),
                v => format!("{} need attention", plural(v.len(), "issue", "issues")),
            };
            (items, h)
        }
    };
    let tone = items
        .iter()
        .map(|i| i.tone)
        .min_by_key(|t| match t {
            Tone::Critical => 0,
            Tone::Warning => 1,
            Tone::Info => 2,
            Tone::Resolved => 3,
        })
        .unwrap_or(Tone::Info);
    Doc {
        tone,
        headline,
        items,
        cluster: payload.cluster.clone(),
        console_url: payload.console_url.clone(),
    }
}

/// Plain text (email body, fallbacks).
pub fn plain(doc: &Doc) -> String {
    let mut out = String::new();
    out.push_str(&format!("{} {}\n", doc.tone.marker(), doc.headline));
    for it in &doc.items {
        out.push('\n');
        out.push_str(&format!(
            "{} [{}] {}\n",
            it.tone.marker(),
            it.tone.label(),
            it.title
        ));
        if let Some(d) = &it.detail {
            out.push_str(&format!("{d}\n"));
        }
        if let Some(r) = &it.reason {
            out.push_str(&format!("Possible reason: {r}\n"));
        }
        if !it.scope.is_empty() {
            let s: Vec<String> = it.scope.iter().map(|(k, v)| format!("{k}: {v}")).collect();
            out.push_str(&format!("{}\n", s.join(" · ")));
        }
        if let Some(l) = &it.link {
            out.push_str(&format!("{l}\n"));
        }
    }
    out.push_str(&format!("\n— FluxMQ, cluster \"{}\"", doc.cluster));
    if let Some(u) = &doc.console_url {
        out.push_str(&format!("\nConsole: {u}"));
    }
    out.push('\n');
    out
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use fluxmq_proto::telemetry::AlertKind;

    pub fn alert(id: &str, sev: Health) -> Alert {
        Alert {
            id: id.into(),
            severity: sev,
            kind: AlertKind::LagGrowth,
            title: "Email queue is growing rapidly".into(),
            detail: "Lag increased from 1.2k → 18k in 6 min".into(),
            possible_reason: Some("Consumers processing slower than producers.".into()),
            link: Some("/channels/email/consumers".into()),
            channel: Some("email".into()),
            group: Some("workers".into()),
            since_ms: 1_000,
            params: Default::default(),
        }
    }

    pub fn payload(status: &str, alerts: Vec<Alert>) -> NotificationPayload {
        NotificationPayload {
            id: "n1".into(),
            cluster: "prod".into(),
            sent_at_ms: 361_000,
            rule: "Ops".into(),
            status: status.into(),
            alerts,
            events: vec![],
            console_url: Some("https://mq.example.com".into()),
        }
    }

    #[test]
    fn single_firing_alert() {
        let d = doc(
            &payload("firing", vec![alert("a", Health::Critical)]),
            Some("https://mq.example.com/"),
        );
        assert_eq!(d.headline, "Email queue is growing rapidly");
        assert_eq!(d.tone, Tone::Critical);
        let it = &d.items[0];
        assert_eq!(
            it.link.as_deref(),
            Some("https://mq.example.com/channels/email/consumers")
        );
        assert_eq!(
            it.scope,
            vec![
                ("Channel", "email".to_string()),
                ("Group", "workers".to_string()),
                ("Open for", "6m".to_string())
            ]
        );
        let text = plain(&d);
        assert!(text.contains("[CRITICAL] Email queue is growing rapidly"));
        assert!(text.contains("Possible reason: Consumers processing slower"));
        assert!(text.contains("Console: https://mq.example.com"));
    }

    #[test]
    fn grouped_and_resolved_headlines() {
        let p = payload(
            "firing",
            vec![alert("a", Health::Warning), alert("b", Health::Critical)],
        );
        let d = doc(&p, None);
        assert_eq!(d.headline, "2 issues need attention");
        assert_eq!(d.tone, Tone::Critical);
        assert!(d.items[0].link.is_none(), "no public url → no links");
        let r = doc(
            &payload("resolved", vec![alert("a", Health::Warning)]),
            None,
        );
        assert_eq!(r.headline, "Resolved: Email queue is growing rapidly");
        assert_eq!(r.tone, Tone::Resolved);
        assert!(r.items[0].reason.is_none());
    }
}
