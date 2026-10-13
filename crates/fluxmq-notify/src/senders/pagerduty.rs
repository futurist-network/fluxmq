//! PagerDuty Events API v2: one event per alert — `trigger` while firing (re-triggers on
//! repeat are deduplicated by PagerDuty), `resolve` on recovery, `dedup_key` = alert id.
//! Broker events trigger `info` incidents keyed `fluxmq-event-<id>`; a test triggers and
//! immediately resolves `fluxmq-test-<payload id>`.

use fluxmq_proto::notify::NotificationPayload;
use fluxmq_proto::telemetry::Health;
use serde_json::{Value, json};

use crate::render::{Doc, truncate};

fn severity(h: Health) -> &'static str {
    match h {
        Health::Critical => "critical",
        Health::Warning => "warning",
        Health::Healthy => "info",
    }
}

fn source(p: &NotificationPayload) -> String {
    format!("fluxmq/{}", p.cluster)
}

fn links(doc: &Doc, link: Option<&str>) -> Value {
    match link.or(doc.console_url.as_deref()) {
        Some(href) => json!([{ "href": href, "text": "Open in FluxMQ console" }]),
        None => json!([]),
    }
}

/// The request bodies to POST, in order.
pub fn events(routing_key: &str, p: &NotificationPayload, doc: &Doc) -> Vec<Value> {
    let mut out = Vec::new();
    match p.status.as_str() {
        "resolved" => {
            for a in &p.alerts {
                out.push(json!({
                    "routing_key": routing_key,
                    "event_action": "resolve",
                    "dedup_key": a.id,
                }));
            }
        }
        "event" => {
            for (e, item) in p.events.iter().zip(&doc.items) {
                out.push(json!({
                    "routing_key": routing_key,
                    "event_action": "trigger",
                    "dedup_key": format!("fluxmq-event-{}", e.id),
                    "client": "FluxMQ",
                    "payload": {
                        "summary": truncate(&format!("{}: {}", e.title, e.subject), 1024),
                        "source": source(p),
                        "severity": severity(e.severity),
                        "class": format!("{:?}", e.kind),
                        "custom_details": { "subject": e.subject, "event_id": e.id },
                    },
                    "links": links(doc, item.link.as_deref()),
                }));
            }
        }
        "test" => {
            let key = format!("fluxmq-test-{}", p.id);
            out.push(json!({
                "routing_key": routing_key,
                "event_action": "trigger",
                "dedup_key": key,
                "client": "FluxMQ",
                "payload": {
                    "summary": "FluxMQ test notification (resolves immediately)",
                    "source": source(p),
                    "severity": "info",
                },
            }));
            out.push(json!({
                "routing_key": routing_key,
                "event_action": "resolve",
                "dedup_key": key,
            }));
        }
        _ => {
            for (a, item) in p.alerts.iter().zip(&doc.items) {
                let mut details = json!({ "detail": a.detail, "alert_id": a.id });
                if let Some(r) = &a.possible_reason {
                    details["possible_reason"] = json!(r);
                }
                let mut payload = json!({
                    "summary": truncate(&format!("[{}] {}", p.cluster, a.title), 1024),
                    "source": source(p),
                    "severity": severity(a.severity),
                    "class": serde_json::to_value(a.kind).unwrap_or_default(),
                    "custom_details": details,
                });
                if let Some(c) = &a.channel {
                    payload["component"] = json!(c);
                }
                if let Some(g) = &a.group {
                    payload["group"] = json!(g);
                }
                out.push(json!({
                    "routing_key": routing_key,
                    "event_action": "trigger",
                    "dedup_key": a.id,
                    "client": "FluxMQ",
                    "payload": payload,
                    "links": links(doc, item.link.as_deref()),
                }));
            }
        }
    }
    if let Some(u) = &doc.console_url {
        for e in &mut out {
            if e["event_action"] == "trigger" {
                e["client_url"] = json!(u);
            }
        }
    }
    out
}
