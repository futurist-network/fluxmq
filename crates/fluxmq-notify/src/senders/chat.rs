//! Chat formats: Telegram (HTML), Slack (Block Kit), Discord (embeds).

use serde_json::{Value, json};

use super::{HttpResponse, SendError, expect_2xx};
use crate::render::{Doc, Item, truncate};

// ---------------------------------------------------------------------------------------
// Telegram
// ---------------------------------------------------------------------------------------

/// Telegram's `sendMessage` text limit is 4096 characters (after entity parsing); the raw
/// HTML we count is never shorter than the parsed text, so staying under this is safe.
pub const TELEGRAM_LIMIT: usize = 4000;

/// Escapes text for Telegram HTML parse mode (also safe inside `href="…"`).
pub fn tg_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            c => out.push(c),
        }
    }
    out
}

fn tg_item(it: &Item) -> String {
    let mut s = format!(
        "{} <b>{}</b> · {}",
        it.tone.marker(),
        it.tone.label(),
        tg_escape(&it.title)
    );
    if let Some(d) = &it.detail {
        s.push('\n');
        s.push_str(&tg_escape(d));
    }
    if let Some(r) = &it.reason {
        s.push_str(&format!("\n<i>Possible reason: {}</i>", tg_escape(r)));
    }
    if !it.scope.is_empty() {
        let parts: Vec<String> = it
            .scope
            .iter()
            .map(|(k, v)| format!("{k}: <code>{}</code>", tg_escape(v)))
            .collect();
        s.push('\n');
        s.push_str(&parts.join(" · "));
    }
    if let Some(l) = &it.link {
        s.push_str(&format!(
            "\n<a href=\"{}\">Open in console</a>",
            tg_escape(l)
        ));
    }
    s
}

/// The message text: headline, items (as many as fit), footer.
pub fn telegram_text(doc: &Doc) -> String {
    let head = if doc.items.len() > 1 || doc.items.is_empty() {
        format!("<b>{}</b>\n\n", tg_escape(&doc.headline))
    } else {
        String::new()
    };
    let mut footer = format!("\n\n<i>FluxMQ · {}</i>", tg_escape(&doc.cluster));
    if let Some(u) = &doc.console_url {
        footer.push_str(&format!(" · <a href=\"{}\">console</a>", tg_escape(u)));
    }
    let budget = TELEGRAM_LIMIT.saturating_sub(head.chars().count() + footer.chars().count() + 40);
    let mut body = String::new();
    let mut used = 0usize;
    let mut shown = 0usize;
    for it in &doc.items {
        let chunk = tg_item(it);
        let sep = if shown == 0 { 0 } else { 2 };
        let n = chunk.chars().count() + sep;
        if used + n > budget {
            if shown == 0 {
                // A single huge item: plain-text fallback, cut to fit (no markup to break).
                body = tg_escape(&truncate(&it.title, budget / 6));
                shown = 1;
            }
            break;
        }
        if sep > 0 {
            body.push_str("\n\n");
        }
        body.push_str(&chunk);
        used += n;
        shown += 1;
    }
    if shown < doc.items.len() {
        body.push_str(&format!("\n\n… and {} more", doc.items.len() - shown));
    }
    format!("{head}{body}{footer}")
}

pub fn telegram_body(doc: &Doc, chat_id: &str, thread_id: Option<i64>) -> Value {
    let chat: Value = match chat_id.parse::<i64>() {
        Ok(n) => json!(n),
        Err(_) => json!(chat_id),
    };
    let mut body = json!({
        "chat_id": chat,
        "text": telegram_text(doc),
        "parse_mode": "HTML",
        "link_preview_options": { "is_disabled": true },
    });
    if let Some(t) = thread_id {
        body["message_thread_id"] = json!(t);
    }
    body
}

/// Telegram answers `{"ok": false, "description": …, "parameters": {"retry_after": n}}`.
pub fn telegram_check(r: HttpResponse) -> Result<(), SendError> {
    if (200..300).contains(&r.status) {
        return Ok(());
    }
    let parsed: Option<Value> = serde_json::from_str(&r.body).ok();
    let desc = parsed
        .as_ref()
        .and_then(|v| v["description"].as_str())
        .map(str::to_string);
    let retry_after = parsed
        .as_ref()
        .and_then(|v| v["parameters"]["retry_after"].as_u64())
        .map(std::time::Duration::from_secs)
        .or(r.retry_after);
    match desc {
        Some(d) => Err(SendError {
            message: format!("HTTP {}", r.status),
            detail: Some(format!("Telegram {}: {d}", r.status)),
            retryable: r.status == 429 || r.status >= 500,
            retry_after,
        }),
        None => expect_2xx(r),
    }
}

// ---------------------------------------------------------------------------------------
// Slack
// ---------------------------------------------------------------------------------------

/// Slack mrkdwn needs `&`, `<`, `>` escaped.
pub fn slack_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Max alert sections per message (Slack allows 50 blocks).
pub const SLACK_MAX_ITEMS: usize = 20;

pub fn slack_body(doc: &Doc) -> Value {
    let mut blocks = vec![json!({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": truncate(&format!("{} {}", doc.tone.marker(), doc.headline), 150),
            "emoji": true
        }
    })];
    for it in doc.items.iter().take(SLACK_MAX_ITEMS) {
        let mut text = format!(
            "{} *{}* · *{}*",
            it.tone.marker(),
            it.tone.label(),
            slack_escape(&it.title)
        );
        if let Some(d) = &it.detail {
            text.push_str(&format!("\n{}", slack_escape(d)));
        }
        if let Some(r) = &it.reason {
            text.push_str(&format!("\n_Possible reason: {}_", slack_escape(r)));
        }
        if let Some(l) = &it.link {
            text.push_str(&format!("\n<{}|Open in console>", slack_escape(l)));
        }
        blocks.push(json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": truncate(&text, 3000) }
        }));
        if !it.scope.is_empty() {
            let parts: Vec<String> = it
                .scope
                .iter()
                .map(|(k, v)| format!("{k}: `{}`", slack_escape(v)))
                .collect();
            blocks.push(json!({
                "type": "context",
                "elements": [{ "type": "mrkdwn", "text": parts.join(" · ") }]
            }));
        }
    }
    if doc.items.len() > SLACK_MAX_ITEMS {
        blocks.push(json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!("… and {} more", doc.items.len() - SLACK_MAX_ITEMS)
            }
        }));
    }
    let mut footer = format!("FluxMQ · cluster *{}*", slack_escape(&doc.cluster));
    if let Some(u) = &doc.console_url {
        footer.push_str(&format!(" · <{}|console>", slack_escape(u)));
    }
    blocks.push(json!({
        "type": "context",
        "elements": [{ "type": "mrkdwn", "text": footer }]
    }));
    json!({
        "text": format!("{} {}", doc.tone.label(), doc.headline),
        "blocks": blocks,
    })
}

// ---------------------------------------------------------------------------------------
// Discord
// ---------------------------------------------------------------------------------------

/// Backslash-escapes Discord markdown.
pub fn discord_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(
            c,
            '\\' | '*' | '_' | '~' | '`' | '|' | '>' | '#' | '[' | ']' | '(' | ')'
        ) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Discord: ≤ 10 embeds, ≤ 6000 characters across all embeds.
pub const DISCORD_MAX_EMBEDS: usize = 10;
const DISCORD_TOTAL: usize = 5500;

pub fn discord_body(doc: &Doc) -> Value {
    let mut embeds = Vec::new();
    let mut total = 0usize;
    for it in doc.items.iter().take(DISCORD_MAX_EMBEDS) {
        let title = truncate(&format!("{} {}", it.tone.label(), it.title), 256);
        let mut desc = String::new();
        if let Some(d) = &it.detail {
            desc.push_str(&discord_escape(d));
        }
        if let Some(r) = &it.reason {
            desc.push_str(&format!("\n*Possible reason: {}*", discord_escape(r)));
        }
        let desc = truncate(&desc, 1500);
        let fields: Vec<Value> = it
            .scope
            .iter()
            .map(|(k, v)| json!({ "name": k, "value": truncate(v, 200), "inline": true }))
            .collect();
        let footer = format!("FluxMQ · {}", doc.cluster);
        let size = title.chars().count()
            + desc.chars().count()
            + footer.chars().count()
            + it.scope
                .iter()
                .map(|(k, v)| k.len() + v.chars().count().min(200))
                .sum::<usize>();
        if total + size > DISCORD_TOTAL && !embeds.is_empty() {
            break;
        }
        total += size;
        let mut e = json!({
            "title": title,
            "color": it.tone.color(),
            "footer": { "text": footer },
        });
        if !desc.is_empty() {
            e["description"] = json!(desc);
        }
        if !fields.is_empty() {
            e["fields"] = json!(fields);
        }
        if let Some(l) = &it.link {
            e["url"] = json!(l);
        }
        embeds.push(e);
    }
    let mut content = format!(
        "{} **{}**",
        doc.tone.marker(),
        discord_escape(&doc.headline)
    );
    if embeds.len() < doc.items.len() {
        content.push_str(&format!(" (+{} more)", doc.items.len() - embeds.len()));
    }
    if let Some(u) = &doc.console_url {
        content.push_str(&format!("\n<{u}>"));
    }
    json!({
        "username": "FluxMQ",
        "content": truncate(&content, 2000),
        "embeds": embeds,
        "allowed_mentions": { "parse": [] },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{doc, tests as r};
    use fluxmq_proto::telemetry::Health;

    #[test]
    fn telegram_escapes_and_limits() {
        let mut a = r::alert("a", Health::Critical);
        a.title = "<script>&\"x\"".into();
        let d = doc(&r::payload("firing", vec![a]), Some("https://mq"));
        let t = telegram_text(&d);
        assert!(t.contains("&lt;script&gt;&amp;&quot;x&quot;"));
        assert!(!t.contains("<script>"));
        assert!(t.contains("<a href=\"https://mq/channels/email/consumers\">Open in console</a>"));

        let many: Vec<_> = (0..200)
            .map(|i| {
                let mut a = r::alert(&format!("a{i}"), Health::Warning);
                a.detail = "x".repeat(500);
                a
            })
            .collect();
        let d = doc(&r::payload("firing", many), None);
        let t = telegram_text(&d);
        assert!(t.chars().count() <= 4096, "{}", t.chars().count());
        assert!(t.starts_with("<b>200 issues need attention</b>"));
        assert!(t.contains("more"));
    }

    #[test]
    fn telegram_chat_id_and_thread() {
        let d = doc(&r::payload("test", vec![]), None);
        let b = telegram_body(&d, "-100123", Some(5));
        assert_eq!(b["chat_id"], -100123);
        assert_eq!(b["message_thread_id"], 5);
        assert_eq!(b["parse_mode"], "HTML");
        let b = telegram_body(&d, "@ops_room", None);
        assert_eq!(b["chat_id"], "@ops_room");
        assert!(b.get("message_thread_id").is_none());
    }

    #[test]
    fn slack_and_discord_limits() {
        let many: Vec<_> = (0..30)
            .map(|i| r::alert(&format!("a{i}"), Health::Warning))
            .collect();
        let d = doc(&r::payload("firing", many), None);
        let s = slack_body(&d);
        assert!(s["blocks"].as_array().unwrap().len() <= 50);
        let dc = discord_body(&d);
        assert!(dc["embeds"].as_array().unwrap().len() <= DISCORD_MAX_EMBEDS);
        assert!(dc["content"].as_str().unwrap().contains("more"));
        assert_eq!(discord_escape("a*b_c"), "a\\*b\\_c");
    }
}
