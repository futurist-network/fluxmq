//! Validation, secret redaction and the uniform secret update rule.
//!
//! Secrets: Telegram `bot_token`, Slack/Discord `webhook_url`, Email `password`, PagerDuty
//! `routing_key`, Webhook `secret` and every Webhook header value. Reads replace them with
//! [`REDACTED`]; on update [`REDACTED`] keeps the stored value, `""` removes it (optional
//! fields / headers; required ones then fail validation), anything else replaces it.

use std::collections::BTreeMap;

use fluxmq_proto::notify::{NotificationRule, Notifier, NotifierKind, REDACTED};
use fluxmq_proto::telemetry::Health;

use crate::NotifyError;

pub const MAX_NAME: usize = 128;
pub const MAX_RECIPIENTS: usize = 20;
pub const MAX_HEADERS: usize = 20;
pub const MAX_NOTIFIERS_PER_RULE: usize = 20;
pub const MAX_CHANNEL_GLOBS: usize = 50;
pub const MAX_GROUP_WAIT_SECS: u32 = 3600;
pub const MAX_REPEAT_SECS: u32 = 7 * 24 * 3600;
pub const DEFAULT_GROUP_WAIT_SECS: u32 = 30;
pub const DEFAULT_REPEAT_SECS: u32 = 4 * 3600;
/// Headers the webhook sender sets itself.
const RESERVED_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "host",
    "user-agent",
    "x-fluxmq-signature",
    "transfer-encoding",
];

fn invalid(msg: impl Into<String>) -> NotifyError {
    NotifyError::Invalid(msg.into())
}

pub fn name(name: &str) -> Result<String, NotifyError> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(invalid("name must be 1-128 characters"));
    }
    Ok(name.to_string())
}

/// `notifier` with every secret replaced by [`REDACTED`] (absent optional ones stay absent).
pub fn redact(notifier: &Notifier) -> Notifier {
    let mut n = notifier.clone();
    n.kind = redact_kind(&n.kind);
    n
}

pub fn redact_kind(kind: &NotifierKind) -> NotifierKind {
    let r = || REDACTED.to_string();
    let mut k = kind.clone();
    match &mut k {
        NotifierKind::Telegram { bot_token, .. } => *bot_token = r(),
        NotifierKind::Slack { webhook_url } | NotifierKind::Discord { webhook_url } => {
            *webhook_url = r()
        }
        NotifierKind::Email { password, .. } => {
            if password.is_some() {
                *password = Some(r());
            }
        }
        NotifierKind::PagerDuty { routing_key } => *routing_key = r(),
        NotifierKind::Webhook {
            secret, headers, ..
        } => {
            if secret.is_some() {
                *secret = Some(r());
            }
            for v in headers.values_mut() {
                *v = r();
            }
        }
    }
    k
}

/// Required secret: `REDACTED` → stored value.
fn keep(new: String, old: Option<&String>, field: &str) -> Result<String, NotifyError> {
    if new == REDACTED {
        return old
            .cloned()
            .ok_or_else(|| invalid(format!("{field}: no stored value to keep; provide it")));
    }
    Ok(new)
}

/// Optional secret: `REDACTED` → stored value, `""` → removed.
fn keep_opt(
    new: Option<String>,
    old: Option<&Option<String>>,
    field: &str,
) -> Result<Option<String>, NotifyError> {
    match new {
        Some(v) if v == REDACTED => match old {
            Some(o) => Ok(o.clone()),
            None => Err(invalid(format!(
                "{field}: no stored value to keep; provide it"
            ))),
        },
        Some(v) if v.is_empty() => Ok(None),
        other => Ok(other),
    }
}

/// Applies the secret update rule against the stored kind (`None` on create, where
/// [`REDACTED`] is rejected). A kind of a different type keeps nothing.
pub fn merge_secrets(
    new: NotifierKind,
    old: Option<&NotifierKind>,
) -> Result<NotifierKind, NotifyError> {
    Ok(match new {
        NotifierKind::Telegram {
            bot_token,
            chat_id,
            thread_id,
        } => {
            let old = match old {
                Some(NotifierKind::Telegram { bot_token, .. }) => Some(bot_token),
                _ => None,
            };
            NotifierKind::Telegram {
                bot_token: keep(bot_token, old, "bot_token")?,
                chat_id,
                thread_id,
            }
        }
        NotifierKind::Slack { webhook_url } => {
            let old = match old {
                Some(NotifierKind::Slack { webhook_url }) => Some(webhook_url),
                _ => None,
            };
            NotifierKind::Slack {
                webhook_url: keep(webhook_url, old, "webhook_url")?,
            }
        }
        NotifierKind::Discord { webhook_url } => {
            let old = match old {
                Some(NotifierKind::Discord { webhook_url }) => Some(webhook_url),
                _ => None,
            };
            NotifierKind::Discord {
                webhook_url: keep(webhook_url, old, "webhook_url")?,
            }
        }
        NotifierKind::Email {
            smtp_host,
            smtp_port,
            username,
            password,
            tls,
            from,
            to,
        } => {
            let old = match old {
                Some(NotifierKind::Email { password, .. }) => Some(password),
                _ => None,
            };
            NotifierKind::Email {
                smtp_host,
                smtp_port,
                username,
                password: keep_opt(password, old, "password")?,
                tls,
                from,
                to,
            }
        }
        NotifierKind::PagerDuty { routing_key } => {
            let old = match old {
                Some(NotifierKind::PagerDuty { routing_key }) => Some(routing_key),
                _ => None,
            };
            NotifierKind::PagerDuty {
                routing_key: keep(routing_key, old, "routing_key")?,
            }
        }
        NotifierKind::Webhook {
            url,
            secret,
            headers,
        } => {
            let (old_secret, old_headers) = match old {
                Some(NotifierKind::Webhook {
                    secret, headers, ..
                }) => (Some(secret), Some(headers)),
                _ => (None, None),
            };
            let mut merged = BTreeMap::new();
            for (k, v) in headers {
                if v.is_empty() {
                    continue;
                }
                let v = if v == REDACTED {
                    old_headers
                        .and_then(|h| h.iter().find(|(ok, _)| ok.eq_ignore_ascii_case(&k)))
                        .map(|(_, ov)| ov.clone())
                        .ok_or_else(|| {
                            invalid(format!("header '{k}': no stored value to keep; provide it"))
                        })?
                } else {
                    v
                };
                merged.insert(k, v);
            }
            NotifierKind::Webhook {
                url,
                secret: keep_opt(secret, old_secret, "secret")?,
                headers: merged,
            }
        }
    })
}

fn http_url(url: &str, field: &str) -> Result<(), NotifyError> {
    let parsed = reqwest::Url::parse(url.trim())
        .map_err(|_| invalid(format!("{field} must be an http(s) URL")))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(invalid(format!("{field} must be an http(s) URL")));
    }
    Ok(())
}

fn mailbox(addr: &str, field: &str) -> Result<(), NotifyError> {
    addr.parse::<lettre::message::Mailbox>()
        .map(drop)
        .map_err(|_| invalid(format!("{field}: '{addr}' is not a valid email address")))
}

/// Normalizes (trims) and validates a notifier kind (after [`merge_secrets`]).
pub fn kind(kind: NotifierKind) -> Result<NotifierKind, NotifyError> {
    Ok(match kind {
        NotifierKind::Telegram {
            bot_token,
            chat_id,
            thread_id,
        } => {
            let bot_token = bot_token.trim().to_string();
            let ok = bot_token.split_once(':').is_some_and(|(id, rest)| {
                !id.is_empty()
                    && id.chars().all(|c| c.is_ascii_digit())
                    && !rest.is_empty()
                    && rest
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            });
            if !ok {
                return Err(invalid(
                    "bot_token must look like 123456789:AA... (from @BotFather)",
                ));
            }
            let chat_id = chat_id.trim().to_string();
            let numeric = chat_id.parse::<i64>().is_ok();
            let username = chat_id.strip_prefix('@').is_some_and(|u| {
                u.len() >= 4 && u.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            });
            if !numeric && !username {
                return Err(invalid(
                    "chat_id must be a numeric chat id (e.g. -1001234567890) or @channelname",
                ));
            }
            if thread_id.is_some_and(|t| t <= 0) {
                return Err(invalid("thread_id must be positive"));
            }
            NotifierKind::Telegram {
                bot_token,
                chat_id,
                thread_id,
            }
        }
        NotifierKind::Slack { webhook_url } => {
            http_url(&webhook_url, "webhook_url")?;
            NotifierKind::Slack {
                webhook_url: webhook_url.trim().to_string(),
            }
        }
        NotifierKind::Discord { webhook_url } => {
            http_url(&webhook_url, "webhook_url")?;
            NotifierKind::Discord {
                webhook_url: webhook_url.trim().to_string(),
            }
        }
        NotifierKind::Email {
            smtp_host,
            smtp_port,
            username,
            password,
            tls,
            from,
            to,
        } => {
            let smtp_host = smtp_host.trim().to_string();
            if smtp_host.is_empty() || smtp_host.chars().any(char::is_whitespace) {
                return Err(invalid("smtp_host is required"));
            }
            if smtp_port == 0 {
                return Err(invalid("smtp_port must be 1-65535"));
            }
            let tls = tls.trim().to_ascii_lowercase();
            if !matches!(tls.as_str(), "starttls" | "tls" | "none") {
                return Err(invalid("tls must be one of: starttls, tls, none"));
            }
            let username = username
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty());
            if password.is_some() && username.is_none() {
                return Err(invalid("password requires a username"));
            }
            let from = from.trim().to_string();
            mailbox(&from, "from")?;
            let to: Vec<String> = to
                .into_iter()
                .map(|t| t.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect();
            if to.is_empty() || to.len() > MAX_RECIPIENTS {
                return Err(invalid(format!(
                    "to must list 1-{MAX_RECIPIENTS} recipients"
                )));
            }
            for t in &to {
                mailbox(t, "to")?;
            }
            NotifierKind::Email {
                smtp_host,
                smtp_port,
                username,
                password,
                tls,
                from,
                to,
            }
        }
        NotifierKind::PagerDuty { routing_key } => {
            let routing_key = routing_key.trim().to_string();
            if routing_key.is_empty()
                || routing_key.len() > 64
                || !routing_key.chars().all(|c| c.is_ascii_alphanumeric())
            {
                return Err(invalid(
                    "routing_key must be the 32-character Events API v2 integration key",
                ));
            }
            NotifierKind::PagerDuty { routing_key }
        }
        NotifierKind::Webhook {
            url,
            secret,
            headers,
        } => {
            http_url(&url, "url")?;
            if headers.len() > MAX_HEADERS {
                return Err(invalid(format!("at most {MAX_HEADERS} headers")));
            }
            for (k, v) in &headers {
                let name = reqwest::header::HeaderName::from_bytes(k.as_bytes())
                    .map_err(|_| invalid(format!("invalid header name '{k}'")))?;
                if RESERVED_HEADERS.contains(&name.as_str()) {
                    return Err(invalid(format!("header '{k}' is set by FluxMQ")));
                }
                reqwest::header::HeaderValue::from_str(v)
                    .map_err(|_| invalid(format!("invalid value for header '{k}'")))?;
            }
            NotifierKind::Webhook {
                url: url.trim().to_string(),
                secret,
                headers,
            }
        }
    })
}

/// Validates a rule's matcher/timing (notifier existence is checked by the service).
pub fn rule(rule: &mut NotificationRule) -> Result<(), NotifyError> {
    rule.name = name(&rule.name)?;
    if rule.notifier_ids.is_empty() || rule.notifier_ids.len() > MAX_NOTIFIERS_PER_RULE {
        return Err(invalid(format!(
            "notifier_ids must list 1-{MAX_NOTIFIERS_PER_RULE} notifiers"
        )));
    }
    let mut seen = Vec::new();
    for id in &rule.notifier_ids {
        if seen.contains(id) {
            return Err(invalid(format!("duplicate notifier '{id}'")));
        }
        seen.push(id.clone());
    }
    let m = &mut rule.matcher;
    if m.min_severity == Health::Healthy {
        return Err(invalid("min_severity must be 'warning' or 'critical'"));
    }
    m.alert_kinds.dedup();
    m.event_kinds.dedup();
    if m.channels.len() > MAX_CHANNEL_GLOBS {
        return Err(invalid(format!(
            "at most {MAX_CHANNEL_GLOBS} channel patterns"
        )));
    }
    for g in &mut m.channels {
        *g = g.trim().to_string();
        if g.is_empty()
            || g.len() > 128
            || !g
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-*?$".contains(c))
        {
            return Err(invalid(format!(
                "invalid channel pattern '{g}' (use [a-zA-Z0-9._-$] with * and ?)"
            )));
        }
    }
    if rule.group_wait_secs > MAX_GROUP_WAIT_SECS {
        return Err(invalid(format!(
            "group_wait_secs must be 0-{MAX_GROUP_WAIT_SECS}"
        )));
    }
    if rule.repeat_interval_secs > MAX_REPEAT_SECS {
        return Err(invalid(format!(
            "repeat_interval_secs must be 0-{MAX_REPEAT_SECS}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webhook(secret: Option<&str>, headers: &[(&str, &str)]) -> NotifierKind {
        NotifierKind::Webhook {
            url: "https://example.com/hook".into(),
            secret: secret.map(Into::into),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn redaction_round_trip_keeps_secrets() {
        let stored = webhook(
            Some("s3cret"),
            &[("Authorization", "Bearer abc"), ("X-A", "1")],
        );
        let shown = redact_kind(&stored);
        assert_eq!(
            shown,
            webhook(
                Some(REDACTED),
                &[("Authorization", REDACTED), ("X-A", REDACTED)]
            )
        );
        // Echo back unchanged → identical.
        assert_eq!(merge_secrets(shown.clone(), Some(&stored)).unwrap(), stored);
        // "" removes, new value replaces.
        let upd = webhook(Some(""), &[("Authorization", "Bearer new"), ("X-A", "")]);
        assert_eq!(
            merge_secrets(upd, Some(&stored)).unwrap(),
            webhook(None, &[("Authorization", "Bearer new")])
        );
        // REDACTED without anything stored is an error (create).
        assert!(merge_secrets(shown, None).is_err());
    }

    #[test]
    fn redaction_per_kind() {
        let t = NotifierKind::Telegram {
            bot_token: "1:abc".into(),
            chat_id: "-100".into(),
            thread_id: None,
        };
        let shown = redact_kind(&t);
        assert!(
            matches!(&shown, NotifierKind::Telegram { bot_token, chat_id, .. } if bot_token == REDACTED && chat_id == "-100")
        );
        assert_eq!(merge_secrets(shown, Some(&t)).unwrap(), t);
        // Type change keeps nothing.
        let slack = NotifierKind::Slack {
            webhook_url: REDACTED.into(),
        };
        assert!(merge_secrets(slack, Some(&t)).is_err());
        let e = NotifierKind::Email {
            smtp_host: "h".into(),
            smtp_port: 25,
            username: None,
            password: None,
            tls: "none".into(),
            from: "a@b.c".into(),
            to: vec!["d@e.f".into()],
        };
        assert_eq!(redact_kind(&e), e, "absent optional secret stays absent");
    }

    #[test]
    fn kind_validation() {
        let ok = NotifierKind::Telegram {
            bot_token: "123456:AA-bb_CC".into(),
            chat_id: "-1001234567890".into(),
            thread_id: Some(7),
        };
        assert!(kind(ok).is_ok());
        for (token, chat) in [
            ("abc", "1"),
            ("1:a b", "1"),
            ("1:abc", "chat"),
            ("1:abc", "@ab"),
        ] {
            let k = NotifierKind::Telegram {
                bot_token: token.into(),
                chat_id: chat.into(),
                thread_id: None,
            };
            assert!(kind(k).is_err(), "{token} {chat}");
        }
        assert!(
            kind(NotifierKind::Slack {
                webhook_url: "ftp://x".into()
            })
            .is_err()
        );
        assert!(
            kind(NotifierKind::PagerDuty {
                routing_key: "".into()
            })
            .is_err()
        );
        assert!(kind(webhook(None, &[("Content-Type", "x")])).is_err());
        assert!(kind(webhook(None, &[("bad header", "x")])).is_err());
        assert!(kind(webhook(None, &[("X-Ok", "v")])).is_ok());
        let e = |from: &str, to: Vec<&str>, tls: &str| NotifierKind::Email {
            smtp_host: "smtp.example.com".into(),
            smtp_port: 587,
            username: Some("u".into()),
            password: Some("p".into()),
            tls: tls.into(),
            from: from.into(),
            to: to.into_iter().map(Into::into).collect(),
        };
        assert!(kind(e("FluxMQ <a@b.io>", vec!["ops@b.io"], "STARTTLS")).is_ok());
        assert!(kind(e("nope", vec!["ops@b.io"], "tls")).is_err());
        assert!(kind(e("a@b.io", vec![], "tls")).is_err());
        assert!(kind(e("a@b.io", vec!["ops@b.io"], "ssl")).is_err());
    }
}
