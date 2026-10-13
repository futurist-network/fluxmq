//! Channel senders. Every sender takes a [`NotificationPayload`] and the notifier's settings
//! and performs one delivery attempt; [`Senders::deliver`] adds retries with backoff.
//!
//! Errors never contain secrets: reqwest errors are stripped of their URL (Telegram puts the
//! bot token in the path, Slack/Discord URLs are credentials), response bodies are truncated.

mod chat;
mod email;
mod pagerduty;
mod webhook;

use std::time::Duration;

use fluxmq_proto::notify::{NotificationPayload, NotifierKind};
use serde_json::Value;

pub use webhook::{SIGNATURE_HEADER, sign, verify_signature};

use crate::config::NotifyConfig;
use crate::render;

/// One failed attempt: a short, human `message` (`connection refused`, `timeout`,
/// `HTTP 503`, `TLS error`, `DNS error`, …) plus the full cause in `detail`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError {
    pub message: String,
    /// Full cause chain / response snippet (never secrets).
    pub detail: Option<String>,
    pub retryable: bool,
    /// Server-requested delay (HTTP 429 `Retry-After`, Telegram `retry_after`).
    pub retry_after: Option<Duration>,
}

impl SendError {
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            detail: None,
            retryable: false,
            retry_after: None,
        }
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            detail: None,
            retryable: true,
            retry_after: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }
}

/// Result of [`Senders::deliver`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    pub attempts: u32,
    /// Short error of the last attempt.
    pub error: Option<String>,
    pub error_detail: Option<String>,
}

/// Shared HTTP client + endpoints. Cheap to clone.
#[derive(Clone)]
pub struct Senders {
    http: reqwest::Client,
    config: NotifyConfig,
}

impl Senders {
    pub fn new(config: NotifyConfig) -> Self {
        let http = reqwest::Client::builder()
            .timeout(config.http_timeout)
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("FluxMQ/", env!("CARGO_PKG_VERSION")))
            .build()
            .unwrap_or_default();
        Self { http, config }
    }

    pub fn config(&self) -> &NotifyConfig {
        &self.config
    }

    /// One attempt.
    pub async fn send_once(
        &self,
        kind: &NotifierKind,
        payload: &NotificationPayload,
    ) -> Result<(), SendError> {
        if let Some(tx) = &self.config.capture {
            let _ = tx.send(crate::config::Captured {
                kind: kind.clone(),
                payload: payload.clone(),
            });
            return Ok(());
        }
        let doc = render::doc(payload, self.config.public_url.as_deref());
        match kind {
            NotifierKind::Telegram {
                bot_token,
                chat_id,
                thread_id,
            } => {
                let url = format!(
                    "{}/bot{}/sendMessage",
                    self.config.endpoints.telegram_api.trim_end_matches('/'),
                    bot_token
                );
                let body = chat::telegram_body(&doc, chat_id, *thread_id);
                let resp = self.post_json(&url, &body, &[]).await?;
                chat::telegram_check(resp)
            }
            NotifierKind::Slack { webhook_url } => {
                let body = chat::slack_body(&doc);
                let resp = self.post_json(webhook_url, &body, &[]).await?;
                expect_2xx(resp)
            }
            NotifierKind::Discord { webhook_url } => {
                let body = chat::discord_body(&doc);
                let resp = self.post_json(webhook_url, &body, &[]).await?;
                expect_2xx(resp)
            }
            NotifierKind::PagerDuty { routing_key } => {
                for body in pagerduty::events(routing_key, payload, &doc) {
                    let resp = self
                        .post_json(&self.config.endpoints.pagerduty_events, &body, &[])
                        .await?;
                    expect_2xx(resp)?;
                }
                Ok(())
            }
            NotifierKind::Webhook {
                url,
                secret,
                headers,
            } => webhook::send(&self.http, url, secret.as_deref(), headers, payload)
                .await
                .and_then(expect_2xx),
            NotifierKind::Email { .. } => email::send(kind, &doc, self.config.smtp_timeout).await,
        }
    }

    /// Up to `config.attempts` attempts with `config.backoff` between them (server-requested
    /// delays are honoured up to 60 s). Permanent errors stop immediately.
    pub async fn deliver(&self, kind: &NotifierKind, payload: &NotificationPayload) -> Outcome {
        let attempts = self.config.attempts.max(1);
        let mut last = None;
        for attempt in 1..=attempts {
            match self.send_once(kind, payload).await {
                Ok(()) => {
                    return Outcome {
                        attempts: attempt,
                        error: None,
                        error_detail: None,
                    };
                }
                Err(e) => {
                    let stop = !e.retryable || attempt == attempts;
                    let delay = e
                        .retry_after
                        .map(|d| d.min(Duration::from_secs(60)))
                        .unwrap_or_else(|| self.config.backoff_for(attempt));
                    if stop {
                        return Outcome {
                            attempts: attempt,
                            error: Some(e.message),
                            error_detail: e.detail,
                        };
                    }
                    last = Some(e);
                    tokio::time::sleep(delay).await;
                }
            }
        }
        Outcome {
            attempts,
            error_detail: last.as_ref().and_then(|e| e.detail.clone()),
            error: last.map(|e| e.message),
        }
    }

    async fn post_json(
        &self,
        url: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Result<HttpResponse, SendError> {
        let mut req = self.http.post(url).json(body);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        execute(req).await
    }
}

/// Status, `Retry-After` and (truncated) body of a response.
#[derive(Debug)]
pub(crate) struct HttpResponse {
    pub status: u16,
    pub retry_after: Option<Duration>,
    pub body: String,
}

pub(crate) async fn execute(req: reqwest::RequestBuilder) -> Result<HttpResponse, SendError> {
    let resp = req.send().await.map_err(http_err)?;
    let status = resp.status().as_u16();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = resp.text().await.unwrap_or_default();
    Ok(HttpResponse {
        status,
        retry_after,
        body: render::truncate(&body, 300),
    })
}

/// reqwest error → short class + cause chain without the URL (it may embed credentials).
fn http_err(e: reqwest::Error) -> SendError {
    let timeout = e.is_timeout();
    let connect = e.is_connect();
    let e = e.without_url();
    let mut msg = e.to_string();
    let mut src = std::error::Error::source(&e);
    while let Some(s) = src {
        let cs = s.to_string();
        if !msg.contains(&cs) {
            msg.push_str(": ");
            msg.push_str(&cs);
        }
        src = s.source();
    }
    let class = fluxmq_proto::api::short_error::classify(&msg, timeout, connect);
    SendError::transient(class).with_detail(msg)
}

/// 2xx → Ok; 429/5xx → retryable; other 4xx (and 3xx: redirects are not followed) →
/// permanent.
pub(crate) fn expect_2xx(r: HttpResponse) -> Result<(), SendError> {
    if (200..300).contains(&r.status) {
        return Ok(());
    }
    let retryable = r.status == 429 || r.status >= 500;
    let body = r.body.trim();
    Err(SendError {
        message: format!("HTTP {}", r.status),
        detail: (!body.is_empty()).then(|| format!("HTTP {}: {body}", r.status)),
        retryable,
        retry_after: r.retry_after,
    })
}
