//! Test helpers: a recording mock HTTP server and a minimal SMTP sink.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use fluxmq_proto::notify::NotificationPayload;
use fluxmq_proto::telemetry::{Alert, AlertKind, Health};
use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Notify;

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    /// Path + query.
    pub uri: String,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl Recorded {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("json body")
    }
}

/// `(status, headers, body)` of a canned response.
pub type Canned = (u16, Vec<(String, String)>, String);

#[derive(Clone, Default)]
pub struct Mock {
    pub requests: Arc<Mutex<Vec<Recorded>>>,
    /// Responses to hand out in order (then 200 "ok").
    pub responses: Arc<Mutex<VecDeque<Canned>>>,
    pub notify: Arc<Notify>,
}

impl Mock {
    pub fn respond(&self, status: u16, body: &str) {
        self.responses
            .lock()
            .push_back((status, vec![], body.to_string()));
    }

    pub fn respond_with_header(&self, status: u16, header: (&str, &str), body: &str) {
        self.responses.lock().push_back((
            status,
            vec![(header.0.to_string(), header.1.to_string())],
            body.to_string(),
        ));
    }

    pub fn take(&self) -> Vec<Recorded> {
        std::mem::take(&mut *self.requests.lock())
    }

    /// Waits until at least `n` requests were recorded (or panics after `secs`).
    pub async fn wait_for(&self, n: usize, secs: u64) -> Vec<Recorded> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(secs);
        loop {
            let notified = self.notify.notified();
            if self.requests.lock().len() >= n {
                return self.requests.lock().clone();
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                panic!("expected {n} requests, got {}", self.requests.lock().len());
            }
        }
    }
}

async fn handle(State(m): State<Mock>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, 10 << 20)
        .await
        .unwrap_or_default();
    m.requests.lock().push(Recorded {
        method: parts.method.to_string(),
        uri: parts.uri.to_string(),
        headers: parts.headers,
        body,
    });
    m.notify.notify_waiters();
    let next = m.responses.lock().pop_front();
    match next {
        Some((status, headers, body)) => {
            let mut resp = (StatusCode::from_u16(status).unwrap(), body).into_response();
            for (k, v) in headers {
                resp.headers_mut().insert(
                    axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                    v.parse().unwrap(),
                );
            }
            resp
        }
        None => (StatusCode::OK, "ok").into_response(),
    }
}

/// Starts the mock on 127.0.0.1:0; returns its base URL (`http://127.0.0.1:port`).
pub async fn mock_http() -> (String, Mock) {
    let mock = Mock::default();
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(mock.clone());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), mock)
}

/// One received mail.
#[derive(Debug, Clone, Default)]
pub struct Mail {
    pub from: String,
    pub to: Vec<String>,
    pub data: String,
    pub auth: Option<String>,
}

/// Minimal SMTP server (no TLS): EHLO, AUTH PLAIN/LOGIN accepted, MAIL/RCPT/DATA recorded.
pub async fn smtp_sink() -> (SocketAddr, Arc<Mutex<Vec<Mail>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mails: Arc<Mutex<Vec<Mail>>> = Arc::default();
    let m = mails.clone();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let m = m.clone();
            tokio::spawn(async move {
                let (r, mut w) = sock.into_split();
                let mut r = BufReader::new(r);
                let _ = w.write_all(b"220 sink ESMTP\r\n").await;
                let mut mail = Mail::default();
                let mut line = String::new();
                loop {
                    line.clear();
                    if r.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let cmd = line.trim_end().to_string();
                    let upper = cmd.to_ascii_uppercase();
                    let reply: &[u8] = if upper.starts_with("EHLO") {
                        b"250-sink\r\n250-AUTH PLAIN LOGIN\r\n250 8BITMIME\r\n"
                    } else if upper.starts_with("AUTH") {
                        mail.auth = Some(cmd.clone());
                        b"235 ok\r\n"
                    } else if upper.starts_with("MAIL FROM:") {
                        mail.from = cmd[10..].trim().to_string();
                        b"250 ok\r\n"
                    } else if upper.starts_with("RCPT TO:") {
                        mail.to.push(cmd[8..].trim().to_string());
                        b"250 ok\r\n"
                    } else if upper == "DATA" {
                        let _ = w.write_all(b"354 go\r\n").await;
                        let mut data = String::new();
                        loop {
                            line.clear();
                            if r.read_line(&mut line).await.unwrap_or(0) == 0 {
                                return;
                            }
                            if line == ".\r\n" {
                                break;
                            }
                            data.push_str(&line);
                        }
                        mail.data = data;
                        m.lock().push(std::mem::take(&mut mail));
                        b"250 queued\r\n"
                    } else if upper == "QUIT" {
                        let _ = w.write_all(b"221 bye\r\n").await;
                        return;
                    } else {
                        b"250 ok\r\n"
                    };
                    if w.write_all(reply).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (addr, mails)
}

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
        id: "p1".into(),
        cluster: "prod".into(),
        sent_at_ms: 361_000,
        rule: "Ops".into(),
        status: status.into(),
        alerts,
        events: vec![],
        console_url: Some("https://mq.example.com".into()),
    }
}
