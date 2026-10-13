//! Every sender against a local mock (exact request shape), retries, SMTP.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use common::{alert, mock_http, payload, smtp_sink};
use fluxmq_notify::senders::{SIGNATURE_HEADER, Senders, verify_signature};
use fluxmq_notify::{Endpoints, NotifyConfig};
use fluxmq_proto::notify::{NotificationPayload, NotifierKind};
use fluxmq_proto::telemetry::Health;

fn senders(base: &str) -> Senders {
    Senders::new(NotifyConfig {
        public_url: Some("https://mq.example.com".into()),
        endpoints: Endpoints {
            telegram_api: base.to_string(),
            pagerduty_events: format!("{base}/v2/enqueue"),
        },
        backoff: vec![Duration::from_millis(10)],
        ..NotifyConfig::default()
    })
}

#[tokio::test]
async fn telegram_request_shape_and_errors() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let kind = NotifierKind::Telegram {
        bot_token: "123456:AAsecret".into(),
        chat_id: "-1001234567890".into(),
        thread_id: Some(42),
    };
    s.send_once(
        &kind,
        &payload("firing", vec![alert("a", Health::Critical)]),
    )
    .await
    .unwrap();
    let r = mock.take().remove(0);
    assert_eq!(r.method, "POST");
    assert_eq!(r.uri, "/bot123456:AAsecret/sendMessage");
    let b = r.json();
    assert_eq!(b["chat_id"], -1001234567890i64);
    assert_eq!(b["message_thread_id"], 42);
    assert_eq!(b["parse_mode"], "HTML");
    let text = b["text"].as_str().unwrap();
    assert!(
        text.starts_with("🔴 <b>CRITICAL</b> · Email queue is growing rapidly\n"),
        "{text}"
    );
    assert!(text.contains("<i>Possible reason: Consumers processing slower than producers.</i>"));
    assert!(text.contains("Channel: <code>email</code> · Group: <code>workers</code>"));
    assert!(text.contains(
        "<a href=\"https://mq.example.com/channels/email/consumers\">Open in console</a>"
    ));
    assert!(text.contains("<i>FluxMQ · prod</i>"));

    // API error: description surfaces, the token never does.
    mock.respond(
        401,
        r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#,
    );
    let e = s
        .send_once(&kind, &payload("test", vec![]))
        .await
        .unwrap_err();
    assert_eq!(e.message, "HTTP 401");
    assert_eq!(e.detail.as_deref(), Some("Telegram 401: Unauthorized"));
    assert!(!e.retryable);
    mock.respond(
        429,
        r#"{"ok":false,"description":"Too Many Requests: retry after 3","parameters":{"retry_after":3}}"#,
    );
    let e = s
        .send_once(&kind, &payload("test", vec![]))
        .await
        .unwrap_err();
    assert!(e.retryable);
    assert_eq!(e.retry_after, Some(Duration::from_secs(3)));
}

#[tokio::test]
async fn connection_errors_do_not_leak_the_url() {
    // Nothing listens on port 9 → connection refused; the token is in the URL path.
    let s = senders("http://127.0.0.1:9");
    let kind = NotifierKind::Telegram {
        bot_token: "123456:AAsecret".into(),
        chat_id: "1".into(),
        thread_id: None,
    };
    let e = s
        .send_once(&kind, &payload("test", vec![]))
        .await
        .unwrap_err();
    assert!(e.retryable);
    assert_eq!(e.message, "connection refused");
    let detail = e.detail.unwrap();
    assert!(!detail.contains("AAsecret"), "{detail}");
    assert!(!detail.contains("127.0.0.1:9"), "{detail}");
}

#[tokio::test]
async fn slack_block_kit() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let kind = NotifierKind::Slack {
        webhook_url: format!("{base}/services/T000/B000/XXX"),
    };
    let p = payload(
        "firing",
        vec![alert("a", Health::Critical), alert("b", Health::Warning)],
    );
    s.send_once(&kind, &p).await.unwrap();
    let r = mock.take().remove(0);
    assert_eq!(r.uri, "/services/T000/B000/XXX");
    assert_eq!(r.headers.get("content-type").unwrap(), "application/json");
    let b = r.json();
    assert_eq!(b["text"], "CRITICAL 2 issues need attention");
    let blocks = b["blocks"].as_array().unwrap();
    assert_eq!(blocks[0]["type"], "header");
    assert_eq!(blocks[0]["text"]["type"], "plain_text");
    assert_eq!(blocks[0]["text"]["text"], "🔴 2 issues need attention");
    assert_eq!(blocks[1]["type"], "section");
    let t = blocks[1]["text"]["text"].as_str().unwrap();
    assert!(
        t.starts_with("🔴 *CRITICAL* · *Email queue is growing rapidly*"),
        "{t}"
    );
    assert!(t.contains("<https://mq.example.com/channels/email/consumers|Open in console>"));
    assert_eq!(blocks[2]["type"], "context");
    assert_eq!(
        blocks[2]["elements"][0]["text"],
        "Channel: `email` · Group: `workers` · Open for: `6m`"
    );
    let last = blocks.last().unwrap();
    assert_eq!(
        last["elements"][0]["text"],
        "FluxMQ · cluster *prod* · <https://mq.example.com|console>"
    );
    // 4xx is permanent, 5xx retryable.
    mock.respond(404, "no_service");
    let e = s.send_once(&kind, &p).await.unwrap_err();
    assert_eq!(e.message, "HTTP 404");
    assert_eq!(e.detail.as_deref(), Some("HTTP 404: no_service"));
    assert!(!e.retryable);
}

#[tokio::test]
async fn discord_embeds() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let kind = NotifierKind::Discord {
        webhook_url: format!("{base}/api/webhooks/1/tok"),
    };
    s.send_once(
        &kind,
        &payload("resolved", vec![alert("a", Health::Critical)]),
    )
    .await
    .unwrap();
    let r = mock.take().remove(0);
    assert_eq!(r.uri, "/api/webhooks/1/tok");
    let b = r.json();
    assert_eq!(b["username"], "FluxMQ");
    assert_eq!(b["allowed_mentions"]["parse"], serde_json::json!([]));
    let e = &b["embeds"][0];
    assert_eq!(e["color"], 0x30A46C);
    assert_eq!(e["title"], "RESOLVED Email queue is growing rapidly");
    assert_eq!(e["url"], "https://mq.example.com/channels/email/consumers");
    assert_eq!(e["fields"][0]["name"], "Channel");
    assert_eq!(e["fields"][0]["value"], "email");
    assert!(
        b["content"]
            .as_str()
            .unwrap()
            .starts_with("✅ **Resolved: Email queue is growing rapidly**")
    );
}

#[tokio::test]
async fn pagerduty_trigger_and_resolve() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let kind = NotifierKind::PagerDuty {
        routing_key: "R0123456789abcdef0123456789abcde".into(),
    };
    let a = alert("lag_growth:email/workers", Health::Critical);
    s.send_once(&kind, &payload("firing", vec![a.clone()]))
        .await
        .unwrap();
    s.send_once(&kind, &payload("resolved", vec![a]))
        .await
        .unwrap();
    let reqs = mock.take();
    assert_eq!(reqs.len(), 2);
    assert!(reqs.iter().all(|r| r.uri == "/v2/enqueue"));
    let t = reqs[0].json();
    assert_eq!(t["routing_key"], "R0123456789abcdef0123456789abcde");
    assert_eq!(t["event_action"], "trigger");
    assert_eq!(t["dedup_key"], "lag_growth:email/workers");
    assert_eq!(t["payload"]["severity"], "critical");
    assert_eq!(t["payload"]["source"], "fluxmq/prod");
    assert_eq!(t["payload"]["component"], "email");
    assert_eq!(t["payload"]["group"], "workers");
    assert_eq!(t["payload"]["class"], "lag_growth");
    assert_eq!(
        t["payload"]["summary"],
        "[prod] Email queue is growing rapidly"
    );
    assert_eq!(
        t["links"][0]["href"],
        "https://mq.example.com/channels/email/consumers"
    );
    let r = reqs[1].json();
    assert_eq!(
        r,
        serde_json::json!({
            "routing_key": "R0123456789abcdef0123456789abcde",
            "event_action": "resolve",
            "dedup_key": "lag_growth:email/workers",
        })
    );
}

#[tokio::test]
async fn webhook_signature_and_headers() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let mut headers = BTreeMap::new();
    headers.insert("Authorization".to_string(), "Bearer xyz".to_string());
    let kind = NotifierKind::Webhook {
        url: format!("{base}/hooks/fluxmq?x=1"),
        secret: Some("whsec".into()),
        headers,
    };
    let p = payload("firing", vec![alert("a", Health::Warning)]);
    s.send_once(&kind, &p).await.unwrap();
    let r = mock.take().remove(0);
    assert_eq!(r.uri, "/hooks/fluxmq?x=1");
    assert_eq!(r.headers.get("authorization").unwrap(), "Bearer xyz");
    assert_eq!(r.headers.get("content-type").unwrap(), "application/json");
    let sig = r
        .headers
        .get(SIGNATURE_HEADER)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let now = fluxmq_proto::now_ms() / 1000;
    assert!(verify_signature("whsec", &sig, &r.body, now, 300), "{sig}");
    assert!(!verify_signature("other", &sig, &r.body, now, 300));
    let got: NotificationPayload = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(got, p);

    // Without a secret: no signature header.
    let kind = NotifierKind::Webhook {
        url: format!("{base}/plain"),
        secret: None,
        headers: BTreeMap::new(),
    };
    s.send_once(&kind, &p).await.unwrap();
    assert!(mock.take()[0].headers.get(SIGNATURE_HEADER).is_none());
}

#[tokio::test]
async fn retries_with_backoff() {
    let (base, mock) = mock_http().await;
    let s = senders(&base);
    let kind = NotifierKind::Webhook {
        url: format!("{base}/h"),
        secret: None,
        headers: BTreeMap::new(),
    };
    let p = payload("test", vec![]);
    mock.respond(500, "boom");
    mock.respond_with_header(429, ("retry-after", "0"), "slow down");
    let out = s.deliver(&kind, &p).await;
    assert_eq!(out.attempts, 3);
    assert_eq!(out.error, None);
    mock.take();

    for _ in 0..3 {
        mock.respond(503, "down");
    }
    let out = s.deliver(&kind, &p).await;
    assert_eq!(out.attempts, 3);
    assert_eq!(out.error.as_deref(), Some("HTTP 503"));
    assert_eq!(out.error_detail.as_deref(), Some("HTTP 503: down"));
    assert_eq!(mock.take().len(), 3);

    mock.respond(400, "bad");
    let out = s.deliver(&kind, &p).await;
    assert_eq!(out.attempts, 1, "4xx is not retried");
    assert_eq!(mock.take().len(), 1);

    // Redirects are not followed (and not retried).
    mock.respond_with_header(302, ("location", "http://169.254.169.254/"), "");
    let out = s.deliver(&kind, &p).await;
    assert_eq!(out.attempts, 1);
    assert_eq!(mock.take().len(), 1);
}

#[tokio::test]
async fn email_via_smtp() {
    let (addr, mails) = smtp_sink().await;
    let s = senders("http://unused");
    let kind = NotifierKind::Email {
        smtp_host: addr.ip().to_string(),
        smtp_port: addr.port(),
        username: Some("bot".into()),
        password: Some("pw".into()),
        tls: "none".into(),
        from: "FluxMQ <fluxmq@example.com>".into(),
        to: vec!["ops@example.com".into(), "oncall@example.com".into()],
    };
    s.send_once(
        &kind,
        &payload("firing", vec![alert("a", Health::Critical)]),
    )
    .await
    .unwrap();
    let m = mails.lock().remove(0);
    assert_eq!(m.from, "<fluxmq@example.com>");
    assert_eq!(m.to, ["<ops@example.com>", "<oncall@example.com>"]);
    assert!(m.auth.is_some(), "credentials used");
    assert!(
        m.data
            .contains("Subject: [FluxMQ CRITICAL] Email queue is growing rapidly"),
        "{}",
        m.data
    );
    assert!(
        m.data
            .contains("Possible reason: Consumers processing slower than producers.")
    );
    assert!(m.data.contains("Console: https://mq.example.com"));
}

#[tokio::test]
async fn tls_stacks_initialize() {
    // rustls needs exactly one crypto provider; building the clients proves the feature
    // set resolves (no network involved).
    reqwest::Client::builder()
        .build()
        .expect("reqwest rustls client");
    let _smtp: lettre::AsyncSmtpTransport<lettre::Tokio1Executor> =
        lettre::AsyncSmtpTransport::<lettre::Tokio1Executor>::relay("smtp.example.com")
            .expect("lettre rustls relay")
            .build();
}
