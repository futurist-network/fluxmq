//! Email via SMTP (lettre, rustls). `tls`: `tls` = implicit TLS (465), `starttls` = upgrade
//! required (587), `none` = plaintext (local relays / tests only).

use std::time::Duration;

use fluxmq_proto::notify::NotifierKind;
use lettre::message::header::ContentType;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

use super::SendError;
use crate::render::{Doc, plain, truncate};

pub fn subject(doc: &Doc) -> String {
    truncate(
        &format!("[FluxMQ {}] {}", doc.tone.label(), doc.headline),
        200,
    )
}

pub(crate) async fn send(
    kind: &NotifierKind,
    doc: &Doc,
    timeout: Duration,
) -> Result<(), SendError> {
    let NotifierKind::Email {
        smtp_host,
        smtp_port,
        username,
        password,
        tls,
        from,
        to,
    } = kind
    else {
        return Err(SendError::permanent("not an email notifier"));
    };
    let mut msg = Message::builder()
        .from(
            from.parse()
                .map_err(|_| SendError::permanent("invalid from address"))?,
        )
        .subject(subject(doc))
        .header(ContentType::TEXT_PLAIN);
    for t in to {
        msg = msg.to(t
            .parse()
            .map_err(|_| SendError::permanent(format!("invalid recipient '{t}'")))?);
    }
    let msg = msg
        .body(plain(doc))
        .map_err(|e| SendError::permanent(format!("build message: {e}")))?;

    let builder = match tls.as_str() {
        "tls" => AsyncSmtpTransport::<Tokio1Executor>::relay(smtp_host),
        "starttls" => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(smtp_host),
        _ => Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(
            smtp_host,
        )),
    }
    .map_err(|e| SendError::permanent(format!("SMTP setup: {e}")))?;
    let mut builder = builder.port(*smtp_port).timeout(Some(timeout));
    if let Some(user) = username {
        builder = builder.credentials(Credentials::new(
            user.clone(),
            password.clone().unwrap_or_default(),
        ));
    }
    let transport = builder.build();
    match transport.send(msg).await {
        Ok(_) => Ok(()),
        Err(e) => {
            // lettre's messages carry SMTP codes / IO causes, never credentials.
            let retryable = !e.is_permanent();
            let mut detail = format!("SMTP: {e}");
            let mut src = std::error::Error::source(&e);
            while let Some(s) = src {
                let cs = s.to_string();
                if !detail.contains(&cs) {
                    detail.push_str(&format!(": {cs}"));
                }
                src = s.source();
            }
            let short = fluxmq_proto::api::short_error::classify(&detail, e.is_timeout(), false);
            let message = match (short, e.status()) {
                (fluxmq_proto::api::short_error::REQUEST_FAILED, Some(code)) => {
                    format!("SMTP {code}")
                }
                (fluxmq_proto::api::short_error::REQUEST_FAILED, None) => "SMTP error".into(),
                (s, _) => s.to_string(),
            };
            Err(SendError {
                message,
                detail: Some(detail),
                retryable,
                retry_after: None,
            })
        }
    }
}
