//! Secrets (token secrets, the bootstrap token) never reach log output. A separate test
//! binary so no other test's callsite-interest caching interferes with log capture.

use std::sync::{Arc, Mutex};

use fluxmq_auth::{Action, AuthService, Principal};
use fluxmq_proto::auth::{ClientScopes, CreateTokenRequest, Role};

fn req(name: &str, role: Role) -> CreateTokenRequest {
    CreateTokenRequest {
        name: name.into(),
        role,
        scopes: ClientScopes::default(),
        ttl_secs: None,
    }
}

#[derive(Clone, Default)]
struct Buf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn secrets_never_logged() {
    let buf = Buf::default();
    let writer = buf.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);

    let svc = AuthService::in_memory(Some("bootstrap-secret".into()));
    let t = svc
        .create_token(req("logged", Role::Admin), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let p = svc.authenticate(Some(&t.token)).unwrap();
    let _ = svc.authenticate(Some("fmq_aaaaaaaaaaaa_wrong"));
    let _ = svc.authenticate(Some("bootstrap-secret"));
    let _ = svc.authorize(&p, Action::Publish("x"));
    let t2 = svc
        .create_token(req("second", Role::Viewer), &p)
        .await
        .unwrap();
    svc.revoke_token(&t2.info.id).await.unwrap();

    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    assert!(out.contains("auth token created"), "logging works: {out}");
    assert!(out.contains("auth token revoked"), "logging works: {out}");
    let secret = t.token.rsplit('_').next().unwrap();
    let secret2 = t2.token.rsplit('_').next().unwrap();
    for s in [secret, secret2, "bootstrap-secret"] {
        assert!(!out.contains(s), "secret leaked into logs");
    }
    // Debug output of a principal / token info carries no secret either.
    assert!(!format!("{p:?} {:?}", t.info).contains(secret));
}
