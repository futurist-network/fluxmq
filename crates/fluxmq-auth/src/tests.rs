use std::time::Duration;

use fluxmq_meta::{LocalMetaStore, MetaStoreExt};

use super::*;

fn req(name: &str, role: Role) -> CreateTokenRequest {
    CreateTokenRequest {
        name: name.into(),
        role,
        scopes: ClientScopes::default(),
        ttl_secs: None,
    }
}

fn scoped(name: &str, publish: &[&str], consume: &[&str]) -> CreateTokenRequest {
    CreateTokenRequest {
        name: name.into(),
        role: Role::Viewer,
        scopes: ClientScopes {
            publish: publish.iter().map(|s| s.to_string()).collect(),
            consume: consume.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        },
        ttl_secs: None,
    }
}

async fn eventually(mut f: impl FnMut() -> bool) {
    for _ in 0..200 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

#[test]
fn glob() {
    assert!(glob_match("*", ""));
    assert!(glob_match("*", "orders.eu"));
    assert!(glob_match("orders.*", "orders.eu"));
    assert!(glob_match("orders.*", "orders."));
    assert!(!glob_match("orders.*", "orders"));
    assert!(!glob_match("orders.*", "payments.eu"));
    assert!(glob_match("orders.??", "orders.eu"));
    assert!(!glob_match("orders.??", "orders.eur"));
    assert!(glob_match("*.dlq", "a.b.dlq"));
    assert!(glob_match("a*b*c", "aXXbYYc"));
    assert!(!glob_match("a*b*c", "aXXbYY"));
    assert!(glob_match("exact", "exact"));
    assert!(!glob_match("exact", "exactly"));
    assert!(glob_match("**", "x"));
    // Pathological pattern stays linear-ish.
    assert!(!glob_match(&"*a".repeat(50), &"a".repeat(40)));
}

#[test]
fn token_parsing_and_hash_codec() {
    assert_eq!(
        parse_token("fmq_abcdefgh2345_deadbeef"),
        Some(("abcdefgh2345", "deadbeef"))
    );
    assert_eq!(parse_token("fmq_short_x"), None);
    assert_eq!(parse_token("fmq_abcdefgh2345_"), None);
    assert_eq!(parse_token("fmq_ABCDEFGH2345_x"), None);
    assert_eq!(parse_token("xyz_abcdefgh2345_x"), None);
    let h = sha256(b"x");
    assert_eq!(decode_hash(&hex(&h)), Some(h));
    assert_eq!(decode_hash("zz"), None);
}

#[test]
fn authorize_role_table() {
    let svc = AuthService::disabled();
    let mk = |role, scopes| Principal {
        name: "t".into(),
        token_id: Some("abcdefgh2345".into()),
        role,
        scopes,
        cert_cn: None,
    };
    let viewer = mk(Role::Viewer, ClientScopes::default());
    let operator = mk(Role::Operator, ClientScopes::default());
    let admin = mk(Role::Admin, ClientScopes::default());
    let svc_tok = mk(
        Role::Viewer,
        ClientScopes {
            publish: vec!["orders.*".into()],
            consume: vec!["orders.eu".into(), "audit.?".into()],
            ..Default::default()
        },
    );
    let ok = |p: &Principal, a| svc.authorize(p, a).is_ok();
    for a in [
        Action::Operate,
        Action::Admin,
        Action::Publish("x"),
        Action::Consume("x"),
        Action::CreateChannel("x"),
    ] {
        assert!(!ok(&viewer, a), "viewer {a:?}");
        assert!(ok(&admin, a), "admin {a:?}");
    }
    assert!(ok(&viewer, Action::Read));
    assert!(ok(&operator, Action::Operate));
    assert!(ok(&operator, Action::Publish("any")));
    assert!(ok(&operator, Action::Consume("any")));
    assert!(!ok(&operator, Action::Admin));
    assert!(!ok(&operator, Action::CreateChannel("x")));

    assert!(ok(&svc_tok, Action::Publish("orders.eu")));
    assert!(!ok(&svc_tok, Action::Publish("payments")));
    assert!(ok(&svc_tok, Action::Consume("orders.eu")));
    assert!(!ok(&svc_tok, Action::Consume("orders.us")));
    assert!(ok(&svc_tok, Action::Consume("audit.1")));
    assert!(!ok(&svc_tok, Action::Operate));
    assert!(!ok(&svc_tok, Action::CreateChannel("orders.new")));
    assert!(matches!(
        svc.authorize(&svc_tok, Action::Publish("payments")),
        Err(AuthError::Forbidden(_))
    ));
}

#[tokio::test]
async fn bootstrap_token_and_open_mode() {
    let open = AuthService::disabled();
    assert_eq!(open.mode(), AuthMode::Open);
    assert_eq!(
        open.authenticate(None).unwrap(),
        Principal::anonymous_admin()
    );
    assert_eq!(
        open.authenticate(Some("garbage")).unwrap(),
        Principal::anonymous_admin()
    );

    let boot = AuthService::in_memory(Some("s3cret".into()));
    assert_eq!(boot.mode(), AuthMode::Enforced);
    assert_eq!(
        boot.authenticate(Some("s3cret")).unwrap(),
        Principal::bootstrap_admin()
    );
    for bad in [
        None,
        Some(""),
        Some("s3cre"),
        Some("s3cret!"),
        Some("S3cret"),
    ] {
        assert!(matches!(
            boot.authenticate(bad),
            Err(AuthError::Unauthenticated)
        ));
    }
    // Empty bootstrap token = none.
    assert_eq!(
        AuthService::in_memory(Some(String::new())).mode(),
        AuthMode::Open
    );
}

#[tokio::test]
async fn token_lifecycle_and_mode_switch() {
    let svc = AuthService::disabled();
    let anon = Principal::anonymous_admin();

    // First token: created by the anonymous admin, switches to enforced mode.
    let created = svc
        .create_token(req("root", Role::Admin), &anon)
        .await
        .unwrap();
    assert_eq!(svc.mode(), AuthMode::Enforced);
    let token = created.token.clone();
    let (id, secret) = parse_token(&token).unwrap();
    assert_eq!(id, created.info.id);
    assert_eq!(id.len(), TOKEN_ID_LEN);
    assert_eq!(secret.len(), SECRET_BYTES * 2);
    assert_eq!(created.info.created_by, "anonymous");
    assert!(matches!(
        svc.authenticate(None),
        Err(AuthError::Unauthenticated)
    ));

    let root = svc.authenticate(Some(&token)).unwrap();
    assert_eq!(root.name, "root");
    assert_eq!(root.role, Role::Admin);
    assert_eq!(root.token_id.as_deref(), Some(id));
    assert_eq!(root.actor(), format!("root#{id}"));

    // Wrong secret with a valid id, unknown id, malformed.
    let tampered = format!("fmq_{id}_{}", "0".repeat(64));
    for bad in [tampered.as_str(), "fmq_aaaaaaaaaaaa_x", "nope"] {
        assert!(matches!(
            svc.authenticate(Some(bad)),
            Err(AuthError::Unauthenticated)
        ));
    }

    // Non-admins cannot create tokens.
    let op = svc
        .create_token(req("ops", Role::Operator), &root)
        .await
        .unwrap();
    let op_p = svc.authenticate(Some(&op.token)).unwrap();
    assert!(matches!(
        svc.create_token(req("x", Role::Viewer), &op_p).await,
        Err(AuthError::Forbidden(_))
    ));

    // Listing never contains secrets.
    let list = svc.list_tokens();
    assert_eq!(list.len(), 2);
    let listed = serde_json::to_string(&list).unwrap();
    assert!(!listed.contains(secret));
    assert!(list[0].last_used_ms.is_some(), "root was used");

    // Revoke.
    svc.revoke_token(&op.info.id).await.unwrap();
    assert!(matches!(
        svc.authenticate(Some(&op.token)),
        Err(AuthError::Unauthenticated)
    ));
    assert!(matches!(
        svc.revoke_token(&op.info.id).await,
        Err(AuthError::NotFound)
    ));
    assert!(matches!(
        svc.revoke_token("../../etc").await,
        Err(AuthError::NotFound)
    ));
    // The last token cannot be revoked without a bootstrap token (would reopen the broker).
    assert!(matches!(
        svc.revoke_token(id).await,
        Err(AuthError::Invalid(_))
    ));
    assert_eq!(svc.mode(), AuthMode::Enforced);

    // With a bootstrap token it can.
    let boot = AuthService::in_memory(Some("b".into()));
    let t = boot
        .create_token(req("only", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    boot.revoke_token(&t.info.id).await.unwrap();
    assert!(boot.list_tokens().is_empty());
    assert_eq!(boot.mode(), AuthMode::Enforced);
}

#[tokio::test]
async fn validation() {
    let svc = AuthService::in_memory(Some("b".into()));
    let admin = Principal::bootstrap_admin();
    for bad in [
        "",
        "   ",
        "admin",
        "Anonymous",
        "a/b",
        "x\ty",
        &"n".repeat(65),
    ] {
        assert!(
            matches!(
                svc.create_token(req(bad, Role::Viewer), &admin).await,
                Err(AuthError::Invalid(_))
            ),
            "name {bad:?}"
        );
    }
    let mut r = req("ttl", Role::Viewer);
    r.ttl_secs = Some(0);
    assert!(matches!(
        svc.create_token(r.clone(), &admin).await,
        Err(AuthError::Invalid(_))
    ));
    r.ttl_secs = Some(MAX_TTL_SECS + 1);
    assert!(matches!(
        svc.create_token(r, &admin).await,
        Err(AuthError::Invalid(_))
    ));
    for bad in ["", "a b", "orders/x", &"x".repeat(129)] {
        assert!(matches!(
            svc.create_token(scoped("s", &[bad], &[]), &admin).await,
            Err(AuthError::Invalid(_))
        ));
    }
    let ok = svc
        .create_token(scoped("svc", &["a.*", "a.*", " b "], &["*"]), &admin)
        .await
        .unwrap();
    assert_eq!(ok.info.scopes.publish, vec!["a.*", "b"]);
}

#[tokio::test]
async fn expiry() {
    let svc = AuthService::in_memory(Some("b".into()));
    let admin = Principal::bootstrap_admin();
    let mut r = req("short", Role::Viewer);
    r.ttl_secs = Some(3600);
    let t = svc.create_token(r, &admin).await.unwrap();
    assert!(t.info.expires_at_ms.unwrap() > t.info.created_at_ms);
    assert!(svc.authenticate(Some(&t.token)).is_ok());

    // Force expiry by rewriting the stored token in the past.
    let key = format!("{TOKEN_PREFIX}{}", t.info.id);
    let mut stored: StoredToken = svc.inner.meta.get_as(&key).unwrap().unwrap();
    stored.info.expires_at_ms = Some(fluxmq_proto::now_ms() - 1);
    let rev = svc.inner.meta.put_as(&key, &stored).await.unwrap();
    svc.inner.index.write().apply(&t.info.id, rev, Some(stored));
    assert!(matches!(
        svc.authenticate(Some(&t.token)),
        Err(AuthError::Unauthenticated)
    ));
    // Expired tokens still count: mode stays enforced.
    assert_eq!(svc.list_tokens().len(), 1);
}

#[tokio::test]
async fn persistence_watch_and_reload() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalMetaStore::open(dir.path().join("meta.json")).unwrap();
    let meta: SharedMetaStore = Arc::new(store.clone());

    let a = AuthService::open(meta.clone(), None).await.unwrap();
    // A second service over the same store plays "another node".
    let b = AuthService::open(meta.clone(), None).await.unwrap();
    assert_eq!(a.mode(), AuthMode::Open);

    // Open mode: the first token must be admin (a viewer first token would lock admins out).
    for role in [Role::Viewer, Role::Operator] {
        let r = a
            .create_token(req("first", role), &Principal::anonymous_admin())
            .await;
        assert!(matches!(r, Err(AuthError::Invalid(_))), "{role:?}: {r:?}");
    }
    assert_eq!(a.mode(), AuthMode::Open);
    let t = a
        .create_token(req("root", Role::Admin), &Principal::anonymous_admin())
        .await
        .unwrap();
    assert!(t.adopt, "the first token switches to enforced mode");
    assert_eq!(a.mode(), AuthMode::Enforced);
    // Stored form: hash only, never the secret.
    let raw = std::fs::read_to_string(dir.path().join("meta.json")).unwrap();
    let (_, secret) = parse_token(&t.token).unwrap();
    assert!(!raw.contains(secret));
    assert!(raw.contains(&hex(&sha256(secret.as_bytes()))));

    // b learns about it via watch.
    eventually(|| b.list_tokens().len() == 1).await;
    assert_eq!(b.mode(), AuthMode::Enforced);
    assert_eq!(b.authenticate(Some(&t.token)).unwrap().name, "root");

    let t2 = b
        .create_token(req("second", Role::Viewer), &Principal::anonymous_admin())
        .await;
    // b is enforced now: the anonymous admin no longer exists... but create_token only checks
    // the passed principal, which is the caller's responsibility (HTTP authenticates first).
    let t2 = t2.unwrap();
    assert!(!t2.adopt, "already enforced");
    eventually(|| a.authenticate(Some(&t2.token)).is_ok()).await;

    a.revoke_token(&t2.info.id).await.unwrap();
    eventually(|| b.authenticate(Some(&t2.token)).is_err()).await;

    // Restart: reload from disk.
    drop((a, b));
    let reopened = LocalMetaStore::open(dir.path().join("meta.json")).unwrap();
    let c = AuthService::open(Arc::new(reopened), Some("boot".into()))
        .await
        .unwrap();
    assert_eq!(c.list_tokens().len(), 1);
    assert!(c.authenticate(Some(&t.token)).is_ok());
    assert!(c.authenticate(Some(&t2.token)).is_err());
    assert!(c.authenticate(Some("boot")).is_ok());
}

#[tokio::test]
async fn lagged_watch_triggers_full_reload() {
    let store = LocalMetaStore::in_memory();
    let meta: SharedMetaStore = Arc::new(store.clone());
    let svc = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    // Flood the store with unrelated writes (> broadcast capacity) plus one token written by
    // "another node" in the middle, so the watcher lags and must reload.
    let other = AuthService::in_memory(Some("b".into()));
    let t = other
        .create_token(req("remote", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let key = format!("{TOKEN_PREFIX}{}", t.info.id);
    let stored: StoredToken = other.inner.meta.get_as(&key).unwrap().unwrap();
    for i in 0..1500 {
        meta.put(&format!("routes/r{i}"), serde_json::json!(i), None)
            .await
            .unwrap();
        if i == 700 {
            meta.put_as(&key, &stored).await.unwrap();
        }
    }
    eventually(|| svc.authenticate(Some(&t.token)).is_ok()).await;
}

#[test]
fn stale_events_cannot_resurrect() {
    let mut index = Index::default();
    let stored = StoredToken {
        info: TokenInfo {
            id: "abcdefgh2345".into(),
            name: "n".into(),
            role: Role::Viewer,
            scopes: ClientScopes::default(),
            created_at_ms: 1,
            created_by: "x".into(),
            expires_at_ms: None,
            last_used_ms: None,
        },
        secret_sha256: hex(&sha256(b"s")),
    };
    index.apply("abcdefgh2345", 5, Some(stored.clone()));
    index.apply("abcdefgh2345", 7, None); // revoked (direct write)
    index.apply("abcdefgh2345", 6, Some(stored)); // late event from before the revoke
    assert!(index.tokens.is_empty());
}

#[tokio::test]
async fn last_used_is_persisted_lazily() {
    let store = LocalMetaStore::in_memory();
    let meta: SharedMetaStore = Arc::new(store.clone());
    let svc = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    let t = svc
        .create_token(req("u", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let key = format!("{TOKEN_PREFIX}{}", t.info.id);
    let rev_before = meta.revision();
    for _ in 0..1000 {
        svc.authenticate(Some(&t.token)).unwrap();
    }
    eventually(|| {
        meta.get_as::<StoredToken>(&key)
            .unwrap()
            .unwrap()
            .info
            .last_used_ms
            .is_some()
    })
    .await;
    // 1000 authentications → exactly one write.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(meta.revision(), rev_before + 1);
}

// ---------------------------------------------------------------------------------------------
// v0.5: live-session revocation, last-admin guard, certificate bindings
// ---------------------------------------------------------------------------------------------

fn binding_req(cn: &str, role: Role, publish: &[&str]) -> CreateCertBindingRequest {
    CreateCertBindingRequest {
        cn: cn.into(),
        role,
        scopes: ClientScopes {
            publish: publish.iter().map(|s| s.to_string()).collect(),
            consume: vec![],
            ..Default::default()
        },
    }
}

async fn ends_within(
    fut: impl std::future::Future<Output = SessionEnd>,
    within: Duration,
) -> Option<SessionEnd> {
    tokio::time::timeout(within, fut).await.ok()
}

#[tokio::test]
async fn session_watch_fires_on_revoke() {
    let svc = AuthService::in_memory(Some("b".into()));
    let t = svc
        .create_token(req("live", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let p = svc.authenticate(Some(&t.token)).unwrap();
    let watch = tokio::spawn(svc.session_watch(&p));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!watch.is_finished(), "valid token: session stays open");
    let started = std::time::Instant::now();
    svc.revoke_token(&t.info.id).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(1), watch)
        .await
        .expect("fires within 1 s")
        .unwrap();
    assert_eq!(end, SessionEnd::TokenRevoked);
    assert_eq!(end.reason(), "token revoked");
    assert!(started.elapsed() < Duration::from_millis(500));
    // A watch created after the revoke resolves immediately.
    assert_eq!(
        ends_within(svc.session_watch(&p), Duration::from_millis(100)).await,
        Some(SessionEnd::TokenRevoked)
    );
}

#[tokio::test]
async fn session_watch_fires_on_expiry() {
    let svc = AuthService::in_memory(Some("b".into()));
    let mut r = req("short", Role::Viewer);
    r.ttl_secs = Some(1);
    let t = svc
        .create_token(r, &Principal::bootstrap_admin())
        .await
        .unwrap();
    let p = svc.authenticate(Some(&t.token)).unwrap();
    let exp = t.info.expires_at_ms.unwrap();
    let end = ends_within(svc.session_watch(&p), Duration::from_secs(3)).await;
    let now = fluxmq_proto::now_ms();
    assert_eq!(end, Some(SessionEnd::TokenExpired));
    assert_eq!(SessionEnd::TokenExpired.reason(), "token expired");
    assert!(now >= exp, "not before the expiry");
    assert!(
        now - exp < 1000,
        "within 1 s of the expiry ({} ms)",
        now - exp
    );
}

#[tokio::test]
async fn session_watch_ignores_bootstrap_and_open_mode() {
    let svc = AuthService::in_memory(Some("b".into()));
    let boot = svc.authenticate(Some("b")).unwrap();
    let anon = AuthService::disabled().authenticate(None).unwrap();
    for p in [boot, anon] {
        assert_eq!(
            ends_within(svc.session_watch(&p), Duration::from_millis(150)).await,
            None
        );
    }
    // Unrelated changes wake but don't end the session.
    let t = svc
        .create_token(req("x", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let other = svc
        .create_token(req("y", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    let p = svc.authenticate(Some(&t.token)).unwrap();
    let watch = tokio::spawn(svc.session_watch(&p));
    svc.revoke_token(&other.info.id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!watch.is_finished());
    watch.abort();
}

#[tokio::test]
async fn session_watch_sees_revocation_from_another_node() {
    let store = LocalMetaStore::in_memory();
    let meta: SharedMetaStore = Arc::new(store);
    let n1 = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    let n2 = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    let t = n1
        .create_token(req("svc", Role::Operator), &Principal::bootstrap_admin())
        .await
        .unwrap();
    eventually(|| n2.authenticate(Some(&t.token)).is_ok()).await;
    let p = n2.authenticate(Some(&t.token)).unwrap();
    let watch = tokio::spawn(n2.session_watch(&p));
    tokio::time::sleep(Duration::from_millis(20)).await;
    n1.revoke_token(&t.info.id).await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(1), watch)
        .await
        .expect("n2 session ends within 1 s")
        .unwrap();
    assert_eq!(end, SessionEnd::TokenRevoked);
}

#[tokio::test]
async fn last_admin_guard() {
    let svc = AuthService::disabled();
    let anon = Principal::anonymous_admin();
    let a = svc
        .create_token(req("a", Role::Admin), &anon)
        .await
        .unwrap();
    let pa = svc.authenticate(Some(&a.token)).unwrap();
    let b = svc.create_token(req("b", Role::Admin), &pa).await.unwrap();
    let v = svc.create_token(req("v", Role::Viewer), &pa).await.unwrap();

    // Two admins: one may go.
    svc.revoke_token(&b.info.id).await.unwrap();
    // The last active admin cannot, while other tokens exist.
    let r = svc.revoke_token(&a.info.id).await;
    assert!(matches!(r, Err(AuthError::LastAdmin(_))), "{r:?}");
    // Non-admins can.
    svc.revoke_token(&v.info.id).await.unwrap();
    // The very last token: still the old 400 (would reopen the broker).
    assert!(matches!(
        svc.revoke_token(&a.info.id).await,
        Err(AuthError::Invalid(_))
    ));

    // An expired admin doesn't count as an admin.
    let c = svc.create_token(req("c", Role::Admin), &pa).await.unwrap();
    let key = format!("{TOKEN_PREFIX}{}", c.info.id);
    let mut stored: StoredToken = svc.inner.meta.get_as(&key).unwrap().unwrap();
    stored.info.expires_at_ms = Some(fluxmq_proto::now_ms() - 1);
    let rev = svc.inner.meta.put_as(&key, &stored).await.unwrap();
    svc.inner.apply_token(&c.info.id, rev, Some(stored));
    assert!(matches!(
        svc.revoke_token(&a.info.id).await,
        Err(AuthError::LastAdmin(_))
    ));
    // ...and revoking the expired admin itself is fine.
    svc.revoke_token(&c.info.id).await.unwrap();

    // With a bootstrap token there is always an admin: no guard.
    let boot = AuthService::in_memory(Some("b".into()));
    let x = boot
        .create_token(req("x", Role::Admin), &Principal::bootstrap_admin())
        .await
        .unwrap();
    boot.create_token(req("y", Role::Viewer), &Principal::bootstrap_admin())
        .await
        .unwrap();
    boot.revoke_token(&x.info.id).await.unwrap();
}

#[tokio::test]
async fn last_admin_guard_survives_a_concurrent_revoke_elsewhere() {
    // A service without a watch task: its index goes stale, like a node that has not yet
    // applied another node's revoke of the other admin.
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::in_memory());
    let svc = AuthService::build(meta.clone(), None, false);
    let anon = Principal::anonymous_admin();
    let a = svc
        .create_token(req("a", Role::Admin), &anon)
        .await
        .unwrap();
    let pa = svc.authenticate(Some(&a.token)).unwrap();
    let b = svc.create_token(req("b", Role::Admin), &pa).await.unwrap();
    svc.create_token(req("v", Role::Viewer), &pa).await.unwrap();
    assert_eq!(
        svc.check_revoke(&a.info.id).unwrap(),
        Some(format!("{TOKEN_PREFIX}{}", b.info.id))
    );
    // "Another node" revokes b; this node's index still lists it.
    meta.delete(&format!("{TOKEN_PREFIX}{}", b.info.id), None)
        .await
        .unwrap();
    assert_eq!(svc.list_tokens().len(), 3);
    let r = svc.revoke_token(&a.info.id).await;
    assert!(matches!(r, Err(AuthError::LastAdmin(_))), "{r:?}");
    assert!(meta.get(&format!("{TOKEN_PREFIX}{}", a.info.id)).is_some());
}

#[tokio::test]
async fn cert_bindings_crud_and_validation() {
    let svc = AuthService::in_memory(Some("b".into()));
    let admin = Principal::bootstrap_admin();
    assert!(!svc.client_mtls_enabled());
    svc.set_client_mtls(true);
    assert!(svc.client_mtls_enabled());

    let created = svc
        .create_cert_binding(
            binding_req("billing-svc", Role::Viewer, &["billing.*"]),
            &admin,
        )
        .await
        .unwrap();
    assert_eq!(created.cn, "billing-svc");
    assert_eq!(created.created_by, "admin");
    assert_eq!(created.scopes.publish, vec!["billing.*"]);
    // Duplicate CN → conflict.
    assert!(matches!(
        svc.create_cert_binding(binding_req("billing-svc", Role::Admin, &[]), &admin)
            .await,
        Err(AuthError::Conflict(_))
    ));
    for bad in ["", "   ", "a/b", "x\ny", &"c".repeat(MAX_CN_LEN + 1)] {
        assert!(
            matches!(
                svc.create_cert_binding(binding_req(bad, Role::Viewer, &[]), &admin)
                    .await,
                Err(AuthError::Invalid(_))
            ),
            "cn {bad:?}"
        );
    }
    assert!(matches!(
        svc.create_cert_binding(binding_req("ok", Role::Viewer, &["bad scope"]), &admin)
            .await,
        Err(AuthError::Invalid(_))
    ));
    // Admin only.
    let t = svc
        .create_token(req("ops", Role::Operator), &admin)
        .await
        .unwrap();
    let op = svc.authenticate(Some(&t.token)).unwrap();
    assert!(matches!(
        svc.create_cert_binding(binding_req("z", Role::Viewer, &[]), &op)
            .await,
        Err(AuthError::Forbidden(_))
    ));
    svc.create_cert_binding(binding_req("alpha", Role::Admin, &[]), &admin)
        .await
        .unwrap();
    let cns: Vec<String> = svc.list_cert_bindings().into_iter().map(|b| b.cn).collect();
    assert_eq!(cns, vec!["alpha", "billing-svc"]);

    svc.delete_cert_binding("alpha").await.unwrap();
    assert!(matches!(
        svc.delete_cert_binding("alpha").await,
        Err(AuthError::BindingNotFound)
    ));
    assert_eq!(svc.list_cert_bindings().len(), 1);
}

#[tokio::test]
async fn cert_binding_authentication() {
    let svc = AuthService::in_memory(Some("b".into()));
    let admin = Principal::bootstrap_admin();
    svc.create_cert_binding(binding_req("svc-a", Role::Viewer, &["a.*"]), &admin)
        .await
        .unwrap();
    let t = svc
        .create_token(req("tok", Role::Operator), &admin)
        .await
        .unwrap();

    // Certificate alone → the binding's principal.
    let p = svc.authenticate_client(None, Some("svc-a")).unwrap();
    assert_eq!(p.actor(), "cert:svc-a");
    assert_eq!(p.cert_cn.as_deref(), Some("svc-a"));
    assert_eq!(p.role, Role::Viewer);
    assert!(svc.authorize(&p, Action::Publish("a.x")).is_ok());
    assert!(svc.authorize(&p, Action::Publish("b.x")).is_err());
    // Empty token counts as none.
    assert_eq!(
        svc.authenticate_client(Some("  "), Some("svc-a")).unwrap(),
        p
    );
    // A token wins over the certificate, even an invalid one (no silent fallback).
    let pt = svc
        .authenticate_client(Some(&t.token), Some("svc-a"))
        .unwrap();
    assert_eq!(pt.name, "tok");
    assert!(matches!(
        svc.authenticate_client(Some("fmq_bad"), Some("svc-a")),
        Err(AuthError::Unauthenticated)
    ));
    // Unknown CN / no certificate.
    for cn in [Some("other"), None] {
        assert!(matches!(
            svc.authenticate_client(None, cn),
            Err(AuthError::Unauthenticated)
        ));
    }
    // Open mode stays anonymous admin.
    let open = AuthService::disabled();
    assert_eq!(
        open.authenticate_client(None, Some("svc-a")).unwrap(),
        Principal::anonymous_admin()
    );

    // Deleting the binding ends sessions that used it.
    let watch = tokio::spawn(svc.session_watch(&p));
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!watch.is_finished());
    svc.delete_cert_binding("svc-a").await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(1), watch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(end, SessionEnd::BindingRevoked);
    assert!(svc.authenticate_client(None, Some("svc-a")).is_err());
}

#[tokio::test]
async fn cert_bindings_replicate_and_persist_last_used() {
    let meta: SharedMetaStore = Arc::new(LocalMetaStore::in_memory());
    let n1 = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    let n2 = AuthService::open(meta.clone(), Some("b".into()))
        .await
        .unwrap();
    n1.create_cert_binding(
        binding_req("edge", Role::Operator, &[]),
        &Principal::bootstrap_admin(),
    )
    .await
    .unwrap();
    eventually(|| n2.authenticate_client(None, Some("edge")).is_ok()).await;
    let key = format!("{CERT_BINDING_PREFIX}edge");
    let rev_before = meta.revision();
    for _ in 0..100 {
        n2.authenticate_client(None, Some("edge")).unwrap();
    }
    eventually(|| {
        meta.get_as::<CertBinding>(&key)
            .unwrap()
            .unwrap()
            .last_used_ms
            .is_some()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(meta.revision(), rev_before + 1, "throttled to one write");
    eventually(|| n1.list_cert_bindings()[0].last_used_ms.is_some()).await;

    // Deleted on n2 → gone on n1, and n1's sessions end.
    let p = n1.authenticate_client(None, Some("edge")).unwrap();
    let watch = tokio::spawn(n1.session_watch(&p));
    n2.delete_cert_binding("edge").await.unwrap();
    let end = tokio::time::timeout(Duration::from_secs(1), watch)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(end, SessionEnd::BindingRevoked);
    assert!(n1.list_cert_bindings().is_empty());

    // Reload picks bindings up from the store.
    n1.create_cert_binding(
        binding_req("again", Role::Viewer, &[]),
        &Principal::bootstrap_admin(),
    )
    .await
    .unwrap();
    let n3 = AuthService::open(meta.clone(), None).await.unwrap();
    assert_eq!(n3.list_cert_bindings().len(), 1);
}
