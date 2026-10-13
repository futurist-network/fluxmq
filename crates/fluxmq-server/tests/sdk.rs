//! End-to-end tests through the `fluxmq-client` SDK.

mod common;

use std::time::Duration;

use common::{TestServer, WAIT};
use fluxmq_client::{
    ChannelConfig, ChannelMode, Client, ClientOptions, ErrorCode, StartFrom, SubscribeOptions,
    message,
};

#[tokio::test(flavor = "multi_thread")]
async fn produce_and_consume_via_sdk() {
    let srv = TestServer::start().await;
    let client = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "sdk-e2e"))
        .await
        .expect("connect");
    assert!(
        client
            .ensure_channel(ChannelConfig::new("sdk.orders", ChannelMode::Keyed))
            .await
            .unwrap()
    );
    assert!(
        !client
            .ensure_channel(ChannelConfig::new("sdk.orders", ChannelMode::Keyed))
            .await
            .unwrap()
    );

    let producer = client.producer("sdk.orders");
    for i in 0..20 {
        producer
            .send(message(format!("order-{i}")).key(format!("k{}", i % 3)))
            .await
            .expect("publish");
    }

    let mut consumer = client
        .subscribe(
            "sdk.orders",
            "billing",
            SubscribeOptions::default()
                .start(StartFrom::Earliest)
                .prefetch(8),
        )
        .await
        .expect("subscribe");
    let mut seen = 0;
    while seen < 20 {
        let d = tokio::time::timeout(WAIT, consumer.next())
            .await
            .expect("delivery in time")
            .expect("open")
            .expect("ok");
        assert!(d.payload().starts_with(b"order-"));
        assert_eq!(d.header("flux-producer"), Some("sdk-e2e"));
        d.ack().await.expect("ack");
        seen += 1;
    }
    common::eventually(|| async {
        (srv.group_lag("sdk.orders", "billing") == Some(0)).then_some(())
    })
    .await;

    // The consumer instance is visible to REST search via its group.
    let results = srv.get_json("/api/search?q=billing").await;
    assert_eq!(results[0]["link"], "/channels/sdk.orders/consumers");
    srv.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn sdk_auth_and_ping() {
    let srv = TestServer::start_with(|c| c.admin_token = Some("tok".into())).await;
    let denied = Client::connect(ClientOptions::new(srv.client_addr.to_string(), "x")).await;
    let Err(err) = denied else {
        panic!("connect must be rejected without a token")
    };
    assert_eq!(err.code(), Some(ErrorCode::Unauthorized), "{err:?}");

    let client =
        Client::connect(ClientOptions::new(srv.client_addr.to_string(), "x").auth_token("tok"))
            .await
            .expect("connect");
    let rtt = client.ping().await.expect("ping");
    assert!(rtt < Duration::from_secs(5));
    srv.stop().await;
}
