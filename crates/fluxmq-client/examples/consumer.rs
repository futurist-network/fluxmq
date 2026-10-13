//! Consumes `examples.orders` in group `examples`, acking everything.
//!
//! ```text
//! cargo run -p fluxmq-client --example consumer -- [addr]
//! ```

use fluxmq_client::{Client, ClientOptions, StartFrom, SubscribeOptions};
use fluxmq_proto::{ChannelConfig, ChannelMode, headers};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7420".into());
    let mut opts = ClientOptions::new(addr, "example-consumer");
    opts.auth_token = std::env::var("FLUXMQ_TOKEN").ok();
    let client = Client::connect(opts).await?;
    client
        .ensure_channel(ChannelConfig::new("examples.orders", ChannelMode::Stream))
        .await?;

    let mut consumer = client
        .subscribe(
            "examples.orders",
            "examples",
            SubscribeOptions::default()
                .prefetch(32)
                .start(StartFrom::Earliest),
        )
        .await?;
    println!(
        "subscribed (id {}), Ctrl-C to stop",
        consumer.subscription_id()
    );

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            next = consumer.next() => {
                let Some(delivery) = next else { break };
                let delivery = delivery?;
                println!(
                    "p{} @{} attempt {} key={:?} type={:?} {}",
                    delivery.id().partition,
                    delivery.id().offset,
                    delivery.attempt(),
                    delivery.key(),
                    delivery.header(headers::EVENT_TYPE),
                    String::from_utf8_lossy(delivery.payload()),
                );
                delivery.ack().await?;
            }
        }
    }
    consumer.unsubscribe().await?;
    Ok(())
}
