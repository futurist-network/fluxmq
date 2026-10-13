//! Publishes a few JSON order events.
//!
//! ```text
//! cargo run -p fluxmq-client --example producer -- [addr] [count]
//! ```

use fluxmq_client::{Client, ClientOptions, message};
use fluxmq_proto::{ChannelConfig, ChannelMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let addr = args.next().unwrap_or_else(|| "127.0.0.1:7420".into());
    let count: u64 = args.next().and_then(|s| s.parse().ok()).unwrap_or(10);

    let mut opts = ClientOptions::new(addr, "example-producer");
    opts.auth_token = std::env::var("FLUXMQ_TOKEN").ok();
    let client = Client::connect(opts).await?;
    println!("connected to {:?}", client.server_info());

    let created = client
        .ensure_channel(ChannelConfig::new("examples.orders", ChannelMode::Stream))
        .await?;
    println!(
        "channel examples.orders {}",
        if created { "created" } else { "exists" }
    );

    let producer = client.producer("examples.orders");
    for i in 0..count {
        let order_id = format!("ord-{i:05}");
        let payload = serde_json::json!({ "order_id": order_id, "amount": 10.0 + i as f64, "currency": "EUR" });
        let id = producer
            .send(
                message(payload.to_string())
                    .key(&order_id)
                    .event_type("OrderCreated")
                    .content_type("application/json")
                    .new_trace_id(),
            )
            .await?;
        println!(
            "{order_id} -> partition {} offset {}",
            id.partition, id.offset
        );
    }
    Ok(())
}
