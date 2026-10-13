//! Prints what the client learns about the cluster topology.
//!
//! ```text
//! cargo run -p fluxmq-client --example metadata -- 127.0.0.1:7420,127.0.0.1:7430
//! ```

use fluxmq_client::{Client, ClientOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let seeds = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7420".into());
    let client = Client::connect(ClientOptions::new(seeds, "metadata-probe")).await?;
    println!("server: {:?}", client.server_info());
    if let Err(e) = client.refresh_metadata().await {
        println!("metadata refresh error: {e}");
    }
    match client.metadata() {
        Some(m) => println!("{}", serde_json::to_string_pretty(&m)?),
        None => println!("single node (broker does not serve cluster metadata)"),
    }
    Ok(())
}
