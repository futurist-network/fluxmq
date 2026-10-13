//! The narrow slice of the broker the observability plane consumes, as a trait so that the
//! hub, sessions and collector are testable without a running core.

use std::future::Future;
use std::pin::Pin;

use fluxmq_core::{Broker, BrokerEvent, BrokerStats, TailReceiver};
use fluxmq_proto::model::{ChannelConfig, Message};
use tokio::sync::broadcast;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A live-tail feed for one channel (see [`fluxmq_core::Broker::tail`]).
pub trait TailFeed: Send + 'static {
    /// Next sampled message; `None` when the channel is gone. Must be cancel-safe.
    fn recv(&mut self) -> BoxFuture<'_, Option<Message>>;
    /// Messages published since the last call.
    fn take_published_count(&mut self) -> u64;
}

/// What the telemetry plane needs from the broker.
pub trait Source: Send + Sync + 'static {
    fn stats(&self) -> BrokerStats;
    fn channel(&self, name: &str) -> Option<ChannelConfig>;
    fn events(&self) -> Option<broadcast::Receiver<BrokerEvent>>;
    fn tail(&self, channel: &str) -> Result<Box<dyn TailFeed>, String>;
}

impl TailFeed for TailReceiver {
    fn recv(&mut self) -> BoxFuture<'_, Option<Message>> {
        Box::pin(TailReceiver::recv(self))
    }
    fn take_published_count(&mut self) -> u64 {
        TailReceiver::take_published_count(self)
    }
}

impl Source for Broker {
    fn stats(&self) -> BrokerStats {
        Broker::stats(self)
    }
    fn channel(&self, name: &str) -> Option<ChannelConfig> {
        Broker::channel(self, name)
    }
    fn events(&self) -> Option<broadcast::Receiver<BrokerEvent>> {
        Some(Broker::events(self))
    }
    fn tail(&self, channel: &str) -> Result<Box<dyn TailFeed>, String> {
        Broker::tail(self, channel)
            .map(|t| Box::new(t) as Box<dyn TailFeed>)
            .map_err(|e| e.to_string())
    }
}
