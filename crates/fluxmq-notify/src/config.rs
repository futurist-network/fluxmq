//! Service configuration.

use std::time::Duration;

/// Fixed third-party endpoints (overridable so tests can point senders at a local mock).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    /// Telegram Bot API base (`{base}/bot{token}/sendMessage`).
    pub telegram_api: String,
    /// PagerDuty Events API v2 enqueue URL.
    pub pagerduty_events: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            telegram_api: "https://api.telegram.org".into(),
            pagerduty_events: "https://events.pagerduty.com/v2/enqueue".into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NotifyConfig {
    /// `--public-url`: absolute console base URL for links in messages.
    pub public_url: Option<String>,
    pub endpoints: Endpoints,
    /// Delivery attempts per message (first try included).
    pub attempts: u32,
    /// Delay before retry `n` (1-based); the last entry repeats.
    pub backoff: Vec<Duration>,
    pub http_timeout: Duration,
    pub smtp_timeout: Duration,
    /// Maintain the `$system.events` pull feed.
    pub feed: bool,
    /// Testing hook: when set, deliveries are handed to this channel instead of the network
    /// (lets dispatcher tests run on a paused clock).
    #[doc(hidden)]
    pub capture: Option<tokio::sync::mpsc::UnboundedSender<Captured>>,
}

/// A delivery intercepted by [`NotifyConfig::capture`].
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct Captured {
    pub kind: fluxmq_proto::notify::NotifierKind,
    pub payload: fluxmq_proto::notify::NotificationPayload,
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            public_url: None,
            endpoints: Endpoints::default(),
            attempts: 3,
            backoff: vec![Duration::from_secs(2), Duration::from_secs(10)],
            http_timeout: Duration::from_secs(10),
            smtp_timeout: Duration::from_secs(15),
            feed: true,
            capture: None,
        }
    }
}

impl NotifyConfig {
    pub fn with_public_url(mut self, url: Option<String>) -> Self {
        self.public_url = url
            .map(|u| u.trim().trim_end_matches('/').to_string())
            .filter(|u| !u.is_empty());
        self
    }

    pub(crate) fn backoff_for(&self, attempt: u32) -> Duration {
        let i = (attempt.max(1) - 1) as usize;
        self.backoff
            .get(i)
            .or(self.backoff.last())
            .copied()
            .unwrap_or(Duration::from_secs(1))
    }
}
