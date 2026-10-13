//! Client error type.

use fluxmq_proto::wire::{ErrorCode, WireError};

/// Result alias used throughout the SDK.
pub type Result<T, E = ClientError> = std::result::Result<T, E>;

/// Everything that can go wrong when talking to a broker.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ClientError {
    /// Socket-level failure (connect, read, write). The message is the `io::Error` text.
    #[error("io: {0}")]
    Io(String),
    /// The broker sent something we could not decode or did not expect.
    #[error("protocol: {0}")]
    Protocol(String),
    /// The broker rejected the request.
    #[error("server: {0}")]
    Server(#[from] WireError),
    /// No response within [`ClientOptions::request_timeout`](crate::ClientOptions::request_timeout)
    /// (for publishes: still no success when
    /// [`ClientOptions::publish_timeout`](crate::ClientOptions::publish_timeout) ran out).
    #[error("request timed out")]
    Timeout,
    /// The connection is gone (closed locally, broker `Goodbye`, I/O failure, or no connection
    /// could be (re-)established in time). The string explains why. Requests in flight on a
    /// dropped connection fail with this; publishes are retried internally first (see
    /// [`ClientOptions::publish_timeout`](crate::ClientOptions::publish_timeout)).
    #[error("connection closed: {0}")]
    Closed(String),
}

impl ClientError {
    /// The broker error code, if this is a [`ClientError::Server`] error.
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            ClientError::Server(e) => Some(e.code),
            _ => None,
        }
    }

    /// True for errors where retrying on a *new* connection might succeed.
    pub fn is_connection_error(&self) -> bool {
        matches!(self, ClientError::Io(_) | ClientError::Closed(_))
    }

    /// True for errors the SDK retries publishes on (until `publish_timeout`): connection loss,
    /// request timeouts, `NotLeader` (leadership moved) and `Overloaded` (incl. "not enough
    /// replicas"). `BadRequest`, `Forbidden`, `Unauthorized` and `NotFound` are not retried
    /// (in a cluster `NotFound` is retried while the metadata still lists the channel).
    pub fn is_retryable(&self) -> bool {
        self.is_connection_error()
            || matches!(self, ClientError::Timeout)
            || matches!(
                self.code(),
                Some(ErrorCode::NotLeader | ErrorCode::Overloaded)
            )
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        ClientError::Io(e.to_string())
    }
}
