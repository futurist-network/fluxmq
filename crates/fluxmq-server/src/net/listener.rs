//! TCP accept loop (plaintext, or TLS when the client port has a certificate).

use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use super::NetContext;
use super::connection;
use crate::tls::ClientPortTls;

/// Accepts connections until `shutdown` fires, then waits for every connection task to say
/// `Goodbye` and finish. With `tls`, every connection must complete a TLS handshake first
/// (bounded by the handshake timeout); plaintext clients are refused and counted.
pub async fn serve(
    listener: TcpListener,
    ctx: Arc<NetContext>,
    shutdown: CancellationToken,
    tls: Option<ClientPortTls>,
) {
    let tracker = TaskTracker::new();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::debug!(%peer, error = %e, "set_nodelay failed");
                    }
                    match &tls {
                        None => {
                            tracker.spawn(connection::run(stream, peer, ctx.clone(), shutdown.clone()));
                        }
                        Some(tls) => {
                            let (tls, ctx, shutdown) = (tls.clone(), ctx.clone(), shutdown.clone());
                            tracker.spawn(async move {
                                let accepted = tokio::select! {
                                    () = shutdown.cancelled() => None,
                                    a = tls.accept(stream, peer, ctx.limits.handshake_timeout) => a,
                                };
                                if let Some((rd, wr, cn)) = accepted {
                                    connection::run_io(rd, wr, peer, cn, ctx, shutdown).await;
                                }
                            });
                        }
                    }
                }
                Err(e) => {
                    // Typically EMFILE/ENFILE: back off instead of spinning.
                    tracing::warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
    drop(listener);
    tracker.close();
    tracing::debug!(
        connections = tracker.len(),
        "client listener stopped; draining connections"
    );
    tracker.wait().await;
}
