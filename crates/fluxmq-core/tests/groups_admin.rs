//! Consumer group administration (`Broker::delete_group`).

mod common;

use std::time::Duration;

use common::*;
use fluxmq_core::CoreError;
use fluxmq_proto::model::{ChannelMode, StartFrom};

#[tokio::test]
async fn delete_group_removes_state_and_persisted_offsets() {
    let dir = tempfile::tempdir().unwrap();
    {
        let b = open(dir.path()).await;
        b.create_channel(channel("c", ChannelMode::Stream, 2))
            .unwrap();
        publish_keyed(&b, "c", 20, 4).await;
        let mut sub = b
            .subscribe("c", "g", info("x", 1), 100, StartFrom::Earliest)
            .unwrap();
        let d = sub.next().await.unwrap();
        sub.ack(&[d.delivery_id]).unwrap();
        // Active subscription: refused.
        let err = b.delete_group("c", "g").unwrap_err();
        assert!(matches!(err, CoreError::InvalidArgument(_)), "{err}");
        drop(sub);
        assert!(matches!(
            b.delete_group("c", "nope").unwrap_err(),
            CoreError::GroupNotFound { .. }
        ));
        assert!(matches!(
            b.delete_group("missing", "g").unwrap_err(),
            CoreError::ChannelNotFound(_)
        ));
        b.delete_group("c", "g").unwrap();
        assert!(!b.has_group("c", "g").unwrap());
        assert!(b.stats().channels[0].groups.is_empty());
        // Re-subscribing creates a fresh group at its start position.
        let mut sub = b
            .subscribe("c", "g", info("x", 2), 100, StartFrom::Latest)
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), sub.next())
                .await
                .is_err()
        );
        drop(sub);
        b.delete_group("c", "g").unwrap();
        b.shutdown().await;
    }
    // Persisted: the group does not come back after a restart.
    let b = open(dir.path()).await;
    assert!(!b.has_group("c", "g").unwrap());
    b.shutdown().await;
}
