//! Messages browser: query stored messages and fetch one with its delivery history.

use axum::extract::State;
use fluxmq_proto::MessageId;
use fluxmq_proto::api::{MessageDetail, MessagePage, MessageQuery};

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Path, Query};
use crate::payload;
use crate::state::AppState;

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;

pub async fn query(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(mut q): Query<MessageQuery>,
) -> ApiResult<axum::Json<MessagePage>> {
    // Cluster: partition leaders answer for their partitions (stage 9).
    if s.cluster.is_clustered() {
        q.limit = Some(q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT));
        return super::cluster::fanout_messages(s, name, q).await;
    }
    query_local(State(s), Path(name), Query(q)).await
}

/// [`query`] against this node's storage only.
pub async fn query_local(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(mut q): Query<MessageQuery>,
) -> ApiResult<axum::Json<MessagePage>> {
    q.limit = Some(q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT));
    if matches!((q.since_ms, q.until_ms), (Some(since), Some(until)) if since > until) {
        return Err(ApiErr::bad_request("since_ms must be <= until_ms"));
    }
    // Filter DSL (`filter=`): parse up front (400 with the error position), then a bounded
    // scan over `read_messages` pages. Without a filter the plain read path is unchanged.
    let conditions = match q.filter.as_deref().map(str::trim) {
        Some(f) if !f.is_empty() => {
            Some(fluxmq_router::filter::parse(f).map_err(|e| ApiErr::filter_syntax(&e))?)
        }
        _ => None,
    };
    // Reads may touch disk: keep them off the async workers.
    let broker = s.broker.clone();
    let (messages, scanned, has_more) = match conditions {
        None => tokio::task::spawn_blocking(move || broker.read_messages(&name, &q)).await??,
        Some(conditions) => {
            let r = tokio::task::spawn_blocking(move || {
                fluxmq_router::scan::search(&broker, &name, &q, &conditions)
            })
            .await??;
            (r.messages, r.scanned, r.has_more)
        }
    };
    Ok(axum::Json(MessagePage {
        messages: messages.into_iter().map(payload::message_view).collect(),
        scanned,
        has_more,
    }))
}

pub async fn detail(
    State(s): State<AppState>,
    Path((name, partition, offset)): Path<(String, u32, u64)>,
) -> ApiResult<axum::Json<MessageDetail>> {
    if s.cluster.is_clustered() {
        return super::cluster::fanout_detail(s, name, partition, offset).await;
    }
    detail_local(State(s), Path((name, partition, offset))).await
}

/// [`detail`] against this node's storage only.
pub async fn detail_local(
    State(s): State<AppState>,
    Path((name, partition, offset)): Path<(String, u32, u64)>,
) -> ApiResult<axum::Json<MessageDetail>> {
    let broker = s.broker.clone();
    let channel = name.clone();
    let found = tokio::task::spawn_blocking(move || {
        broker.get_message(&channel, MessageId { partition, offset })
    })
    .await??;
    match found {
        Some((message, deliveries)) => Ok(axum::Json(MessageDetail {
            message: payload::message_view(message),
            deliveries,
        })),
        None => Err(ApiErr::not_found(format!(
            "message {partition}/{offset} not found in channel '{name}'"
        ))),
    }
}
