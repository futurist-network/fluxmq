//! Delayed messages (v0.12): the soonest pending ones (all nodes), cancel and release now.
//! Control operations are served by the leader of the entry's `$scheduled` partition; another
//! node forwards them. Contract: `fluxmq_proto::scheduled`.

use std::collections::BTreeMap;

use axum::extract::State;
use axum::http::StatusCode;
use fluxmq_cluster::PartitionRoute;
use fluxmq_proto::MessageId;
use fluxmq_proto::scheduled::{
    NodeScheduled, SCHEDULED_CHANNEL, SCHEDULED_PARTITIONS, ScheduledChannel, ScheduledList,
};
use fluxmq_scheduler::ControlError;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Path, Query};
use crate::state::AppState;

const INTERNAL_LIST_PATH: &str = "/internal/v1/scheduled";
const INTERNAL_CONTROL_PATH: &str = "/internal/v1/scheduled/{partition}/{offset}/{op}";

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ListQuery {
    pub channel: Option<String>,
    pub limit: Option<u32>,
}

fn limit_of(q: &ListQuery) -> usize {
    q.limit.unwrap_or(100).clamp(1, 1000) as usize
}

/// `GET /api/scheduled?channel=&limit=`.
pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> axum::Json<ScheduledList> {
    let limit = limit_of(&q);
    let mut nodes = vec![s.scheduler.local(q.channel.clone(), limit).await];
    if s.cluster.is_clustered() {
        nodes.extend(
            super::webhooks::from_peers::<_, NodeScheduled>(&s, INTERNAL_LIST_PATH, &q).await,
        );
    }
    axum::Json(merge(nodes, limit))
}

/// Merges the nodes' reports: soonest items first, counts summed per channel.
pub fn merge(nodes: Vec<NodeScheduled>, limit: usize) -> ScheduledList {
    let mut items = Vec::new();
    let mut channels: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let (mut released, mut cancelled) = (0, 0);
    for n in nodes {
        items.extend(n.items);
        for c in n.channels {
            let e = channels.entry(c.channel).or_insert((0, u64::MAX));
            e.0 += c.count;
            e.1 = e.1.min(c.next_due_ms);
        }
        released += n.released;
        cancelled += n.cancelled;
    }
    items.sort_by_key(|i| (i.due_ms, i.id.partition, i.id.offset));
    items.truncate(limit);
    let mut channels: Vec<ScheduledChannel> = channels
        .into_iter()
        .map(|(channel, (count, next_due_ms))| ScheduledChannel {
            channel,
            count,
            next_due_ms,
        })
        .collect();
    channels.sort_by(|a, b| {
        a.next_due_ms
            .cmp(&b.next_due_ms)
            .then(a.channel.cmp(&b.channel))
    });
    ScheduledList {
        total: channels.iter().map(|c| c.count).sum(),
        items,
        channels,
        released,
        cancelled,
    }
}

/// `DELETE /api/scheduled/{partition}/{offset}`: cancel.
pub async fn cancel(
    State(s): State<AppState>,
    Path((partition, offset)): Path<(u32, u64)>,
) -> ApiResult<StatusCode> {
    control(&s, MessageId { partition, offset }, false).await?;
    s.audit(
        "scheduled.cancel",
        SCHEDULED_CHANNEL,
        json!({ "id": format!("{partition}/{offset}") }),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /api/scheduled/{partition}/{offset}/release`: publish now.
pub async fn release(
    State(s): State<AppState>,
    Path((partition, offset)): Path<(u32, u64)>,
) -> ApiResult<StatusCode> {
    control(&s, MessageId { partition, offset }, true).await?;
    s.audit(
        "scheduled.release",
        SCHEDULED_CHANNEL,
        json!({ "id": format!("{partition}/{offset}") }),
    );
    Ok(StatusCode::NO_CONTENT)
}

async fn control(s: &AppState, id: MessageId, now: bool) -> ApiResult<()> {
    let not_found = || ApiErr::not_found("no pending delayed message with this id");
    if id.partition >= SCHEDULED_PARTITIONS {
        return Err(not_found());
    }
    match s.scheduler.control(id, now).await {
        Ok(()) => Ok(()),
        Err(ControlError::NotFound) => Err(not_found()),
        Err(ControlError::Failed(m)) => Err(ApiErr::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            m,
        )),
        Err(ControlError::NotLeader(p)) => {
            let unavailable = || {
                ApiErr::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "unavailable",
                    format!(
                        "the leader of {SCHEDULED_CHANNEL} partition {p} is not available; retry"
                    ),
                )
            };
            let PartitionRoute::Remote(node) = s.cluster.route_partition(SCHEDULED_CHANNEL, p)
            else {
                return Err(unavailable());
            };
            let path = INTERNAL_CONTROL_PATH
                .replace("{partition}", &id.partition.to_string())
                .replace("{offset}", &id.offset.to_string())
                .replace("{op}", if now { "release" } else { "cancel" });
            match s
                .cluster
                .internal_http()
                .post::<_, serde_json::Value>(&node.http_addr, &path, &json!({}))
                .await
            {
                Ok(_) => Ok(()),
                Err(fluxmq_cluster::rpc::TransportError::Status(404, _)) => Err(not_found()),
                Err(_) => Err(unavailable()),
            }
        }
    }
}

async fn internal_list(
    State(s): State<AppState>,
    Query(q): Query<ListQuery>,
) -> axum::Json<NodeScheduled> {
    let limit = limit_of(&q);
    axum::Json(s.scheduler.local(q.channel, limit).await)
}

async fn internal_control(
    State(s): State<AppState>,
    Path((partition, offset, op)): Path<(u32, u64, String)>,
) -> ApiResult<axum::Json<serde_json::Value>> {
    let id = MessageId { partition, offset };
    match s.scheduler.control(id, op == "release").await {
        Ok(()) => Ok(axum::Json(json!({ "ok": true }))),
        Err(ControlError::NotFound) => {
            Err(ApiErr::not_found("no pending delayed message with this id"))
        }
        Err(e) => Err(ApiErr::new(
            StatusCode::CONFLICT,
            "unavailable",
            e.to_string(),
        )),
    }
}

/// Node-to-node routes, merged into `cluster::internal`.
pub fn internal() -> axum::Router<AppState> {
    axum::Router::new()
        .route(INTERNAL_LIST_PATH, axum::routing::get(internal_list))
        .route(INTERNAL_CONTROL_PATH, axum::routing::post(internal_control))
}
