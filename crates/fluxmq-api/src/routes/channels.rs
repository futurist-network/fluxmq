//! Channel CRUD, publish, replay, consumer-group offset reset and group deletion.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use fluxmq_proto::api::{
    CreateChannelRequest, PublishRequest, PublishResponse, ReplayRequest, ResetOffsetsRequest,
    UpdateChannelRequest,
};
use fluxmq_proto::model::{
    ChannelConfig, PublishMessage, is_system_channel, validate_channel_name,
};
use fluxmq_proto::scheduled::{DELAY_HEADER, DELIVER_AT_HEADER};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, Path};
use crate::payload;
use crate::state::AppState;

/// System channels (`$…`) are maintained by the broker: 403 for user writes.
fn ensure_not_system(name: &str) -> ApiResult<()> {
    if is_system_channel(name) {
        return Err(ApiErr::new(
            StatusCode::FORBIDDEN,
            "forbidden",
            format!("'{name}' is a system channel and is read-only"),
        ));
    }
    Ok(())
}

pub async fn list(State(s): State<AppState>) -> axum::Json<Vec<ChannelConfig>> {
    let mut channels = s.broker.channels();
    channels.sort_by(|a, b| a.name.cmp(&b.name));
    axum::Json(channels)
}

pub async fn create(
    State(s): State<AppState>,
    Json(req): Json<CreateChannelRequest>,
) -> ApiResult<(StatusCode, axum::Json<ChannelConfig>)> {
    validate_channel_name(&req.name).map_err(ApiErr::bad_request)?;
    if is_system_channel(&req.name) {
        return Err(ApiErr::bad_request(
            "channel names starting with '$' are reserved for system channels",
        ));
    }
    let mut config = ChannelConfig::new(req.name, req.mode);
    if let Some(p) = req.partitions {
        if p == 0 {
            return Err(ApiErr::bad_request("partitions must be >= 1"));
        }
        config.partitions = p;
    }
    if let Some(r) = req.retention {
        config.retention = r;
    }
    if let Some(r) = req.retry {
        config.retry = r;
    }
    if let Some(d) = req.dlq_enabled {
        config.dlq_enabled = d;
    }
    config.ordering_key = req.ordering_key;
    // Through the controller when clustered (replicated to every node), locally otherwise.
    let created = s.cluster.create_channel(config).await?;
    s.audit(
        "channel.create",
        &created.name,
        serde_json::to_value(&created).unwrap_or_default(),
    );
    Ok((StatusCode::CREATED, axum::Json(created)))
}

pub async fn get(
    State(s): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<axum::Json<ChannelConfig>> {
    s.broker
        .channel(&name)
        .map(axum::Json)
        .ok_or_else(|| ApiErr::not_found(format!("channel '{name}' not found")))
}

pub async fn update(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateChannelRequest>,
) -> ApiResult<axum::Json<ChannelConfig>> {
    ensure_not_system(&name)?;
    let detail = serde_json::to_value(&req).unwrap_or_default();
    let updated = s.cluster.update_channel(&name, req).await?;
    s.audit("channel.update", &name, detail);
    Ok(axum::Json(updated))
}

pub async fn delete(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult<StatusCode> {
    ensure_not_system(&name)?;
    s.cluster.delete_channel(&name).await?;
    s.audit("channel.delete", &name, json!({}));
    Ok(StatusCode::NO_CONTENT)
}

pub async fn publish(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<PublishRequest>,
) -> ApiResult<axum::Json<PublishResponse>> {
    ensure_not_system(&name)?;
    if req.messages.is_empty() {
        return Err(ApiErr::bad_request("messages must not be empty"));
    }
    let messages = req
        .messages
        .into_iter()
        .map(|m| {
            let mut headers = m.headers;
            // v0.12: delayed delivery, as the headers the client protocol uses.
            match (m.deliver_at_ms, m.delay_ms) {
                (Some(_), Some(_)) => {
                    return Err(ApiErr::bad_request(
                        "deliver_at_ms and delay_ms are mutually exclusive",
                    ));
                }
                (Some(at), None) => {
                    headers.insert(DELIVER_AT_HEADER.into(), at.to_string());
                }
                (None, Some(d)) => {
                    headers.insert(DELAY_HEADER.into(), d.to_string());
                }
                (None, None) => {}
            }
            Ok(PublishMessage {
                key: m.key,
                headers,
                payload: payload::decode(m.payload)?,
            })
        })
        .collect::<ApiResult<Vec<_>>>()?;
    let count = messages.len();
    let delayed = messages
        .iter()
        .filter(|m| fluxmq_proto::scheduled::is_delayed(&m.headers))
        .count();
    // Cluster: routed to partition leaders (stage 9); single node: `Broker::publish`; delayed
    // messages to `$scheduled` (v0.12).
    let ids = s
        .scheduler
        .publish(&name, None, messages)
        .await
        .map_err(super::cluster::wire_to_api)?;
    s.audit(
        "channel.publish",
        &name,
        if delayed > 0 {
            json!({ "count": count, "delayed": delayed })
        } else {
            json!({ "count": count })
        },
    );
    Ok(axum::Json(PublishResponse { ids }))
}

pub async fn replay(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<ReplayRequest>,
) -> ApiResult<axum::Json<PublishResponse>> {
    ensure_not_system(req.target_channel.as_deref().unwrap_or(&name))?;
    if req.ids.is_empty() {
        return Err(ApiErr::bad_request("ids must not be empty"));
    }
    let ids = if s.cluster.is_clustered() {
        // Leaders read their partitions; copies go through the cluster publish path.
        let target = req.target_channel.clone().unwrap_or_else(|| name.clone());
        for c in [&name, &target] {
            if s.broker.channel(c).is_none() {
                return Err(ApiErr::not_found(format!("channel '{c}' not found")));
            }
        }
        super::cluster::fanout_replay(&s, &name, &req.ids, &target).await?
    } else {
        s.broker
            .replay(&name, &req.ids, req.target_channel.as_deref())
            .await?
    };
    s.audit(
        "channel.replay",
        &name,
        json!({ "count": req.ids.len(), "target_channel": req.target_channel.as_deref().unwrap_or(&name) }),
    );
    Ok(axum::Json(PublishResponse { ids }))
}

pub async fn reset_group(
    State(s): State<AppState>,
    Path((name, group)): Path<(String, String)>,
    Json(body): Json<ResetOffsetsRequest>,
) -> ApiResult<impl IntoResponse> {
    // Core would create an unknown group (with lag and no consumers → a `no_consumers`
    // alert, and no API to delete it), so a typo in the path must be a 404 here.
    if !s.broker.has_group(&name, &group)? {
        return Err(ApiErr::not_found(format!(
            "consumer group '{group}' not found on channel '{name}'"
        )));
    }
    if s.cluster.is_clustered() {
        // Group offsets are owned by partition leaders (stage 9).
        super::cluster::fanout_group_op(&s, &name, &group, super::cluster::GroupOp::Reset(body.to))
            .await?;
    } else {
        s.broker.reset_offsets(&name, &group, body.to)?;
    }
    s.audit(
        "group.reset",
        &format!("{name}/{group}"),
        json!({ "to": body.to }),
    );
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/channels/{name}/groups/{group}` (admin): removes an idle consumer group
/// (offsets, retry queue, persisted state). 404 unknown group, 409 while it has active
/// consumers, 409 for a reserved `$…` group still in use (`$router` while any route has this
/// channel as its source).
pub async fn delete_group(
    State(s): State<AppState>,
    Path((name, group)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
    if s.broker.channel(&name).is_none() {
        return Err(ApiErr::not_found(format!("channel '{name}' not found")));
    }
    if group == fluxmq_proto::ROUTER_GROUP && s.router.uses_source(&name) {
        return Err(ApiErr::conflict(format!(
            "group '{group}' is used by the routes of channel '{name}'; delete those routes first"
        )));
    }
    if s.webhooks.uses_group(&name, &group) {
        return Err(ApiErr::conflict(format!(
            "group '{group}' belongs to a webhook subscription; delete the webhook first"
        )));
    }
    delete_group_everywhere(&s, &name, &group).await?;
    s.audit("group.delete", &format!("{name}/{group}"), json!({}));
    Ok(StatusCode::NO_CONTENT)
}

/// Deletes `group` on this node, or on every partition leader when clustered.
pub(crate) async fn delete_group_everywhere(
    s: &AppState,
    name: &str,
    group: &str,
) -> ApiResult<()> {
    if s.cluster.is_clustered() {
        super::cluster::fanout_group_op(s, name, group, super::cluster::GroupOp::Delete).await
    } else {
        super::cluster::check_group_deletable(s, name, group)?;
        s.broker.delete_group(name, group).map_err(|e| match e {
            fluxmq_core::CoreError::InvalidArgument(m) => ApiErr::conflict(m),
            e => e.into(),
        })
    }
}
