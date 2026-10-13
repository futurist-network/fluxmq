//! Dead-letter inbox: summary, listing, retry/discard and NDJSON export.

use std::collections::{BTreeMap, VecDeque};

use axum::body::Body;
use axum::extract::State;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use fluxmq_core::{Broker, DlqSelector};
use fluxmq_proto::api::{
    DeadLetterView, DlqActionRequest, DlqActionResponse, DlqPage, DlqQuery, DlqSummary,
};
use serde_json::json;

use crate::error::{ApiErr, ApiResult};
use crate::extract::{Json, Query};
use crate::payload;
use crate::state::AppState;

use super::cluster::PartSet;

pub const DEFAULT_LIMIT: u32 = 100;
pub const MAX_LIMIT: u32 = 1000;
/// Entries fetched per page while streaming an export.
const EXPORT_BATCH: usize = 500;

pub async fn summary(State(s): State<AppState>) -> ApiResult<axum::Json<DlqSummary>> {
    if s.cluster.is_clustered() {
        return super::cluster::fanout_dlq_summary(s).await;
    }
    Ok(axum::Json(s.broker.dlq_summary()))
}

pub async fn list(
    State(s): State<AppState>,
    Query(q): Query<DlqQuery>,
) -> ApiResult<axum::Json<DlqPage>> {
    if s.cluster.is_clustered() {
        return super::cluster::fanout_dlq_list(s, q).await;
    }
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize;
    let broker = s.broker.clone();
    let (entries, total) = tokio::task::spawn_blocking(move || {
        broker.dlq_list(q.channel.as_deref(), q.reason.as_deref(), q.before, limit)
    })
    .await?;
    Ok(axum::Json(DlqPage {
        entries: entries.into_iter().map(payload::dead_letter_view).collect(),
        total,
    }))
}

/// Converts the request into exactly one [`DlqSelector`].
///
/// Valid shapes: `{ids}`, `{reason}`, `{reason, channel}`, `{channel}`, `{all: true}`.
pub fn selector(req: DlqActionRequest) -> ApiResult<DlqSelector> {
    let all = req.all.unwrap_or(false);
    let selectors = usize::from(req.ids.is_some())
        + usize::from(req.reason.is_some())
        + usize::from(all)
        // `channel` alone is a selector; with `reason` it is a restriction.
        + usize::from(req.channel.is_some() && req.reason.is_none());
    if selectors != 1 {
        return Err(ApiErr::bad_request(
            "exactly one selector is required: ids, reason (optionally with channel), channel, or all=true",
        ));
    }
    Ok(match (req.ids, req.reason, req.channel) {
        (Some(ids), _, _) if ids.is_empty() => {
            return Err(ApiErr::bad_request("ids must not be empty"));
        }
        (Some(ids), _, _) => DlqSelector::Ids(ids),
        (None, Some(reason), channel) => DlqSelector::Reason { reason, channel },
        (None, None, Some(channel)) => DlqSelector::Channel(channel),
        (None, None, None) => DlqSelector::All,
    })
}

fn selector_detail(sel: &DlqSelector) -> serde_json::Value {
    match sel {
        DlqSelector::Ids(ids) => json!({ "ids": ids }),
        DlqSelector::Reason { reason, channel } => json!({ "reason": reason, "channel": channel }),
        DlqSelector::Channel(c) => json!({ "channel": c }),
        DlqSelector::All => json!({ "all": true }),
    }
}

pub async fn retry(
    State(s): State<AppState>,
    Json(req): Json<DlqActionRequest>,
) -> ApiResult<axum::Json<DlqActionResponse>> {
    action(s, req, "dlq.retry", |b, sel| b.dlq_retry(sel)).await
}

pub async fn discard(
    State(s): State<AppState>,
    Json(req): Json<DlqActionRequest>,
) -> ApiResult<axum::Json<DlqActionResponse>> {
    action(s, req, "dlq.discard", |b, sel| b.dlq_discard(sel)).await
}

async fn action(
    s: AppState,
    req: DlqActionRequest,
    name: &'static str,
    op: fn(&Broker, DlqSelector) -> fluxmq_core::Result<u64>,
) -> ApiResult<axum::Json<DlqActionResponse>> {
    if s.cluster.is_clustered() {
        let detail = selector_detail(&selector(req.clone())?);
        let r = super::cluster::fanout_dlq_action(s.clone(), req, name == "dlq.retry").await?;
        let mut detail = detail;
        detail["affected"] = json!(r.affected);
        s.audit(name, "dlq", detail);
        return Ok(r);
    }
    let sel = selector(req)?;
    let mut detail = selector_detail(&sel);
    let broker = s.broker.clone();
    let affected = tokio::task::spawn_blocking(move || op(&broker, sel)).await??;
    detail["affected"] = json!(affected);
    s.audit(name, "dlq", detail);
    Ok(axum::Json(DlqActionResponse { affected }))
}

/// Streams matching dead letters as NDJSON (`DeadLetterView` per line), newest first per
/// channel (per channel and partition leader in a cluster). `limit` caps the total; absent =
/// everything. In a cluster every partition leader streams its partitions; the coverage of
/// all partitions is checked before streaming starts (503 while leadership moves) and again
/// on every page (a leadership change mid-export aborts the download).
pub async fn export(State(s): State<AppState>, Query(q): Query<DlqQuery>) -> Response {
    // DLQ ids are unique per channel only, so an id cursor is only sound within one channel:
    // page each channel separately.
    let names: Vec<String> = match &q.channel {
        Some(c) => vec![c.clone()],
        None => {
            let mut names: Vec<String> = s.broker.channels().into_iter().map(|c| c.name).collect();
            names.sort();
            names
        }
    };
    let mut sources = VecDeque::new();
    if s.cluster.is_clustered() {
        let md = s.cluster.metadata(&names);
        for c in names {
            let mut per_leader: BTreeMap<String, PartSet> = BTreeMap::new();
            for p in md.channels.get(&c).into_iter().flatten() {
                if let Some(l) = &p.leader {
                    per_leader
                        .entry(l.clone())
                        .or_default()
                        .insert((c.clone(), p.partition));
                }
            }
            for (node, expected) in per_leader {
                sources.push_back(Source {
                    channel: c.clone(),
                    leader: Some((node, expected)),
                });
            }
        }
        // Up-front coverage probe: a proper 503 instead of a truncated download.
        let probe = DlqQuery {
            limit: Some(1),
            ..q.clone()
        };
        for src in &sources {
            if let Some((node, expected)) = &src.leader {
                let pq = DlqQuery {
                    channel: Some(src.channel.clone()),
                    ..probe.clone()
                };
                if let Err(e) = super::cluster::led_dlq_page(&s, node, &pq, expected).await {
                    return e.into_response();
                }
            }
        }
    } else {
        sources.extend(names.into_iter().map(|channel| Source {
            channel,
            leader: None,
        }));
    }
    let state = ExportState {
        app: s,
        sources,
        reason: q.reason,
        start_before: q.before,
        before: q.before,
        remaining: q.limit.map(u64::from),
    };
    let stream = futures::stream::unfold(state, |mut st| async move {
        let chunk = st.next_chunk().await?;
        Some((chunk, st))
    });
    let filename = format!("fluxmq-dlq-{}.ndjson", fluxmq_proto::now_ms());
    (
        [
            (header::CONTENT_TYPE, "application/x-ndjson".to_string()),
            (
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{filename}\""),
            ),
        ],
        Body::from_stream(stream),
    )
        .into_response()
}

/// One export cursor: a channel, on this node (single node) or on one partition leader with
/// the partitions it must cover.
struct Source {
    channel: String,
    leader: Option<(String, PartSet)>,
}

struct ExportState {
    app: AppState,
    sources: VecDeque<Source>,
    reason: Option<String>,
    /// The request's `before`, applied to every source.
    start_before: Option<u64>,
    /// Cursor within the current source.
    before: Option<u64>,
    remaining: Option<u64>,
}

impl ExportState {
    async fn fetch(
        &self,
        src: &Source,
        batch: usize,
    ) -> Result<Vec<DeadLetterView>, std::io::Error> {
        match &src.leader {
            None => {
                let broker = self.app.broker.clone();
                let channel = src.channel.clone();
                let (reason, before) = (self.reason.clone(), self.before);
                tokio::task::spawn_blocking(move || {
                    broker
                        .dlq_list(Some(&channel), reason.as_deref(), before, batch)
                        .0
                        .into_iter()
                        .map(payload::dead_letter_view)
                        .collect()
                })
                .await
                .map_err(std::io::Error::other)
            }
            Some((node, expected)) => {
                let q = DlqQuery {
                    channel: Some(src.channel.clone()),
                    reason: self.reason.clone(),
                    limit: Some(u32::try_from(batch).unwrap_or(u32::MAX)),
                    before: self.before,
                };
                super::cluster::led_dlq_page(&self.app, node, &q, expected)
                    .await
                    .map_err(|e| std::io::Error::other(e.body.message))
            }
        }
    }

    /// Next non-empty chunk of NDJSON lines; `None` when done.
    async fn next_chunk(&mut self) -> Option<Result<Bytes, std::io::Error>> {
        loop {
            if self.remaining == Some(0) {
                return None;
            }
            let src = self.sources.pop_front()?;
            let batch = match self.remaining {
                Some(r) => EXPORT_BATCH.min(usize::try_from(r).unwrap_or(EXPORT_BATCH)),
                None => EXPORT_BATCH,
            };
            let entries = match self.fetch(&src, batch).await {
                Ok(e) => e,
                Err(e) => return Some(Err(e)),
            };
            if entries.len() < batch {
                // Source exhausted: move on.
                self.before = self.start_before;
            } else {
                self.before = entries.iter().map(|e| e.id).min();
                self.sources.push_front(src);
            }
            if entries.is_empty() {
                continue;
            }
            if let Some(r) = self.remaining.as_mut() {
                *r = r.saturating_sub(entries.len() as u64);
            }
            let mut buf = Vec::with_capacity(entries.len() * 256);
            for e in entries {
                if let Err(err) = serde_json::to_writer(&mut buf, &e) {
                    return Some(Err(std::io::Error::other(err)));
                }
                buf.push(b'\n');
            }
            return Some(Ok(Bytes::from(buf)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(ids: bool, reason: bool, channel: bool, all: Option<bool>) -> DlqActionRequest {
        DlqActionRequest {
            ids: ids.then(|| vec![("orders".to_string(), 1)]),
            reason: reason.then(|| "timeout".to_string()),
            channel: channel.then(|| "orders".to_string()),
            all,
        }
    }

    #[test]
    fn selector_rules() {
        assert!(matches!(
            selector(req(true, false, false, None)),
            Ok(DlqSelector::Ids(_))
        ));
        assert!(matches!(
            selector(req(false, true, false, None)),
            Ok(DlqSelector::Reason { channel: None, .. })
        ));
        assert!(matches!(
            selector(req(false, true, true, None)),
            Ok(DlqSelector::Reason {
                channel: Some(_),
                ..
            })
        ));
        assert!(matches!(
            selector(req(false, false, true, None)),
            Ok(DlqSelector::Channel(_))
        ));
        assert!(matches!(
            selector(req(false, false, false, Some(true))),
            Ok(DlqSelector::All)
        ));

        assert!(selector(req(false, false, false, None)).is_err());
        assert!(selector(req(false, false, false, Some(false))).is_err());
        assert!(selector(req(true, true, false, None)).is_err());
        assert!(selector(req(true, false, true, None)).is_err());
        assert!(selector(req(false, false, true, Some(true))).is_err());
        let empty = DlqActionRequest {
            ids: Some(vec![]),
            ..Default::default()
        };
        assert!(selector(empty).is_err());
    }
}
