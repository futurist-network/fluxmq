//! `GET /api/history` and `GET /api/alerts`.

use axum::extract::State;
use fluxmq_proto::telemetry::{AlertsState, HistoryRange, HistorySeries, metrics};
use serde::Deserialize;

use crate::error::{ApiErr, ApiResult};
use crate::extract::Query;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct HistoryQuery {
    pub metric: String,
    pub range: String,
}

/// Parses `5m`/`15m`/`1h`/`24h`/`7d`.
pub fn parse_range(range: &str) -> ApiResult<HistoryRange> {
    serde_json::from_value(serde_json::Value::String(range.to_string())).map_err(|_| {
        ApiErr::bad_request(format!(
            "invalid range '{range}' (expected 5m, 15m, 1h, 24h or 7d)"
        ))
    })
}

/// Metric ids: one of [`metrics::ALL`], or `channel.<name>.<metric>`. Returns the channel
/// name for channel metrics.
pub fn parse_metric(metric: &str) -> ApiResult<Option<&str>> {
    if metrics::ALL.contains(&metric) {
        return Ok(None);
    }
    let invalid = || {
        ApiErr::bad_request(format!(
            "invalid metric '{metric}' (expected one of {} or channel.<name>.<metric>)",
            metrics::ALL.join(", ")
        ))
    };
    let rest = metric.strip_prefix("channel.").ok_or_else(invalid)?;
    let (name, m) = rest.rsplit_once('.').ok_or_else(invalid)?;
    if name.is_empty() || !metrics::ALL.contains(&m) {
        return Err(invalid());
    }
    Ok(Some(name))
}

pub async fn history(
    State(s): State<AppState>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<axum::Json<HistorySeries>> {
    let range = parse_range(&q.range)?;
    if let Some(channel) = parse_metric(&q.metric)?
        && s.broker.channel(channel).is_none()
    {
        return Err(ApiErr::not_found(format!("channel '{channel}' not found")));
    }
    s.telemetry
        .history(&q.metric, range)
        .map(axum::Json)
        .map_err(ApiErr::bad_request)
}

pub async fn alerts(State(s): State<AppState>) -> axum::Json<AlertsState> {
    axum::Json(s.telemetry.alerts())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        assert_eq!(parse_range("5m").unwrap(), HistoryRange::M5);
        assert_eq!(parse_range("7d").unwrap(), HistoryRange::D7);
        assert!(parse_range("2h").is_err());
        assert!(parse_range("").is_err());
    }

    #[test]
    fn metric_ids() {
        assert_eq!(parse_metric("ingress_rate").unwrap(), None);
        assert_eq!(parse_metric("channel.orders.lag").unwrap(), Some("orders"));
        assert_eq!(
            parse_metric("channel.orders.v2.publish_p99").unwrap(),
            Some("orders.v2")
        );
        assert!(parse_metric("bogus").is_err());
        assert!(parse_metric("channel.orders.bogus").is_err());
        assert!(parse_metric("channel..lag").is_err());
        assert!(parse_metric("channel.lag").is_err());
    }
}
