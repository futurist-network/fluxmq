//! `GET /api/search?q=` — command palette backing.

use std::collections::BTreeSet;

use axum::extract::State;
use fluxmq_proto::api::SearchResult;
use serde::Deserialize;

use crate::extract::Query;
use crate::state::AppState;

/// Max results returned.
pub const MAX_RESULTS: usize = 50;

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    #[serde(default)]
    pub q: String,
}

pub async fn search(
    State(s): State<AppState>,
    Query(q): Query<SearchQuery>,
) -> axum::Json<Vec<SearchResult>> {
    let needle = q.q.trim().to_lowercase();
    let mut channels: Vec<String> = s.broker.channels().into_iter().map(|c| c.name).collect();
    channels.sort();
    // (channel, group) pairs; stats() is a snapshot meant for periodic polling, fine per keystroke.
    let groups: BTreeSet<(String, String)> = s
        .broker
        .stats()
        .channels
        .into_iter()
        .flat_map(|c| {
            let name = c.config.name;
            c.groups.into_iter().map(move |g| (name.clone(), g.name))
        })
        .collect();
    axum::Json(matches(&needle, &channels, &groups))
}

/// Case-insensitive substring match; channels first, then groups. Empty needle matches all.
pub fn matches(
    needle: &str,
    channels: &[String],
    groups: &BTreeSet<(String, String)>,
) -> Vec<SearchResult> {
    let hit = |s: &str| needle.is_empty() || s.to_lowercase().contains(needle);
    let channel_hits = channels.iter().filter(|c| hit(c)).map(|c| SearchResult {
        kind: "channel".into(),
        label: c.clone(),
        link: format!("/channels/{c}"),
    });
    let group_hits = groups
        .iter()
        .filter(|(_, g)| hit(g))
        .map(|(c, g)| SearchResult {
            kind: "group".into(),
            label: format!("{g} ({c})"),
            link: format!("/channels/{c}/consumers"),
        });
    channel_hits.chain(group_hits).take(MAX_RESULTS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_case_insensitive() {
        let channels = vec!["Orders".to_string(), "emails".to_string()];
        let groups = BTreeSet::from([
            ("Orders".to_string(), "billing".to_string()),
            ("emails".to_string(), "order-audit".to_string()),
        ]);
        let r = matches("order", &channels, &groups);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].link, "/channels/Orders");
        assert_eq!(r[0].kind, "channel");
        assert_eq!(r[1].link, "/channels/emails/consumers");
        assert_eq!(r[1].kind, "group");
        assert_eq!(matches("", &channels, &groups).len(), 4);
        assert!(matches("zzz", &channels, &groups).is_empty());
    }
}
