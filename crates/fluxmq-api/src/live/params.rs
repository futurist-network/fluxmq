//! Subscription parameters (query string of both endpoints, or the WS `subscribe` op) and
//! the server-side filter.

use std::collections::{BTreeMap, HashSet};

use fluxmq_proto::Message;
use fluxmq_proto::routing::Condition;
use serde::Deserialize;

/// Max keys in `key=`.
pub const MAX_KEYS: usize = 64;
/// Max `header.<name>=` conditions.
pub const MAX_HEADERS: usize = 32;
/// Max length of `filter=`.
pub const MAX_FILTER_LEN: usize = 4096;
/// Max partitions named in `partitions=` / `from=offsets:`.
pub const MAX_PARTITIONS: usize = 4096;

/// Where a subscription starts (per partition) when no cursor names the partition.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StartFrom {
    #[default]
    Latest,
    Earliest,
    /// First message with timestamp >= this (unix ms).
    Time(u64),
    /// Explicit offsets; unlisted partitions start at `latest`.
    Offsets(BTreeMap<u32, u64>),
}

impl StartFrom {
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        match s {
            "" | "latest" => Ok(Self::Latest),
            "earliest" => Ok(Self::Earliest),
            _ => {
                if let Some(t) = s.strip_prefix("time:") {
                    return t
                        .trim()
                        .parse()
                        .map(Self::Time)
                        .map_err(|_| format!("from=time:<unix ms>: bad timestamp '{t}'"));
                }
                if let Some(list) = s.strip_prefix("offsets:") {
                    let mut out = BTreeMap::new();
                    for item in list.split(',').filter(|i| !i.trim().is_empty()) {
                        let (p, o) = item
                            .split_once('=')
                            .or_else(|| item.split_once(':'))
                            .ok_or_else(|| format!("from=offsets: bad item '{item}' (p=offset)"))?;
                        let p: u32 = p
                            .trim()
                            .parse()
                            .map_err(|_| format!("from=offsets: bad partition '{p}'"))?;
                        let o: u64 = o
                            .trim()
                            .parse()
                            .map_err(|_| format!("from=offsets: bad offset '{o}'"))?;
                        out.insert(p, o);
                        if out.len() > MAX_PARTITIONS {
                            return Err("from=offsets: too many partitions".into());
                        }
                    }
                    if out.is_empty() {
                        return Err("from=offsets: needs at least one p=offset".into());
                    }
                    return Ok(Self::Offsets(out));
                }
                Err(format!(
                    "from must be latest, earliest, time:<unix ms> or offsets:<p>=<offset>,…; got '{s}'"
                ))
            }
        }
    }
}

/// Server-side filter: exact keys (any of), header equalities (all), filter-language
/// conditions (all). Empty = everything.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    pub keys: Option<HashSet<String>>,
    pub headers: Vec<(String, String)>,
    pub conditions: Option<Vec<Condition>>,
}

impl Filter {
    pub fn is_empty(&self) -> bool {
        self.keys.is_none() && self.headers.is_empty() && self.conditions.is_none()
    }

    pub fn matches(&self, m: &Message) -> bool {
        if let Some(keys) = &self.keys
            && !m.key.as_deref().is_some_and(|k| keys.contains(k))
        {
            return false;
        }
        for (name, want) in &self.headers {
            let got = m.headers.get(name).or_else(|| {
                m.headers
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .map(|(_, v)| v)
            });
            if got != Some(want) {
                return false;
            }
        }
        match &self.conditions {
            Some(c) => fluxmq_router::filter::matches(c, m),
            None => true,
        }
    }
}

/// A validated subscription request (channel resolved by the caller).
#[derive(Debug, Clone, Default)]
pub struct SubSpec {
    pub from: StartFrom,
    /// Raw cursor (decoded against the channel later).
    pub cursor: Option<String>,
    /// Restrict to these partitions (sorted, deduplicated).
    pub partitions: Option<Vec<u32>>,
    pub filter: Filter,
}

/// Bad request text.
pub type ParamError = String;

fn add_keys(keys: &mut HashSet<String>, v: &str) -> Result<(), ParamError> {
    for k in v.split(',').filter(|k| !k.is_empty()) {
        if k.len() > 1024 {
            return Err("key too long (max 1024 bytes)".into());
        }
        keys.insert(k.to_string());
        if keys.len() > MAX_KEYS {
            return Err(format!("at most {MAX_KEYS} keys"));
        }
    }
    Ok(())
}

fn parse_partitions(v: &str) -> Result<Vec<u32>, ParamError> {
    let mut out = Vec::new();
    for p in v.split(',').filter(|p| !p.trim().is_empty()) {
        out.push(
            p.trim()
                .parse::<u32>()
                .map_err(|_| format!("bad partition '{p}'"))?,
        );
        if out.len() > MAX_PARTITIONS {
            return Err("too many partitions".into());
        }
    }
    out.sort_unstable();
    out.dedup();
    if out.is_empty() {
        return Err("partitions= needs at least one partition".into());
    }
    Ok(out)
}

fn parse_filter(f: &str) -> Result<Option<Vec<Condition>>, ParamError> {
    let f = f.trim();
    if f.is_empty() {
        return Ok(None);
    }
    if f.len() > MAX_FILTER_LEN {
        return Err(format!("filter too long (max {MAX_FILTER_LEN} bytes)"));
    }
    fluxmq_router::filter::parse(f)
        .map(Some)
        .map_err(|e| e.to_string())
}

fn add_header(headers: &mut Vec<(String, String)>, name: &str, v: &str) -> Result<(), ParamError> {
    if name.is_empty() || name.len() > 256 {
        return Err("header.<name>: name must be 1-256 bytes".into());
    }
    headers.push((name.to_string(), v.to_string()));
    if headers.len() > MAX_HEADERS {
        return Err(format!("at most {MAX_HEADERS} header conditions"));
    }
    Ok(())
}

/// Parses query pairs. Returns the spec and the SSE `access_token`, if given. Unknown
/// parameters are ignored (cache busters etc.).
pub fn from_query(
    pairs: impl IntoIterator<Item = (String, String)>,
) -> Result<(SubSpec, Option<String>), ParamError> {
    let mut spec = SubSpec::default();
    let mut keys = HashSet::new();
    let mut token = None;
    for (k, v) in pairs {
        match k.as_str() {
            "from" => spec.from = StartFrom::parse(&v)?,
            "cursor" => spec.cursor = Some(v).filter(|c| !c.trim().is_empty()),
            "partitions" => spec.partitions = Some(parse_partitions(&v)?),
            "key" => add_keys(&mut keys, &v)?,
            "filter" => spec.filter.conditions = parse_filter(&v)?,
            "access_token" => token = Some(v).filter(|t| !t.is_empty()),
            _ => {
                if let Some(name) = k.strip_prefix("header.") {
                    add_header(&mut spec.filter.headers, name, &v)?;
                }
            }
        }
    }
    if !keys.is_empty() {
        spec.filter.keys = Some(keys);
    }
    Ok((spec, token))
}

/// `key` in a WS op: one string or a list.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

/// Client → server WebSocket commands.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ClientOp {
    Subscribe(SubscribeOp),
    Unsubscribe { id: String },
    Ping,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SubscribeOp {
    pub id: String,
    /// Defaults to the channel of the URL.
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub partitions: Option<Vec<u32>>,
    #[serde(default)]
    pub key: Option<OneOrMany>,
    #[serde(default)]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub filter: Option<String>,
}

impl SubscribeOp {
    pub fn into_spec(self) -> Result<SubSpec, ParamError> {
        let mut spec = SubSpec {
            from: StartFrom::parse(self.from.as_deref().unwrap_or(""))?,
            cursor: self.cursor.filter(|c| !c.trim().is_empty()),
            ..Default::default()
        };
        if let Some(mut p) = self.partitions {
            p.sort_unstable();
            p.dedup();
            if p.is_empty() || p.len() > MAX_PARTITIONS {
                return Err("partitions must list 1..4096 partitions".into());
            }
            spec.partitions = Some(p);
        }
        let mut keys = HashSet::new();
        match self.key {
            None => {}
            Some(OneOrMany::One(k)) => add_keys(&mut keys, &k)?,
            Some(OneOrMany::Many(ks)) => {
                for k in ks {
                    if k.len() > 1024 {
                        return Err("key too long (max 1024 bytes)".into());
                    }
                    keys.insert(k);
                }
                if keys.len() > MAX_KEYS {
                    return Err(format!("at most {MAX_KEYS} keys"));
                }
            }
        }
        if !keys.is_empty() {
            spec.filter.keys = Some(keys);
        }
        for (name, v) in self.headers.unwrap_or_default() {
            add_header(&mut spec.filter.headers, &name, &v)?;
        }
        if let Some(f) = self.filter {
            spec.filter.conditions = parse_filter(&f)?;
        }
        Ok(spec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(s: &[(&str, &str)]) -> Result<(SubSpec, Option<String>), ParamError> {
        from_query(s.iter().map(|(k, v)| (k.to_string(), v.to_string())))
    }

    fn msg(key: Option<&str>, headers: &[(&str, &str)], payload: &str) -> Message {
        Message {
            partition: 0,
            offset: 0,
            key: key.map(str::to_string),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            timestamp_ms: 0,
            payload: payload.to_string().into(),
        }
    }

    #[test]
    fn start_from() {
        assert_eq!(StartFrom::parse("").unwrap(), StartFrom::Latest);
        assert_eq!(StartFrom::parse("earliest").unwrap(), StartFrom::Earliest);
        assert_eq!(StartFrom::parse("time:17").unwrap(), StartFrom::Time(17));
        assert_eq!(
            StartFrom::parse("offsets:0=5,2=7").unwrap(),
            StartFrom::Offsets([(0, 5), (2, 7)].into_iter().collect())
        );
        assert!(StartFrom::parse("offsets:").is_err());
        assert!(StartFrom::parse("yesterday").is_err());
        assert!(StartFrom::parse("time:x").is_err());
    }

    #[test]
    fn query_and_filter() {
        let (s, tok) = q(&[
            ("key", "a,b"),
            ("key", "c"),
            ("header.type", "x"),
            ("filter", "amount >= 10"),
            ("partitions", "2,0,2"),
            ("access_token", "t"),
            ("_", "123"),
        ])
        .unwrap();
        assert_eq!(tok.as_deref(), Some("t"));
        assert_eq!(s.partitions, Some(vec![0, 2]));
        let f = &s.filter;
        assert!(f.matches(&msg(Some("a"), &[("type", "x")], r#"{"amount":10}"#)));
        assert!(!f.matches(&msg(Some("z"), &[("type", "x")], r#"{"amount":10}"#)));
        assert!(!f.matches(&msg(Some("c"), &[("type", "y")], r#"{"amount":10}"#)));
        assert!(f.matches(&msg(Some("c"), &[("Type", "x")], r#"{"amount":11}"#)));
        assert!(!f.matches(&msg(Some("c"), &[("type", "x")], r#"{"amount":9}"#)));
        assert!(!f.matches(&msg(None, &[("type", "x")], r#"{"amount":19}"#)));
        assert!(q(&[("filter", "a = 1 OR b = 2")]).is_err());
        assert!(q(&[("partitions", "x")]).is_err());
        assert!(Filter::default().is_empty());
    }

    #[test]
    fn ws_ops() {
        let op: ClientOp = serde_json::from_str(
            r#"{"op":"subscribe","id":"s1","from":"earliest","key":["a","b"],"headers":{"t":"x"}}"#,
        )
        .unwrap();
        let ClientOp::Subscribe(op) = op else {
            panic!("subscribe")
        };
        let spec = op.into_spec().unwrap();
        assert_eq!(spec.from, StartFrom::Earliest);
        assert_eq!(spec.filter.keys.as_ref().map(|k| k.len()), Some(2));
        assert!(matches!(
            serde_json::from_str::<ClientOp>(r#"{"op":"unsubscribe","id":"s1"}"#).unwrap(),
            ClientOp::Unsubscribe { .. }
        ));
        assert!(serde_json::from_str::<ClientOp>(r#"{"op":"subscribe"}"#).is_err());
    }
}
