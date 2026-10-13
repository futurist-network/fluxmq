//! SSRF guard: webhook URLs must not reach loopback, link-local, private, shared (CGNAT),
//! unspecified, broadcast or multicast addresses unless private targets are allowed.
//!
//! Checked twice: when a subscription is saved (the host is resolved) and on every request —
//! the HTTP client resolves names through [`GuardedResolver`], which drops blocked addresses
//! (defeats DNS rebinding), and IP-literal hosts are checked before sending. Redirects are not
//! followed and no proxy is used, so the checked address is the one connected to.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Whether private targets are currently allowed. `Fixed` from `--webhooks-allow-private`;
/// `Dynamic` (flag unset) asks a callback — the server passes "auth is in open mode", so dev
/// setups can target localhost while enforced deployments are protected.
#[derive(Clone)]
pub enum PrivatePolicy {
    Fixed(bool),
    Dynamic(Arc<dyn Fn() -> bool + Send + Sync>),
}

impl PrivatePolicy {
    pub fn allowed(&self) -> bool {
        match self {
            PrivatePolicy::Fixed(b) => *b,
            PrivatePolicy::Dynamic(f) => f(),
        }
    }
}

impl std::fmt::Debug for PrivatePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrivatePolicy::Fixed(b) => write!(f, "Fixed({b})"),
            PrivatePolicy::Dynamic(_) => write!(f, "Dynamic"),
        }
    }
}

/// True for addresses a webhook must not reach by default.
pub fn is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => blocked_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return blocked_v4(v4);
            }
            let seg0 = v6.segments()[0];
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg0 & 0xffc0) == 0xfe80 // link-local fe80::/10
                || (seg0 & 0xfe00) == 0xfc00 // unique local fc00::/7
                || (seg0 == 0x0064 && v6.segments()[1] == 0xff9b) // NAT64 64:ff9b::/96
                    && blocked_v4(Ipv4Addr::from(
                        (u32::from(v6.segments()[6]) << 16) | u32::from(v6.segments()[7]),
                    ))
        }
    }
}

fn blocked_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || ip.is_unspecified()
        || o[0] == 0 // "this network" 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // shared / CGNAT 100.64.0.0/10
        || o[0] >= 240 // reserved
}

/// Resolves `host:port` (bounded by a timeout) and rejects it when any address is blocked and
/// private targets are not allowed. Used when a subscription is saved and by `test`.
pub async fn check_host(host: &str, port: u16, policy: &PrivatePolicy) -> Result<(), String> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let addrs: Vec<IpAddr> = if let Ok(ip) = host.parse::<IpAddr>() {
        vec![ip]
    } else {
        let lookup = tokio::net::lookup_host((host, port));
        match tokio::time::timeout(Duration::from_secs(5), lookup).await {
            Ok(Ok(it)) => it.map(|a| a.ip()).collect(),
            Ok(Err(e)) => return Err(format!("cannot resolve host '{host}': {e}")),
            Err(_) => return Err(format!("cannot resolve host '{host}': timeout")),
        }
    };
    if addrs.is_empty() {
        return Err(format!("host '{host}' has no addresses"));
    }
    if !policy.allowed()
        && let Some(ip) = addrs.iter().find(|ip| is_blocked(**ip))
    {
        return Err(format!(
            "url resolves to a private/loopback/link-local address ({ip}); start the broker \
             with --webhooks-allow-private to allow it"
        ));
    }
    Ok(())
}

/// Same rule for an IP-literal host at request time (the resolver doesn't see those).
pub fn check_literal(host: &str, policy: &PrivatePolicy) -> Result<(), String> {
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<IpAddr>() {
        Ok(ip) if is_blocked(ip) && !policy.allowed() => Err(format!(
            "blocked address {ip} (private targets are not allowed)"
        )),
        _ => Ok(()),
    }
}

/// DNS resolver for the delivery client: system lookup, blocked addresses removed.
pub struct GuardedResolver {
    pub policy: PrivatePolicy,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = self.policy.clone();
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let allow = policy.allowed();
            let ok: Vec<SocketAddr> = addrs
                .into_iter()
                .filter(|a| allow || !is_blocked(a.ip()))
                .collect();
            if ok.is_empty() {
                return Err(format!(
                    "'{host}' resolves only to blocked addresses (private targets are not allowed)"
                )
                .into());
            }
            let it: Addrs = Box::new(ok.into_iter());
            Ok(it)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_ranges() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "100.64.0.1",
            "224.0.0.1",
            "255.255.255.255",
            "::1",
            "::",
            "fe80::1",
            "fd00::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.1",
        ] {
            assert!(is_blocked(ip.parse().unwrap()), "{ip}");
        }
        for ip in ["8.8.8.8", "1.1.1.1", "2606:4700::1111", "172.32.0.1"] {
            assert!(!is_blocked(ip.parse().unwrap()), "{ip}");
        }
    }

    #[tokio::test]
    async fn check_host_policy() {
        let deny = PrivatePolicy::Fixed(false);
        let allow = PrivatePolicy::Fixed(true);
        assert!(check_host("127.0.0.1", 80, &deny).await.is_err());
        assert!(check_host("localhost", 80, &deny).await.is_err());
        assert!(check_host("[::1]", 80, &deny).await.is_err());
        assert!(check_host("localhost", 80, &allow).await.is_ok());
        assert!(check_literal("10.0.0.1", &deny).is_err());
        assert!(check_literal("10.0.0.1", &allow).is_ok());
        assert!(check_literal("example.com", &deny).is_ok());
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let f = flag.clone();
        let dynamic = PrivatePolicy::Dynamic(Arc::new(move || {
            f.load(std::sync::atomic::Ordering::Relaxed)
        }));
        assert!(check_literal("127.0.0.1", &dynamic).is_ok());
        flag.store(false, std::sync::atomic::Ordering::Relaxed);
        assert!(check_literal("127.0.0.1", &dynamic).is_err());
    }
}
