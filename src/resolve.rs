use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::Url;

/// One `--resolve HOST:PORT::TARGET[:TARGET_PORT]` rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolveRule {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) target_host: String,
    pub(crate) target_port: u16,
}

pub(crate) fn parse_resolve_rule(raw: &str) -> Result<ResolveRule> {
    let err = || anyhow!("invalid --resolve {raw:?}: expected HOST:PORT::TARGET[:TARGET_PORT]");
    let (host, rest) = split_host(raw).ok_or_else(err)?;
    let (port, target) = rest
        .strip_prefix(':')
        .and_then(|rest| rest.split_once("::"))
        .ok_or_else(err)?;
    let port = parse_port(port).ok_or_else(err)?;
    let (target_host, target_rest) = split_host(target).ok_or_else(err)?;
    let target_port = match target_rest {
        "" => port,
        rest => rest
            .strip_prefix(':')
            .and_then(parse_port)
            .ok_or_else(err)?,
    };
    if host.is_empty() || target_host.is_empty() {
        return Err(err());
    }
    Ok(ResolveRule {
        host: host.to_ascii_lowercase(),
        port,
        target_host: target_host.to_string(),
        target_port,
    })
}

/// Splits a leading host (bracketed for IPv6) from the rest of the string.
fn split_host(value: &str) -> Option<(&str, &str)> {
    if let Some(inner) = value.strip_prefix('[') {
        let end = inner.find(']')?;
        Some((&inner[..end], &inner[end + 1..]))
    } else {
        let end = value.find(':').unwrap_or(value.len());
        Some((&value[..end], &value[end..]))
    }
}

fn parse_port(value: &str) -> Option<u16> {
    value.parse().ok().filter(|port| *port != 0)
}

/// DNS resolver that redirects connections for overridden hosts and falls back to the system
/// resolver for everything else.
///
/// reqwest only passes the host name to resolvers, so rules are keyed by host. The port of a
/// resolved address is honored when the URL uses its scheme's default port; for URLs with an
/// explicit port the connection always uses that port, which `check_url` enforces up front.
#[derive(Debug, Clone, Default)]
pub(crate) struct ResolveOverrides {
    rules: Arc<HashMap<String, ResolveRule>>,
}

impl ResolveOverrides {
    pub(crate) fn from_args(raw: &[String]) -> Result<Self> {
        let mut rules: HashMap<String, ResolveRule> = HashMap::new();
        for raw in raw {
            let rule = parse_resolve_rule(raw)?;
            if let Some(existing) = rules.get(&rule.host) {
                if existing != &rule {
                    bail!(
                        "conflicting --resolve rules for host {:?}: only one rule per host is supported",
                        rule.host
                    );
                }
                continue;
            }
            rules.insert(rule.host.clone(), rule);
        }
        Ok(Self {
            rules: Arc::new(rules),
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// Rejects URLs whose connection target cannot be expressed through DNS overriding.
    pub(crate) fn check_url(&self, raw_url: &str) -> Result<()> {
        let Ok(url) = Url::parse(raw_url) else {
            return Ok(());
        };
        let Some(host) = url.host_str() else {
            return Ok(());
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let Some(rule) = self.rules.get(&host.to_ascii_lowercase()) else {
            return Ok(());
        };
        let Some(port) = url.port_or_known_default() else {
            return Ok(());
        };
        if port != rule.port {
            bail!(
                "--resolve rule for {}:{} does not match {raw_url} (port {port}); rules apply per host, so add a rule for port {port} instead",
                rule.host,
                rule.port
            );
        }
        if url.port().is_some() && rule.target_port != port {
            bail!(
                "--resolve cannot change the port of {raw_url}, which names port {port} explicitly; use the scheme's default port in the URL or keep the target port at {port}"
            );
        }
        Ok(())
    }
}

impl Resolve for ResolveOverrides {
    fn resolve(&self, name: Name) -> Resolving {
        let rule = self.rules.get(&name.as_str().to_ascii_lowercase()).cloned();
        let name = name.as_str().to_string();
        Box::pin(async move {
            let (host, port) = match &rule {
                Some(rule) => (rule.target_host.clone(), rule.target_port),
                None => (name, 0),
            };
            let addrs: Vec<SocketAddr> = match host.parse::<IpAddr>() {
                Ok(ip) => vec![SocketAddr::new(ip, port)],
                Err(_) => tokio::net::lookup_host((host.as_str(), port))
                    .await
                    .with_context(|| format!("failed to resolve {host}"))?
                    .collect(),
            };
            if addrs.is_empty() {
                return Err(anyhow!("no addresses found for {host}").into());
            }
            let addrs: Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(host: &str, port: u16, target_host: &str, target_port: u16) -> ResolveRule {
        ResolveRule {
            host: host.into(),
            port,
            target_host: target_host.into(),
            target_port,
        }
    }

    #[test]
    fn parses_resolve_rules() {
        assert_eq!(
            parse_resolve_rule("www.google.com:443::127.0.0.1:1443").unwrap(),
            rule("www.google.com", 443, "127.0.0.1", 1443)
        );
        assert_eq!(
            parse_resolve_rule("WWW.Google.com:443::www.yahoo.com.sg:15423").unwrap(),
            rule("www.google.com", 443, "www.yahoo.com.sg", 15423)
        );
        assert_eq!(
            parse_resolve_rule("example.com:80::10.0.0.1").unwrap(),
            rule("example.com", 80, "10.0.0.1", 80)
        );
        assert_eq!(
            parse_resolve_rule("[::1]:443::[fe80::1]:8443").unwrap(),
            rule("::1", 443, "fe80::1", 8443)
        );
    }

    #[test]
    fn rejects_malformed_resolve_rules() {
        for raw in [
            "example.com",
            "example.com:443",
            "example.com:443:127.0.0.1",
            "example.com::127.0.0.1",
            "example.com:0::127.0.0.1",
            "example.com:443::",
            "example.com:443::127.0.0.1:x",
            ":443::127.0.0.1",
        ] {
            assert!(parse_resolve_rule(raw).is_err(), "{raw} should be rejected");
        }
    }

    #[test]
    fn checks_urls_against_rules() {
        let overrides =
            ResolveOverrides::from_args(&["example.com:443::127.0.0.1:1443".into()]).unwrap();
        overrides.check_url("https://example.com/file").unwrap();
        overrides.check_url("https://example.com:443/file").unwrap();
        overrides.check_url("https://other.com:8443/file").unwrap();
        assert!(overrides.check_url("http://example.com/file").is_err());

        let same_port =
            ResolveOverrides::from_args(&["example.com:8443::127.0.0.1:8443".into()]).unwrap();
        same_port
            .check_url("https://example.com:8443/file")
            .unwrap();
        let moved_port =
            ResolveOverrides::from_args(&["example.com:8443::127.0.0.1:9443".into()]).unwrap();
        assert!(
            moved_port
                .check_url("https://example.com:8443/file")
                .is_err()
        );
    }

    #[test]
    fn rejects_conflicting_rules_for_one_host() {
        assert!(
            ResolveOverrides::from_args(&[
                "example.com:443::127.0.0.1:1443".into(),
                "example.com:80::127.0.0.1:1080".into(),
            ])
            .is_err()
        );
    }
}
