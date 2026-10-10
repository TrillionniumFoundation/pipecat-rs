// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GladiaError, GladiaSttConfig};
use std::net::{IpAddr, SocketAddr};
use url::{Host, Url};
fn parse(value: &str) -> Result<Url, GladiaError> {
    if value.len() > 8192 {
        return Err(GladiaError::Endpoint);
    }
    let url = Url::parse(value).map_err(|_| GladiaError::Endpoint)?;
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(GladiaError::Endpoint);
    }
    Ok(url)
}
fn literal(url: &Url) -> Option<IpAddr> {
    match url.host()? {
        Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
        Host::Ipv6(ip) => Some(IpAddr::V6(ip)),
        _ => None,
    }
}
fn scheme(url: &Url, secure: &str, plain: &str, allow: bool) -> Result<(), GladiaError> {
    if url.scheme() == secure {
        if let Some(ip) = literal(url) {
            if !public(ip) && !(allow && ip.is_loopback()) {
                return Err(GladiaError::Endpoint);
            }
        }
        return Ok(());
    }
    if url.scheme() == plain && allow && literal(url).is_some_and(|ip| ip.is_loopback()) {
        Ok(())
    } else {
        Err(GladiaError::Endpoint)
    }
}
pub(crate) fn init_url(config: &GladiaSttConfig) -> Result<Url, GladiaError> {
    let mut url = parse(&config.endpoint)?;
    scheme(&url, "https", "http", config.allow_loopback)?;
    if url.query().is_some() {
        return Err(GladiaError::Endpoint);
    }
    if let Some(region) = config.region {
        url.query_pairs_mut().append_pair("region", region.wire());
    }
    Ok(url)
}
pub(crate) fn validate_origins(config: &GladiaSttConfig) -> Result<(), GladiaError> {
    if config.allowed_session_origins.is_empty() || config.allowed_session_origins.len() > 16 {
        return Err(GladiaError::Endpoint);
    }
    for origin in &config.allowed_session_origins {
        let url = parse(origin)?;
        scheme(&url, "wss", "ws", config.allow_loopback)?;
        if url.path() != "/" || url.query().is_some() {
            return Err(GladiaError::Endpoint);
        }
    }
    Ok(())
}
pub(crate) fn session_url(value: &str, config: &GladiaSttConfig) -> Result<Url, GladiaError> {
    let url = parse(value)?;
    scheme(&url, "wss", "ws", config.allow_loopback)?;
    if !config
        .allowed_session_origins
        .iter()
        .any(|v| Url::parse(v).is_ok_and(|a| a.origin() == url.origin()))
    {
        return Err(GladiaError::Endpoint);
    }
    Ok(url)
}
/// Resolve once, reject mixed private/public answers, and connect to these exact
/// addresses. The TLS name remains the validated URL host. No proxy is used.
pub(crate) async fn addresses(
    url: &Url,
    allow_loopback: bool,
) -> Result<Vec<SocketAddr>, GladiaError> {
    let port = url.port_or_known_default().ok_or(GladiaError::Endpoint)?;
    if let Some(ip) = literal(url) {
        if public(ip) || (allow_loopback && ip.is_loopback()) {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        return Err(GladiaError::Endpoint);
    }
    let host = url.host_str().ok_or(GladiaError::Endpoint)?;
    let answers = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| GladiaError::Transport)?;
    let mut out = Vec::new();
    for address in answers {
        if !public(address.ip()) || out.len() >= 32 {
            return Err(GladiaError::Endpoint);
        }
        if !out.contains(&address) {
            out.push(address)
        }
    }
    if out.is_empty() {
        return Err(GladiaError::Endpoint);
    }
    Ok(out)
}
fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let a = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_documentation()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && a[0] != 0
                && a[0] < 224
                && !(a[0] == 100 && (64..=127).contains(&a[1]))
                && !(a[0] == 198 && (a[1] == 18 || a[1] == 19))
                && !(a[0] == 192 && a[1] == 0 && a[2] == 0)
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && (s[0] & 0xfe00) != 0xfc00
                && (s[0] & 0xffc0) != 0xfe80
                && !(s[0] == 0x2001 && s[1] == 0xdb8)
                && !(s[0] == 0x2001 && s[1] < 0x0200)
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && (s[1] & 0xf000) == 0)
                && ip.to_ipv4_mapped().is_none()
                && (s[0] & 0xe000) == 0x2000
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malicious_returned_urls_and_private_addresses() {
        let c = GladiaSttConfig::new("synthetic");
        for value in [
            "ws://api.gladia.io/x?token=secret",
            "wss://api.gladia.io.evil.test/x",
            "wss://evil@api.gladia.io/x",
            "wss://api.gladia.io:444/x",
            "wss://127.0.0.1/x",
            "wss://api.gladia.io/x#secret",
            "file:///tmp/secret",
        ] {
            assert!(session_url(value, &c).is_err(), "{value}");
        }
        assert!(session_url("wss://api.gladia.io/v2/live?token=secret", &c).is_ok());
        for ip in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.169.254",
            "100.64.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "fd00::1",
            "fe80::1",
            "2001:db8::1",
            "2001:2::1",
            "2002:7f00:1::1",
            "3fff::1",
        ] {
            assert!(!public(ip.parse().unwrap()))
        }
    }
    #[test]
    fn ordinary_global_addresses_are_accepted() {
        for ip in [
            "1.1.1.1",
            "8.8.8.8",
            "2606:4700:4700::1111",
            "2001:4860:4860::8888",
        ] {
            assert!(public(ip.parse().unwrap()));
        }
    }
    #[test]
    fn loopback_is_explicit_and_literal() {
        let mut c = GladiaSttConfig::new("synthetic");
        c.endpoint = "http://127.0.0.1:1234/v2/live".into();
        assert!(init_url(&c).is_err());
        c.allow_loopback = true;
        assert!(init_url(&c).is_ok());
        c.endpoint = "http://localhost:1234/v2/live".into();
        assert!(init_url(&c).is_err());
    }
}
