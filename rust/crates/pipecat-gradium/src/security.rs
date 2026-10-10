// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GradiumError, GradiumSttConfig};
use std::net::{IpAddr, SocketAddr};
use url::{Host, Url};
fn parse(value: &str) -> Result<Url, GradiumError> {
    if value.len() > 8192 {
        return Err(GradiumError::Endpoint);
    }
    let url = Url::parse(value).map_err(|_| GradiumError::Endpoint)?;
    if url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(GradiumError::Endpoint);
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
fn scheme(url: &Url, secure: &str, plain: &str, allow: bool) -> Result<(), GradiumError> {
    if url.scheme() == secure {
        if let Some(ip) = literal(url) {
            if !public(ip) && !(allow && ip.is_loopback()) {
                return Err(GradiumError::Endpoint);
            }
        }
        return Ok(());
    }
    if url.scheme() == plain && allow && literal(url).is_some_and(|ip| ip.is_loopback()) {
        Ok(())
    } else {
        Err(GradiumError::Endpoint)
    }
}
pub(crate) fn endpoint(config: &GradiumSttConfig) -> Result<Url, GradiumError> {
    endpoint_url(&config.endpoint, config.allow_loopback)
}
pub(crate) fn endpoint_url(value: &str, allow_loopback: bool) -> Result<Url, GradiumError> {
    let url = parse(value)?;
    scheme(&url, "wss", "ws", allow_loopback)?;
    if url.query().is_some() {
        return Err(GradiumError::Endpoint);
    }
    Ok(url)
}
/// Resolve once, reject mixed private/public answers, and connect to these exact
/// addresses. The TLS name remains the validated URL host. No proxy is used.
pub(crate) async fn addresses(
    url: &Url,
    allow_loopback: bool,
) -> Result<Vec<SocketAddr>, GradiumError> {
    let port = url.port_or_known_default().ok_or(GradiumError::Endpoint)?;
    if let Some(ip) = literal(url) {
        if public(ip) || (allow_loopback && ip.is_loopback()) {
            return Ok(vec![SocketAddr::new(ip, port)]);
        }
        return Err(GradiumError::Endpoint);
    }
    let host = url.host_str().ok_or(GradiumError::Endpoint)?;
    let answers = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| GradiumError::Transport)?;
    let mut out = Vec::new();
    for address in answers {
        if !public(address.ip()) || out.len() >= 32 {
            return Err(GradiumError::Endpoint);
        }
        if !out.contains(&address) {
            out.push(address)
        }
    }
    if out.is_empty() {
        return Err(GradiumError::Endpoint);
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
