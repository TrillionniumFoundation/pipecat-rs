use crate::{ImageError, Result};
use std::net::{IpAddr, SocketAddr};
use url::{Host, Url};

/// Hosted results are deny-by-default. Each entry is an exact HTTPS origin.
/// A separate constructor enables literal loopback HTTP origins for local fixtures.
#[derive(Clone, Default)]
pub struct HostedImagePolicy {
    origins: Vec<Url>,
    loopback: bool,
}
impl std::fmt::Debug for HostedImagePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HostedImagePolicy")
            .field("origin_count", &self.origins.len())
            .field("loopback", &self.loopback)
            .finish()
    }
}
impl HostedImagePolicy {
    pub fn https_origins(origins: &[&str]) -> Result<Self> {
        Self::build(origins, false)
    }
    pub fn loopback_fixtures(origins: &[&str]) -> Result<Self> {
        Self::build(origins, true)
    }
    fn build(origins: &[&str], loopback: bool) -> Result<Self> {
        if origins.len() > 32 {
            return Err(ImageError::Invalid("too many hosted origins"));
        }
        let mut parsed = Vec::new();
        for value in origins {
            let url = parse_url(value)?;
            if url.path() != "/"
                || url.query().is_some()
                || !valid_scheme(&url, loopback)
                || (loopback && !literal_ip(&url).is_some_and(|ip| ip.is_loopback()))
            {
                return Err(ImageError::AddressDenied);
            }
            parsed.push(url);
        }
        Ok(Self {
            origins: parsed,
            loopback,
        })
    }
    pub(crate) fn check(&self, url: &Url) -> Result<()> {
        if !valid_scheme(url, self.loopback)
            || !self
                .origins
                .iter()
                .any(|allowed| allowed.origin() == url.origin())
        {
            return Err(ImageError::AddressDenied);
        }
        Ok(())
    }
    pub(crate) async fn addresses(&self, url: &Url) -> Result<Vec<SocketAddr>> {
        self.check(url)?;
        let port = url
            .port_or_known_default()
            .ok_or(ImageError::AddressDenied)?;
        let addresses = if let Some(ip) = literal_ip(url) {
            vec![SocketAddr::new(ip, port)]
        } else {
            let host = url.host_str().ok_or(ImageError::AddressDenied)?;
            let mut addresses = Vec::new();
            for address in tokio::net::lookup_host((host, port))
                .await
                .map_err(|_| ImageError::Transport)?
            {
                if addresses.len() >= 16 {
                    return Err(ImageError::AddressDenied);
                }
                if !addresses.contains(&address) {
                    addresses.push(address);
                }
            }
            addresses
        };
        if addresses.is_empty()
            || addresses.iter().any(|a| {
                !public_address(a.ip())
                    && !(self.loopback && a.ip().is_loopback() && literal_ip(url).is_some())
            })
        {
            return Err(ImageError::AddressDenied);
        }
        Ok(addresses)
    }
}
pub(crate) fn parse_url(value: &str) -> Result<Url> {
    if value.len() > 8192 {
        return Err(ImageError::TooLarge);
    }
    let url = Url::parse(value).map_err(|_| ImageError::AddressDenied)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
    {
        return Err(ImageError::AddressDenied);
    }
    Ok(url)
}
pub(crate) fn literal_ip(url: &Url) -> Option<IpAddr> {
    match url.host()? {
        Host::Ipv4(ip) => Some(ip.into()),
        Host::Ipv6(ip) => Some(ip.into()),
        Host::Domain(_) => None,
    }
}
pub(crate) fn valid_scheme(url: &Url, loopback: bool) -> bool {
    url.scheme() == "https"
        || (loopback
            && url.scheme() == "http"
            && literal_ip(url).is_some_and(|ip| ip.is_loopback()))
}
/// Conservative public-unicast allowlist excludes special, metadata, documentation,
/// transition, mapped and reserved blocks, including IPv6 translation mechanisms.
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 0 || b == 168 || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(ip) => {
            let s = ip.segments();
            (s[0] & 0xe000) == 0x2000
                && !(s[0] == 0x2001 && s[1] < 0x200)
                && !(s[0] == 0x2001 && s[1] == 0xdb8)
                && s[0] != 0x2002
                && !(s[0] == 0x3fff && (s[1] & 0xf000) == 0)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deny_special_addresses() {
        for address in [
            "0.0.0.0",
            "127.1.2.3",
            "10.0.0.1",
            "100.100.100.200",
            "169.254.169.254",
            "172.16.0.1",
            "192.168.1.1",
            "192.0.0.1",
            "198.18.0.1",
            "224.0.0.1",
            "::1",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "fc00::1",
            "fe80::1",
            "2002:0808:0808::1",
            "2001:db8::1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
        assert!(public_address("8.8.8.8".parse().unwrap()));
        assert!(public_address("2606:4700:4700::1111".parse().unwrap()));
    }
}

#[cfg(test)]
mod independent_review {
    use super::*;
    #[test]
    fn exact_origin_canonicalization_cannot_be_confused_with_suffix_or_port() {
        let policy = HostedImagePolicy::https_origins(&["https://IMAGES.example:443"]).unwrap();
        assert!(policy
            .check(&parse_url("https://images.example/p?signature=private").unwrap())
            .is_ok());
        for url in [
            "https://images.example.evil/p",
            "https://evilimages.example/p",
            "https://images.example:444/p",
            "http://images.example/p",
        ] {
            assert_eq!(
                policy.check(&parse_url(url).unwrap()),
                Err(ImageError::AddressDenied)
            );
        }
        for origin in [
            "https://images.example/p",
            "https://user@images.example",
            "https://images.example#secret",
            "https://images.example?x",
        ] {
            assert!(HostedImagePolicy::https_origins(&[origin]).is_err());
        }
    }
    #[test]
    fn mapped_transition_documentation_multicast_and_metadata_ranges_stay_denied() {
        for address in [
            "100.64.0.0",
            "100.127.255.255",
            "192.88.99.1",
            "198.19.255.255",
            "198.51.100.8",
            "203.0.113.1",
            "240.0.0.1",
            "255.255.255.255",
            "::ffff:169.254.169.254",
            "64:ff9b:1::a00:1",
            "2001::1",
            "2001:20::1",
            "2002:7f00:1::1",
            "3fff:fff::1",
            "ff02::1",
        ] {
            assert!(!public_address(address.parse().unwrap()), "{address}");
        }
    }
}
