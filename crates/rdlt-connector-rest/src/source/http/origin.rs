//! Outbound origin pinning: the ONE rule both the redirect policy and
//! the pagination next-url seat answer to.

use reqwest::Url;

/// Whether two absolute URLs share an origin — scheme, host, and the
/// effective port (explicit or scheme default).
///
/// Every request this source makes rides credentials (bearer header,
/// api-key header — or an api key located IN the query string), so a
/// response-directed hop to another host is a credential exfiltration
/// channel: a compromised or poisoned API response naming its own
/// `next` page, or any 3xx along the chain, must not be able to aim
/// that credential elsewhere. Both seats that follow server-named
/// locations call this before following; divergence refuses typed.
pub(crate) fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host_str() == b.host_str()
        && a.port_or_known_default() == b.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(s: &str) -> Url {
        Url::parse(s).expect("test URL parses")
    }

    #[test]
    fn identical_urls_share_an_origin() {
        assert!(same_origin(
            &url("https://api.example.com/v1/items"),
            &url("https://api.example.com/v1/next"),
        ));
    }

    #[test]
    fn default_ports_normalize() {
        assert!(same_origin(
            &url("https://api.example.com/a"),
            &url("https://api.example.com:443/b"),
        ));
        assert!(same_origin(
            &url("http://api.example.com/a"),
            &url("http://api.example.com:80/b"),
        ));
    }

    #[test]
    fn host_and_scheme_and_port_differences_diverge() {
        let base = url("https://api.example.com/a");
        assert!(!same_origin(&base, &url("https://evil.example.com/a")));
        // Scheme downgrade is divergence too: https → http must refuse.
        assert!(!same_origin(&base, &url("http://api.example.com/a")));
        assert!(!same_origin(&base, &url("https://api.example.com:8443/a")));
    }
}
