//! Bloomberg session transport configuration.
//!
//! `Transport` expresses the three mutually-exclusive ways to reach Bloomberg:
//! a direct TCP connection (with or without per-server SOCKS5 proxies), or
//! the ZFP leased-line path via `ZfpUtil::getOptionsForLeasedLines`.
//!
//! The SDK treats these as mode-exclusive — `blpapi_zfputil.h` states the
//! `SessionOptions` returned by `ZfpUtil` is "only valid for private leased
//! line connectivity". Representing that as an enum makes it impossible to
//! layer a direct server address on top of a ZFP-configured session at the
//! type level.
//!
//! TLS is orthogonal to transport: required by `Zfp`, optional on `Direct`
//! (B-PIPE over TLS).

use std::fmt;

use xbbg_core::BlpError;
use xbbg_core::zfp::ZfpRemote;

/// A SOCKS5 proxy applied to a single [`ServerAddr`].
///
/// Held as plain data (not the FFI wrapper) so transports are `Clone` and
/// can be built once and reused across session starts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Socks5Proxy {
    pub host: String,
    pub port: u16,
}

/// A single Bloomberg server endpoint, with an optional per-server proxy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerAddr {
    pub host: String,
    pub port: u16,
    pub proxy: Option<Socks5Proxy>,
}

impl ServerAddr {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            proxy: None,
        }
    }

    pub fn with_proxy(mut self, proxy: Socks5Proxy) -> Self {
        self.proxy = Some(proxy);
        self
    }
}

/// How the Bloomberg session reaches the infrastructure.
#[derive(Clone, Debug)]
pub enum Transport {
    /// Direct TCP to one or more Bloomberg endpoints. Must contain at least
    /// one entry. Each entry may carry its own SOCKS5 proxy.
    Direct(Vec<ServerAddr>),
    /// Zero-footprint private leased line. Endpoints are supplied by the SDK
    /// via `ZfpUtil::getOptionsForLeasedLines`; requires TLS at session start.
    Zfp(ZfpRemote),
}

impl Transport {
    /// Bloomberg's documented default: a single local Terminal on port 8194.
    pub fn default_direct() -> Self {
        Transport::Direct(vec![ServerAddr::new("localhost", 8194)])
    }

    /// Validate invariants that aren't expressible in the type.
    ///
    /// Today: `Direct` must not be empty. Called at session start so
    /// configuration errors surface before the SDK sees them.
    pub fn validate(&self) -> Result<(), BlpError> {
        match self {
            Transport::Direct(servers) if servers.is_empty() => Err(BlpError::InvalidArgument {
                detail: "Transport::Direct requires at least one server".into(),
            }),
            _ => Ok(()),
        }
    }
}

impl Default for Transport {
    fn default() -> Self {
        Self::default_direct()
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Transport::Direct(servers) => match servers.split_first() {
                Some((first, rest)) => {
                    write!(f, "{}:{}", first.host, first.port)?;
                    if !rest.is_empty() {
                        write!(f, " (+{} failover)", rest.len())?;
                    }
                    Ok(())
                }
                None => f.write_str("direct:<empty>"),
            },
            Transport::Zfp(remote) => write!(f, "zfp:{remote}"),
        }
    }
}

/// TLS material used to build a `xbbg_core::tls::TlsOptions` at session start.
///
/// Held as owned strings (paths + password) rather than an FFI wrapper so the
/// config stays `Clone` and one `EngineConfig` can spawn many sessions.
#[derive(Clone, Debug)]
pub struct TlsConfig {
    pub client_credentials: String,
    pub client_credentials_password: String,
    pub trust_material: String,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
}

impl TlsConfig {
    /// Construct a live `TlsOptions` for use with the SDK. Called once per
    /// session start; the returned handle is consumed by either
    /// `ZfpUtil::getOptionsForLeasedLines` or `SessionOptions::set_tls_options`.
    pub fn build(&self) -> Result<xbbg_core::tls::TlsOptions, BlpError> {
        let mut tls = xbbg_core::tls::TlsOptions::from_files(
            &self.client_credentials,
            &self.client_credentials_password,
            &self.trust_material,
        )?;
        if let Some(ms) = self.handshake_timeout_ms {
            tls.set_tls_handshake_timeout_ms(ms);
        }
        if let Some(ms) = self.crl_fetch_timeout_ms {
            tls.set_crl_fetch_timeout_ms(ms);
        }
        Ok(tls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_direct_localhost() {
        match Transport::default() {
            Transport::Direct(servers) => {
                assert_eq!(servers.len(), 1);
                assert_eq!(servers[0].host, "localhost");
                assert_eq!(servers[0].port, 8194);
                assert!(servers[0].proxy.is_none());
            }
            other => panic!("expected Direct, got {other}"),
        }
    }

    #[test]
    fn display_direct_single() {
        let t = Transport::Direct(vec![ServerAddr::new("bpipe.firm.com", 8194)]);
        assert_eq!(t.to_string(), "bpipe.firm.com:8194");
    }

    #[test]
    fn display_direct_multi() {
        let t = Transport::Direct(vec![
            ServerAddr::new("primary", 8194),
            ServerAddr::new("secondary", 8196),
            ServerAddr::new("tertiary", 8194),
        ]);
        assert_eq!(t.to_string(), "primary:8194 (+2 failover)");
    }

    #[test]
    fn display_zfp() {
        assert_eq!(
            Transport::Zfp(ZfpRemote::Remote8194).to_string(),
            "zfp:8194"
        );
        assert_eq!(
            Transport::Zfp(ZfpRemote::Remote8196).to_string(),
            "zfp:8196"
        );
    }

    #[test]
    fn validate_rejects_empty_direct() {
        let err = Transport::Direct(vec![]).validate().unwrap_err();
        assert!(matches!(err, BlpError::InvalidArgument { .. }));
    }

    #[test]
    fn validate_accepts_zfp() {
        Transport::Zfp(ZfpRemote::Remote8194).validate().unwrap();
    }

    #[test]
    fn server_addr_with_proxy() {
        let addr = ServerAddr::new("host", 8194).with_proxy(Socks5Proxy {
            host: "proxy".into(),
            port: 1080,
        });
        assert_eq!(addr.proxy.as_ref().map(|p| p.host.as_str()), Some("proxy"));
        assert_eq!(addr.proxy.as_ref().map(|p| p.port), Some(1080));
    }
}
