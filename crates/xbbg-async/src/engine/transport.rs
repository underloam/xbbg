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
use std::sync::Arc;

use xbbg_core::BlpError;
use xbbg_core::zfp::ZfpRemote;

const TLS_MATERIAL_PAIR_ERROR: &str =
    "tls_client_credentials and tls_trust_material must both be file paths or both be blobs";

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

/// A file path or in-memory DER-encoded TLS material.
///
/// Blobs use shared immutable storage so cloning an engine configuration does
/// not copy credentials. Blob contents are never included in `Debug` output.
#[derive(Clone, PartialEq, Eq)]
pub enum TlsMaterial {
    Path(String),
    Blob(Arc<[u8]>),
}

impl TlsMaterial {
    pub(crate) fn is_blob(&self) -> bool {
        matches!(self, Self::Blob(_))
    }
}

impl From<String> for TlsMaterial {
    fn from(path: String) -> Self {
        Self::Path(path)
    }
}

impl From<&str> for TlsMaterial {
    fn from(path: &str) -> Self {
        Self::Path(path.to_owned())
    }
}

impl fmt::Debug for TlsMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => f.debug_tuple("Path").field(path).finish(),
            Self::Blob(_) => f.write_str("Blob(<redacted>)"),
        }
    }
}

/// TLS material used to build a `xbbg_core::tls::TlsOptions` at session start.
///
/// Both values must be file paths, or both must be DER blobs (PKCS#12 client
/// credentials and PKCS#7 trust material). No temporary files are created.
#[derive(Clone)]
pub struct TlsConfig {
    pub client_credentials: TlsMaterial,
    pub client_credentials_password: String,
    pub trust_material: TlsMaterial,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("client_credentials", &self.client_credentials)
            .field("client_credentials_password", &"<redacted>")
            .field("trust_material", &self.trust_material)
            .field("handshake_timeout_ms", &self.handshake_timeout_ms)
            .field("crl_fetch_timeout_ms", &self.crl_fetch_timeout_ms)
            .finish()
    }
}

impl TlsConfig {
    /// Validate the representation pair without opening files or calling the SDK.
    pub fn validate(&self) -> Result<(), BlpError> {
        if self.client_credentials.is_blob() != self.trust_material.is_blob() {
            return Err(BlpError::InvalidArgument {
                detail: TLS_MATERIAL_PAIR_ERROR.into(),
            });
        }
        Ok(())
    }

    /// Construct a live `TlsOptions` for use with the SDK. Called once per
    /// session start; the returned handle is consumed by either
    /// `ZfpUtil::getOptionsForLeasedLines` or `SessionOptions::set_tls_options`.
    pub fn build(&self) -> Result<xbbg_core::tls::TlsOptions, BlpError> {
        let mut tls = match (&self.client_credentials, &self.trust_material) {
            (TlsMaterial::Path(credentials), TlsMaterial::Path(trust)) => {
                xbbg_core::tls::TlsOptions::from_files(
                    credentials,
                    &self.client_credentials_password,
                    trust,
                )?
            }
            (TlsMaterial::Blob(credentials), TlsMaterial::Blob(trust)) => {
                xbbg_core::tls::TlsOptions::from_blobs(
                    credentials,
                    &self.client_credentials_password,
                    trust,
                )?
            }
            _ => {
                return Err(BlpError::InvalidArgument {
                    detail: TLS_MATERIAL_PAIR_ERROR.into(),
                });
            }
        };
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

    fn blob_config() -> TlsConfig {
        TlsConfig {
            client_credentials: TlsMaterial::Blob(Arc::from(&b"synthetic-client-secret"[..])),
            client_credentials_password: "synthetic-password-secret".into(),
            trust_material: TlsMaterial::Blob(Arc::from(&b"synthetic-trust-secret"[..])),
            handshake_timeout_ms: Some(0),
            crl_fetch_timeout_ms: Some(25),
        }
    }

    #[test]
    fn tls_config_debug_redacts_password_and_blobs() {
        let config = blob_config();
        let debug = format!("{config:?}");
        for secret in [
            "synthetic-client-secret",
            "synthetic-password-secret",
            "synthetic-trust-secret",
        ] {
            assert!(!debug.contains(secret));
            assert!(!debug.contains(&format!("{:?}", secret.as_bytes())));
        }
        let mut paths = config;
        paths.client_credentials = "fixtures/client.p12".into();
        paths.trust_material = "fixtures/trust.p7".into();
        assert!(!format!("{paths:?}").contains("synthetic-password-secret"));
    }

    #[test]
    fn tls_config_clone_shares_blob_storage() {
        let config = blob_config();
        let cloned = config.clone();
        for (original, copied) in [
            (&config.client_credentials, &cloned.client_credentials),
            (&config.trust_material, &cloned.trust_material),
        ] {
            let (TlsMaterial::Blob(original), TlsMaterial::Blob(copied)) = (original, copied)
            else {
                panic!("expected blob material");
            };
            assert!(Arc::ptr_eq(original, copied));
        }
    }

    #[test]
    fn tls_config_rejects_mixed_pairs_before_sdk_construction() {
        for credentials_are_blob in [false, true] {
            let mut config = blob_config();
            if credentials_are_blob {
                config.trust_material = "fixtures/trust.p7".into();
            } else {
                config.client_credentials = "fixtures/client.p12".into();
            }
            assert!(matches!(
                config.validate(),
                Err(BlpError::InvalidArgument { .. })
            ));
            assert!(matches!(
                config.build(),
                Err(BlpError::InvalidArgument { .. })
            ));
        }
    }

    #[test]
    fn tls_config_accepts_matching_pairs_without_loading_material() {
        let mut config = blob_config();
        config.validate().unwrap();
        config.client_credentials = "fixtures/client.p12".into();
        config.trust_material = "fixtures/trust.p7".into();
        config.validate().unwrap();
    }

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
