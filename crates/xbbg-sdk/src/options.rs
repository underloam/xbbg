//! Provisioning and network options. Nothing here is read from the environment.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use crate::source::MIN_PYPI_VERSION;

/// How requests reach the network.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ProxyConfig {
    /// The proxy from `HTTPS_PROXY`/`HTTP_PROXY`/`ALL_PROXY`/`NO_PROXY` or the operating
    /// system's settings (Windows registry, macOS System Configuration); direct otherwise.
    #[default]
    System,
    /// Direct connections only.
    Direct,
    /// This proxy for every request (`http://`, `https://`, `socks5://`, `socks5h://`,
    /// optionally with `user:password@`).
    Explicit {
        /// Proxy URL.
        url: String,
        /// Hosts that bypass the proxy (`NO_PROXY` syntax).
        no_proxy: Vec<String>,
    },
}

/// Which certificate authorities TLS trusts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsConfig {
    /// The operating system's trust store, plus the PEM bundle if given (for TLS-intercepting
    /// corporate proxies).
    System {
        /// Extra CA certificates.
        extra_ca_pem: Option<PathBuf>,
    },
    /// Only the certificates in this PEM bundle.
    Exclusive {
        /// CA certificates.
        ca_pem: PathBuf,
    },
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::System { extra_ca_pem: None }
    }
}

/// Credentials for one host.
#[derive(Clone, PartialEq, Eq)]
pub enum Credential {
    /// HTTP basic authentication.
    Basic {
        /// User name.
        username: String,
        /// Password or API key.
        password: String,
    },
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: String,
    },
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Bearer { .. } => f.debug_struct("Bearer").field("token", &"***").finish(),
        }
    }
}

/// Network settings shared by the PyPI, conda and artifact paths.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct NetworkOptions {
    /// Proxy selection.
    pub proxy: ProxyConfig,
    /// Certificate trust.
    pub tls: TlsConfig,
    /// Per-host credentials; the key is a host (`repo.example.com`) or a wildcard
    /// (`*.example.com`). Credentials embedded in source URLs are added automatically.
    /// Credentials (also from `netrc`) are sent over `https`, and over plain `http` only to the
    /// scheme, host and port of a configured source URL.
    pub credentials: Vec<(String, Credential)>,
    /// A netrc file to read credentials from (never read implicitly).
    pub netrc: Option<PathBuf>,
    /// Forbid network access: provisioning only returns cached installs, resolution only uses
    /// cached index data.
    pub offline: bool,
    /// TCP connect timeout.
    pub connect_timeout: Duration,
    /// Maximum time between two reads of a response body.
    pub read_timeout: Duration,
    /// Retries for transient failures (connect errors, HTTP 408/429/5xx, interrupted bodies).
    pub retries: u32,
    /// `User-Agent` header.
    pub user_agent: String,
}

impl Default for NetworkOptions {
    fn default() -> Self {
        Self {
            proxy: ProxyConfig::default(),
            tls: TlsConfig::default(),
            credentials: Vec::new(),
            netrc: None,
            offline: false,
            connect_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(60),
            retries: 3,
            user_agent: concat!("xbbg-sdk/", env!("CARGO_PKG_VERSION")).to_string(),
        }
    }
}

/// Options for a [`crate::Provisioner`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProvisionOptions {
    /// Cache root (see [`crate::default_cache_root`]); installs go to `<cache>/sdk/`.
    pub cache_dir: PathBuf,
    /// Network settings.
    pub network: NetworkOptions,
    /// Reject remote (`http`/`https`) artifacts that come without an expected SHA-256. Local
    /// files are always accepted and verified against their `RECORD` or `paths.json`.
    pub require_artifact_hash: bool,
    /// Oldest wheel version accepted from PyPI indexes and wheel artifacts (PEP 440); default
    /// [`MIN_PYPI_VERSION`].
    pub min_pypi_version: Option<String>,
}

impl ProvisionOptions {
    /// Default options for a cache root.
    pub fn new(cache_dir: impl Into<PathBuf>) -> Self {
        Self {
            cache_dir: cache_dir.into(),
            network: NetworkOptions::default(),
            require_artifact_hash: true,
            min_pypi_version: Some(MIN_PYPI_VERSION.to_string()),
        }
    }
}
