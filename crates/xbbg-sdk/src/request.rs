//! Shared JSON codec for Bloomberg SDK provisioning requests and results.
//!
//! Every language binding (Java via `jni-xbbg-sdk`, Python via `pyo3-xbbg`, Node via `napi-xbbg`,
//! and the MCP CLI via `xbbg-host`) uses this schema so options, validation rules and result
//! fields stay identical across hosts.
//!
//! Request JSON:
//! ```json
//! {
//!   "source": "conda:conda-forge"
//!          | {"kind": "pypi", "url": "…", "project": "blpapi"}
//!          | {"kind": "conda", "channel": "…", "package": "libblpapi"}
//!          | {"kind": "artifact", "location": "…", "sha256": "…"},
//!   "version": "3.26.9.1",
//!   "platform": "windows-x86_64",
//!   "cacheDir": "…",
//!   "offline": false,
//!   "proxy": {"mode": "system"} | {"mode": "direct"} | {"mode": "explicit", "url": "…", "noProxy": ["…"]},
//!   "credentials": [{"host": "…", "username": "…", "password": "…"} | {"host": "…", "token": "…"}],
//!   "netrc": "…",
//!   "extraCaPem": "…",
//!   "connectTimeoutMillis": 10000,
//!   "readTimeoutMillis": 60000,
//!   "retries": 3,
//!   "requireArtifactHash": true
//! }
//! ```
//! Unknown fields are rejected. `cacheDir` may be omitted (resolving to [`crate::default_cache_root`])
//! unless the caller passes `require_cache_dir = true`.
//!
//! Result JSON (also each element of `installed`):
//! `{"version", "platform", "libraryPath", "licensePaths": [], "source", "artifactUrl",
//! "artifactSha256", "libSha256", "cacheHit"}`.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    Credential, DEFAULT_CONDA_PACKAGE, DEFAULT_PYPI_PROJECT, Platform, ProvisionOptions,
    Provisioned, ProxyConfig, Source, TlsConfig, VersionSpec, default_cache_root,
};

/// A parsed `provision` request.
#[derive(Debug)]
pub struct ProvisionRequest {
    /// Provisioner options (cache directory, network/TLS/proxy settings, hash policy).
    pub options: ProvisionOptions,
    /// Where to obtain the runtime.
    pub source: Source,
    /// Target runtime platform.
    pub platform: Platform,
    /// Version constraint (`Latest` or exact).
    pub version: VersionSpec,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestJson {
    source: SourceInput,
    version: Option<String>,
    platform: Option<String>,
    cache_dir: Option<String>,
    #[serde(default)]
    offline: bool,
    proxy: Option<ProxyInput>,
    #[serde(default)]
    credentials: Vec<CredentialJson>,
    netrc: Option<String>,
    extra_ca_pem: Option<String>,
    connect_timeout_millis: Option<u64>,
    read_timeout_millis: Option<u64>,
    retries: Option<u32>,
    require_artifact_hash: Option<bool>,
}

enum SourceInput {
    Spec(String),
    Structured(SourceJson),
}

impl<'de> Deserialize<'de> for SourceInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(spec) => Ok(Self::Spec(spec)),
            other => serde_json::from_value(other)
                .map(Self::Structured)
                .map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
enum SourceJson {
    Pypi {
        url: String,
        project: Option<String>,
    },
    Conda {
        channel: String,
        package: Option<String>,
    },
    Artifact {
        location: String,
        sha256: Option<String>,
    },
}

enum ProxyInput {
    Url(String),
    Structured(ProxyJson),
}

impl<'de> Deserialize<'de> for ProxyInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(url) => Ok(Self::Url(url)),
            other => serde_json::from_value(other)
                .map(Self::Structured)
                .map_err(serde::de::Error::custom),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "camelCase", deny_unknown_fields)]
enum ProxyJson {
    System,
    Direct,
    #[serde(rename_all = "camelCase")]
    Explicit {
        url: String,
        #[serde(default)]
        no_proxy: Vec<String>,
    },
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CredentialJson {
    host: String,
    username: Option<String>,
    password: Option<String>,
    token: Option<String>,
}

/// Parses and validates a provisioning request JSON string.
///
/// When `require_cache_dir` is `false` and `cacheDir` is omitted, [`default_cache_root`] is used.
pub fn parse_json(json: &str, require_cache_dir: bool) -> Result<ProvisionRequest, String> {
    let request: RequestJson =
        serde_json::from_str(json).map_err(|err| format!("invalid provisioning request: {err}"))?;
    let platform = match request.platform.as_deref() {
        Some(name) => name.parse::<Platform>().map_err(|err| err.to_string())?,
        None => Platform::host().ok_or_else(|| {
            format!(
                "this platform has no Bloomberg runtime; pass one of: {}",
                Platform::ALL.map(Platform::as_str).join(", ")
            )
        })?,
    };
    let version = match request.version {
        Some(version) if version.trim().is_empty() => {
            return Err("version must not be blank".into());
        }
        Some(version) if version.trim() != "latest" => VersionSpec::exact(version.trim()),
        _ => VersionSpec::Latest,
    };
    let source = match request.source {
        SourceInput::Spec(spec) => Source::parse(&spec)?,
        SourceInput::Structured(SourceJson::Pypi { url, project }) => Source::PypiIndex {
            url,
            project: project.unwrap_or_else(|| DEFAULT_PYPI_PROJECT.into()),
        },
        SourceInput::Structured(SourceJson::Conda { channel, package }) => Source::CondaChannel {
            channel,
            package: package.unwrap_or_else(|| DEFAULT_CONDA_PACKAGE.into()),
        },
        SourceInput::Structured(SourceJson::Artifact { location, sha256 }) => {
            let sha256 = match sha256 {
                Some(hash) => Some(crate::source::validate_sha256_hex(&hash)?),
                None => None,
            };
            Source::Artifact { location, sha256 }
        }
    };
    let cache_dir = match request.cache_dir {
        Some(dir) if dir.trim().is_empty() => {
            return Err("cacheDir must not be blank".into());
        }
        Some(dir) => PathBuf::from(dir),
        None if require_cache_dir => {
            return Err("invalid provisioning request: missing field `cacheDir`".into());
        }
        None => default_cache_root().ok_or_else(|| {
            "cannot determine the xbbg cache directory; pass cacheDir or set XBBG_CACHE_DIR"
                .to_owned()
        })?,
    };

    let mut options = ProvisionOptions::new(cache_dir);
    let network = &mut options.network;
    network.offline = request.offline;
    network.proxy = match request.proxy {
        None | Some(ProxyInput::Structured(ProxyJson::System)) => ProxyConfig::System,
        Some(ProxyInput::Structured(ProxyJson::Direct)) => ProxyConfig::Direct,
        Some(ProxyInput::Structured(ProxyJson::Explicit { url, no_proxy })) => {
            ProxyConfig::Explicit { url, no_proxy }
        }
        Some(ProxyInput::Url(url)) => {
            let trimmed = url.trim();
            if trimmed.eq_ignore_ascii_case("system") {
                ProxyConfig::System
            } else if trimmed.eq_ignore_ascii_case("direct") {
                ProxyConfig::Direct
            } else if trimmed.is_empty() {
                return Err("proxy must not be blank".into());
            } else {
                ProxyConfig::Explicit {
                    url: trimmed.to_owned(),
                    no_proxy: Vec::new(),
                }
            }
        }
    };
    for credential in request.credentials {
        let secret = match (credential.username, credential.password, credential.token) {
            (Some(username), Some(password), None) => Credential::Basic { username, password },
            (None, None, Some(token)) => Credential::Bearer { token },
            _ => {
                return Err(format!(
                    "credential for {} needs either username and password or a token",
                    credential.host
                ));
            }
        };
        network.credentials.push((credential.host, secret));
    }
    network.netrc = request.netrc.map(PathBuf::from);
    network.tls = TlsConfig::System {
        extra_ca_pem: request.extra_ca_pem.map(PathBuf::from),
    };
    if let Some(millis) = request.connect_timeout_millis {
        network.connect_timeout = positive(millis, "connectTimeoutMillis")?;
    }
    if let Some(millis) = request.read_timeout_millis {
        network.read_timeout = positive(millis, "readTimeoutMillis")?;
    }
    if let Some(retries) = request.retries {
        network.retries = retries;
    }
    if let Some(require) = request.require_artifact_hash {
        options.require_artifact_hash = require;
    }
    Ok(ProvisionRequest {
        options,
        source,
        platform,
        version,
    })
}

fn positive(millis: u64, name: &str) -> Result<Duration, String> {
    if millis == 0 {
        Err(format!("{name} must be positive"))
    } else {
        Ok(Duration::from_millis(millis))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResultJson<'a> {
    version: &'a str,
    platform: &'static str,
    library_path: String,
    license_paths: Vec<String>,
    source: &'a str,
    artifact_url: &'a str,
    artifact_sha256: &'a str,
    lib_sha256: &'a str,
    cache_hit: bool,
}

impl<'a> From<&'a Provisioned> for ResultJson<'a> {
    fn from(installed: &'a Provisioned) -> Self {
        Self {
            version: &installed.version,
            platform: installed.platform.as_str(),
            library_path: installed.lib_path.to_string_lossy().into_owned(),
            license_paths: installed
                .license_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
            source: &installed.source,
            artifact_url: &installed.artifact_url,
            artifact_sha256: &installed.artifact_sha256,
            lib_sha256: &installed.lib_sha256,
            cache_hit: installed.cache_hit,
        }
    }
}

/// Encodes one install as a camelCase JSON object.
pub fn result_json(installed: &Provisioned) -> String {
    serde_json::to_string(&ResultJson::from(installed))
        .unwrap_or_else(|err| format!("{{\"error\":\"{err}\"}}"))
}

/// Encodes a slice of installs as a camelCase JSON array.
pub fn results_json(installed: &[Provisioned]) -> String {
    let results: Vec<ResultJson<'_>> = installed.iter().map(ResultJson::from).collect();
    serde_json::to_string(&results).unwrap_or_else(|_| "[]".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_string_source_and_string_proxy() {
        let req = parse_json(
            r#"{"source":"conda:conda-forge","platform":"linux-x86_64","cacheDir":"cache","proxy":"http://proxy:8080"}"#,
            false,
        )
        .unwrap();
        assert_eq!(req.source, Source::conda("conda-forge"));
        assert_eq!(req.platform, Platform::LinuxX86_64);
        assert_eq!(
            req.options.network.proxy,
            ProxyConfig::Explicit {
                url: "http://proxy:8080".into(),
                no_proxy: Vec::new(),
            }
        );
    }

    #[test]
    fn require_cache_dir_rejects_omitted_field() {
        assert!(
            parse_json(
                r#"{"source":"conda-forge","platform":"linux-x86_64"}"#,
                true
            )
            .is_err()
        );
    }
}
