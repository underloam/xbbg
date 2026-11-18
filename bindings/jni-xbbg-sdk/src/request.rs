//! The JSON exchanged with `io.github.underloam.xbbg.sdk` (built by
//! `ProvisionOptions.toRequest`, decoded by `ProvisionResult.fromNative`).
//!
//! Request:
//! ```json
//! {"source": {"kind": "pypi", "url": "…", "project": "blpapi"}
//!          | {"kind": "conda", "channel": "…", "package": "libblpapi"}
//!          | {"kind": "artifact", "location": "…", "sha256": "…"},
//!  "version": "3.26.9.1", "platform": "windows-x86_64", "cacheDir": "…", "offline": false,
//!  "proxy": {"mode": "system"} | {"mode": "direct"} | {"mode": "explicit", "url": "…", "noProxy": ["…"]},
//!  "credentials": [{"host": "…", "username": "…", "password": "…"} | {"host": "…", "token": "…"}],
//!  "netrc": "…", "extraCaPem": "…", "connectTimeoutMillis": 10000, "readTimeoutMillis": 60000,
//!  "retries": 3, "requireArtifactHash": true}
//! ```
//! Only `source` and `cacheDir` are required; unknown fields are rejected.
//!
//! Result (also each element of the `installed` array):
//! `{"version", "platform", "libraryPath", "licensePaths": [], "source", "artifactUrl",
//! "artifactSha256", "libSha256", "cacheHit"}`.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use xbbg_runtime::CacheScan;
use xbbg_sdk::{
    Credential, Platform, ProvisionOptions, Provisioned, ProxyConfig, Source, TlsConfig,
    VersionSpec,
};

/// A parsed `provision` request.
pub(crate) struct Request {
    pub(crate) options: ProvisionOptions,
    pub(crate) source: Source,
    pub(crate) platform: Platform,
    pub(crate) version: VersionSpec,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RequestJson {
    source: SourceJson,
    version: Option<String>,
    platform: Option<String>,
    cache_dir: String,
    #[serde(default)]
    offline: bool,
    proxy: Option<ProxyJson>,
    #[serde(default)]
    credentials: Vec<CredentialJson>,
    netrc: Option<String>,
    extra_ca_pem: Option<String>,
    connect_timeout_millis: Option<u64>,
    read_timeout_millis: Option<u64>,
    retries: Option<u32>,
    require_artifact_hash: Option<bool>,
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

/// Parses and validates a request; the error is the message of an ARGUMENT failure.
pub(crate) fn parse(json: &str) -> Result<Request, String> {
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
        SourceJson::Pypi { url, project } => Source::PypiIndex {
            url,
            project: project.unwrap_or_else(|| xbbg_sdk::DEFAULT_PYPI_PROJECT.into()),
        },
        SourceJson::Conda { channel, package } => Source::CondaChannel {
            channel,
            package: package.unwrap_or_else(|| xbbg_sdk::DEFAULT_CONDA_PACKAGE.into()),
        },
        SourceJson::Artifact { location, sha256 } => Source::Artifact { location, sha256 },
    };
    if request.cache_dir.trim().is_empty() {
        return Err("cacheDir must not be blank".into());
    }

    let mut options = ProvisionOptions::new(PathBuf::from(request.cache_dir));
    let network = &mut options.network;
    network.offline = request.offline;
    network.proxy = match request.proxy {
        None | Some(ProxyJson::System) => ProxyConfig::System,
        Some(ProxyJson::Direct) => ProxyConfig::Direct,
        Some(ProxyJson::Explicit { url, no_proxy }) => ProxyConfig::Explicit { url, no_proxy },
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
    Ok(Request {
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

/// One install as a JSON object.
pub(crate) fn result_json(installed: &Provisioned) -> String {
    serde_json::to_string(&ResultJson::from(installed))
        .unwrap_or_else(|err| format!("{{\"error\":\"{err}\"}}"))
}

/// Installs as a JSON array.
pub(crate) fn results_json(installed: &[Provisioned]) -> String {
    let results: Vec<ResultJson<'_>> = installed.iter().map(ResultJson::from).collect();
    serde_json::to_string(&results).unwrap_or_else(|_| "[]".into())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ScanJson<'a> {
    runtimes: Vec<RuntimeJson<'a>>,
    rejected: Vec<RejectedJson<'a>>,
}

/// A verified install of a cache scan.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeJson<'a> {
    version: &'a str,
    platform: &'static str,
    library_path: String,
    lib_sha256: &'a str,
    source: &'a str,
    install_dir: String,
}

/// An install the scan refused, with the human-readable reason.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RejectedJson<'a> {
    install_dir: String,
    reason: &'a str,
}

/// A cache scan as `{"runtimes": [<install>...], "rejected": [{"installDir", "reason"}...]}`;
/// runtimes are the verified installs, best first.
pub(crate) fn scan_json(scan: &CacheScan) -> String {
    serde_json::to_string(&ScanJson {
        runtimes: scan
            .runtimes
            .iter()
            .map(|runtime| RuntimeJson {
                version: &runtime.version,
                platform: runtime.platform.as_str(),
                library_path: runtime.lib_path.to_string_lossy().into_owned(),
                lib_sha256: &runtime.lib_sha256,
                source: &runtime.source,
                install_dir: runtime.install_dir.to_string_lossy().into_owned(),
            })
            .collect(),
        rejected: scan
            .rejected
            .iter()
            .map(|rejected| RejectedJson {
                install_dir: rejected.install_dir.to_string_lossy().into_owned(),
                reason: &rejected.reason,
            })
            .collect(),
    })
    .unwrap_or_else(|_| r#"{"runtimes":[],"rejected":[]}"#.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_request_maps_every_field() {
        let json = r#"{
            "source": {"kind": "pypi", "url": "https://idx/simple/", "project": "blpapi"},
            "version": "3.26.9.1", "platform": "linux-x86_64", "cacheDir": "cache", "offline": true,
            "proxy": {"mode": "explicit", "url": "http://proxy:8080", "noProxy": ["localhost"]},
            "credentials": [{"host": "idx", "username": "u", "password": "p"}, {"host": "*.corp", "token": "t"}],
            "netrc": "n", "extraCaPem": "ca.pem", "connectTimeoutMillis": 1500, "readTimeoutMillis": 2500,
            "retries": 0, "requireArtifactHash": false
        }"#;
        let request = parse(json).unwrap();
        assert_eq!(request.platform, Platform::LinuxX86_64);
        assert_eq!(request.version, VersionSpec::exact("3.26.9.1"));
        assert_eq!(request.source, Source::pypi("https://idx/simple/"));
        let options = request.options;
        assert_eq!(options.cache_dir, PathBuf::from("cache"));
        assert!(!options.require_artifact_hash);
        let network = options.network;
        assert!(network.offline);
        assert_eq!(
            network.proxy,
            ProxyConfig::Explicit {
                url: "http://proxy:8080".into(),
                no_proxy: vec!["localhost".into()]
            }
        );
        assert_eq!(network.credentials.len(), 2);
        assert_eq!(
            network.credentials[1],
            (
                "*.corp".to_string(),
                Credential::Bearer { token: "t".into() }
            )
        );
        assert_eq!(network.netrc, Some(PathBuf::from("n")));
        assert_eq!(
            network.tls,
            TlsConfig::System {
                extra_ca_pem: Some(PathBuf::from("ca.pem"))
            }
        );
        assert_eq!(network.connect_timeout, Duration::from_millis(1500));
        assert_eq!(network.read_timeout, Duration::from_millis(2500));
        assert_eq!(network.retries, 0);
    }

    #[test]
    fn minimal_request_uses_defaults() {
        let request =
            parse(r#"{"source": {"kind": "conda", "channel": "conda-forge"}, "cacheDir": "c"}"#)
                .unwrap();
        assert_eq!(request.source, Source::conda("conda-forge"));
        assert_eq!(request.version, VersionSpec::Latest);
        assert_eq!(Some(request.platform), Platform::host());
        assert_eq!(request.options.network.proxy, ProxyConfig::System);
        assert!(request.options.require_artifact_hash);
        assert_eq!(
            request.options.min_pypi_version.as_deref(),
            Some(xbbg_sdk::MIN_PYPI_VERSION)
        );
    }

    #[test]
    fn malformed_requests_are_rejected_with_the_reason() {
        let cases = [
            (r#"{"source": {"kind": "pypi", "url": "u"}}"#, "cacheDir"),
            (
                r#"{"source": {"kind": "svn", "url": "u"}, "cacheDir": "c"}"#,
                "svn",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u", "extra": 1}, "cacheDir": "c"}"#,
                "extra",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "colour": 1}"#,
                "colour",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "platform": "linux-aarch64"}"#,
                "linux-aarch64",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "version": " "}"#,
                "blank",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "readTimeoutMillis": 0}"#,
                "readTimeoutMillis",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "credentials": [{"host": "h", "username": "u"}]}"#,
                "username and password",
            ),
            (
                r#"{"source": {"kind": "pypi", "url": "u"}, "cacheDir": "c", "proxy": {"mode": "pac"}}"#,
                "pac",
            ),
        ];
        for (json, needle) in cases {
            let err = parse(json)
                .err()
                .unwrap_or_else(|| panic!("accepted {json}"));
            assert!(err.contains(needle), "{json}: {err}");
        }
    }

    #[test]
    fn results_use_the_java_field_names() {
        let installed = Provisioned {
            version: "3.26.9.1".into(),
            platform: Platform::WindowsX86_64,
            lib_path: PathBuf::from("lib").join("blpapi3_64.dll"),
            license_paths: vec![PathBuf::from("License.txt")],
            source: "pypi:https://idx/simple/".into(),
            artifact_url: "https://idx/x.whl".into(),
            artifact_sha256: "a".repeat(64),
            lib_sha256: "b".repeat(64),
            cache_hit: true,
        };
        let value: serde_json::Value = serde_json::from_str(&result_json(&installed)).unwrap();
        assert_eq!(value["platform"], "windows-x86_64");
        assert_eq!(
            value["libraryPath"],
            PathBuf::from("lib")
                .join("blpapi3_64.dll")
                .to_string_lossy()
                .as_ref()
        );
        assert_eq!(value["licensePaths"][0], "License.txt");
        assert_eq!(value["cacheHit"], true);
        assert_eq!(value["libSha256"], "b".repeat(64));
        let array: serde_json::Value = serde_json::from_str(&results_json(&[installed])).unwrap();
        assert_eq!(array.as_array().unwrap().len(), 1);
    }

    #[test]
    fn scans_carry_runtimes_and_rejections() {
        use sha2::{Digest, Sha256};

        let cache = tempfile::tempdir().unwrap();
        let platform_dir = cache
            .path()
            .join("sdk")
            .join("3.26.9.1")
            .join("linux-x86_64");
        let write_install = |name: &str, content: &[u8], recorded: &[u8]| {
            let install = platform_dir.join(name);
            std::fs::create_dir_all(install.join("lib")).unwrap();
            std::fs::write(install.join("lib").join("libblpapi3_64.so"), content).unwrap();
            let provenance = serde_json::json!({
                "version": "3.26.9.1",
                "platform": "linux-x86_64",
                "source": "pypi:https://idx/simple/",
                "artifactUrl": "https://idx/x.whl",
                "artifactSha256": "d".repeat(64),
                "lib": "lib/libblpapi3_64.so",
                "libSha256": hex::encode(Sha256::digest(recorded)),
                "licenses": [],
            });
            std::fs::write(install.join("provenance.json"), provenance.to_string()).unwrap();
        };
        write_install("pypi-good", b"synthetic runtime", b"synthetic runtime");
        write_install("pypi-bad", b"tampered runtime", b"synthetic runtime");

        let scan = xbbg_runtime::scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        let value: serde_json::Value = serde_json::from_str(&scan_json(&scan)).unwrap();
        assert_eq!(value["runtimes"].as_array().unwrap().len(), 1);
        let runtime = &value["runtimes"][0];
        assert_eq!(runtime["version"], "3.26.9.1");
        assert_eq!(runtime["platform"], "linux-x86_64");
        assert_eq!(runtime["source"], "pypi:https://idx/simple/");
        assert!(
            runtime["libraryPath"]
                .as_str()
                .unwrap()
                .ends_with("libblpapi3_64.so")
        );
        assert_eq!(
            runtime["libSha256"],
            hex::encode(Sha256::digest(b"synthetic runtime"))
        );
        assert_eq!(value["rejected"].as_array().unwrap().len(), 1);
        let rejected = &value["rejected"][0];
        assert!(
            rejected["installDir"]
                .as_str()
                .unwrap()
                .ends_with("pypi-bad")
        );
        assert!(rejected["reason"].as_str().unwrap().contains("sha256"));
    }
}
