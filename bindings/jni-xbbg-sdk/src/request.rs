//! The JSON exchanged with `io.github.underloam.xbbg.sdk` (built by
//! `ProvisionOptions.toRequest`, decoded by `ProvisionResult.fromNative`).
//!
//! Delegates to [`xbbg_sdk::request`] with `require_cache_dir = true` so Java's
//! JSON contract stays shared with every other xbbg host.

pub(crate) use xbbg_sdk::request::{ProvisionRequest as Request, result_json, results_json};

/// Parses and validates a request; the error is the message of an ARGUMENT failure.
pub(crate) fn parse(json: &str) -> Result<Request, String> {
    xbbg_sdk::request::parse_json(json, true)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use xbbg_sdk::{
        Credential, Platform, Provisioned, ProxyConfig, Source, TlsConfig, VersionSpec,
    };

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
}
