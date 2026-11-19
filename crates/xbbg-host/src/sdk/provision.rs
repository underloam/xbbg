//! Bloomberg runtime provisioning shared across Python, Node/JS, MCP and Rust hosts.
//!
//! Enabled by the `provision` Cargo feature (`xbbg-sdk/provision`). Java keeps its own
//! separate `xbbg_sdk_jni` library and therefore uses `xbbg-host` with default features;
//! Python, Node/JS and `xbbg-mcp` enable this module so every surface can provision the
//! shared `<cache>/sdk/<version>/<platform>/` cache programmatically, from the CLI, or
//! automatically via `XBBG_SDK_SOURCE` when no local runtime is installed.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::PoisonError;

use xbbg_core::SdkLoadError;
pub use xbbg_sdk::cli::run as cli_run;
use xbbg_sdk::{
    BLOOMBERG_PYPI_INDEX, Error as SdkError, Platform, ProvisionOptions, Provisioned, Provisioner,
    SOURCE_ENV, Source, VERSION_ENV, VersionSpec, default_cache_root, redact_user_info, request,
};

use super::{
    LOADED, LoadedSdk, LocateRequest, Rejected, SdkNotFound, SourceKind, load_failure,
    locate_and_load, runtime_version,
};
use crate::errors::{HostError, HostErrorKind};

/// Optional programmatic provisioning source and version passed by a host.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProvisionFallback {
    /// Source specification (`pypi:...`, `conda:...`, `artifact:...`, or bare URL/path).
    /// Overrides `XBBG_SDK_SOURCE` when set.
    pub source: Option<String>,
    /// Optional exact version (or `"latest"`). Overrides `XBBG_SDK_VERSION` when set.
    pub version: Option<String>,
}

/// Failure from [`locate_and_load_or_provision`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SdkLoadFailure {
    /// No local or cached runtime loaded and no provisioning source was configured
    /// (or an explicit path failed, which is never replaced by provisioning).
    NotFound(SdkNotFound),
    /// Provisioning was configured and failed, or the provisioned library could not be loaded.
    Provision {
        /// Stable `xbbg-sdk` error code (`INVALID_ARGUMENT`, `NOT_FOUND`, `NETWORK`, ...).
        code: &'static str,
        /// Human-readable description with credentials redacted.
        message: String,
        /// Refusals recorded before provisioning was attempted.
        rejected: Vec<Rejected>,
    },
}

impl fmt::Display for SdkLoadFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(error) => fmt::Display::fmt(error, f),
            Self::Provision { code, message, .. } => {
                write!(
                    f,
                    "failed to provision the Bloomberg runtime [{code}]: {message}"
                )
            }
        }
    }
}

impl std::error::Error for SdkLoadFailure {}

/// Searches for a Bloomberg runtime with [`locate_and_load`], and if none loads and no explicit
/// path was given, provisions one into the shared xbbg cache when a source is configured.
///
/// An explicit path ([`LocateRequest::explicit`]) is always authoritative and is never replaced
/// by provisioning. When `request.env` is `Some`, `XBBG_SDK_SOURCE`, `XBBG_SDK_VERSION` and
/// `XBBG_CACHE_DIR` are read from that map instead of the process environment (so Node/Bun
/// `process.env` assignments take effect).
pub fn locate_and_load_or_provision(
    request: &LocateRequest,
    fallback: &ProvisionFallback,
) -> Result<LoadedSdk, SdkLoadFailure> {
    let not_found = match locate_and_load(request) {
        Ok(loaded) => return Ok(loaded),
        Err(error) => error,
    };

    // An explicit runtime path is authoritative: never substitute a download.
    if request.explicit.is_some() {
        return Err(SdkLoadFailure::NotFound(not_found));
    }

    let source_spec = non_empty(fallback.source.as_deref())
        .map(str::to_owned)
        .or_else(|| env_var(request, SOURCE_ENV));
    let Some(source_spec) = source_spec else {
        return Err(SdkLoadFailure::NotFound(not_found));
    };

    let source = Source::parse(&source_spec).map_err(|err| SdkLoadFailure::Provision {
        code: "INVALID_ARGUMENT",
        message: format!(
            "invalid Bloomberg runtime source '{}': {err}",
            redact_user_info(&source_spec)
        ),
        rejected: not_found.rejected.clone(),
    })?;

    let version_raw = non_empty(fallback.version.as_deref())
        .map(str::to_owned)
        .or_else(|| env_var(request, VERSION_ENV));
    let version = match version_raw.as_deref() {
        None => VersionSpec::Latest,
        Some(raw) if raw.trim().eq_ignore_ascii_case("latest") => VersionSpec::Latest,
        Some(raw) => VersionSpec::exact(raw.trim()),
    };

    let platform =
        request
            .platform
            .or_else(Platform::host)
            .ok_or_else(|| SdkLoadFailure::Provision {
                code: "UNSUPPORTED_PLATFORM",
                message: format!(
                    "this platform has no Bloomberg runtime (supported: {})",
                    Platform::ALL.map(Platform::as_str).join(", ")
                ),
                rejected: not_found.rejected.clone(),
            })?;

    let cache_dir = resolve_cache_dir(request).ok_or_else(|| SdkLoadFailure::Provision {
        code: "INVALID_ARGUMENT",
        message: "cannot determine the xbbg cache directory; set XBBG_CACHE_DIR".into(),
        rejected: not_found.rejected.clone(),
    })?;

    let options = ProvisionOptions::new(cache_dir);
    let provisioned =
        run_provision_blocking(options, source, platform, version).map_err(|err| {
            SdkLoadFailure::Provision {
                code: err.code(),
                message: err.to_string(),
                rejected: not_found.rejected.clone(),
            }
        })?;

    load_provisioned(provisioned, not_found.rejected)
}

fn load_provisioned(
    provisioned: Provisioned,
    mut rejected: Vec<Rejected>,
) -> Result<LoadedSdk, SdkLoadFailure> {
    let install_dir = provisioned
        .lib_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or(&provisioned.lib_path)
        .display()
        .to_string();
    let mut slot = LOADED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(loaded) = slot.as_ref() {
        return Ok(loaded.clone());
    }
    match xbbg_core::load(&provisioned.lib_path) {
        Ok(()) => {
            let loaded = LoadedSdk {
                path: provisioned.lib_path,
                source: SourceKind::Cache,
                detail: install_dir,
                version: runtime_version(),
                rejected,
            };
            *slot = Some(loaded.clone());
            Ok(loaded)
        }
        Err(SdkLoadError::AlreadyLoaded { loaded, .. }) => {
            let adopted = LoadedSdk {
                path: loaded,
                source: SourceKind::Explicit,
                detail: "loaded before the search".to_owned(),
                version: runtime_version(),
                rejected,
            };
            *slot = Some(adopted.clone());
            Ok(adopted)
        }
        Err(error) => {
            let reason = load_failure(&error);
            rejected.push(Rejected {
                path: provisioned.lib_path.clone(),
                source: SourceKind::Cache,
                detail: install_dir,
                reason: reason.clone(),
            });
            Err(SdkLoadFailure::Provision {
                code: "INVALID_ARTIFACT",
                message: format!(
                    "provisioned runtime at {} could not be loaded: {reason}",
                    provisioned.lib_path.display()
                ),
                rejected,
            })
        }
    }
}

/// [`locate_and_load_or_provision`] with JSON on both sides, for hosts like Node.
///
/// Accepts the same camelCase `LocateRequest` JSON as [`super::locate_and_load_json`], plus
/// optional `sdkSource` and `sdkVersion` fields.
pub fn locate_and_load_or_provision_json(request_json: &str) -> Result<String, String> {
    let mut raw: serde_json::Value = serde_json::from_str(request_json)
        .map_err(|error| super::failure_json(&format!("invalid locate request: {error}"), &[]))?;
    let mut fallback = ProvisionFallback::default();
    if let Some(obj) = raw.as_object_mut() {
        if let Some(source) = obj.remove("sdkSource") {
            fallback.source = source.as_str().map(str::to_owned);
        }
        if let Some(version) = obj.remove("sdkVersion") {
            fallback.version = version.as_str().map(str::to_owned);
        }
    }
    let request: LocateRequest = serde_json::from_value(raw)
        .map_err(|error| super::failure_json(&format!("invalid locate request: {error}"), &[]))?;
    match locate_and_load_or_provision(&request, &fallback) {
        Ok(loaded) => serde_json::to_string(&loaded).map_err(|error| {
            super::failure_json(&format!("cannot encode the loaded runtime: {error}"), &[])
        }),
        Err(SdkLoadFailure::NotFound(not_found)) => Err(super::failure_json(
            &not_found.to_string(),
            &not_found.rejected,
        )),
        Err(SdkLoadFailure::Provision {
            code,
            message,
            rejected,
        }) => Err(super::failure_json(
            &format!("failed to provision the Bloomberg runtime [{code}]: {message}"),
            &rejected,
        )),
    }
}

/// Runs [`xbbg_sdk::Provisioner::provision`] from a JSON request (async).
pub async fn provision_json_async(json: &str) -> Result<String, HostError> {
    let parsed = request::parse_json(json, false).map_err(HostError::argument)?;
    let provisioner = Provisioner::new(parsed.options).map_err(sdk_error_to_host)?;
    let installed = provisioner
        .provision(&parsed.source, parsed.platform, &parsed.version)
        .await
        .map_err(sdk_error_to_host)?;
    Ok(request::result_json(&installed))
}

/// Runs [`xbbg_sdk::blocking::Provisioner::provision`] from a JSON request (sync).
///
/// Safe to call whether or not the caller is already inside a Tokio runtime.
pub fn provision_json_blocking(json: &str) -> Result<String, HostError> {
    let parsed = request::parse_json(json, false).map_err(HostError::argument)?;
    let installed = run_provision_blocking(
        parsed.options,
        parsed.source,
        parsed.platform,
        parsed.version,
    )
    .map_err(sdk_error_to_host)?;
    Ok(request::result_json(&installed))
}

/// Lists valid provisioned runtimes in `cache_dir` (defaulting to [`default_cache_root`]) as JSON.
pub fn installed_json(
    cache_dir: Option<&Path>,
    platform: Option<&str>,
) -> Result<String, HostError> {
    let dir = match cache_dir {
        Some(path) if path.as_os_str().is_empty() => {
            return Err(HostError::argument("cacheDir must not be blank"));
        }
        Some(path) => path.to_path_buf(),
        None => default_cache_root().ok_or_else(|| {
            HostError::argument(
                "cannot determine the xbbg cache directory; pass cacheDir or set XBBG_CACHE_DIR",
            )
        })?,
    };
    let platform = match platform {
        None => None,
        Some(name) if name.trim().is_empty() => None,
        Some(name) => Some(
            name.trim()
                .parse::<Platform>()
                .map_err(|err| sdk_error_to_host(SdkError::from(err)))?,
        ),
    };
    let installs = xbbg_sdk::installed(&dir, platform).map_err(sdk_error_to_host)?;
    Ok(request::results_json(&installs))
}

/// Default shared xbbg cache root (`XBBG_CACHE_DIR` or platform user cache).
pub fn default_sdk_cache_root() -> Option<PathBuf> {
    default_cache_root()
}

/// Bloomberg's public PyPI simple index URL.
pub fn bloomberg_pypi_index() -> &'static str {
    BLOOMBERG_PYPI_INDEX
}

/// Converts an [`xbbg_sdk::Error`] into a [`HostError`] carrying the stable `SDK_*` code.
pub fn sdk_error_to_host(error: SdkError) -> HostError {
    let kind = match &error {
        SdkError::InvalidArgument(_) | SdkError::UnsupportedPlatform(_) => HostErrorKind::Argument,
        SdkError::NotFound(_) | SdkError::Offline(_) => HostErrorKind::SdkNotFound,
        SdkError::Cancelled => HostErrorKind::Cancelled,
        SdkError::Network(_)
        | SdkError::Integrity(_)
        | SdkError::InvalidArtifact(_)
        | SdkError::Io { .. }
        | SdkError::Internal(_) => HostErrorKind::Internal,
        _ => HostErrorKind::Internal,
    };
    let code = format!("SDK_{}", error.code());
    HostError::new(kind, error.to_string()).with_code(code)
}

fn run_provision_blocking(
    options: ProvisionOptions,
    source: Source,
    platform: Platform,
    version: VersionSpec,
) -> xbbg_sdk::Result<Provisioned> {
    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(move || {
                    let provisioner = xbbg_sdk::blocking::Provisioner::new(options)?;
                    provisioner.provision(&source, platform, &version)
                })
                .join()
                .map_err(|_| SdkError::Internal("provisioning thread panicked".into()))?
        })
    } else {
        let provisioner = xbbg_sdk::blocking::Provisioner::new(options)?;
        provisioner.provision(&source, platform, &version)
    }
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn env_var(request: &LocateRequest, name: &str) -> Option<String> {
    match &request.env {
        Some(map) => {
            if let Some(value) = map.get(name) {
                return non_empty(Some(value.as_str())).map(str::to_owned);
            }
            if cfg!(windows) {
                map.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(name))
                    .and_then(|(_, v)| non_empty(Some(v.as_str())))
                    .map(str::to_owned)
            } else {
                None
            }
        }
        None => std::env::var(name)
            .ok()
            .and_then(|v| non_empty(Some(v.as_str())).map(str::to_owned)),
    }
}

fn resolve_cache_dir(request: &LocateRequest) -> Option<PathBuf> {
    if let Some(root) = &request.cache_root {
        return Some(root.clone());
    }
    if let Some(dir) = env_var(request, "XBBG_CACHE_DIR") {
        return Some(PathBuf::from(dir));
    }
    default_cache_root()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn cli_run_reexport_serves_help() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        let code = cli_run("xbbg-sdk", &["--help".into()], &no_env, &mut out, &mut err);
        assert_eq!(code, 0);
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains(SOURCE_ENV));
        assert!(text.contains("xbbg-sdk cpp"));
        assert!(err.is_empty());
    }
}
