//! Finds and loads the Bloomberg runtime for a host.
//!
//! [`xbbg_sdk::locate`] owns the one search order; this module walks its candidates and opens the
//! first one `xbbg_core::load` accepts. A host calls [`locate_and_load`] (or
//! [`locate_and_load_json`] across a JNI boundary) right after its native module loads, passing
//! only what it alone knows: an explicit path, directories to try, its view of the environment.
//!
//! A candidate that cannot be loaded (not a library, too old for the functions this build calls,
//! wrong architecture) becomes a [`Rejected`] entry with the loader's message and the search goes
//! on, so one stale `BLPAPI_ROOT` does not hide a good Terminal install.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use serde::Serialize;
use xbbg_core::SdkLoadError;
pub use xbbg_sdk::locate::{Candidate, Hint, LocateRequest, Located, Rejected, SourceKind, locate};

/// The runtime a host ended up with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadedSdk {
    /// The library that was loaded.
    pub path: PathBuf,
    /// The search rule that found it.
    pub source: SourceKind,
    /// What that rule searched: the variable's name, the hint's label, the Terminal root, ...
    pub detail: String,
    /// The runtime's version, `a.b.c.d`.
    pub version: String,
    /// Places and libraries that were refused on the way, in the order they were met.
    pub rejected: Vec<Rejected>,
}

/// No candidate could be loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SdkNotFound {
    /// Every place that was searched and refused, with the reason.
    pub rejected: Vec<Rejected>,
}

impl fmt::Display for SdkNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("no Bloomberg runtime library could be found or loaded")?;
        if self.rejected.is_empty() {
            return f.write_str(
                " (nothing to search: no path was given and no default location holds one)",
            );
        }
        f.write_str(":")?;
        for refused in &self.rejected {
            write!(
                f,
                "\n  {} ({}): {}: {}",
                refused.source,
                refused.detail,
                refused.path.display(),
                refused.reason
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for SdkNotFound {}

/// The first successful load of this process, so later calls report the same runtime.
static LOADED: Mutex<Option<LoadedSdk>> = Mutex::new(None);

/// Searches for the runtime as `request` says and loads the first candidate that opens.
///
/// An explicit path ([`LocateRequest::explicit`]) is authoritative: only the candidates it yields
/// are tried, and when none loads the call fails, naming that path and why, instead of falling
/// through to the other rules and loading a runtime the caller did not name. A directory that
/// holds no library is that failure too. Without an explicit path every rule is tried in order.
///
/// A second call, with any request, returns the runtime the first one loaded. A runtime loaded by
/// other means (`xbbg_core::load`) is returned too, with the rule that would have found it if the
/// search lists it.
///
/// # Errors
///
/// [`SdkNotFound`] when no candidate loads; it lists every refusal.
pub fn locate_and_load(request: &LocateRequest) -> Result<LoadedSdk, SdkNotFound> {
    let mut slot = LOADED.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(loaded) = slot.as_ref() {
        return Ok(loaded.clone());
    }

    // An explicit path is authoritative: when the host names one, no other rule may substitute a
    // different runtime for it.
    let explicit_only = request.explicit.is_some();
    let keep = |source: SourceKind| !explicit_only || source == SourceKind::Explicit;
    let located = locate(request);
    let candidates: Vec<Candidate> = located
        .candidates
        .into_iter()
        .filter(|candidate| keep(candidate.source))
        .collect();
    let mut rejected: Vec<Rejected> = located
        .rejected
        .into_iter()
        .filter(|refused| keep(refused.source))
        .collect();

    if let Some(path) = xbbg_core::loaded_sdk_path() {
        let found = candidates
            .iter()
            .find(|candidate| same_file(&candidate.path, path));
        let loaded = LoadedSdk {
            path: path.to_path_buf(),
            source: found.map_or(SourceKind::Explicit, |candidate| candidate.source),
            detail: found.map_or_else(
                || "loaded before the search".to_owned(),
                |candidate| candidate.detail.clone(),
            ),
            version: runtime_version(),
            rejected,
        };
        *slot = Some(loaded.clone());
        return Ok(loaded);
    }

    for candidate in candidates {
        match xbbg_core::load(&candidate.path) {
            Ok(()) => {
                let loaded = LoadedSdk {
                    path: candidate.path,
                    source: candidate.source,
                    detail: candidate.detail,
                    version: runtime_version(),
                    rejected,
                };
                *slot = Some(loaded.clone());
                return Ok(loaded);
            }
            Err(error) => rejected.push(Rejected {
                path: candidate.path,
                source: candidate.source,
                detail: candidate.detail,
                reason: load_failure(&error),
            }),
        }
    }
    Err(SdkNotFound { rejected })
}

/// [`locate_and_load`] with JSON on both sides, for hosts that cannot share Rust types.
///
/// `request_json` is a [`LocateRequest`] in camelCase (`explicit`, `hints`, `env`, `cacheRoot`,
/// `repoRoot`, `platform`; unknown fields are an error). `Ok` is a [`LoadedSdk`] as camelCase JSON.
/// `Err` is always the JSON object `{"message": <text>, "rejected": [...]}`, with an empty
/// `rejected` when the request itself was malformed.
pub fn locate_and_load_json(request_json: &str) -> Result<String, String> {
    let request: LocateRequest = serde_json::from_str(request_json)
        .map_err(|error| failure_json(&format!("invalid locate request: {error}"), &[]))?;
    match locate_and_load(&request) {
        Ok(loaded) => serde_json::to_string(&loaded).map_err(|error| {
            failure_json(&format!("cannot encode the loaded runtime: {error}"), &[])
        }),
        Err(not_found) => Err(failure_json(&not_found.to_string(), &not_found.rejected)),
    }
}

/// [`locate`] with JSON on both sides: the search without loading, for reporting what a host
/// would pick (`get_sdk_info`, diagnostics).
///
/// `Ok` is a [`Located`] as camelCase JSON (`candidates`, `rejected`). `Err` is the same
/// `{"message", "rejected"}` object as [`locate_and_load_json`], for a malformed request.
pub fn locate_json(request_json: &str) -> Result<String, String> {
    let request: LocateRequest = serde_json::from_str(request_json)
        .map_err(|error| failure_json(&format!("invalid locate request: {error}"), &[]))?;
    serde_json::to_string(&locate(&request))
        .map_err(|error| failure_json(&format!("cannot encode the search result: {error}"), &[]))
}

fn failure_json(message: &str, rejected: &[Rejected]) -> String {
    #[derive(Serialize)]
    struct Failure<'a> {
        message: &'a str,
        rejected: &'a [Rejected],
    }
    serde_json::to_string(&Failure { message, rejected })
        .unwrap_or_else(|_| r#"{"message":"cannot encode the failure","rejected":[]}"#.to_owned())
}

/// The loader's own words, without repeating the path the list line already shows.
fn load_failure(error: &SdkLoadError) -> String {
    match error {
        SdkLoadError::Open { message, .. } => message.clone(),
        SdkLoadError::MissingSymbol { message, .. } => {
            format!("lacks a required function: {message}")
        }
        other => other.to_string(),
    }
}

/// The loaded runtime's `a.b.c.d`.
fn runtime_version() -> String {
    let (major, minor, patch, build) = xbbg_core::sdk_version();
    format!("{major}.{minor}.{patch}.{build}")
}

fn same_file(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (a.canonicalize(), b.canonicalize()),
            (Ok(a), Ok(b)) if a == b
        )
}
