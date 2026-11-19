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
//!
//! The order is walked in stages, cheapest first: the rules that only look at the file system,
//! then the operating system's own search for the library's file name
//! (`xbbg_core::load_system`, where a runtime was found while it was still linked), and only then
//! the xbbg cache, whose installs are verified by hashing their libraries. A process that finds
//! its runtime the usual way never pays for the cache.

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use serde::Serialize;
use xbbg_core::SdkLoadError;
use xbbg_sdk::Platform;
pub use xbbg_sdk::locate::{Candidate, Hint, LocateRequest, Located, Rejected, SourceKind, locate};
use xbbg_sdk::locate::{Stage, locate_stage, platform_of};

/// [`Candidate::detail`] and [`Rejected::detail`] of the operating system's own search.
const SYSTEM_DETAIL: &str = "the operating system's library search";

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
/// holds no library is that failure too. Without an explicit path every rule is tried in order;
/// the cache is only read, and its libraries hashed, when nothing before it loaded.
///
/// A second call returns the runtime the first one loaded, whatever else it asks for, unless it
/// names an explicit path that does not resolve to that file: a process holds one runtime, so
/// that call fails with the loaded and requested paths instead of silently ignoring the request.
/// A runtime loaded by other means (`xbbg_core::load`) counts as loaded, with the rule that would
/// have found it if the search lists it.
///
/// # Errors
///
/// [`SdkNotFound`] when no candidate loads, or when a loaded runtime cannot be replaced by the
/// explicit path; it lists every refusal.
pub fn locate_and_load(request: &LocateRequest) -> Result<LoadedSdk, SdkNotFound> {
    let mut slot = LOADED.lock().unwrap_or_else(PoisonError::into_inner);
    if request.explicit.is_none()
        && let Some(loaded) = slot.as_ref()
    {
        return Ok(loaded.clone());
    }
    let already = slot
        .as_ref()
        .map(|loaded| loaded.path.clone())
        .or_else(|| xbbg_core::loaded_sdk_path().map(Path::to_path_buf));
    let loaded = match already {
        Some(path) => adopt(request, path, slot.as_ref())?,
        None => search_and_load(request)?,
    };
    *slot = Some(loaded.clone());
    Ok(loaded)
}

/// Describes the runtime this process already holds, as `request` sees it.
fn adopt(
    request: &LocateRequest,
    path: PathBuf,
    known: Option<&LoadedSdk>,
) -> Result<LoadedSdk, SdkNotFound> {
    // An explicit path is authoritative, so it has to name the loaded file; only a runtime loaded
    // by other means needs the whole search, to say which rule would have found it.
    let located = match (&request.explicit, known) {
        (Some(_), _) => locate_stage(request, Stage::Explicit),
        (None, Some(known)) => return Ok(known.clone()),
        (None, None) => locate(request),
    };
    let found = located
        .candidates
        .iter()
        .find(|candidate| same_file(&candidate.path, &path));
    if let Some(explicit) = &request.explicit
        && found.is_none()
    {
        let requested = std::path::absolute(explicit).unwrap_or_else(|_| explicit.clone());
        return Err(SdkNotFound {
            rejected: vec![Rejected {
                reason: format!(
                    "a Bloomberg runtime is already loaded from {}; it cannot be replaced by {} \
                     in the same process",
                    path.display(),
                    requested.display()
                ),
                detail: requested.display().to_string(),
                path: requested,
                source: SourceKind::Explicit,
            }],
        });
    }
    if let Some(known) = known {
        return Ok(known.clone());
    }
    Ok(LoadedSdk {
        source: found.map_or(SourceKind::Explicit, |candidate| candidate.source),
        detail: found.map_or_else(
            || "loaded before the search".to_owned(),
            |candidate| candidate.detail.clone(),
        ),
        path,
        version: runtime_version(),
        rejected: located.rejected,
    })
}

/// Walks the search order and loads the first candidate that opens.
fn search_and_load(request: &LocateRequest) -> Result<LoadedSdk, SdkNotFound> {
    let mut walk = Walk::default();
    let found = if request.explicit.is_some() {
        // An explicit path is authoritative: when the host names one, no other rule may
        // substitute a different runtime for it.
        walk.stage(request, Stage::Explicit)
    } else {
        let mut found = walk.stage(request, Stage::Local);
        if found.is_none() {
            found = walk.system(request);
        }
        if found.is_none() {
            found = walk.stage(request, Stage::Cache);
        }
        found
    };
    match found {
        Some(candidate) => Ok(LoadedSdk {
            path: candidate.path,
            source: candidate.source,
            detail: candidate.detail,
            version: runtime_version(),
            rejected: walk.rejected,
        }),
        None => Err(SdkNotFound {
            rejected: walk.rejected,
        }),
    }
}

/// One search in progress: what was refused so far, and which files were already tried.
#[derive(Default)]
struct Walk {
    rejected: Vec<Rejected>,
    tried: HashSet<String>,
}

impl Walk {
    /// Tries the candidates of one stage in order; the one that loaded, if any.
    fn stage(&mut self, request: &LocateRequest, stage: Stage) -> Option<Candidate> {
        let located = locate_stage(request, stage);
        self.rejected.extend(located.rejected);
        for candidate in located.candidates {
            // A later stage can name a file an earlier one already failed to load.
            if !self.tried.insert(file_key(&candidate.path)) {
                continue;
            }
            match xbbg_core::load(&candidate.path) {
                Ok(()) => return Some(candidate),
                Err(error) => self.rejected.push(Rejected {
                    path: candidate.path,
                    source: candidate.source,
                    detail: candidate.detail,
                    reason: load_failure(&error),
                }),
            }
        }
        None
    }

    /// Rule 9: lets the operating system find the library by its file name.
    fn system(&mut self, request: &LocateRequest) -> Option<Candidate> {
        // The operating system can only search for this process's own platform.
        let foreign = request
            .platform
            .is_some_and(|platform| Platform::host() != Some(platform));
        if request.skip_system || foreign {
            return None;
        }
        let name = platform_of(request).library_name();
        match xbbg_core::load_system(name) {
            Ok(path) => Some(Candidate {
                path: path.to_path_buf(),
                source: SourceKind::System,
                detail: SYSTEM_DETAIL.to_owned(),
                version: None,
            }),
            Err(error) => {
                // A library the system found and that was refused is named by its own path;
                // one it did not find, by the file name that was searched for.
                let path = match &error {
                    SdkLoadError::Open { path, .. }
                    | SdkLoadError::MissingSymbol { path, .. }
                    | SdkLoadError::Substituted { path, .. } => path.clone(),
                    _ => PathBuf::from(name),
                };
                self.rejected.push(Rejected {
                    path,
                    source: SourceKind::System,
                    detail: SYSTEM_DETAIL.to_owned(),
                    reason: load_failure(&error),
                });
                None
            }
        }
    }
}

/// [`locate_and_load`] with JSON on both sides, for hosts that cannot share Rust types.
///
/// `request_json` is a [`LocateRequest`] in camelCase (`explicit`, `hints`, `env`, `cacheRoot`,
/// `repoRoot`, `platform`, `skipSystem`; unknown fields are an error). `Ok` is a [`LoadedSdk`] as
/// camelCase JSON. `Err` is always the JSON object `{"message": <text>, "rejected": [...]}`, with
/// an empty `rejected` when the request itself was malformed.
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
        SdkLoadError::MissingSymbol { message, .. } => format!("lacks {message}"),
        other => other.to_string(),
    }
}

/// The loaded runtime's `a.b.c.d`.
fn runtime_version() -> String {
    let (major, minor, patch, build) = xbbg_core::sdk_version();
    format!("{major}.{minor}.{patch}.{build}")
}

/// Same path, or two spellings (links, `..`, and on Windows letter case) of one file.
fn same_file(a: &Path, b: &Path) -> bool {
    let same = |a: &Path, b: &Path| {
        a == b
            || (cfg!(windows)
                && a.to_string_lossy()
                    .eq_ignore_ascii_case(&b.to_string_lossy()))
    };
    same(a, b)
        || matches!(
            (a.canonicalize(), b.canonicalize()),
            (Ok(a), Ok(b)) if same(&a, &b)
        )
}

/// One key per file, whatever its spelling: the canonical path, case-folded on Windows.
fn file_key(path: &Path) -> String {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let key = canonical.to_string_lossy().into_owned();
    if cfg!(windows) {
        key.to_lowercase()
    } else {
        key
    }
}
