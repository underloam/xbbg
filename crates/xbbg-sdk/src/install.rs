//! Cache layout, cross-process install locks, atomic commits and the install listing.
//!
//! ```text
//! <cache>/
//!   http/uv/        uv's HTTP cache for PyPI index pages
//!   http/rattler/   rattler's repodata cache (shard indexes and shards)
//!   tmp/            downloads (dl-*) and staging directories (staging-*); same volume, so
//!                   renames are atomic
//!   locks/          one lock file per install directory
//!   sdk/<version>/<platform>/<pypi|conda|artifact>-<archive sha256[..12]>/
//!       lib/<blpapi3_64.dll | libblpapi3_64.so>
//!       licenses/...
//!       provenance.json   written last; directories without it are incomplete
//!       sources.json      other source labels that resolved to the same archive (optional)
//! ```

use std::cmp::Ordering;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::platform::Platform;
use crate::source::SourceKind;
use crate::wheel::safe_relative;

const PROVENANCE: &str = "provenance.json";
/// A JSON array of the source labels besides the provenance's own that resolved to an install's
/// archive (the same wheel on a mirror and on Bloomberg's index, say). Lookups by source match
/// them like the provenance label, so each source finds the install offline.
const SOURCES: &str = "sources.json";
/// Name prefixes of what [`Layout`] creates in `<cache>/tmp`; nothing else there is ever removed.
const TEMPORARY_PREFIXES: [&str; 2] = ["dl-", "staging-"];

/// A runtime installed in the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provisioned {
    /// Runtime version, e.g. `3.26.9.1`.
    pub version: String,
    /// Platform the library is built for.
    pub platform: Platform,
    /// Absolute path of the Bloomberg library.
    pub lib_path: PathBuf,
    /// Absolute paths of the licence files.
    pub license_paths: Vec<PathBuf>,
    /// Source label of the install: `pypi:<index>[#project]`, `conda:<channel url>[#package]`
    /// or `artifact:<url or path>` (never with credentials).
    pub source: String,
    /// URL of the archive (`file:` for local files), without credentials.
    pub artifact_url: String,
    /// SHA-256 of the archive (lowercase hex).
    pub artifact_sha256: String,
    /// SHA-256 of the library, verified against `RECORD`/`paths.json` and on every cache hit.
    pub lib_sha256: String,
    /// Whether the install already existed.
    pub cache_hit: bool,
}

/// `provenance.json`: the contract between the provisioner and every loader of the cache.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Provenance {
    pub(crate) version: String,
    pub(crate) platform: Platform,
    pub(crate) source: String,
    pub(crate) artifact_url: String,
    pub(crate) artifact_sha256: String,
    /// Relative, `/`-separated.
    pub(crate) lib: String,
    pub(crate) lib_sha256: String,
    /// Relative, `/`-separated.
    pub(crate) licenses: Vec<String>,
}

/// The directories of one cache root.
#[derive(Clone, Debug)]
pub(crate) struct Layout {
    root: PathBuf,
}

impl Layout {
    pub(crate) fn new(root: &Path) -> Result<Self> {
        let root = std::path::absolute(root).map_err(|err| {
            Error::io(format!("resolving cache directory {}", root.display()), err)
        })?;
        for dir in ["http", "tmp", "locks", "sdk"] {
            let path = root.join(dir);
            fs::create_dir_all(&path)
                .map_err(|err| Error::io(format!("creating {}", path.display()), err))?;
        }
        Ok(Self { root })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn http_dir(&self, stack: &str) -> PathBuf {
        self.root.join("http").join(stack)
    }

    pub(crate) fn install_dir(
        &self,
        version: &str,
        platform: Platform,
        kind: SourceKind,
        sha256: &str,
    ) -> PathBuf {
        self.root
            .join("sdk")
            .join(file_name_safe(version))
            .join(platform.as_str())
            .join(format!("{}-{}", kind.as_str(), &sha256[..12]))
    }

    pub(crate) fn staging(&self) -> Result<tempfile::TempDir> {
        tempfile::Builder::new()
            .prefix(TEMPORARY_PREFIXES[1])
            .tempdir_in(self.root.join("tmp"))
            .map_err(|err| {
                Error::io(
                    format!(
                        "creating a staging directory in {}",
                        self.root.join("tmp").display()
                    ),
                    err,
                )
            })
    }

    /// A temporary file that keeps `name`'s extension (conda tooling dispatches on it).
    pub(crate) fn temp_file(&self, name: &str) -> Result<tempfile::TempPath> {
        tempfile::Builder::new()
            .prefix(TEMPORARY_PREFIXES[0])
            .suffix(&format!("-{}", file_name_safe(name)))
            .tempfile_in(self.root.join("tmp"))
            .map(tempfile::NamedTempFile::into_temp_path)
            .map_err(|err| {
                Error::io(
                    format!(
                        "creating a download file in {}",
                        self.root.join("tmp").display()
                    ),
                    err,
                )
            })
    }

    /// Exclusive cross-process lock for one install directory, held until dropped.
    pub(crate) async fn lock(&self, install_dir: &Path) -> Result<InstallLock> {
        let relative = install_dir
            .strip_prefix(self.root.join("sdk"))
            .unwrap_or(install_dir);
        let key = file_name_safe(&relative.to_string_lossy().replace(['/', '\\'], "-"));
        let path = self.root.join("locks").join(format!("{key}.lock"));
        tokio::task::spawn_blocking(move || {
            let file = fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .map_err(|err| Error::io(format!("opening lock {}", path.display()), err))?;
            // LockFileEx on Windows, flock elsewhere; also excludes other handles in this process.
            file.lock()
                .map_err(|err| Error::io(format!("locking {}", path.display()), err))?;
            Ok(InstallLock { _file: file })
        })
        .await
        .map_err(|err| Error::Internal(format!("lock task failed: {err}")))?
    }

    /// Best-effort removal of the downloads and staging directories that crashed runs left in
    /// `<cache>/tmp`. Only entries named like the ones this layout creates are touched (the cache
    /// directory is caller input), and links are removed without being followed.
    pub(crate) fn remove_stale_temporaries(&self, older_than: Duration) {
        let Ok(entries) = fs::read_dir(self.root.join("tmp")) else {
            return;
        };
        let Some(cutoff) = SystemTime::now().checked_sub(older_than) else {
            return;
        };
        for entry in entries.flatten() {
            let ours = entry.file_name().to_str().is_some_and(|name| {
                TEMPORARY_PREFIXES
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
            });
            if !ours {
                continue;
            }
            let path = entry.path();
            let Ok(meta) = fs::symlink_metadata(&path) else {
                continue;
            };
            if !meta.modified().is_ok_and(|modified| modified < cutoff) {
                continue;
            }
            let _ = if meta.is_symlink() {
                // A directory link is removed with `remove_dir` on Windows; neither follows it.
                fs::remove_file(&path).or_else(|_| fs::remove_dir(&path))
            } else if meta.is_dir() {
                fs::remove_dir_all(&path)
            } else {
                fs::remove_file(&path)
            };
        }
    }
}

/// Held while one install directory is checked, downloaded and committed.
pub(crate) struct InstallLock {
    _file: fs::File,
}

/// What an install directory holds.
pub(crate) enum InstallState {
    Missing,
    /// Present but unusable (no provenance, or a library that no longer matches it).
    Invalid,
    Valid(Provisioned),
}

/// Reads and verifies an install directory, re-hashing its library.
pub(crate) fn read_install(dir: &Path) -> InstallState {
    if !dir.is_dir() {
        return InstallState::Missing;
    }
    match load(dir) {
        Some(provisioned) => InstallState::Valid(provisioned),
        None => InstallState::Invalid,
    }
}

fn load(dir: &Path) -> Option<Provisioned> {
    let bytes = fs::read(dir.join(PROVENANCE)).ok()?;
    let provenance: Provenance = serde_json::from_slice(&bytes).ok()?;
    // Provenance paths are `/`-separated; join per component for native separators.
    let join = |relative: &str| {
        safe_relative(relative).ok().map(|relative| {
            relative
                .split('/')
                .fold(dir.to_path_buf(), |path, part| path.join(part))
        })
    };
    let lib_path = join(&provenance.lib)?;
    let actual = rattler_digest::compute_file_digest::<rattler_digest::Sha256>(&lib_path).ok()?;
    if hex::encode(actual) != provenance.lib_sha256 {
        return None;
    }
    let license_paths = provenance
        .licenses
        .iter()
        .map(|license| join(license))
        .collect::<Option<Vec<_>>>()?;
    Some(Provisioned {
        version: provenance.version,
        platform: provenance.platform,
        lib_path,
        license_paths,
        source: provenance.source,
        artifact_url: provenance.artifact_url,
        artifact_sha256: provenance.artifact_sha256,
        lib_sha256: provenance.lib_sha256,
        cache_hit: true,
    })
}

/// The valid install in `dir` when it holds the archive `sha256`, with `source` recorded as one
/// of its labels. A different archive that shares the directory's 12-digit hash prefix is an
/// integrity error. The caller holds the [`InstallLock`] for `dir`.
pub(crate) fn existing_install(
    dir: &Path,
    sha256: &str,
    source: &str,
) -> Result<Option<Provisioned>> {
    match read_install(dir) {
        InstallState::Valid(existing) => {
            same_archive(dir, &existing, sha256)?;
            add_source(dir, &existing.source, source);
            Ok(Some(existing))
        }
        InstallState::Missing | InstallState::Invalid => Ok(None),
    }
}

fn same_archive(dir: &Path, existing: &Provisioned, sha256: &str) -> Result<()> {
    if existing.artifact_sha256 == sha256 {
        Ok(())
    } else {
        Err(Error::Integrity(format!(
            "{} already holds the archive with sha256 {}, not {sha256}; both share the install \
             directory's hash prefix",
            dir.display(),
            existing.artifact_sha256
        )))
    }
}

/// The labels in `dir`'s [`SOURCES`] file; none when it is missing or unreadable.
fn extra_sources(dir: &Path) -> Vec<String> {
    fs::read(dir.join(SOURCES))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// Records `source` as another label of the install in `dir` (whose provenance says
/// `provenance_source`). Best effort: the install is valid without it, only offline and
/// exact-pin lookups by that source miss it. The caller holds the [`InstallLock`] for `dir`.
pub(crate) fn add_source(dir: &Path, provenance_source: &str, source: &str) {
    if provenance_source == source {
        return;
    }
    let mut sources = extra_sources(dir);
    if sources.iter().any(|known| known == source) {
        return;
    }
    sources.push(source.to_string());
    sources.sort();
    let Ok(json) = serde_json::to_vec_pretty(&sources) else {
        return;
    };
    // Replaced atomically: lookups read the file without the lock.
    let Ok(mut file) = tempfile::NamedTempFile::new_in(dir) else {
        return;
    };
    if file
        .write_all(&json)
        .and_then(|()| file.as_file().sync_all())
        .is_ok()
    {
        let _ = file.persist(dir.join(SOURCES));
    }
}

/// Writes `provenance.json` into `staging` and renames it to `final_dir`. The caller holds the
/// [`InstallLock`] for `final_dir`.
pub(crate) fn commit(
    staging: tempfile::TempDir,
    final_dir: &Path,
    provenance: &Provenance,
) -> Result<Provisioned> {
    match read_install(final_dir) {
        // Another process committed first; installs are immutable.
        InstallState::Valid(existing) => {
            same_archive(final_dir, &existing, &provenance.artifact_sha256)?;
            add_source(final_dir, &existing.source, &provenance.source);
            return Ok(existing);
        }
        InstallState::Invalid => fs::remove_dir_all(final_dir).map_err(|err| {
            Error::io(
                format!("removing incomplete install {}", final_dir.display()),
                err,
            )
        })?,
        InstallState::Missing => {}
    }
    let json = serde_json::to_vec_pretty(provenance)
        .map_err(|err| Error::Internal(format!("serializing provenance: {err}")))?;
    let provenance_path = staging.path().join(PROVENANCE);
    fs::write(&provenance_path, json)
        .map_err(|err| Error::io(format!("writing {}", provenance_path.display()), err))?;
    if let Some(parent) = final_dir.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| Error::io(format!("creating {}", parent.display()), err))?;
    }
    let staged = staging.keep();
    if let Err(err) = rename(&staged, final_dir) {
        let _ = fs::remove_dir_all(&staged);
        return Err(Error::io(
            format!("moving {} to {}", staged.display(), final_dir.display()),
            err,
        ));
    }
    match read_install(final_dir) {
        InstallState::Valid(installed) => Ok(Provisioned {
            cache_hit: false,
            ..installed
        }),
        _ => Err(Error::Integrity(format!(
            "the install in {} failed verification right after it was written",
            final_dir.display()
        ))),
    }
}

/// `fs::rename`, retried on Windows while another process (typically a virus scanner looking at
/// the files just written) holds a handle that blocks it: up to 10 retries from 10 ms, doubling,
/// as uv does.
pub(crate) fn rename(from: &Path, to: &Path) -> io::Result<()> {
    let mut delay = Duration::from_millis(10);
    let mut retries = if cfg!(windows) { 10 } else { 0 };
    loop {
        match fs::rename(from, to) {
            Err(err) if retries > 0 && transient_rename_error(&err) => {
                retries -= 1;
                std::thread::sleep(delay);
                delay *= 2;
            }
            result => return result,
        }
    }
}

/// Access denied, a sharing violation or a lock violation: what an open handle causes.
fn transient_rename_error(err: &io::Error) -> bool {
    const ERROR_SHARING_VIOLATION: i32 = 32;
    const ERROR_LOCK_VIOLATION: i32 = 33;
    err.kind() == io::ErrorKind::PermissionDenied
        || matches!(
            err.raw_os_error(),
            Some(ERROR_SHARING_VIOLATION | ERROR_LOCK_VIOLATION)
        )
}

/// Every valid install under `cache_root` (optionally for one platform), newest version first.
/// Incomplete installs and libraries that no longer match their provenance are skipped.
pub fn installed(cache_root: &Path, platform: Option<Platform>) -> Result<Vec<Provisioned>> {
    Ok(installs(cache_root, platform)?
        .into_iter()
        .map(|(_, provisioned)| provisioned)
        .collect())
}

/// [`installed`] with each install's directory.
fn installs(cache_root: &Path, platform: Option<Platform>) -> Result<Vec<(PathBuf, Provisioned)>> {
    let sdk = cache_root.join("sdk");
    let mut found = Vec::new();
    for version_dir in subdirectories(&sdk)? {
        for platform_dir in subdirectories(&version_dir)? {
            let matches_platform = platform_dir
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| Platform::from_str(name).ok())
                .is_some_and(|found| platform.is_none_or(|wanted| wanted == found));
            if !matches_platform {
                continue;
            }
            for install in subdirectories(&platform_dir)? {
                if let InstallState::Valid(provisioned) = read_install(&install) {
                    found.push((install, provisioned));
                }
            }
        }
    }
    found.sort_by(|(a_dir, a), (b_dir, b)| {
        compare_versions(&b.version, &a.version)
            .then(a.platform.cmp(&b.platform))
            .then(a_dir.cmp(b_dir))
    });
    Ok(found)
}

/// The newest valid install from `source` for `platform` (exactly `version` if given, and not
/// older than `minimum`). Labels recorded in an install's [`SOURCES`] count as its source too.
pub(crate) fn find_by_source(
    cache_root: &Path,
    source: &str,
    platform: Platform,
    version: Option<&str>,
    minimum: Option<&uv_pep440::Version>,
) -> Result<Option<Provisioned>> {
    Ok(installs(cache_root, Some(platform))?
        .into_iter()
        .find(|(dir, install)| {
            version.is_none_or(|version| same_version(&install.version, version))
                && minimum.is_none_or(|minimum| {
                    uv_pep440::Version::from_str(&install.version)
                        .is_ok_and(|version| version >= *minimum)
                })
                && (install.source == source
                    || extra_sources(dir).iter().any(|known| known == source))
        })
        .map(|(_, install)| install))
}

/// The directory and install of the archive with `sha256` for `platform`.
pub(crate) fn find_by_archive(
    cache_root: &Path,
    kind: SourceKind,
    platform: Platform,
    sha256: &str,
) -> Result<Option<(PathBuf, Provisioned)>> {
    let prefix = format!("{}-{}", kind.as_str(), &sha256[..12]);
    for version_dir in subdirectories(&cache_root.join("sdk"))? {
        let dir = version_dir.join(platform.as_str()).join(&prefix);
        if let InstallState::Valid(install) = read_install(&dir)
            && install.artifact_sha256 == sha256
        {
            return Ok(Some((dir, install)));
        }
    }
    Ok(None)
}

fn subdirectories(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(Error::io(format!("listing {}", dir.display()), err)),
    };
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| Error::io(format!("listing {}", dir.display()), err))?;
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            dirs.push(entry.path());
        }
    }
    Ok(dirs)
}

/// PEP 440 order where both sides parse (PyPI and conda-forge versions do), else text order.
pub(crate) fn compare_versions(a: &str, b: &str) -> Ordering {
    match (
        uv_pep440::Version::from_str(a),
        uv_pep440::Version::from_str(b),
    ) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

fn same_version(installed: &str, wanted: &str) -> bool {
    installed == wanted || compare_versions(installed, wanted) == Ordering::Equal
}

/// Replaces characters that are unsafe in file names (and never yields `.` or `..`).
fn file_name_safe(text: &str) -> String {
    let safe: String = text
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '+') {
                c
            } else {
                '_'
            }
        })
        .collect();
    if safe.chars().all(|c| c == '.') {
        "_".repeat(safe.len().max(1))
    } else {
        safe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_order_by_pep440_then_text() {
        assert_eq!(compare_versions("3.26.10.1", "3.26.9.1"), Ordering::Greater);
        assert_eq!(compare_versions("3.26.9.1", "3.26.9.1.0"), Ordering::Equal);
        assert!(same_version("3.26.9.1", "3.26.09.1"));
        assert!(!same_version("3.26.9.1", "3.26.9"));
    }

    #[test]
    fn file_names_are_sanitized() {
        assert_eq!(file_name_safe("3.26.9.1"), "3.26.9.1");
        assert_eq!(file_name_safe("1!2.0/../x"), "1_2.0_.._x");
    }

    #[test]
    fn extra_sources_are_recorded_once_and_sorted() {
        let dir = tempfile::tempdir().unwrap();
        assert!(extra_sources(dir.path()).is_empty());
        add_source(dir.path(), "pypi:https://a/", "pypi:https://a/");
        assert!(!dir.path().join(SOURCES).exists(), "the provenance label");
        for source in ["pypi:https://c/", "pypi:https://b/", "pypi:https://c/"] {
            add_source(dir.path(), "pypi:https://a/", source);
        }
        assert_eq!(
            extra_sources(dir.path()),
            ["pypi:https://b/", "pypi:https://c/"]
        );
        fs::write(dir.path().join(SOURCES), b"not json").unwrap();
        assert!(
            extra_sources(dir.path()).is_empty(),
            "unreadable means none"
        );
    }

    #[test]
    fn transient_rename_errors_are_handle_conflicts_only() {
        assert!(transient_rename_error(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(transient_rename_error(&io::Error::from_raw_os_error(32)));
        assert!(!transient_rename_error(&io::Error::from(
            io::ErrorKind::NotFound
        )));
    }
}
