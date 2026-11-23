//! Stage Bloomberg's C++ SDK (headers and libraries) for local source builds.
//!
//! Replaces the legacy shell/PowerShell `sdktool` scripts with one cross-platform
//! Rust implementation shared by the `xbbg-sdk cpp` CLI. Uses the same `http`
//! stack as the runtime provisioner (rustls with the OS trust store, explicit
//! proxy/netrc, retries) and guards archive extraction against path traversal.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive as TarArchive;
use url::Url;
use zip::ZipArchive;

use crate::cache::compare_versions;
use crate::error::{Error, Result, from_middleware, from_reqwest};
use crate::http::{Http, fetch};
use crate::options::NetworkOptions;
use crate::platform::Platform;
use crate::provenance::safe_relative;

/// Bloomberg's public PyPI simple page for `blpapi` (used to discover versions).
pub const BLOOMBERG_PYPI_BLPAPI_INDEX: &str =
    "https://blpapi.bloomberg.com/repository/releases/python/simple/blpapi/";

/// Base URL serving Bloomberg's raw C++ SDK archives.
pub const BLOOMBERG_CPP_RAW_BASE: &str =
    "https://blpapi.bloomberg.com/download/releases/raw/files/";

/// Default staging directory relative to the repository root.
pub const DEFAULT_VENDOR_DIR: &str = "vendor/blpapi-sdk";

/// Default `.env` file updated with `BLPAPI_ROOT`.
pub const DEFAULT_ENV_FILE: &str = ".env";

/// Archive container format for a Bloomberg C++ SDK release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveKind {
    TarGz,
    Zip,
}

/// Options for staging a Bloomberg C++ SDK into `vendor_dir/<version>`.
#[derive(Clone, Debug)]
pub struct StageOptions {
    /// Specific version to stage, or `None` to resolve the latest downloadable release.
    pub version: Option<String>,
    /// Target platform (defaults to host platform).
    pub platform: Platform,
    /// Directory containing staged versions and `.cache/` (default: `vendor/blpapi-sdk`).
    pub vendor_dir: PathBuf,
    /// `.env` file path updated when `set_active` is `true` (default: `.env`).
    pub env_file: PathBuf,
    /// Write `BLPAPI_ROOT=<vendor_dir>/<version>` to `env_file` (default: `true`).
    pub set_active: bool,
    /// Re-download and re-extract even if the version is already staged.
    pub force: bool,
    /// Override the PyPI simple index page URL used for latest-version discovery.
    pub index_url: String,
    /// Override the base URL for `blpapi_cpp_<version>-<platform>` archives.
    pub raw_base_url: String,
    /// Network settings (proxy, netrc, timeouts, retries, TLS).
    pub network: NetworkOptions,
}

impl StageOptions {
    pub fn new(platform: Platform) -> Self {
        Self {
            version: None,
            platform,
            vendor_dir: PathBuf::from(DEFAULT_VENDOR_DIR),
            env_file: PathBuf::from(DEFAULT_ENV_FILE),
            set_active: true,
            force: false,
            index_url: BLOOMBERG_PYPI_BLPAPI_INDEX.to_owned(),
            raw_base_url: BLOOMBERG_CPP_RAW_BASE.to_owned(),
            network: NetworkOptions::default(),
        }
    }
}

/// Validates a 3- or 4-part numeric Bloomberg SDK version (`3.26.5.1`).
pub fn validate_version(version: &str) -> Result<String> {
    let trimmed = version.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidArgument("version is required".into()));
    }
    let parts: Vec<&str> = trimmed.split('.').collect();
    if !(3..=4).contains(&parts.len())
        || parts
            .iter()
            .any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(Error::InvalidArgument(format!(
            "invalid version format '{trimmed}': expected 3 or 4 numeric segments (e.g. 3.26.5.1)"
        )));
    }
    Ok(trimmed.to_owned())
}

/// Archive file name and format for `version` on `platform`.
pub fn archive_for_platform(version: &str, platform: Platform) -> (String, ArchiveKind) {
    match platform {
        Platform::LinuxX86_64 => (
            format!("blpapi_cpp_{version}-linux.tar.gz"),
            ArchiveKind::TarGz,
        ),
        Platform::MacosAarch64 => (
            format!("blpapi_cpp_{version}-macos-arm64.tar.gz"),
            ArchiveKind::TarGz,
        ),
        Platform::WindowsX86_64 => (
            format!("blpapi_cpp_{version}-windows.zip"),
            ArchiveKind::Zip,
        ),
    }
}

/// Extracts unique `blpapi-<ver>` numeric versions from PEP 503 HTML, newest first.
pub fn extract_versions_from_index(html: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let bytes = html.as_bytes();
    let needle = b"blpapi-";
    let mut pos = 0;
    while pos + needle.len() <= bytes.len() {
        let Some(offset) = html[pos..].find("blpapi-") else {
            break;
        };
        let start = pos + offset + needle.len();
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b'.') {
            end += 1;
        }
        let candidate = html[start..end].trim_end_matches('.');
        let rest = &html[start + candidate.len()..];
        let valid_boundary = rest.starts_with(".tar.gz")
            || rest.starts_with('-')
            || rest.starts_with(".zip")
            || rest.starts_with('"')
            || rest.starts_with('\'');
        if valid_boundary && validate_version(candidate).is_ok() {
            seen.insert(candidate.to_owned());
        }
        pos = end.max(start);
    }
    let mut versions: Vec<String> = seen.into_iter().collect();
    versions.sort_by(|a, b| compare_versions(b, a));
    versions
}

/// Lists staged version directories under `vendor_dir`, oldest first.
pub fn list_installed(vendor_dir: &Path) -> Result<Vec<String>> {
    if !vendor_dir.exists() {
        return Ok(Vec::new());
    }
    let mut versions = Vec::new();
    let entries = fs::read_dir(vendor_dir)
        .map_err(|err| Error::io(format!("reading {}", vendor_dir.display()), err))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| Error::io(format!("reading entry in {}", vendor_dir.display()), err))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str()
            && validate_version(name).is_ok()
        {
            versions.push(name.to_owned());
        }
    }
    versions.sort_by(|a, b| compare_versions(a, b));
    Ok(versions)
}

/// Reads the active SDK version from a `BLPAPI_ROOT=.../<version>` line in `env_file`.
pub fn active_version(env_file: &Path) -> Option<String> {
    let content = fs::read_to_string(env_file).ok()?;
    for line in content.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("BLPAPI_ROOT") else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(value) = rest.strip_prefix('=') else {
            continue;
        };
        let value = value.trim().trim_matches('"').trim_matches('\'');
        let last = value.rsplit(['/', '\\']).find(|seg| !seg.is_empty())?;
        if validate_version(last).is_ok() {
            return Some(last.to_owned());
        }
    }
    None
}

/// Writes or replaces `BLPAPI_ROOT=<vendor_dir>/<version>` in `env_file`.
pub fn set_active_version(env_file: &Path, vendor_dir: &Path, version: &str) -> Result<String> {
    let existing = if env_file.exists() {
        fs::read_to_string(env_file)
            .map_err(|err| Error::io(format!("reading {}", env_file.display()), err))?
    } else {
        String::new()
    };
    let mut lines: Vec<String> = existing
        .lines()
        .filter(|line| !line.trim_start().starts_with("BLPAPI_ROOT="))
        .map(str::to_owned)
        .collect();
    let root_value = format!(
        "{}/{}",
        vendor_dir
            .to_string_lossy()
            .replace('\\', "/")
            .trim_end_matches('/'),
        version
    );
    let env_line = format!("BLPAPI_ROOT={root_value}");
    lines.push(env_line.clone());
    if let Some(parent) = env_file.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|err| Error::io(format!("creating {}", parent.display()), err))?;
    }
    let mut output = lines.join("\n");
    output.push('\n');
    fs::write(env_file, output)
        .map_err(|err| Error::io(format!("writing {}", env_file.display()), err))?;
    Ok(env_line)
}

/// Clears `BLPAPI_ROOT=.../<version>` from `env_file` (deleting the file if empty).
pub fn clear_active_version(env_file: &Path, version: &str) -> Result<bool> {
    if !env_file.exists() {
        return Ok(false);
    }
    let existing = fs::read_to_string(env_file)
        .map_err(|err| Error::io(format!("reading {}", env_file.display()), err))?;
    let suffix_slash = format!("/{version}");
    let suffix_backslash = format!("\\{version}");
    let mut removed = false;
    let mut kept = Vec::new();
    for line in existing.lines() {
        let trimmed = line.trim();
        let is_target = trimmed.starts_with("BLPAPI_ROOT=")
            && (trimmed.ends_with(&suffix_slash) || trimmed.ends_with(&suffix_backslash));
        if is_target {
            removed = true;
        } else {
            kept.push(line.to_owned());
        }
    }
    if !removed {
        return Ok(false);
    }
    if kept.is_empty() {
        fs::remove_file(env_file)
            .map_err(|err| Error::io(format!("removing {}", env_file.display()), err))?;
    } else {
        let mut output = kept.join("\n");
        output.push('\n');
        fs::write(env_file, output)
            .map_err(|err| Error::io(format!("writing {}", env_file.display()), err))?;
    }
    Ok(true)
}

/// Deletes cached C++ SDK archives under `<vendor_dir>/.cache/`.
pub fn clean_cache(vendor_dir: &Path) -> Result<Vec<String>> {
    let cache_dir = vendor_dir.join(".cache");
    if !cache_dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut removed = Vec::new();
    let entries = fs::read_dir(&cache_dir)
        .map_err(|err| Error::io(format!("reading {}", cache_dir.display()), err))?;
    for entry in entries {
        let entry = entry
            .map_err(|err| Error::io(format!("reading entry in {}", cache_dir.display()), err))?;
        let path = entry.path();
        if path.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            fs::remove_file(&path)
                .map_err(|err| Error::io(format!("removing {}", path.display()), err))?;
            removed.push(name);
        }
    }
    removed.sort();
    Ok(removed)
}

/// Removes `<vendor_dir>/<version>`, matching `.cache/blpapi_cpp_<version>-*`, and `.env` entry.
pub fn remove_version(
    vendor_dir: &Path,
    env_file: &Path,
    version: &str,
    out: &mut dyn Write,
) -> Result<()> {
    let version = validate_version(version)?;
    let version_dir = vendor_dir.join(&version);
    if !version_dir.is_dir() {
        return Err(Error::NotFound(format!(
            "version {version} is not installed under {}",
            vendor_dir.display()
        )));
    }
    fs::remove_dir_all(&version_dir)
        .map_err(|err| Error::io(format!("removing {}", version_dir.display()), err))?;
    let _ = writeln!(out, "Removed: {}", version_dir.display());

    let cache_dir = vendor_dir.join(".cache");
    let prefix = format!("blpapi_cpp_{version}-");
    if cache_dir.is_dir() {
        let entries = fs::read_dir(&cache_dir)
            .map_err(|err| Error::io(format!("reading {}", cache_dir.display()), err))?;
        for entry in entries {
            let entry = entry.map_err(|err| {
                Error::io(format!("reading entry in {}", cache_dir.display()), err)
            })?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&prefix) && entry.path().is_file() {
                fs::remove_file(entry.path()).map_err(|err| {
                    Error::io(format!("removing {}", entry.path().display()), err)
                })?;
                let _ = writeln!(out, "Removed: {name}");
            }
        }
    }

    if clear_active_version(env_file, &version)? {
        let _ = writeln!(out, "Cleared BLPAPI_ROOT from {}", env_file.display());
    }
    Ok(())
}

/// Extracts a `.tar.gz` or `.zip` C++ SDK archive safely into `dest_dir`, unwrapping a single
/// top-level archive folder.
pub fn extract_archive(archive_path: &Path, kind: ArchiveKind, dest_dir: &Path) -> Result<()> {
    let parent = dest_dir
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|err| Error::io(format!("creating {}", parent.display()), err))?;
    let staging = tempfile::Builder::new()
        .prefix(".xbbg-cpp-extract-")
        .tempdir_in(parent)
        .map_err(|err| Error::io(format!("creating staging dir in {}", parent.display()), err))?;

    match kind {
        ArchiveKind::TarGz => extract_tar_gz(archive_path, staging.path())?,
        ArchiveKind::Zip => extract_zip(archive_path, staging.path())?,
    }

    let mut top_entries = Vec::new();
    for entry in fs::read_dir(staging.path())
        .map_err(|err| Error::io("reading extracted staging directory", err))?
    {
        let entry = entry.map_err(|err| Error::io("reading staging entry", err))?;
        top_entries.push(entry.path());
    }

    let source_root = if top_entries.len() == 1 && top_entries[0].is_dir() {
        top_entries.remove(0)
    } else {
        staging.path().to_path_buf()
    };

    if dest_dir.exists() {
        fs::remove_dir_all(dest_dir)
            .map_err(|err| Error::io(format!("replacing {}", dest_dir.display()), err))?;
    }

    if source_root == staging.path() {
        let staged_path = staging.keep();
        fs::rename(&staged_path, dest_dir)
            .map_err(|err| Error::io(format!("installing {}", dest_dir.display()), err))?;
    } else {
        fs::rename(&source_root, dest_dir)
            .map_err(|err| Error::io(format!("installing {}", dest_dir.display()), err))?;
    }
    Ok(())
}

fn extract_tar_gz(archive_path: &Path, staging: &Path) -> Result<()> {
    let file = File::open(archive_path)
        .map_err(|err| Error::io(format!("opening {}", archive_path.display()), err))?;
    let mut archive = TarArchive::new(GzDecoder::new(file));
    let entries = archive
        .entries()
        .map_err(|err| Error::InvalidArtifact(format!("invalid tar.gz archive: {err}")))?;
    for entry in entries {
        let mut entry =
            entry.map_err(|err| Error::InvalidArtifact(format!("corrupt tar.gz entry: {err}")))?;
        let raw_path = entry
            .path()
            .map_err(|err| Error::InvalidArtifact(format!("invalid tar entry path: {err}")))?
            .into_owned();
        let entry_type = entry.header().entry_type();
        let raw_str = raw_path.to_string_lossy();
        // Directory entries end with a slash, as in the zip path below.
        let normalized = raw_str.trim_start_matches("./").trim_end_matches('/');
        if normalized.is_empty() || normalized == "." {
            continue;
        }
        let rel = safe_relative(normalized).ok_or_else(|| {
            Error::InvalidArtifact(format!(
                "tar archive entry escapes target directory: {raw_str}"
            ))
        })?;
        let target = staging.join(&rel);
        if entry_type.is_dir() {
            fs::create_dir_all(&target)
                .map_err(|err| Error::io(format!("creating {}", target.display()), err))?;
        } else if entry_type.is_file() {
            if let Some(dir) = target.parent() {
                fs::create_dir_all(dir)
                    .map_err(|err| Error::io(format!("creating {}", dir.display()), err))?;
            }
            let mut out_file = File::create(&target)
                .map_err(|err| Error::io(format!("creating {}", target.display()), err))?;
            io::copy(&mut entry, &mut out_file)
                .map_err(|err| Error::io(format!("extracting {}", target.display()), err))?;
            #[cfg(unix)]
            if let Ok(mode) = entry.header().mode() {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o777));
            }
        } else if entry_type.is_symlink() {
            let Some(link_target) = entry
                .link_name()
                .map_err(|err| Error::InvalidArtifact(format!("invalid tar symlink: {err}")))?
            else {
                continue;
            };
            let link_str = link_target.to_string_lossy();
            if link_target.is_absolute() {
                return Err(Error::InvalidArtifact(format!(
                    "tar archive symlink points outside target directory: {link_str}"
                )));
            }
            let base = Path::new(&rel).parent().unwrap_or_else(|| Path::new(""));
            let combined = base.join(&link_target);
            let mut depth = 0isize;
            for comp in combined.components() {
                match comp {
                    std::path::Component::Normal(_) => depth += 1,
                    std::path::Component::ParentDir => {
                        depth -= 1;
                        if depth < 0 {
                            return Err(Error::InvalidArtifact(format!(
                                "tar archive symlink escapes target directory: {link_str}"
                            )));
                        }
                    }
                    std::path::Component::CurDir => {}
                    _ => {
                        return Err(Error::InvalidArtifact(format!(
                            "tar archive symlink has unsafe component: {link_str}"
                        )));
                    }
                }
            }
            if let Some(dir) = target.parent() {
                fs::create_dir_all(dir)
                    .map_err(|err| Error::io(format!("creating {}", dir.display()), err))?;
            }
            #[cfg(unix)]
            std::os::unix::fs::symlink(&link_target, &target)
                .map_err(|err| Error::io(format!("creating symlink {}", target.display()), err))?;
        } else if entry_type.is_hard_link() {
            return Err(Error::InvalidArtifact(format!(
                "tar archive contains unsupported hard link: {raw_str}"
            )));
        }
    }
    Ok(())
}

fn extract_zip(archive_path: &Path, staging: &Path) -> Result<()> {
    let file = File::open(archive_path)
        .map_err(|err| Error::io(format!("opening {}", archive_path.display()), err))?;
    let mut archive = ZipArchive::new(file)
        .map_err(|err| Error::InvalidArtifact(format!("invalid zip archive: {err}")))?;
    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|err| Error::InvalidArtifact(format!("corrupt zip entry: {err}")))?;
        let raw = entry
            .name()
            .map_err(|err| Error::InvalidArtifact(format!("invalid zip entry name: {err}")))?
            .into_owned();
        if raw.is_empty() || raw == "." || raw == "./" {
            continue;
        }
        let is_dir = raw.ends_with('/');
        let trimmed = raw.trim_end_matches('/').trim_start_matches("./");
        if trimmed.is_empty() {
            continue;
        }
        let rel = safe_relative(trimmed).ok_or_else(|| {
            Error::InvalidArtifact(format!("zip archive entry escapes target directory: {raw}"))
        })?;
        let target = staging.join(rel);
        if is_dir {
            fs::create_dir_all(&target)
                .map_err(|err| Error::io(format!("creating {}", target.display()), err))?;
        } else {
            if let Some(dir) = target.parent() {
                fs::create_dir_all(dir)
                    .map_err(|err| Error::io(format!("creating {}", dir.display()), err))?;
            }
            let mut out_file = File::create(&target)
                .map_err(|err| Error::io(format!("creating {}", target.display()), err))?;
            io::copy(&mut entry, &mut out_file)
                .map_err(|err| Error::io(format!("extracting {}", target.display()), err))?;
            #[cfg(unix)]
            if let Some(mode) = entry.unix_mode() {
                use std::os::unix::fs::PermissionsExt;
                let _ = fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o777));
            }
        }
    }
    Ok(())
}

async fn resolve_latest_version(http: &Http, options: &StageOptions) -> Result<String> {
    let index_url = Url::parse(&options.index_url)
        .map_err(|err| Error::InvalidArgument(format!("invalid index URL: {err}")))?;
    let response = http
        .client
        .get(index_url.clone())
        .send()
        .await
        .map_err(|err| from_middleware(&index_url, err))?;
    let status = response.status();
    if !status.is_success() {
        return Err(Error::Network(format!(
            "HTTP {status} fetching Bloomberg SDK version index"
        )));
    }
    let body = response
        .text()
        .await
        .map_err(|err| from_reqwest(&index_url, err))?;
    let versions = extract_versions_from_index(&body);
    if versions.is_empty() {
        return Err(Error::NotFound(
            "failed to resolve latest Bloomberg C++ SDK version from index".into(),
        ));
    }
    let base = options.raw_base_url.trim_end_matches('/');
    for version in &versions {
        let (archive_name, _) = archive_for_platform(version, options.platform);
        let probe_url = format!("{base}/{archive_name}");
        if let Ok(url) = Url::parse(&probe_url)
            && let Ok(head) = http.client.head(url).send().await
            && head.status().is_success()
        {
            return Ok(version.clone());
        }
    }
    Ok(versions[0].clone())
}

/// Stages a Bloomberg C++ SDK release (async).
pub async fn stage_async(options: StageOptions, out: &mut dyn Write) -> Result<PathBuf> {
    let http = Http::new(&options.network)?;
    let version = match &options.version {
        Some(explicit) => validate_version(explicit)?,
        None => resolve_latest_version(&http, &options).await?,
    };
    let (archive_name, kind) = archive_for_platform(&version, options.platform);
    let version_dir = options.vendor_dir.join(&version);
    let cache_dir = options.vendor_dir.join(".cache");
    let archive_path = cache_dir.join(&archive_name);

    let _ = writeln!(out, "Bloomberg C++ SDK");
    let _ = writeln!(out, "  Version : {version}");
    let _ = writeln!(out, "  Platform: {}", options.platform.as_str());
    let _ = writeln!(out, "  Target  : {}", version_dir.display());

    if version_dir.is_dir() && !options.force {
        let _ = writeln!(out, "[OK] Version {version} is already present.");
        if options.set_active {
            let line = set_active_version(&options.env_file, &options.vendor_dir, &version)?;
            let _ = writeln!(out, "[OK] {} updated: {line}", options.env_file.display());
        }
        return Ok(version_dir);
    }

    fs::create_dir_all(&cache_dir)
        .map_err(|err| Error::io(format!("creating {}", cache_dir.display()), err))?;

    if archive_path.is_file() && !options.force {
        let _ = writeln!(out, "[OK] Using cached download: {archive_name}");
    } else {
        if archive_path.exists() {
            let _ = fs::remove_file(&archive_path);
        }
        let base = options.raw_base_url.trim_end_matches('/');
        let url_str = format!("{base}/{archive_name}");
        let url = Url::parse(&url_str).map_err(|err| {
            Error::InvalidArgument(format!("invalid archive URL '{url_str}': {err}"))
        })?;
        let _ = writeln!(out, "[..] Downloading {archive_name}");
        let temp = tempfile::NamedTempFile::new_in(&cache_dir)
            .map_err(|err| {
                Error::io(
                    format!("creating temp file in {}", cache_dir.display()),
                    err,
                )
            })?
            .into_temp_path();
        let fetched = fetch(&http, &url, temp).await?;
        if let Some(temp_path) = fetched._temp {
            temp_path.persist(&archive_path).map_err(|err| {
                Error::io(format!("saving {}", archive_path.display()), err.error)
            })?;
        } else {
            fs::copy(&fetched.path, &archive_path)
                .map_err(|err| Error::io(format!("copying {}", archive_path.display()), err))?;
        }
    }

    extract_archive(&archive_path, kind, &version_dir)?;

    if options.set_active {
        let line = set_active_version(&options.env_file, &options.vendor_dir, &version)?;
        let _ = writeln!(out, "[OK] {} updated: {line}", options.env_file.display());
    }

    let _ = writeln!(out, "[OK] Extracted to {}", version_dir.display());
    for rel in ["include", "lib", "Linux", "Darwin", "bin"] {
        let sub = version_dir.join(rel);
        if sub.exists() {
            let _ = writeln!(out, "  {rel}/ : {}", sub.display());
        }
    }
    Ok(version_dir)
}

/// Blocking wrapper for [`stage_async`], safe inside or outside a Tokio runtime.
pub fn stage_blocking(options: StageOptions, out: &mut dyn Write) -> Result<PathBuf> {
    let mut buf = Vec::new();
    let result = if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(move || -> Result<(PathBuf, Vec<u8>)> {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|err| Error::Internal(format!("tokio runtime: {err}")))?;
                    let mut local = Vec::new();
                    let dir = rt.block_on(stage_async(options, &mut local))?;
                    Ok((dir, local))
                })
                .join()
                .map_err(|_| Error::Internal("C++ SDK staging thread panicked".into()))?
        })
    } else {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Error::Internal(format!("tokio runtime: {err}")))?;
        let dir = rt.block_on(stage_async(options, &mut buf))?;
        Ok((dir, buf))
    };
    let (dir, captured) = result?;
    let _ = out.write_all(&captured);
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;

    #[test]
    fn validates_three_and_four_part_numeric_versions() {
        assert_eq!(validate_version("3.26.5.1").unwrap(), "3.26.5.1");
        assert_eq!(validate_version("3.25.12").unwrap(), "3.25.12");
        assert!(validate_version("").is_err());
        assert!(validate_version("3.26").is_err());
        assert!(validate_version("3.26.5.1.0").is_err());
        assert!(validate_version("../3.26.5.1").is_err());
        assert!(validate_version("3.26.5-rc1").is_err());
    }

    #[test]
    fn extracts_versions_newest_first_from_simple_html() {
        let html = r#"
            <a href="blpapi-3.24.6.1.tar.gz">blpapi-3.24.6.1.tar.gz</a>
            <a href="blpapi-3.26.10.1.tar.gz">blpapi-3.26.10.1.tar.gz</a>
            <a href="blpapi-3.26.2.1-cp312-cp312-manylinux.whl">whl</a>
            <a href="blpapi-3.26.10.1.tar.gz">dup</a>
        "#;
        assert_eq!(
            extract_versions_from_index(html),
            vec!["3.26.10.1", "3.26.2.1", "3.24.6.1"]
        );
    }

    #[test]
    fn env_file_set_get_and_clear_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let env_file = dir.path().join(".env");
        fs::write(&env_file, "FOO=bar\n").unwrap();

        let line =
            set_active_version(&env_file, Path::new("vendor/blpapi-sdk"), "3.26.5.1").unwrap();
        assert_eq!(line, "BLPAPI_ROOT=vendor/blpapi-sdk/3.26.5.1");
        assert_eq!(active_version(&env_file).as_deref(), Some("3.26.5.1"));

        set_active_version(&env_file, Path::new("vendor/blpapi-sdk"), "3.26.9.1").unwrap();
        assert_eq!(active_version(&env_file).as_deref(), Some("3.26.9.1"));
        let text = fs::read_to_string(&env_file).unwrap();
        assert!(text.contains("FOO=bar"));
        assert_eq!(text.matches("BLPAPI_ROOT=").count(), 1);

        assert!(!clear_active_version(&env_file, "3.26.5.1").unwrap());
        assert!(clear_active_version(&env_file, "3.26.9.1").unwrap());
        assert_eq!(active_version(&env_file), None);
        assert_eq!(fs::read_to_string(&env_file).unwrap(), "FOO=bar\n");
    }

    #[test]
    fn extracts_tar_gz_and_zip_and_rejects_traversal() {
        let tmp = tempfile::tempdir().unwrap();

        // Valid tar.gz with top-level directory unwrap. Real SDK archives list each
        // directory as its own entry, named with a trailing slash.
        let tgz = tmp.path().join("sdk.tar.gz");
        {
            let file = File::create(&tgz).unwrap();
            let enc = GzEncoder::new(file, Compression::default());
            let mut builder = tar::Builder::new(enc);
            for dir in ["blpapi_cpp_3.26.5.1/", "blpapi_cpp_3.26.5.1/include/"] {
                let mut header = tar::Header::new_gnu();
                header.set_entry_type(tar::EntryType::Directory);
                header.set_size(0);
                header.set_mode(0o755);
                builder.append_data(&mut header, dir, io::empty()).unwrap();
            }
            let data = b"/* header */";
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(
                    &mut header,
                    "blpapi_cpp_3.26.5.1/include/blpapi_defs.h",
                    &data[..],
                )
                .unwrap();
            builder.finish().unwrap();
        }
        let dest_tgz = tmp.path().join("out_tgz");
        extract_archive(&tgz, ArchiveKind::TarGz, &dest_tgz).unwrap();
        assert!(dest_tgz.join("include/blpapi_defs.h").is_file());

        // Valid zip with top-level directory unwrap
        let zip_path = tmp.path().join("sdk.zip");
        {
            let file = File::create(&zip_path).unwrap();
            let mut writer = zip::ZipWriter::new(file);
            let opts = zip::write::SimpleFileOptions::default();
            writer
                .start_file("blpapi_cpp_3.26.5.1/include/blpapi_defs.h", opts)
                .unwrap();
            writer.write_all(b"/* header */").unwrap();
            writer.finish().unwrap();
        }
        let dest_zip = tmp.path().join("out_zip");
        extract_archive(&zip_path, ArchiveKind::Zip, &dest_zip).unwrap();
        assert!(dest_zip.join("include/blpapi_defs.h").is_file());

        // Zip with path traversal is refused
        let bad_zip = tmp.path().join("bad.zip");
        {
            let file = File::create(&bad_zip).unwrap();
            let mut writer = zip::ZipWriter::new(file);
            let opts = zip::write::SimpleFileOptions::default();
            writer.start_file("../escape.txt", opts).unwrap();
            writer.write_all(b"nope").unwrap();
            writer.finish().unwrap();
        }
        let err =
            extract_archive(&bad_zip, ArchiveKind::Zip, &tmp.path().join("out_bad")).unwrap_err();
        assert_eq!(err.code(), "INVALID_ARTIFACT");
    }

    #[test]
    fn list_remove_and_clean_cache_work_end_to_end() {
        let tmp = tempfile::tempdir().unwrap();
        let vendor = tmp.path().join("vendor/blpapi-sdk");
        let cache = vendor.join(".cache");
        let env_file = tmp.path().join(".env");
        fs::create_dir_all(vendor.join("3.24.6.1/include")).unwrap();
        fs::create_dir_all(vendor.join("3.26.5.1/include")).unwrap();
        fs::create_dir_all(&cache).unwrap();
        fs::write(cache.join("blpapi_cpp_3.26.5.1-linux.tar.gz"), b"tgz").unwrap();
        fs::write(cache.join("blpapi_cpp_3.24.6.1-linux.tar.gz"), b"tgz").unwrap();
        set_active_version(&env_file, &vendor, "3.26.5.1").unwrap();

        assert_eq!(
            list_installed(&vendor).unwrap(),
            vec!["3.24.6.1", "3.26.5.1"]
        );

        let mut out = Vec::new();
        remove_version(&vendor, &env_file, "3.26.5.1", &mut out).unwrap();
        assert!(!vendor.join("3.26.5.1").exists());
        assert!(!cache.join("blpapi_cpp_3.26.5.1-linux.tar.gz").exists());
        assert!(cache.join("blpapi_cpp_3.24.6.1-linux.tar.gz").exists());
        assert_eq!(active_version(&env_file), None);

        let cleaned = clean_cache(&vendor).unwrap();
        assert_eq!(cleaned, vec!["blpapi_cpp_3.24.6.1-linux.tar.gz"]);
        assert!(clean_cache(&vendor).unwrap().is_empty());
    }
}
