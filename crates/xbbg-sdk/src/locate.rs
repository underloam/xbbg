//! Where on disk the Bloomberg runtime is: the one search order every xbbg host uses.
//!
//! [`locate`] lists the library files that might be the runtime, best first, and the places it
//! looked at and refused. It opens nothing: a host (through `xbbg_host::sdk::locate_and_load`)
//! tries to load each candidate in turn, so a library that exists but cannot be used (too old,
//! wrong architecture, not Bloomberg's) costs one rejection, not the whole search.
//!
//! # The order
//!
//! 1. [`LocateRequest::explicit`]: a library file, or a directory that holds one.
//! 2. `BLPAPI_LIB_DIR`: the directory that holds the library.
//! 3. `BLPAPI_ROOT`: a Bloomberg C++ SDK root (see below).
//! 4. `XBBG_DEV_SDK_ROOT`: an SDK root; a relative value is relative to
//!    [`LocateRequest::repo_root`].
//! 5. The vendored SDK, `<repo_root>/vendor/blpapi-sdk`, newest version first.
//! 6. [`LocateRequest::hints`], in the given order: directories only the host knows, such as the
//!    directory of the `blpapi` Python wheel.
//! 7. Bloomberg Terminal roots. Windows: `<SYSTEMDRIVE>\blp\DAPI` (and `C:\blp\DAPI`),
//!    `Bloomberg\Blp\DAPI` under `PROGRAMFILES` and `PROGRAMFILES(X86)` (and the same under
//!    `<drive>\Program Files`, `<drive>\Program Files (x86)`), and `Bloomberg\DAPI` and
//!    `Bloomberg\Blp\DAPI` under `LOCALAPPDATA` (else `USERPROFILE\AppData\Local`). Elsewhere:
//!    `~/blp/DAPI` and `/opt/bloomberg/DAPI`.
//! 8. The operating system's library search path: `PATH` on Windows, `LD_LIBRARY_PATH` on Linux,
//!    `DYLD_LIBRARY_PATH` then `DYLD_FALLBACK_LIBRARY_PATH` on macOS.
//! 9. The xbbg cache ([`LocateRequest::cache_root`], else `XBBG_CACHE_DIR`, else the default
//!    root): verified installs, best first. Installs that fail verification are
//!    [`Located::rejected`].
//!
//! # Layouts
//!
//! An *SDK root* (`BLPAPI_ROOT`, `XBBG_DEV_SDK_ROOT`, the vendored directory, an explicit
//! directory) is searched in the root itself and its usual layouts (`bin`, `lib`, `Lib`, `lib64`,
//! `lib/win64`, `lib/win32`, `Linux`, `Darwin`, ...), then the same under each subdirectory one
//! and two levels down, versioned ones (`3.26.5.1`) newest first, so a container of SDKs works as
//! well as one SDK. Hints and Terminal roots are searched in the directory and its layouts only;
//! `BLPAPI_LIB_DIR` and the search-path directories only directly.
//!
//! # File names
//!
//! [`Platform::library_names`] is the only list. Every copy of the old per-host searches
//! accepted a different subset, so it is their union that makes sense for one platform:
//! `blpapi3_32.dll` is not on it (a 32-bit library cannot load into the 64-bit processes this
//! workspace ships), and the `.dll`/`.dylib` names are not tried on other platforms.
//!
//! # Environment
//!
//! Every variable is read through [`LocateRequest::env`]: a host that already holds an environment
//! view (Node's `process.env`, a JVM that overlays system properties) passes it; `None` reads the
//! process environment. Empty variables count as unset. On Windows, names in a given view match
//! case-insensitively. A relative path is made absolute against the current directory (except
//! `XBBG_DEV_SDK_ROOT`, see above).
//!
//! Candidates are deduplicated by canonical file (case-folded on Windows) and keep the position of
//! their first occurrence. A directory that was named (explicitly, by a variable or by a hint) but
//! holds no library is reported in [`Located::rejected`], so "not found" can say what was tried.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::cache::{cache_root_from, compare_versions, scan_cache};
use crate::platform::Platform;

/// Directories inside an SDK root that hold the runtime, in the order they are tried: the root
/// itself, then the layouts of the Windows (`bin`, `lib/win64`), Linux (`Linux`) and macOS
/// (`Darwin`) SDK archives and of the packaged SDKs (`lib`, `Lib`, `lib64`).
const SDK_ROOT_SUBDIRS: &[&str] = &[
    "",
    "bin",
    "lib",
    "Lib",
    "lib64",
    "lib/win64",
    "lib/win32",
    "Linux",
    "linux",
    "Darwin",
    "darwin",
    "MacOS",
    "macos",
];

/// What to search and how; every field is optional.
///
/// `LocateRequest::default()` searches everything with the process environment.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase", deny_unknown_fields)]
pub struct LocateRequest {
    /// A library file or a directory that holds one, from the host's setter or property.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explicit: Option<PathBuf>,
    /// Host-only directories, searched after the vendored SDK and before the Terminal.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hints: Vec<Hint>,
    /// The host's view of the environment; `None` reads the process environment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<BTreeMap<String, String>>,
    /// The cache root, overriding `XBBG_CACHE_DIR` and the default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_root: Option<PathBuf>,
    /// A development checkout: enables `<repo_root>/vendor/blpapi-sdk` and is the base of a
    /// relative `XBBG_DEV_SDK_ROOT`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo_root: Option<PathBuf>,
    /// The platform to search for; the host platform by default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

/// A directory only the host knows about, such as `("blpapi_python", <wheel directory>)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Hint {
    /// What the directory is, for messages; it becomes [`Candidate::detail`].
    pub label: String,
    /// The directory.
    pub dir: PathBuf,
}

/// Which rule produced a candidate or a rejection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// [`LocateRequest::explicit`].
    Explicit,
    /// `BLPAPI_LIB_DIR`.
    BlpapiLibDir,
    /// `BLPAPI_ROOT`.
    BlpapiRoot,
    /// `XBBG_DEV_SDK_ROOT`.
    DevSdkRoot,
    /// `<repo_root>/vendor/blpapi-sdk`.
    Vendored,
    /// One of [`LocateRequest::hints`].
    Hint,
    /// A Bloomberg Terminal installation.
    Terminal,
    /// A directory of the operating system's library search path.
    SearchPath,
    /// The xbbg cache.
    Cache,
}

impl SourceKind {
    /// The serialized name, e.g. `blpapi_lib_dir`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::BlpapiLibDir => "blpapi_lib_dir",
            Self::BlpapiRoot => "blpapi_root",
            Self::DevSdkRoot => "dev_sdk_root",
            Self::Vendored => "vendored",
            Self::Hint => "hint",
            Self::Terminal => "terminal",
            Self::SearchPath => "search_path",
            Self::Cache => "cache",
        }
    }
}

impl std::fmt::Display for SourceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A library file that might be the runtime.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Candidate {
    /// The absolute path of the library file.
    pub path: PathBuf,
    /// The rule that found it.
    pub source: SourceKind,
    /// What the rule searched: the explicit path, the variable's name, the hint's label, the
    /// Terminal root, the vendored directory or the cache install directory.
    pub detail: String,
    /// The runtime's version when the source records it (cache installs); otherwise `None`.
    pub version: Option<String>,
}

/// A place that was searched or a library that was refused.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejected {
    /// The directory, library file or cache install directory.
    pub path: PathBuf,
    /// The rule that named it.
    pub source: SourceKind,
    /// As [`Candidate::detail`].
    pub detail: String,
    /// Why it cannot be used, for people.
    pub reason: String,
}

/// The result of a search.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Located {
    /// Library files to try, best first, without duplicates.
    pub candidates: Vec<Candidate>,
    /// Places that were named but hold no usable runtime, in the order they were met.
    pub rejected: Vec<Rejected>,
}

impl Located {
    /// The path of the first candidate, for callers (tests, examples) that load it without
    /// further ceremony.
    pub fn into_first_path(self) -> Option<PathBuf> {
        self.candidates.into_iter().next().map(|found| found.path)
    }
}

/// Lists the runtime candidates for `request` in the order documented on this module.
pub fn locate(request: &LocateRequest) -> Located {
    let platform = request
        .platform
        .or_else(Platform::host)
        .unwrap_or_else(os_family_platform);
    let env = Env {
        vars: request.env.as_ref(),
        ignore_case: platform == Platform::WindowsX86_64,
    };
    let mut search = Search {
        platform,
        found: Located::default(),
        seen: HashSet::new(),
    };
    let repo_root = request.repo_root.as_deref().map(absolute);

    // 1. Explicit.
    if let Some(explicit) = &request.explicit {
        let path = absolute(explicit);
        let detail = path.display().to_string();
        if path.is_file() {
            search.push(path, SourceKind::Explicit, &detail, None);
        } else {
            search.directory(&path, Layout::SdkTree, SourceKind::Explicit, &detail, true);
        }
    }

    // 2..4. The environment.
    for (var, layout, source) in [
        ("BLPAPI_LIB_DIR", Layout::Flat, SourceKind::BlpapiLibDir),
        ("BLPAPI_ROOT", Layout::SdkTree, SourceKind::BlpapiRoot),
        ("XBBG_DEV_SDK_ROOT", Layout::SdkTree, SourceKind::DevSdkRoot),
    ] {
        let Some(value) = env.get(var) else { continue };
        let dir = match (&repo_root, source) {
            (Some(base), SourceKind::DevSdkRoot) => base.join(value),
            _ => absolute(Path::new(&value)),
        };
        search.directory(&dir, layout, source, var, true);
    }

    // 5. The vendored SDK.
    if let Some(root) = &repo_root {
        let vendor = root.join("vendor").join("blpapi-sdk");
        let detail = vendor.display().to_string();
        search.directory(
            &vendor,
            Layout::SdkTree,
            SourceKind::Vendored,
            &detail,
            false,
        );
    }

    // 6. Host hints.
    for hint in &request.hints {
        let dir = absolute(&hint.dir);
        if dir.is_file() {
            search.push(dir, SourceKind::Hint, &hint.label, None);
        } else {
            search.directory(&dir, Layout::Sdk, SourceKind::Hint, &hint.label, true);
        }
    }

    // 7. Terminal roots.
    for root in terminal_roots(&env, platform) {
        let detail = root.display().to_string();
        search.directory(&root, Layout::Sdk, SourceKind::Terminal, &detail, false);
    }

    // 8. The operating system's library search path.
    for var in search_path_vars(platform) {
        let Some(value) = env.get(var) else { continue };
        for dir in split_search_path(&value.to_string_lossy(), platform) {
            search.directory(&dir, Layout::Flat, SourceKind::SearchPath, var, false);
        }
    }

    // 9. The cache.
    let cache_root = request
        .cache_root
        .as_deref()
        .map(absolute)
        .or_else(|| cache_root_from(|name| env.get(name).map(PathBuf::from)));
    if let Some(root) = cache_root {
        search.cache(&root);
    }

    search.found
}

/// The platform whose file names and layouts fit the host operating system when the
/// architecture is not one xbbg ships for; the search still helps there, loading may not.
fn os_family_platform() -> Platform {
    if cfg!(windows) {
        Platform::WindowsX86_64
    } else if cfg!(target_os = "macos") {
        Platform::MacosAarch64
    } else {
        Platform::LinuxX86_64
    }
}

/// How deep to look inside a directory.
#[derive(Clone, Copy)]
enum Layout {
    /// The library directly in the directory.
    Flat,
    /// The directory and its SDK layouts (`bin`, `lib`, `Linux`, ...).
    Sdk,
    /// [`Layout::Sdk`] for the directory and for each subdirectory one and two levels down,
    /// versioned ones newest first.
    SdkTree,
}

struct Search {
    platform: Platform,
    found: Located,
    /// Canonical keys of the candidates so far.
    seen: HashSet<String>,
}

impl Search {
    fn push(&mut self, path: PathBuf, source: SourceKind, detail: &str, version: Option<String>) {
        let canonical = fs::canonicalize(&path).unwrap_or_else(|_| path.clone());
        let mut key = canonical.to_string_lossy().into_owned();
        if self.platform == Platform::WindowsX86_64 {
            key = key.to_lowercase();
        }
        if self.seen.insert(key) {
            self.found.candidates.push(Candidate {
                path,
                source,
                detail: detail.to_owned(),
                version,
            });
        }
    }

    fn reject(&mut self, path: &Path, source: SourceKind, detail: &str, reason: String) {
        self.found.rejected.push(Rejected {
            path: path.to_path_buf(),
            source,
            detail: detail.to_owned(),
            reason,
        });
    }

    /// Adds the libraries in `dir`; with `report`, a directory with none is a rejection.
    fn directory(
        &mut self,
        dir: &Path,
        layout: Layout,
        source: SourceKind,
        detail: &str,
        report: bool,
    ) {
        let libraries = libraries(dir, layout, self.platform);
        if libraries.is_empty() {
            if report {
                let reason = if !dir.exists() {
                    "does not exist".to_owned()
                } else if !dir.is_dir() {
                    "is not a directory".to_owned()
                } else {
                    format!(
                        "holds no Bloomberg runtime library (expected {})",
                        self.platform.library_names().join(" or ")
                    )
                };
                self.reject(dir, source, detail, reason);
            }
            return;
        }
        for library in libraries {
            self.push(library, source, detail, None);
        }
    }

    fn cache(&mut self, root: &Path) {
        match scan_cache(root, self.platform) {
            Ok(scan) => {
                for runtime in scan.runtimes {
                    let detail = runtime.install_dir.display().to_string();
                    self.push(
                        runtime.lib_path,
                        SourceKind::Cache,
                        &detail,
                        Some(runtime.version),
                    );
                }
                for refused in scan.rejected {
                    let detail = refused.install_dir.display().to_string();
                    self.reject(
                        &refused.install_dir,
                        SourceKind::Cache,
                        &detail,
                        refused.reason,
                    );
                }
            }
            Err(error) => {
                let detail = root.display().to_string();
                self.reject(root, SourceKind::Cache, &detail, error.to_string());
            }
        }
    }
}

/// The host's view of the environment.
struct Env<'a> {
    vars: Option<&'a BTreeMap<String, String>>,
    /// Match names in `vars` regardless of case (Windows).
    ignore_case: bool,
}

impl Env<'_> {
    /// A non-empty variable.
    fn get(&self, name: &str) -> Option<OsString> {
        let value = match self.vars {
            None => std::env::var_os(name),
            Some(vars) => vars
                .get(name)
                .or_else(|| {
                    self.ignore_case
                        .then(|| {
                            vars.iter()
                                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                                .map(|(_, value)| value)
                        })
                        .flatten()
                })
                .map(OsString::from),
        };
        value.filter(|value| !value.is_empty())
    }
}

/// An absolute spelling of `path`: relative paths resolve against the current directory.
fn absolute(path: &Path) -> PathBuf {
    std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf())
}

/// The existing library files for `dir` under `layout`, in search order.
fn libraries(dir: &Path, layout: Layout, platform: Platform) -> Vec<PathBuf> {
    match layout {
        Layout::Flat => library_files(dir, platform),
        Layout::Sdk => sdk_layouts(dir, platform),
        Layout::SdkTree => sdk_roots(dir)
            .iter()
            .flat_map(|root| sdk_layouts(root, platform))
            .collect(),
    }
}

fn sdk_layouts(root: &Path, platform: Platform) -> Vec<PathBuf> {
    SDK_ROOT_SUBDIRS
        .iter()
        .flat_map(|subdir| library_files(&root.join(subdir), platform))
        .collect()
}

/// The library files directly inside `dir`, in the order of the platform's file names.
fn library_files(dir: &Path, platform: Platform) -> Vec<PathBuf> {
    platform
        .library_names()
        .iter()
        .map(|name| dir.join(name))
        .filter(|candidate| candidate.is_file())
        .collect()
}

/// `root`, then its subdirectories, then theirs; each level newest version first.
fn sdk_roots(root: &Path) -> Vec<PathBuf> {
    let children = child_dirs(root);
    let grandchildren: Vec<PathBuf> = children.iter().flat_map(|dir| child_dirs(dir)).collect();
    let mut roots = vec![root.to_path_buf()];
    roots.extend(children);
    roots.extend(grandchildren);
    roots
}

fn child_dirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort_by(|a, b| newest_first(a, b));
    dirs
}

/// Version-named directories (`3.26.5.1`) before the rest, newest first; the rest by name.
fn newest_first(a: &Path, b: &Path) -> Ordering {
    let name = |path: &Path| {
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    };
    let (a, b) = (name(a), name(b));
    match (is_version(&a), is_version(&b)) {
        (true, true) => compare_versions(&b, &a),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => a.cmp(&b),
    }
}

/// `3.26.5.1`: three or four dot-separated numbers.
fn is_version(name: &str) -> bool {
    let parts: Vec<&str> = name.split('.').collect();
    (3..=4).contains(&parts.len())
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
}

/// The Bloomberg Terminal's `DAPI` directories on this platform, most specific first, without
/// duplicates (case-folded on Windows).
fn terminal_roots(env: &Env<'_>, platform: Platform) -> Vec<PathBuf> {
    let mut roots: Vec<String> = Vec::new();
    let mut add = |root: String| {
        let duplicate = if platform == Platform::WindowsX86_64 {
            roots.iter().any(|known| known.eq_ignore_ascii_case(&root))
        } else {
            roots.contains(&root)
        };
        if !duplicate {
            roots.push(root);
        }
    };
    if platform == Platform::WindowsX86_64 {
        let text = |name: &str| {
            env.get(name)
                .map(|value| value.to_string_lossy().into_owned())
        };
        let drive = text("SYSTEMDRIVE")
            .and_then(|value| drive_prefix(&value))
            .unwrap_or_else(|| "C:".to_owned());
        add(format!(r"{drive}\blp\DAPI"));
        add(r"C:\blp\DAPI".to_owned());
        for (var, fallback) in [
            ("PROGRAMFILES", "Program Files"),
            ("PROGRAMFILES(X86)", "Program Files (x86)"),
        ] {
            for base in [text(var), Some(format!(r"{drive}\{fallback}"))]
                .into_iter()
                .flatten()
            {
                add(format!(
                    r"{}\Bloomberg\Blp\DAPI",
                    base.trim_end_matches(['\\', '/'])
                ));
            }
        }
        let local = text("LOCALAPPDATA").or_else(|| {
            text("USERPROFILE")
                .map(|home| format!(r"{}\AppData\Local", home.trim_end_matches(['\\', '/'])))
        });
        if let Some(local) = local {
            let local = local.trim_end_matches(['\\', '/']);
            add(format!(r"{local}\Bloomberg\DAPI"));
            add(format!(r"{local}\Bloomberg\Blp\DAPI"));
        }
    } else {
        if let Some(home) = env.get("HOME") {
            add(home.to_string_lossy().trim_end_matches('/').to_owned() + "/blp/DAPI");
        }
        add("/opt/bloomberg/DAPI".to_owned());
    }
    roots.into_iter().map(PathBuf::from).collect()
}

/// `D:` from `D:\` or `D:`; `None` for anything else.
fn drive_prefix(value: &str) -> Option<String> {
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(letter), Some(':')) if letter.is_ascii_alphabetic() => Some(format!("{letter}:")),
        _ => None,
    }
}

/// The variables of the operating system's library search path, in the order they are read.
fn search_path_vars(platform: Platform) -> &'static [&'static str] {
    match platform {
        Platform::WindowsX86_64 => &["PATH"],
        Platform::LinuxX86_64 => &["LD_LIBRARY_PATH"],
        Platform::MacosAarch64 => &["DYLD_LIBRARY_PATH", "DYLD_FALLBACK_LIBRARY_PATH"],
    }
}

/// The directories of a path list: `;`-separated on Windows (entries trimmed, surrounding double
/// quotes removed), `:`-separated elsewhere. Empty entries are skipped.
fn split_search_path(value: &str, platform: Platform) -> Vec<PathBuf> {
    let windows = platform == Platform::WindowsX86_64;
    value
        .split(if windows { ';' } else { ':' })
        .map(|entry| {
            let entry = entry.trim();
            if windows {
                entry.trim_matches('"').trim()
            } else {
                entry
            }
        })
        .filter(|entry| !entry.is_empty())
        .map(|entry| absolute(Path::new(entry)))
        .collect()
}

#[cfg(test)]
mod tests {
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::cache::INSTALLS_DIR;
    use crate::provenance::{PROVENANCE_FILE, Provenance};

    const LINUX: Platform = Platform::LinuxX86_64;
    const LIBRARY: &[u8] = b"synthetic library bytes";

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().expect("a file has a directory")).expect("create dirs");
        fs::write(path, b"").expect("create file");
    }

    fn temp() -> tempfile::TempDir {
        tempfile::tempdir().expect("temp dir")
    }

    fn text(path: &Path) -> String {
        path.display().to_string()
    }

    /// A request that reads only `vars`, never the process environment or the default cache.
    fn request(platform: Platform, vars: &[(&str, String)]) -> LocateRequest {
        LocateRequest {
            env: Some(
                vars.iter()
                    .map(|(name, value)| ((*name).to_owned(), value.clone()))
                    .collect(),
            ),
            platform: Some(platform),
            ..LocateRequest::default()
        }
    }

    fn sources(found: &Located) -> Vec<SourceKind> {
        let mut sources: Vec<_> = found.candidates.iter().map(|c| c.source).collect();
        sources.dedup();
        sources
    }

    fn paths(found: &Located) -> Vec<&Path> {
        found.candidates.iter().map(|c| c.path.as_path()).collect()
    }

    /// Installs a Linux runtime as `<cache>/sdk/<version>/linux-x86_64/<name>`; the recorded hash
    /// is the library's unless `corrupt`.
    fn install(cache: &Path, version: &str, name: &str, corrupt: bool) -> PathBuf {
        let dir = cache
            .join(INSTALLS_DIR)
            .join(version)
            .join(LINUX.as_str())
            .join(name);
        fs::create_dir_all(dir.join("lib")).expect("create install");
        fs::write(dir.join("lib/libblpapi3_64.so"), LIBRARY).expect("write library");
        let recorded: &[u8] = if corrupt { b"other bytes" } else { LIBRARY };
        let provenance = Provenance {
            version: version.to_owned(),
            platform: LINUX,
            source: "artifact:synthetic".into(),
            artifact_url: "file:synthetic".into(),
            artifact_sha256: hex::encode(Sha256::digest(version.as_bytes())),
            lib: "lib/libblpapi3_64.so".into(),
            lib_sha256: hex::encode(Sha256::digest(recorded)),
            licenses: vec![],
        };
        fs::write(
            dir.join(PROVENANCE_FILE),
            serde_json::to_vec(&provenance).expect("provenance json"),
        )
        .expect("write provenance");
        dir
    }

    #[test]
    fn version_directories_sort_newest_first() {
        let mut names = vec!["3.9.0", "docs", "3.26.5.1", "3.26.10.0", "bin"];
        names.sort_by(|a, b| newest_first(Path::new(a), Path::new(b)));
        assert_eq!(names, ["3.26.10.0", "3.26.5.1", "3.9.0", "bin", "docs"]);
    }

    #[test]
    fn versions_need_three_or_four_numbers() {
        assert!(is_version("3.26.5.1"));
        assert!(is_version("3.26.5"));
        assert!(!is_version("3.26"));
        assert!(!is_version("3.x.5"));
        assert!(!is_version("Darwin"));
    }

    #[test]
    fn every_rule_in_order() {
        let tmp = temp();
        let root = tmp.path();
        let explicit = root.join("explicit/anything.bin");
        touch(&explicit);
        touch(&root.join("libdir/libblpapi3_64.so"));
        touch(&root.join("sdk/Linux/libblpapi3_64.so"));
        let repo = root.join("repo");
        touch(&repo.join("dev/lib/libblpapi3_64.so"));
        touch(&repo.join("vendor/blpapi-sdk/3.26.5.1/Linux/libblpapi3_64.so"));
        touch(&root.join("wheel/libblpapi3.so"));
        touch(&root.join("home/blp/DAPI/libblpapi3_64.so"));
        touch(&root.join("ld2/libblpapi3_64.so"));
        install(&root.join("cache"), "3.26.9.1", "artifact-1", false);

        let mut request = request(
            LINUX,
            &[
                ("BLPAPI_LIB_DIR", text(&root.join("libdir"))),
                ("BLPAPI_ROOT", text(&root.join("sdk"))),
                ("XBBG_DEV_SDK_ROOT", "dev".to_owned()),
                ("HOME", text(&root.join("home"))),
                (
                    "LD_LIBRARY_PATH",
                    format!(
                        "{}:{}",
                        root.join("ld1").display(),
                        root.join("ld2").display()
                    ),
                ),
            ],
        );
        request.explicit = Some(explicit);
        request.repo_root = Some(repo);
        request.cache_root = Some(root.join("cache"));
        request.hints = vec![Hint {
            label: "blpapi_python".into(),
            dir: root.join("wheel"),
        }];

        let found = locate(&request);
        assert_eq!(
            sources(&found),
            [
                SourceKind::Explicit,
                SourceKind::BlpapiLibDir,
                SourceKind::BlpapiRoot,
                SourceKind::DevSdkRoot,
                SourceKind::Vendored,
                SourceKind::Hint,
                SourceKind::Terminal,
                SourceKind::SearchPath,
                SourceKind::Cache,
            ]
        );
        let details: Vec<_> = found.candidates.iter().map(|c| c.detail.as_str()).collect();
        assert_eq!(details[1], "BLPAPI_LIB_DIR");
        assert_eq!(details[2], "BLPAPI_ROOT");
        assert_eq!(details[3], "XBBG_DEV_SDK_ROOT");
        assert_eq!(details[5], "blpapi_python");
        assert_eq!(details[7], "LD_LIBRARY_PATH");
        assert_eq!(found.candidates[0].path, root.join("explicit/anything.bin"));
        assert_eq!(found.candidates[5].path, root.join("wheel/libblpapi3.so"));
        assert_eq!(found.candidates[8].version.as_deref(), Some("3.26.9.1"));
    }

    #[test]
    fn a_host_environment_view_replaces_the_process_environment() {
        // The process may name a runtime in BLPAPI_ROOT / BLPAPI_LIB_DIR; an empty view hides it.
        let found = locate(&request(LINUX, &[]));
        assert!(
            found
                .candidates
                .iter()
                .all(|c| !matches!(c.source, SourceKind::BlpapiLibDir | SourceKind::BlpapiRoot))
        );
        assert!(found.rejected.is_empty(), "{:?}", found.rejected);

        let tmp = temp();
        touch(&tmp.path().join("Linux/libblpapi3_64.so"));
        let found = locate(&request(LINUX, &[("BLPAPI_ROOT", text(tmp.path()))]));
        assert_eq!(paths(&found), [tmp.path().join("Linux/libblpapi3_64.so")]);
    }

    #[test]
    fn empty_variables_count_as_unset() {
        let found = locate(&request(
            LINUX,
            &[
                ("BLPAPI_LIB_DIR", String::new()),
                ("BLPAPI_ROOT", String::new()),
            ],
        ));
        assert!(found.candidates.is_empty());
        assert!(found.rejected.is_empty());
    }

    #[test]
    fn one_file_found_by_several_rules_is_listed_once_at_its_first_rule() {
        let tmp = temp();
        let library = tmp.path().join("sdk/libblpapi3_64.so");
        touch(&library);
        let mut request = request(
            LINUX,
            &[
                ("BLPAPI_LIB_DIR", text(&tmp.path().join("sdk"))),
                ("BLPAPI_ROOT", text(tmp.path())),
            ],
        );
        request.explicit = Some(library.clone());
        request.hints = vec![Hint {
            label: "again".into(),
            dir: tmp.path().join("sdk"),
        }];
        let found = locate(&request);
        assert_eq!(paths(&found), [library]);
        assert_eq!(found.candidates[0].source, SourceKind::Explicit);
    }

    #[test]
    #[cfg(unix)]
    fn a_link_to_a_listed_file_is_a_duplicate() {
        let tmp = temp();
        let library = tmp.path().join("sdk/libblpapi3_64.so");
        touch(&library);
        fs::create_dir_all(tmp.path().join("link")).expect("create dir");
        std::os::unix::fs::symlink(&library, tmp.path().join("link/libblpapi3.so"))
            .expect("symlink");
        let found = locate(&request(
            LINUX,
            &[
                ("BLPAPI_LIB_DIR", text(&tmp.path().join("sdk"))),
                ("BLPAPI_ROOT", text(&tmp.path().join("link"))),
            ],
        ));
        assert_eq!(paths(&found), [library]);
    }

    #[test]
    fn windows_paths_are_deduplicated_ignoring_case() {
        let tmp = temp();
        // A case-insensitive file system names one file two ways; on others only one exists and
        // the key comparison is what is under test.
        let mut search = Search {
            platform: Platform::WindowsX86_64,
            found: Located::default(),
            seen: HashSet::new(),
        };
        search.push(
            tmp.path().join("Bin/Blpapi3_64.DLL"),
            SourceKind::Hint,
            "a",
            None,
        );
        search.push(
            tmp.path().join("bin/blpapi3_64.dll"),
            SourceKind::Hint,
            "b",
            None,
        );
        assert_eq!(search.found.candidates.len(), 1);
        assert_eq!(search.found.candidates[0].detail, "a");
    }

    #[test]
    fn a_relative_dev_root_is_relative_to_the_repo_root() {
        let tmp = temp();
        touch(&tmp.path().join("repo/sdk-dev/Linux/libblpapi3_64.so"));
        let mut request = request(LINUX, &[("XBBG_DEV_SDK_ROOT", "sdk-dev".to_owned())]);
        request.repo_root = Some(tmp.path().join("repo"));
        let found = locate(&request);
        assert_eq!(
            paths(&found),
            [tmp.path().join("repo/sdk-dev/Linux/libblpapi3_64.so")]
        );
        assert_eq!(found.candidates[0].source, SourceKind::DevSdkRoot);

        // An absolute value ignores the repo root.
        request.env = Some(BTreeMap::from([(
            "XBBG_DEV_SDK_ROOT".to_owned(),
            text(&tmp.path().join("repo/sdk-dev")),
        )]));
        request.repo_root = Some(tmp.path().join("elsewhere"));
        assert_eq!(locate(&request).candidates.len(), 1);
    }

    #[test]
    fn a_dev_root_that_holds_nothing_is_rejected_with_its_variable() {
        let tmp = temp();
        let mut request = request(LINUX, &[("XBBG_DEV_SDK_ROOT", "missing".to_owned())]);
        request.repo_root = Some(tmp.path().to_path_buf());
        let found = locate(&request);
        assert!(found.candidates.is_empty());
        assert_eq!(found.rejected.len(), 1);
        let refused = &found.rejected[0];
        assert_eq!(refused.source, SourceKind::DevSdkRoot);
        assert_eq!(refused.detail, "XBBG_DEV_SDK_ROOT");
        assert_eq!(refused.path, tmp.path().join("missing"));
        assert_eq!(refused.reason, "does not exist");
    }

    #[test]
    fn the_vendored_sdk_lists_the_newest_version_first() {
        let tmp = temp();
        for version in ["3.9.0", "3.26.5.1", "3.26.10.0"] {
            touch(
                &tmp.path()
                    .join("vendor/blpapi-sdk")
                    .join(version)
                    .join("Linux/libblpapi3_64.so"),
            );
        }
        let mut request = request(LINUX, &[]);
        request.repo_root = Some(tmp.path().to_path_buf());
        let found = locate(&request);
        let order: Vec<_> = found
            .candidates
            .iter()
            .map(|c| {
                assert_eq!(c.source, SourceKind::Vendored);
                c.path
                    .strip_prefix(tmp.path().join("vendor/blpapi-sdk"))
                    .expect("under the vendor directory")
                    .components()
                    .next()
                    .expect("a version directory")
                    .as_os_str()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(order, ["3.26.10.0", "3.26.5.1", "3.9.0"]);

        // No repo root, no vendored rule.
        assert!(locate(&self::request(LINUX, &[])).candidates.is_empty());
    }

    #[test]
    fn root_layouts_cover_the_sdk_archives() {
        for (platform, relative) in [
            (LINUX, "Linux/libblpapi3_64.so"),
            (Platform::MacosAarch64, "Darwin/libblpapi3_64.so"),
            (Platform::MacosAarch64, "lib/libblpapi3_64.dylib"),
            (Platform::MacosAarch64, "lib/libblpapi3.dylib"),
            (Platform::WindowsX86_64, "bin/blpapi3_64.dll"),
            (Platform::WindowsX86_64, "lib/win64/blpapi3_64.dll"),
            (LINUX, "libblpapi3_64.so"),
            (LINUX, "lib64/libblpapi3.so"),
        ] {
            let root = temp();
            touch(&root.path().join(relative));
            let found = locate(&request(platform, &[("BLPAPI_ROOT", text(root.path()))]));
            assert_eq!(
                paths(&found),
                [root.path().join(relative)],
                "{platform} {relative}"
            );
        }
    }

    #[test]
    fn root_may_contain_versioned_sdks_newest_first() {
        let root = temp();
        touch(&root.path().join("3.9.0/Linux/libblpapi3_64.so"));
        touch(&root.path().join("3.26.5.1/Linux/libblpapi3_64.so"));
        // One level deeper, as in a container holding a nested SDK.
        touch(&root.path().join("sdk/3.30.0.0/Linux/libblpapi3_64.so"));
        let found = locate(&request(LINUX, &[("BLPAPI_ROOT", text(root.path()))]));
        assert_eq!(
            paths(&found),
            [
                root.path().join("3.26.5.1/Linux/libblpapi3_64.so"),
                root.path().join("3.9.0/Linux/libblpapi3_64.so"),
                root.path().join("sdk/3.30.0.0/Linux/libblpapi3_64.so"),
            ]
        );
    }

    #[test]
    fn a_directory_lists_its_file_names_in_preference_order() {
        let dir = temp();
        touch(&dir.path().join("libblpapi3.so"));
        touch(&dir.path().join("libblpapi3_64.so"));
        let found = locate(&request(LINUX, &[("BLPAPI_LIB_DIR", text(dir.path()))]));
        assert_eq!(
            paths(&found),
            [
                dir.path().join("libblpapi3_64.so"),
                dir.path().join("libblpapi3.so")
            ]
        );
    }

    #[test]
    fn lib_dir_is_searched_directly_only() {
        let dir = temp();
        touch(&dir.path().join("Linux/libblpapi3_64.so"));
        let found = locate(&request(LINUX, &[("BLPAPI_LIB_DIR", text(dir.path()))]));
        assert!(found.candidates.is_empty());
        assert_eq!(found.rejected.len(), 1);
        assert!(
            found.rejected[0]
                .reason
                .contains("holds no Bloomberg runtime library")
        );
        assert!(found.rejected[0].reason.contains("libblpapi3_64.so"));
    }

    #[test]
    fn explicit_paths_are_files_or_directories() {
        let tmp = temp();
        touch(&tmp.path().join("odd name"));
        touch(&tmp.path().join("sdk/Linux/libblpapi3_64.so"));
        touch(&tmp.path().join("empty/readme.txt"));

        let explicit = |path: PathBuf| {
            let mut request = request(LINUX, &[]);
            request.explicit = Some(path);
            locate(&request)
        };
        // A file is taken as it is, whatever its name.
        assert_eq!(
            paths(&explicit(tmp.path().join("odd name"))),
            [tmp.path().join("odd name")]
        );
        // A directory goes through the SDK layouts.
        assert_eq!(
            paths(&explicit(tmp.path().join("sdk"))),
            [tmp.path().join("sdk/Linux/libblpapi3_64.so")]
        );
        let nothing = explicit(tmp.path().join("empty"));
        assert!(nothing.candidates.is_empty());
        assert_eq!(nothing.rejected[0].source, SourceKind::Explicit);
        assert!(
            nothing.rejected[0]
                .reason
                .starts_with("holds no Bloomberg runtime library")
        );
        let missing = explicit(tmp.path().join("missing"));
        assert_eq!(missing.rejected[0].reason, "does not exist");
    }

    #[test]
    fn a_hint_that_is_a_file_is_a_candidate_and_an_empty_one_is_rejected() {
        let tmp = temp();
        touch(&tmp.path().join("one/libblpapi3_64.so"));
        let mut request = request(LINUX, &[]);
        request.hints = vec![
            Hint {
                label: "first".into(),
                dir: tmp.path().join("none"),
            },
            Hint {
                label: "second".into(),
                dir: tmp.path().join("one/libblpapi3_64.so"),
            },
        ];
        let found = locate(&request);
        assert_eq!(found.candidates.len(), 1);
        assert_eq!(found.candidates[0].detail, "second");
        assert_eq!(found.rejected.len(), 1);
        assert_eq!(found.rejected[0].detail, "first");
    }

    #[test]
    fn search_path_directories_are_searched_in_variable_order() {
        let tmp = temp();
        let (a, b, c) = (
            tmp.path().join("a"),
            tmp.path().join("b"),
            tmp.path().join("c"),
        );
        touch(&a.join("libblpapi3_64.so"));
        touch(&b.join("libblpapi3_64.so"));
        touch(&c.join("libblpapi3_64.dylib"));

        let found = locate(&request(
            LINUX,
            &[(
                "LD_LIBRARY_PATH",
                format!(":{}::{}:", b.display(), a.display()),
            )],
        ));
        assert_eq!(
            paths(&found),
            [b.join("libblpapi3_64.so"), a.join("libblpapi3_64.so")]
        );
        assert!(
            found
                .candidates
                .iter()
                .all(|c| c.detail == "LD_LIBRARY_PATH")
        );

        // macOS reads DYLD_LIBRARY_PATH, then DYLD_FALLBACK_LIBRARY_PATH, and ignores the others.
        let found = locate(&request(
            Platform::MacosAarch64,
            &[
                ("DYLD_FALLBACK_LIBRARY_PATH", text(&c)),
                ("DYLD_LIBRARY_PATH", text(&a)),
                ("LD_LIBRARY_PATH", text(&b)),
            ],
        ));
        assert_eq!(
            paths(&found),
            [a.join("libblpapi3_64.so"), c.join("libblpapi3_64.dylib")]
        );
        assert_eq!(found.candidates[1].detail, "DYLD_FALLBACK_LIBRARY_PATH");
    }

    #[test]
    fn windows_path_entries_are_trimmed_unquoted_and_names_match_in_any_case() {
        let tmp = temp();
        let (a, b, c) = (
            tmp.path().join("a"),
            tmp.path().join("b c"),
            tmp.path().join("c"),
        );
        touch(&b.join("blpapi3_64.dll"));
        touch(&c.join("blpapi3_64.dll"));
        fs::create_dir_all(&a).expect("create dir");
        // The view's own spelling of the name, as Node's process.env has it on Windows.
        let found = locate(&request(
            Platform::WindowsX86_64,
            &[(
                "Path",
                format!("{} ; \"{}\" ;; {}", a.display(), b.display(), c.display()),
            )],
        ));
        assert_eq!(
            paths(&found),
            [b.join("blpapi3_64.dll"), c.join("blpapi3_64.dll")]
        );
        assert_eq!(found.candidates[0].detail, "PATH");

        // Other platforms match names exactly.
        let found = locate(&request(LINUX, &[("Ld_Library_Path", text(&b))]));
        assert!(found.candidates.is_empty());
    }

    fn windows_roots(vars: &[(&str, &str)]) -> Vec<String> {
        let vars: BTreeMap<String, String> = vars
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        let env = Env {
            vars: Some(&vars),
            ignore_case: true,
        };
        terminal_roots(&env, Platform::WindowsX86_64)
            .iter()
            .map(|root| text(root))
            .collect()
    }

    #[test]
    fn windows_terminal_roots_follow_the_environment() {
        assert_eq!(
            windows_roots(&[
                ("SystemDrive", "D:"),
                ("PROGRAMFILES", r"E:\PF"),
                ("PROGRAMFILES(X86)", ""),
                ("LOCALAPPDATA", r"E:\Local\"),
            ]),
            [
                r"D:\blp\DAPI",
                r"C:\blp\DAPI",
                r"E:\PF\Bloomberg\Blp\DAPI",
                r"D:\Program Files\Bloomberg\Blp\DAPI",
                r"D:\Program Files (x86)\Bloomberg\Blp\DAPI",
                r"E:\Local\Bloomberg\DAPI",
                r"E:\Local\Bloomberg\Blp\DAPI",
            ]
        );
    }

    #[test]
    fn windows_terminal_roots_default_to_the_c_drive_and_dedupe_ignoring_case() {
        // A missing or malformed SYSTEMDRIVE is C:; repeats collapse whatever their case.
        assert_eq!(
            windows_roots(&[("SYSTEMDRIVE", "Q"), ("PROGRAMFILES", r"c:\program files")]),
            [
                r"C:\blp\DAPI",
                r"c:\program files\Bloomberg\Blp\DAPI",
                r"C:\Program Files (x86)\Bloomberg\Blp\DAPI",
            ]
        );
        // USERPROFILE stands in for LOCALAPPDATA.
        let roots = windows_roots(&[("USERPROFILE", r"C:\Users\someone")]);
        assert_eq!(
            roots[roots.len() - 2..],
            [
                r"C:\Users\someone\AppData\Local\Bloomberg\DAPI",
                r"C:\Users\someone\AppData\Local\Bloomberg\Blp\DAPI",
            ]
        );
    }

    #[test]
    fn posix_terminal_roots_are_home_and_opt() {
        let vars = BTreeMap::from([("HOME".to_owned(), "/home/someone/".to_owned())]);
        let env = Env {
            vars: Some(&vars),
            ignore_case: false,
        };
        assert_eq!(
            terminal_roots(&env, LINUX),
            [
                PathBuf::from("/home/someone/blp/DAPI"),
                PathBuf::from("/opt/bloomberg/DAPI")
            ]
        );
        let none = BTreeMap::new();
        let env = Env {
            vars: Some(&none),
            ignore_case: false,
        };
        assert_eq!(
            terminal_roots(&env, Platform::MacosAarch64),
            [PathBuf::from("/opt/bloomberg/DAPI")]
        );
    }

    #[test]
    fn terminal_roots_are_searched_in_their_layouts() {
        let home = temp();
        touch(&home.path().join("blp/DAPI/libblpapi3_64.so"));
        let found = locate(&request(LINUX, &[("HOME", text(home.path()))]));
        let terminal: Vec<_> = found
            .candidates
            .iter()
            .filter(|c| c.source == SourceKind::Terminal)
            .collect();
        assert_eq!(terminal.len(), 1);
        assert_eq!(
            terminal[0].path,
            home.path().join("blp/DAPI/libblpapi3_64.so")
        );
        assert_eq!(terminal[0].detail, text(&home.path().join("blp/DAPI")));
    }

    #[test]
    fn the_cache_lists_verified_installs_best_first_and_rejects_the_rest() {
        let tmp = temp();
        let cache = tmp.path().join("cache");
        let old = install(&cache, "3.26.5.1", "artifact-a", false);
        let new = install(&cache, "3.26.9.1", "artifact-b", false);
        let bad = install(&cache, "3.27.0.0", "artifact-c", true);

        let mut request = request(LINUX, &[]);
        request.cache_root = Some(cache.clone());
        let found = locate(&request);
        assert_eq!(
            paths(&found),
            [
                new.join("lib/libblpapi3_64.so"),
                old.join("lib/libblpapi3_64.so")
            ]
        );
        assert_eq!(found.candidates[0].version.as_deref(), Some("3.26.9.1"));
        assert_eq!(found.candidates[0].detail, text(&new));
        assert_eq!(found.rejected.len(), 1);
        assert_eq!(found.rejected[0].path, bad);
        assert_eq!(found.rejected[0].source, SourceKind::Cache);
        assert_eq!(found.rejected[0].detail, text(&bad));
        assert!(!found.rejected[0].reason.is_empty());

        // Without a request root the view's XBBG_CACHE_DIR names the cache.
        let by_env = locate(&self::request(LINUX, &[("XBBG_CACHE_DIR", text(&cache))]));
        assert_eq!(by_env, found);
    }

    #[test]
    fn requests_and_results_round_trip_as_camel_case_json() {
        let request = LocateRequest {
            explicit: Some(PathBuf::from("lib")),
            hints: vec![Hint {
                label: "blpapi_python".into(),
                dir: PathBuf::from("wheel"),
            }],
            env: Some(BTreeMap::from([("PATH".to_owned(), "x".to_owned())])),
            cache_root: Some(PathBuf::from("cache")),
            repo_root: Some(PathBuf::from("repo")),
            platform: Some(Platform::MacosAarch64),
        };
        let json = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "explicit": "lib",
                "hints": [{"label": "blpapi_python", "dir": "wheel"}],
                "env": {"PATH": "x"},
                "cacheRoot": "cache",
                "repoRoot": "repo",
                "platform": "macos-aarch64",
            })
        );
        assert_eq!(
            serde_json::from_value::<LocateRequest>(json).expect("parse"),
            request
        );
        assert_eq!(
            serde_json::from_str::<LocateRequest>("{}").expect("parse"),
            LocateRequest::default()
        );
        assert!(serde_json::from_str::<LocateRequest>(r#"{"cacheDir": "x"}"#).is_err());

        let located = Located {
            candidates: vec![Candidate {
                path: PathBuf::from("a"),
                source: SourceKind::BlpapiLibDir,
                detail: "BLPAPI_LIB_DIR".into(),
                version: Some("3.26.5.1".into()),
            }],
            rejected: vec![Rejected {
                path: PathBuf::from("b"),
                source: SourceKind::SearchPath,
                detail: "PATH".into(),
                reason: "why".into(),
            }],
        };
        let json = serde_json::to_value(&located).expect("serialize");
        assert_eq!(json["candidates"][0]["source"], "blpapi_lib_dir");
        assert_eq!(json["rejected"][0]["source"], "search_path");
        assert_eq!(
            serde_json::from_value::<Located>(json).expect("parse"),
            located
        );
    }

    #[test]
    fn every_source_name_matches_its_serialized_form() {
        for source in [
            SourceKind::Explicit,
            SourceKind::BlpapiLibDir,
            SourceKind::BlpapiRoot,
            SourceKind::DevSdkRoot,
            SourceKind::Vendored,
            SourceKind::Hint,
            SourceKind::Terminal,
            SourceKind::SearchPath,
            SourceKind::Cache,
        ] {
            assert_eq!(
                serde_json::to_value(source).expect("serialize"),
                source.as_str()
            );
        }
    }
}
