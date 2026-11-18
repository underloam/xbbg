//! The cache root, its install listing and the verification of one install.

use std::cmp::Ordering;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use crate::platform::Platform;
use crate::provenance::{PROVENANCE_FILE, Provenance, safe_relative};

/// Directory of the cache root that holds the installs: `<cache>/sdk/<version>/<platform>/<install>`.
pub const INSTALLS_DIR: &str = "sdk";

/// A verified runtime install in the cache.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CachedRuntime {
    /// Runtime version, e.g. `3.26.9.1`.
    pub version: String,
    /// Platform the library is built for.
    pub platform: Platform,
    /// Absolute path of the Bloomberg library, inside [`Self::install_dir`].
    pub lib_path: PathBuf,
    /// SHA-256 of the library (lowercase hex); it matched the file when the install was verified.
    pub lib_sha256: String,
    /// Source label of the install: `pypi:<index>[#project]`, `conda:<channel url>[#package]`
    /// or `artifact:<url or path>` (never with credentials).
    pub source: String,
    /// Absolute path of the install directory.
    pub install_dir: PathBuf,
    /// URL of the archive the install came from (`file:` for local files), without credentials.
    pub artifact_url: String,
    /// SHA-256 of that archive (lowercase hex).
    pub artifact_sha256: String,
    /// Absolute paths of the licence files (not checked for existence).
    pub license_paths: Vec<PathBuf>,
    /// Modification time of `provenance.json`, which is written last: when the install landed.
    pub provenance_modified: Option<SystemTime>,
}

/// An install directory that cannot be used.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rejected {
    /// The install directory (absolute).
    pub install_dir: PathBuf,
    /// Why it was refused, for people.
    pub reason: String,
}

/// What a cache holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CacheScan {
    /// The verified installs, best first: version descending (see [`compare_versions`]), then
    /// platform, then `provenance.json` modification time descending, then directory.
    pub runtimes: Vec<CachedRuntime>,
    /// The install directories that were refused, ordered by directory.
    pub rejected: Vec<Rejected>,
}

/// The shared xbbg cache root: `XBBG_CACHE_DIR` if set, else `%LOCALAPPDATA%\xbbg` (falling
/// back to `%USERPROFILE%\AppData\Local\xbbg`) on Windows, `~/Library/Caches/xbbg` on macOS,
/// and `$XDG_CACHE_HOME/xbbg` or `~/.cache/xbbg` elsewhere. `None` when no home directory is
/// known. JVM hosts consult the `xbbg.cache.dir` system property before calling this.
pub fn default_cache_root() -> Option<PathBuf> {
    cache_root_from(|name| {
        std::env::var_os(name)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    })
}

fn cache_root_from(var: impl Fn(&str) -> Option<PathBuf>) -> Option<PathBuf> {
    if let Some(dir) = var("XBBG_CACHE_DIR") {
        return Some(dir);
    }
    if cfg!(windows) {
        var("LOCALAPPDATA")
            .or_else(|| var("USERPROFILE").map(|home| home.join("AppData").join("Local")))
            .map(|base| base.join("xbbg"))
    } else if cfg!(target_os = "macos") {
        var("HOME").map(|home| home.join("Library").join("Caches").join("xbbg"))
    } else {
        var("XDG_CACHE_HOME")
            .filter(|dir| dir.is_absolute())
            .or_else(|| var("HOME").map(|home| home.join(".cache")))
            .map(|base| base.join("xbbg"))
    }
}

/// Every install under `cache_root` for `platform`, verified. A missing cache root is an empty
/// scan; an unreadable directory is an error.
pub fn scan_cache(cache_root: &Path, platform: Platform) -> io::Result<CacheScan> {
    scan(cache_root, Some(platform))
}

/// [`scan_cache`] across every platform.
pub fn scan_cache_all(cache_root: &Path) -> io::Result<CacheScan> {
    scan(cache_root, None)
}

/// The best verified install for `platform` (default: [`Platform::host`]) in `cache_root`
/// (default: [`default_cache_root`]). `None` when there is none, or when the default root or
/// platform is unknown.
pub fn resolve_cached(
    cache_root: Option<&Path>,
    platform: Option<Platform>,
) -> io::Result<Option<CachedRuntime>> {
    let root = match cache_root {
        Some(root) => root.to_path_buf(),
        None => match default_cache_root() {
            Some(root) => root,
            None => return Ok(None),
        },
    };
    let Some(platform) = platform.or_else(Platform::host) else {
        return Ok(None);
    };
    Ok(scan_cache(&root, platform)?.runtimes.into_iter().next())
}

fn scan(cache_root: &Path, platform: Option<Platform>) -> io::Result<CacheScan> {
    let root = std::path::absolute(cache_root)
        .map_err(|err| context(err, "resolving cache directory", cache_root))?;
    let mut scan = CacheScan::default();
    for version_dir in subdirectories(&root.join(INSTALLS_DIR))? {
        for platform_dir in subdirectories(&version_dir)? {
            let Some(found) = platform_dir
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| Platform::from_str(name).ok())
                .filter(|found| platform.is_none_or(|wanted| wanted == *found))
            else {
                continue;
            };
            for install_dir in subdirectories(&platform_dir)? {
                match verify_install(&install_dir, found) {
                    Ok(runtime) => scan.runtimes.push(runtime),
                    Err(rejected) => scan.rejected.push(rejected),
                }
            }
        }
    }
    scan.runtimes.sort_by(|a, b| {
        compare_versions(&b.version, &a.version)
            .then(a.platform.cmp(&b.platform))
            .then(b.provenance_modified.cmp(&a.provenance_modified))
            .then(a.install_dir.cmp(&b.install_dir))
    });
    scan.rejected
        .sort_by(|a, b| a.install_dir.cmp(&b.install_dir));
    Ok(scan)
}

/// Verifies one install directory for `platform`: `provenance.json` parses and names `platform`,
/// the library path and every licence path are safe relative paths, the library is a regular
/// file inside the directory (links included) and its SHA-256 matches the recorded hash.
pub fn verify_install(install_dir: &Path, platform: Platform) -> Result<CachedRuntime, Rejected> {
    let install_dir =
        std::path::absolute(install_dir).unwrap_or_else(|_| install_dir.to_path_buf());
    verify(&install_dir, platform).map_err(|reason| Rejected {
        install_dir,
        reason,
    })
}

fn verify(dir: &Path, platform: Platform) -> Result<CachedRuntime, String> {
    let record = dir.join(PROVENANCE_FILE);
    let bytes = fs::read(&record).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            format!("{PROVENANCE_FILE} is missing (incomplete install)")
        } else {
            format!("{PROVENANCE_FILE} cannot be read: {err}")
        }
    })?;
    let provenance: Provenance = serde_json::from_slice(&bytes)
        .map_err(|err| format!("{PROVENANCE_FILE} is malformed: {err}"))?;
    if provenance.platform != platform {
        return Err(format!("built for {}, not {platform}", provenance.platform));
    }
    let lib_path = join_relative(dir, &provenance.lib).ok_or_else(|| {
        format!(
            "library path `{}` is not a relative path inside the install directory",
            provenance.lib
        )
    })?;
    let license_paths = provenance
        .licenses
        .iter()
        .map(|license| {
            join_relative(dir, license).ok_or_else(|| {
                format!(
                    "licence path `{license}` is not a relative path inside the install directory"
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    // Metadata follows links, so a link to a file counts as that file.
    let meta = fs::metadata(&lib_path).map_err(|err| {
        if err.kind() == io::ErrorKind::NotFound {
            format!("library `{}` is missing", provenance.lib)
        } else {
            format!("library `{}` cannot be read: {err}", provenance.lib)
        }
    })?;
    if !meta.is_file() {
        return Err(format!(
            "library `{}` is not a regular file",
            provenance.lib
        ));
    }
    let inside = fs::canonicalize(dir)
        .and_then(|dir| fs::canonicalize(&lib_path).map(|lib| lib.starts_with(dir)))
        .map_err(|err| format!("library `{}` cannot be resolved: {err}", provenance.lib))?;
    if !inside {
        return Err(format!(
            "library `{}` is a link that leaves the install directory",
            provenance.lib
        ));
    }
    let actual = hash_file(&lib_path)
        .map_err(|err| format!("library `{}` cannot be read: {err}", provenance.lib))?;
    if actual != provenance.lib_sha256 {
        return Err(format!(
            "library `{}` does not match its recorded hash (hash mismatch): expected sha256 {}, \
             found {actual}",
            provenance.lib, provenance.lib_sha256
        ));
    }
    Ok(CachedRuntime {
        version: provenance.version,
        platform: provenance.platform,
        lib_path,
        lib_sha256: provenance.lib_sha256,
        source: provenance.source,
        install_dir: dir.to_path_buf(),
        artifact_url: provenance.artifact_url,
        artifact_sha256: provenance.artifact_sha256,
        license_paths,
        provenance_modified: fs::metadata(&record).and_then(|meta| meta.modified()).ok(),
    })
}

/// `dir` joined with a provenance path (`/`-separated; joined per component for native
/// separators), `None` when the path is unsafe.
fn join_relative(dir: &Path, relative: &str) -> Option<PathBuf> {
    safe_relative(relative).map(|relative| {
        relative
            .split('/')
            .fold(dir.to_path_buf(), |path, part| path.join(part))
    })
}

fn hash_file(path: &Path) -> io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex::encode(hasher.finalize()))
}

/// The subdirectories of `dir`; none when it does not exist.
fn subdirectories(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(context(err, "listing", dir)),
    };
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| context(err, "listing", dir))?;
        if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            dirs.push(entry.path());
        }
    }
    Ok(dirs)
}

fn context(err: io::Error, doing: &str, path: &Path) -> io::Error {
    io::Error::new(err.kind(), format!("{doing} {}: {err}", path.display()))
}

/// Orders runtime versions by their dot-separated numeric components (`3.26.10.1` is newer
/// than `3.26.9.1`; missing components count as zero, so `3.26.9.1` equals `3.26.9.1.0`). A
/// component reads as its leading digits, and as zero without any; anything after them, such as
/// a pre-release tag, is ignored.
pub fn compare_versions(a: &str, b: &str) -> Ordering {
    let (mut a, mut b) = (a.split('.'), b.split('.'));
    loop {
        let (left, right) = (a.next(), b.next());
        if left.is_none() && right.is_none() {
            return Ordering::Equal;
        }
        let (left, right) = (
            leading_number(left.unwrap_or("")),
            leading_number(right.unwrap_or("")),
        );
        match left.len().cmp(&right.len()).then_with(|| left.cmp(right)) {
            Ordering::Equal => {}
            other => return other,
        }
    }
}

/// The leading ASCII digits of `component` without leading zeros (empty is zero).
fn leading_number(component: &str) -> &str {
    let end = component
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(component.len());
    component[..end].trim_start_matches('0')
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use super::*;

    fn root(vars: &[(&str, &str)]) -> Option<PathBuf> {
        let vars: HashMap<&str, &str> = vars.iter().copied().collect();
        cache_root_from(|name| vars.get(name).map(PathBuf::from))
    }

    #[test]
    fn explicit_cache_dir_wins() {
        let dir = Path::new("custom").join("cache");
        let found = root(&[
            ("XBBG_CACHE_DIR", dir.to_str().unwrap()),
            ("LOCALAPPDATA", "x"),
            ("USERPROFILE", "x"),
            ("XDG_CACHE_HOME", "x"),
            ("HOME", "y"),
        ]);
        assert_eq!(found, Some(dir));
    }

    #[test]
    #[cfg(windows)]
    fn windows_prefers_local_app_data_then_user_profile() {
        assert_eq!(
            root(&[("LOCALAPPDATA", r"C:\Local"), ("USERPROFILE", r"C:\Home")]),
            Some(PathBuf::from(r"C:\Local\xbbg"))
        );
        assert_eq!(
            root(&[("USERPROFILE", r"C:\Home")]),
            Some(PathBuf::from(r"C:\Home\AppData\Local\xbbg"))
        );
        assert_eq!(root(&[]), None);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn linux_uses_xdg_cache_home_when_absolute() {
        assert_eq!(
            root(&[("XDG_CACHE_HOME", "/xdg"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/xdg/xbbg"))
        );
        assert_eq!(
            root(&[("XDG_CACHE_HOME", "relative"), ("HOME", "/home/u")]),
            Some(PathBuf::from("/home/u/.cache/xbbg"))
        );
        assert_eq!(root(&[]), None);
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn macos_uses_library_caches() {
        assert_eq!(
            root(&[("HOME", "home")]),
            Some(PathBuf::from("home/Library/Caches/xbbg"))
        );
        assert_eq!(root(&[]), None);
    }

    #[test]
    fn versions_order_by_numeric_components() {
        assert_eq!(compare_versions("3.26.10.1", "3.26.9.1"), Ordering::Greater);
        assert_eq!(compare_versions("3.26.8.1", "3.26.9.1"), Ordering::Less);
        assert_eq!(compare_versions("3.26.9.1", "3.26.9.1.0"), Ordering::Equal);
        assert_eq!(compare_versions("3.26.9.1", "3.26.09.1"), Ordering::Equal);
        assert_eq!(compare_versions("3.26.9.1", "3.26.9"), Ordering::Greater);
        assert_eq!(
            compare_versions("3.26.99999999999999999999999", "3.26.9"),
            Ordering::Greater
        );
        assert_eq!(compare_versions("3.26.9.1rc1", "3.26.9.1"), Ordering::Equal);
    }

    const LIBRARY: &[u8] = b"synthetic library bytes";

    fn sha256_hex(bytes: &[u8]) -> String {
        hex::encode(Sha256::digest(bytes))
    }

    /// A provenance for a Linux install whose library holds [`LIBRARY`].
    fn provenance(version: &str) -> Provenance {
        Provenance {
            version: version.to_string(),
            platform: Platform::LinuxX86_64,
            source: "artifact:synthetic".into(),
            artifact_url: "file:synthetic".into(),
            artifact_sha256: sha256_hex(version.as_bytes()),
            lib: "lib/libblpapi3_64.so".into(),
            lib_sha256: sha256_hex(LIBRARY),
            licenses: vec!["licenses/LICENSE".into()],
        }
    }

    /// Installs `provenance` as `<cache>/sdk/<version>/<platform>/<name>` with the library bytes
    /// the provenance expects, and returns the install directory.
    fn install(cache: &Path, name: &str, provenance: &Provenance) -> PathBuf {
        let dir = cache
            .join(INSTALLS_DIR)
            .join(&provenance.version)
            .join(provenance.platform.as_str())
            .join(name);
        fs::create_dir_all(dir.join("lib")).unwrap();
        fs::write(dir.join("lib").join("libblpapi3_64.so"), LIBRARY).unwrap();
        write_provenance(&dir, provenance);
        dir
    }

    fn write_provenance(dir: &Path, provenance: &Provenance) {
        fs::write(
            dir.join(PROVENANCE_FILE),
            serde_json::to_vec_pretty(provenance).unwrap(),
        )
        .unwrap();
    }

    /// Sets the modification time of `provenance.json` in `dir` to `age` ago.
    fn age_provenance(dir: &Path, age: Duration) {
        let file = fs::File::options()
            .write(true)
            .open(dir.join(PROVENANCE_FILE))
            .unwrap();
        file.set_modified(SystemTime::now() - age).unwrap();
    }

    fn only_rejection(scan: &CacheScan) -> &Rejected {
        assert_eq!(scan.rejected.len(), 1, "{:?}", scan.rejected);
        &scan.rejected[0]
    }

    #[test]
    fn a_missing_cache_root_is_an_empty_scan() {
        let cache = tempfile::tempdir().unwrap();
        let scan = scan_cache(&cache.path().join("missing"), Platform::LinuxX86_64).unwrap();
        assert_eq!(scan, CacheScan::default());
        let empty = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert_eq!(empty, CacheScan::default());
        assert_eq!(
            resolve_cached(
                Some(&cache.path().join("missing")),
                Some(Platform::LinuxX86_64)
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn a_valid_install_resolves_to_an_absolute_library() {
        let cache = tempfile::tempdir().unwrap();
        let dir = install(
            cache.path(),
            "artifact-aaaaaaaaaaaa",
            &provenance("3.26.9.1"),
        );
        let runtime = resolve_cached(Some(cache.path()), Some(Platform::LinuxX86_64))
            .unwrap()
            .unwrap();
        assert_eq!(runtime.version, "3.26.9.1");
        assert_eq!(runtime.platform, Platform::LinuxX86_64);
        assert!(runtime.lib_path.is_absolute());
        assert_eq!(runtime.lib_path, dir.join("lib").join("libblpapi3_64.so"));
        assert_eq!(runtime.install_dir, dir);
        assert_eq!(runtime.lib_sha256, sha256_hex(LIBRARY));
        assert_eq!(runtime.source, "artifact:synthetic");
        assert_eq!(
            runtime.license_paths,
            [dir.join("licenses").join("LICENSE")]
        );
        assert!(runtime.provenance_modified.is_some());
    }

    #[test]
    fn newer_versions_come_first_by_number_not_by_text() {
        let cache = tempfile::tempdir().unwrap();
        for version in ["3.26.9.1", "3.26.10.1"] {
            install(cache.path(), "artifact-aaaaaaaaaaaa", &provenance(version));
        }
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        let versions: Vec<_> = scan.runtimes.iter().map(|r| r.version.as_str()).collect();
        assert_eq!(versions, ["3.26.10.1", "3.26.9.1"]);
        let best = resolve_cached(Some(cache.path()), Some(Platform::LinuxX86_64)).unwrap();
        assert_eq!(best.unwrap().version, "3.26.10.1");
    }

    #[test]
    fn one_version_orders_by_provenance_modification_time() {
        let cache = tempfile::tempdir().unwrap();
        let older = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        let newer = install(cache.path(), "conda-222222222222", &provenance("3.26.9.1"));
        let oldest = install(
            cache.path(),
            "artifact-333333333333",
            &provenance("3.26.9.1"),
        );
        age_provenance(&oldest, Duration::from_secs(3 * 86_400));
        age_provenance(&older, Duration::from_secs(86_400));
        age_provenance(&newer, Duration::from_secs(60));
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        let dirs: Vec<_> = scan.runtimes.iter().map(|r| &r.install_dir).collect();
        assert_eq!(dirs, [&newer, &older, &oldest]);
        // A higher version beats a fresher install of a lower one.
        let lower = install(cache.path(), "pypi-444444444444", &provenance("3.26.8.1"));
        age_provenance(&lower, Duration::from_secs(1));
        let best = resolve_cached(Some(cache.path()), Some(Platform::LinuxX86_64))
            .unwrap()
            .unwrap();
        assert_eq!(best.install_dir, newer);
    }

    #[test]
    fn a_modified_library_is_rejected_and_the_next_install_is_selected() {
        let cache = tempfile::tempdir().unwrap();
        let good = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        let tampered = install(cache.path(), "pypi-222222222222", &provenance("3.26.10.1"));
        let lib = tampered.join("lib").join("libblpapi3_64.so");
        let mut bytes = fs::read(&lib).unwrap();
        bytes[0] ^= 1;
        fs::write(&lib, bytes).unwrap();

        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert_eq!(scan.runtimes.len(), 1);
        assert_eq!(scan.runtimes[0].install_dir, good);
        let rejected = only_rejection(&scan);
        assert_eq!(rejected.install_dir, tampered);
        assert!(
            rejected.reason.contains("hash mismatch"),
            "{}",
            rejected.reason
        );
        let best = resolve_cached(Some(cache.path()), Some(Platform::LinuxX86_64)).unwrap();
        assert_eq!(best.unwrap().install_dir, good);
    }

    #[test]
    fn a_missing_library_or_a_directory_in_its_place_is_rejected() {
        let cache = tempfile::tempdir().unwrap();
        let gone = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        fs::remove_file(gone.join("lib").join("libblpapi3_64.so")).unwrap();
        let dir = install(cache.path(), "pypi-222222222222", &provenance("3.26.10.1"));
        let lib = dir.join("lib").join("libblpapi3_64.so");
        fs::remove_file(&lib).unwrap();
        fs::create_dir(&lib).unwrap();
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert!(scan.runtimes.is_empty());
        let reasons: Vec<_> = scan.rejected.iter().map(|r| r.reason.as_str()).collect();
        assert_eq!(scan.rejected.len(), 2);
        assert!(
            reasons.iter().any(|r| r.contains("is missing")),
            "{reasons:?}"
        );
        assert!(
            reasons.iter().any(|r| r.contains("not a regular file")),
            "{reasons:?}"
        );
    }

    #[test]
    fn missing_or_malformed_provenance_is_rejected() {
        let cache = tempfile::tempdir().unwrap();
        let missing = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        fs::remove_file(missing.join(PROVENANCE_FILE)).unwrap();
        let garbage = install(cache.path(), "pypi-222222222222", &provenance("3.26.9.1"));
        fs::write(garbage.join(PROVENANCE_FILE), b"not json").unwrap();
        let partial = install(cache.path(), "pypi-333333333333", &provenance("3.26.9.1"));
        fs::write(partial.join(PROVENANCE_FILE), br#"{"version":"3.26.9.1"}"#).unwrap();
        let unknown_platform = install(cache.path(), "pypi-444444444444", &provenance("3.26.9.1"));
        let mut json: serde_json::Value =
            serde_json::from_slice(&fs::read(unknown_platform.join(PROVENANCE_FILE)).unwrap())
                .unwrap();
        json["platform"] = "linux-aarch64".into();
        fs::write(
            unknown_platform.join(PROVENANCE_FILE),
            serde_json::to_vec(&json).unwrap(),
        )
        .unwrap();

        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert!(scan.runtimes.is_empty());
        let by_dir: HashMap<_, _> = scan
            .rejected
            .iter()
            .map(|r| (r.install_dir.clone(), r.reason.as_str()))
            .collect();
        assert!(by_dir[&missing].contains("provenance.json is missing"));
        assert!(by_dir[&garbage].contains("provenance.json is malformed"));
        assert!(by_dir[&partial].contains("provenance.json is malformed"));
        assert!(by_dir[&unknown_platform].contains("linux-aarch64"));
    }

    #[test]
    fn library_paths_that_escape_or_are_absolute_are_rejected() {
        let cache = tempfile::tempdir().unwrap();
        // A real file outside the install that the recorded paths point at.
        let outside = cache.path().join("outside.so");
        fs::write(&outside, LIBRARY).unwrap();
        let absolute = outside.to_string_lossy().into_owned();
        let cases = [
            ("pypi-111111111111", "../../../../../outside.so".to_string()),
            ("pypi-222222222222", "lib/../../x".to_string()),
            ("pypi-333333333333", absolute),
            ("pypi-444444444444", "C:\\x\\blpapi3_64.dll".to_string()),
            (
                "pypi-555555555555",
                "..\\..\\..\\..\\..\\outside.so".to_string(),
            ),
            ("pypi-666666666666", String::new()),
        ];
        for (name, lib) in &cases {
            let mut record = provenance("3.26.9.1");
            record.lib.clone_from(lib);
            install(cache.path(), name, &record);
        }
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert!(scan.runtimes.is_empty(), "{:?}", scan.runtimes);
        assert_eq!(scan.rejected.len(), cases.len());
        for rejected in &scan.rejected {
            assert!(
                rejected
                    .reason
                    .contains("not a relative path inside the install directory"),
                "{}",
                rejected.reason
            );
        }
    }

    #[test]
    fn licence_paths_that_escape_are_rejected() {
        let cache = tempfile::tempdir().unwrap();
        let mut record = provenance("3.26.9.1");
        record.licenses = vec!["../LICENSE".into()];
        install(cache.path(), "pypi-111111111111", &record);
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert!(scan.runtimes.is_empty());
        assert!(only_rejection(&scan).reason.contains("licence path"));
    }

    #[test]
    #[cfg(unix)]
    fn a_library_link_that_leaves_the_install_is_rejected() {
        let cache = tempfile::tempdir().unwrap();
        let dir = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        let outside = cache.path().join("outside.so");
        fs::write(&outside, LIBRARY).unwrap();
        let lib = dir.join("lib").join("libblpapi3_64.so");
        fs::remove_file(&lib).unwrap();
        std::os::unix::fs::symlink(&outside, &lib).unwrap();
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert!(scan.runtimes.is_empty());
        assert!(
            only_rejection(&scan)
                .reason
                .contains("leaves the install directory")
        );
    }

    #[test]
    fn another_platform_is_not_listed_and_a_mislabelled_install_is_rejected() {
        let cache = tempfile::tempdir().unwrap();
        let mut windows = provenance("3.26.9.1");
        windows.platform = Platform::WindowsX86_64;
        windows.lib = "lib/blpapi3_64.dll".into();
        let win_dir = cache
            .path()
            .join(INSTALLS_DIR)
            .join("3.26.9.1")
            .join(Platform::WindowsX86_64.as_str())
            .join("pypi-111111111111");
        fs::create_dir_all(win_dir.join("lib")).unwrap();
        fs::write(win_dir.join("lib").join("blpapi3_64.dll"), LIBRARY).unwrap();
        write_provenance(&win_dir, &windows);

        // Linux provenance filed under the macOS directory.
        let mislabelled = cache
            .path()
            .join(INSTALLS_DIR)
            .join("3.26.9.1")
            .join(Platform::MacosAarch64.as_str())
            .join("pypi-222222222222");
        fs::create_dir_all(mislabelled.join("lib")).unwrap();
        fs::write(mislabelled.join("lib").join("libblpapi3_64.so"), LIBRARY).unwrap();
        write_provenance(&mislabelled, &provenance("3.26.9.1"));

        let linux = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert_eq!(
            linux,
            CacheScan::default(),
            "other platforms are not scanned"
        );
        let windows_scan = scan_cache(cache.path(), Platform::WindowsX86_64).unwrap();
        assert_eq!(windows_scan.runtimes.len(), 1);
        assert_eq!(windows_scan.runtimes[0].platform, Platform::WindowsX86_64);
        let mac = scan_cache(cache.path(), Platform::MacosAarch64).unwrap();
        assert!(mac.runtimes.is_empty());
        let rejected = only_rejection(&mac);
        assert_eq!(rejected.install_dir, mislabelled);
        assert!(
            rejected
                .reason
                .contains("built for linux-x86_64, not macos-aarch64")
        );

        let all = scan_cache_all(cache.path()).unwrap();
        assert_eq!(all.runtimes.len(), 1);
        assert_eq!(all.rejected.len(), 1);
    }

    #[test]
    fn scan_cache_all_orders_by_version_then_platform() {
        let cache = tempfile::tempdir().unwrap();
        install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        let mut windows = provenance("3.26.9.1");
        windows.platform = Platform::WindowsX86_64;
        windows.lib = "lib/libblpapi3_64.so".into();
        install(cache.path(), "pypi-111111111111", &windows);
        install(cache.path(), "pypi-111111111111", &provenance("3.26.10.1"));
        let all = scan_cache_all(cache.path()).unwrap();
        let order: Vec<_> = all
            .runtimes
            .iter()
            .map(|r| (r.version.as_str(), r.platform))
            .collect();
        assert_eq!(
            order,
            [
                ("3.26.10.1", Platform::LinuxX86_64),
                ("3.26.9.1", Platform::WindowsX86_64),
                ("3.26.9.1", Platform::LinuxX86_64),
            ]
        );
    }

    #[test]
    fn verify_install_checks_one_directory() {
        let cache = tempfile::tempdir().unwrap();
        let dir = install(cache.path(), "pypi-111111111111", &provenance("3.26.9.1"));
        assert!(verify_install(&dir, Platform::LinuxX86_64).is_ok());
        let wrong = verify_install(&dir, Platform::MacosAarch64).unwrap_err();
        assert_eq!(wrong.install_dir, dir);
        assert!(wrong.reason.contains("not macos-aarch64"));
        let missing = verify_install(&cache.path().join("nowhere"), Platform::LinuxX86_64);
        assert!(missing.unwrap_err().reason.contains("is missing"));
    }

    #[test]
    fn files_in_the_listing_levels_are_ignored() {
        let cache = tempfile::tempdir().unwrap();
        let sdk = cache.path().join(INSTALLS_DIR);
        fs::create_dir_all(sdk.join("3.26.9.1").join(Platform::LinuxX86_64.as_str())).unwrap();
        fs::write(sdk.join("stray.txt"), b"x").unwrap();
        fs::write(sdk.join("3.26.9.1").join("stray.txt"), b"x").unwrap();
        fs::write(
            sdk.join("3.26.9.1")
                .join(Platform::LinuxX86_64.as_str())
                .join("stray.txt"),
            b"x",
        )
        .unwrap();
        fs::create_dir_all(sdk.join("3.26.9.1").join("not-a-platform").join("x")).unwrap();
        let scan = scan_cache(cache.path(), Platform::LinuxX86_64).unwrap();
        assert_eq!(scan, CacheScan::default());
    }
}
