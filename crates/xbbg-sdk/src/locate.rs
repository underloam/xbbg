//! Where on disk the Bloomberg runtime is, from the environment and SDK directory layouts.
//!
//! Every function returns the absolute path of an existing library file or `None`; nothing here
//! opens the library. This is the one place that knows the SDK directory layouts, so a search
//! order built on it (the explicit path, host hint directories, the Terminal, the cache, ...)
//! shares them.

use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};

use crate::cache::compare_versions;
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

/// The runtime named by the environment: `BLPAPI_LIB_DIR`, else `BLPAPI_ROOT`.
///
/// `BLPAPI_LIB_DIR` is the directory holding the library. `BLPAPI_ROOT` is an SDK root; see
/// [`in_sdk_root`]. Empty variables count as unset, and relative paths resolve against the current
/// directory.
pub fn from_env(platform: Platform) -> Option<PathBuf> {
    from_dirs(
        platform,
        env_path("BLPAPI_LIB_DIR").as_deref(),
        env_path("BLPAPI_ROOT").as_deref(),
    )
}

/// [`from_env`] with the two directories given instead of read.
pub fn from_dirs(
    platform: Platform,
    lib_dir: Option<&Path>,
    sdk_root: Option<&Path>,
) -> Option<PathBuf> {
    lib_dir
        .and_then(|dir| library_in(dir, platform))
        .or_else(|| sdk_root.and_then(|root| in_sdk_root(root, platform)))
}

/// The library directly inside `dir`, trying the platform's file names in order.
pub fn library_in(dir: &Path, platform: Platform) -> Option<PathBuf> {
    platform
        .library_names()
        .iter()
        .map(|name| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// The library in an SDK root: in the root itself and the usual layouts under it (`bin/`, `lib/`,
/// `Linux/`, `Darwin/`, ...), and the same under each versioned directory one or two levels below
/// (`vendor/blpapi-sdk/3.26.5.1/`), newest version first, so a container of SDKs works as well as
/// one SDK.
pub fn in_sdk_root(root: &Path, platform: Platform) -> Option<PathBuf> {
    sdk_roots(root).into_iter().find_map(|sdk_root| {
        SDK_ROOT_SUBDIRS
            .iter()
            .find_map(|subdir| library_in(&sdk_root.join(subdir), platform))
    })
}

/// A non-empty environment variable as an absolute path.
fn env_path(name: &str) -> Option<PathBuf> {
    let value = std::env::var_os(name).filter(|value| !value.is_empty())?;
    std::path::absolute(value).ok()
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

#[cfg(test)]
mod tests {
    use super::*;

    fn touch(path: &Path) {
        fs::create_dir_all(path.parent().expect("a file has a directory")).expect("create dirs");
        fs::write(path, b"").expect("create file");
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
    fn lib_dir_holds_the_library_directly() {
        let platform = Platform::LinuxX86_64;
        let dir = tempfile::tempdir().expect("temp dir");
        assert_eq!(from_dirs(platform, Some(dir.path()), None), None);
        let library = dir.path().join("libblpapi3_64.so");
        touch(&library);
        assert_eq!(from_dirs(platform, Some(dir.path()), None), Some(library));
    }

    #[test]
    fn lib_dir_wins_over_root() {
        let platform = Platform::LinuxX86_64;
        let lib = tempfile::tempdir().expect("temp dir");
        let root = tempfile::tempdir().expect("temp dir");
        touch(&lib.path().join("libblpapi3_64.so"));
        touch(&root.path().join("Linux/libblpapi3_64.so"));
        assert_eq!(
            from_dirs(platform, Some(lib.path()), Some(root.path())),
            Some(lib.path().join("libblpapi3_64.so"))
        );
        // A lib dir without the library falls through to the root.
        let empty = tempfile::tempdir().expect("temp dir");
        assert_eq!(
            from_dirs(platform, Some(empty.path()), Some(root.path())),
            Some(root.path().join("Linux/libblpapi3_64.so"))
        );
    }

    #[test]
    fn root_layouts_cover_the_sdk_archives() {
        for (platform, relative) in [
            (Platform::LinuxX86_64, "Linux/libblpapi3_64.so"),
            (Platform::MacosAarch64, "Darwin/libblpapi3_64.so"),
            (Platform::MacosAarch64, "lib/libblpapi3_64.dylib"),
            (Platform::WindowsX86_64, "bin/blpapi3_64.dll"),
            (Platform::WindowsX86_64, "lib/win64/blpapi3_64.dll"),
            (Platform::LinuxX86_64, "libblpapi3_64.so"),
        ] {
            let root = tempfile::tempdir().expect("temp dir");
            touch(&root.path().join(relative));
            assert_eq!(
                in_sdk_root(root.path(), platform),
                Some(root.path().join(relative)),
                "{platform} {relative}"
            );
        }
    }

    #[test]
    fn root_may_contain_versioned_sdks_newest_first() {
        let platform = Platform::LinuxX86_64;
        let root = tempfile::tempdir().expect("temp dir");
        touch(&root.path().join("3.9.0/Linux/libblpapi3_64.so"));
        touch(&root.path().join("3.26.5.1/Linux/libblpapi3_64.so"));
        // One level deeper, as in a container holding a symlinked or nested SDK.
        touch(&root.path().join("sdk/3.30.0.0/Linux/libblpapi3_64.so"));
        assert_eq!(
            in_sdk_root(root.path(), platform),
            Some(root.path().join("3.26.5.1/Linux/libblpapi3_64.so"))
        );
    }

    #[test]
    fn nothing_found_is_none() {
        let root = tempfile::tempdir().expect("temp dir");
        assert_eq!(in_sdk_root(root.path(), Platform::LinuxX86_64), None);
        assert_eq!(
            in_sdk_root(&root.path().join("missing"), Platform::LinuxX86_64),
            None
        );
    }
}
