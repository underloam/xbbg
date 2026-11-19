//! Run-time loading of the Bloomberg runtime.
//!
//! No part of this workspace links `libblpapi3_64`. A host picks one library by its own search
//! order and calls [`load`] once, right after its own module loads; every BLPAPI call then goes
//! through the function table [`sdk`] returns. [`load_system`] is the last resort of such a search:
//! it lets the operating system find the library, as it did when the runtime was linked.
//!
//! The loaded library is process-wide and is never unloaded: the SDK keeps threads and callbacks
//! that point into it. Loading a second, different library is refused.

use std::ffi::c_void;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use crate::ffi::Blpapi;

#[cfg(unix)]
type Library = libloading::os::unix::Library;
#[cfg(windows)]
type Library = libloading::os::windows::Library;

/// The loaded runtime: the resolved function table (which owns the open library) and its path.
struct Loaded {
    table: Blpapi,
    path: PathBuf,
}

/// The one loaded runtime. Set once and never cleared, so `sdk()` is a single atomic load.
static SDK: OnceLock<Loaded> = OnceLock::new();

/// Serializes loading, so concurrent callers never open the same library twice.
static LOAD_LOCK: Mutex<()> = Mutex::new(());

/// Why a Bloomberg runtime could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SdkLoadError {
    /// [`load`] takes an absolute path: a relative one would be resolved by the platform's library
    /// search order, which is exactly what that function exists to avoid.
    #[error("the Bloomberg runtime path must be absolute: {}", path.display())]
    RelativePath {
        /// The path that was given.
        path: PathBuf,
    },
    /// The file could not be opened as a library, or the operating system found none.
    #[error("cannot open the Bloomberg runtime {}: {message}", path.display())]
    Open {
        /// The library that was opened; for [`load_system`], the file name that was searched for.
        path: PathBuf,
        /// The operating system's message.
        message: String,
    },
    /// The library opened but lacks functions this build calls: it is not a BLPAPI runtime, or it
    /// predates a function in use.
    #[error("the Bloomberg runtime {} lacks {message}", path.display())]
    MissingSymbol {
        /// The library that was opened.
        path: PathBuf,
        /// The missing functions, by name.
        message: String,
    },
    /// A different runtime is already loaded in this process.
    #[error(
        "a Bloomberg runtime is already loaded from {}; cannot load {}",
        loaded.display(),
        requested.display()
    )]
    AlreadyLoaded {
        /// The library this process already uses.
        loaded: PathBuf,
        /// The library that was requested.
        requested: PathBuf,
    },
    /// The dynamic loader mapped a different file than the one opened. macOS's dyld looks in the
    /// `DYLD_LIBRARY_PATH` directories by file name before it uses an absolute path, so the library
    /// that was chosen (and, for the xbbg cache, verified) would not be the one in use.
    #[error(
        "the dynamic loader mapped {} instead of the Bloomberg runtime {} (on macOS, \
         DYLD_LIBRARY_PATH is searched by file name first)",
        mapped.display(),
        path.display()
    )]
    Substituted {
        /// The library that was opened.
        path: PathBuf,
        /// The file the loader mapped instead.
        mapped: PathBuf,
    },
    /// [`load_system`] takes a bare file name: with a directory in it, the operating system would
    /// not search for it.
    #[error("not a library file name: {name}")]
    NotAFileName {
        /// The name that was given.
        name: String,
    },
}

/// Loads the Bloomberg runtime at `path` into this process.
///
/// Call it once, before anything else in this crate touches BLPAPI. Calling it again with the same
/// path is a no-op; a different path is [`SdkLoadError::AlreadyLoaded`]. The library stays loaded
/// for the life of the process.
///
/// `path` must be absolute. On Windows the library's own directory is searched for its
/// dependencies (`LOAD_WITH_ALTERED_SEARCH_PATH`); elsewhere the library is opened with
/// `RTLD_NOW | RTLD_LOCAL`, so its symbols stay out of the global namespace and a missing one fails
/// here rather than at the first call.
pub fn load(path: impl AsRef<Path>) -> Result<(), SdkLoadError> {
    let path = path.as_ref();
    if !path.is_absolute() {
        return Err(SdkLoadError::RelativePath {
            path: path.to_path_buf(),
        });
    }

    if let Some(loaded) = SDK.get() {
        return same_runtime(loaded, path);
    }

    let _guard = LOAD_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    // A concurrent load may have finished while this one waited for the lock.
    if let Some(loaded) = SDK.get() {
        return same_runtime(loaded, path);
    }

    let table = open(path)?;
    store(table, path.to_path_buf());
    Ok(())
}

/// Loads the Bloomberg runtime the operating system finds for the library file name `name`
/// (`libblpapi3_64.so`, `blpapi3_64.dll`), and returns the file that was loaded.
///
/// This is where the operating system looked for the runtime when it was linked at build time, for
/// installs no explicit path names:
///
/// * Linux: the dynamic loader's own search (`LD_LIBRARY_PATH`, the `ldconfig` cache, the default
///   library directories).
/// * macOS: `/usr/local/lib`, then `/usr/lib`, dyld's default fallback directories. The file name
///   is not handed to `dlopen`, which would also search the current directory.
/// * Windows: the application directory, the system directory, and directories added with
///   `AddDllDirectory` or `SetDllDirectory` (`LOAD_LIBRARY_SEARCH_DEFAULT_DIRS`); not the current
///   directory and not `PATH`.
///
/// When a runtime is already loaded, its path is returned and nothing is searched.
///
/// # Errors
///
/// [`SdkLoadError::NotAFileName`] if `name` holds a directory; [`SdkLoadError::Open`] if the
/// operating system finds no such library or cannot open it; [`SdkLoadError::MissingSymbol`] if the
/// library it finds lacks functions this build calls.
pub fn load_system(name: &str) -> Result<&'static Path, SdkLoadError> {
    let bare = !name.is_empty()
        && !name.contains(['/', '\\'])
        && Path::new(name).file_name().is_some_and(|file| file == name);
    if !bare {
        return Err(SdkLoadError::NotAFileName {
            name: name.to_owned(),
        });
    }

    if let Some(loaded) = SDK.get() {
        return Ok(&loaded.path);
    }

    let _guard = LOAD_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(loaded) = SDK.get() {
        return Ok(&loaded.path);
    }

    let (table, path) = open_system(name)?;
    Ok(&store(table, path).path)
}

/// Publishes the loaded runtime; call with [`LOAD_LOCK`] held and [`SDK`] checked empty, so the
/// initializer always runs.
fn store(table: Blpapi, path: PathBuf) -> &'static Loaded {
    SDK.get_or_init(|| Loaded { table, path })
}

fn same_runtime(loaded: &Loaded, requested: &Path) -> Result<(), SdkLoadError> {
    if is_same_file(&loaded.path, requested) {
        Ok(())
    } else {
        Err(SdkLoadError::AlreadyLoaded {
            loaded: loaded.path.clone(),
            requested: requested.to_path_buf(),
        })
    }
}

/// Same path, or two spellings (a symlink, `..` components) of one file.
fn is_same_file(a: &Path, b: &Path) -> bool {
    a == b
        || matches!(
            (a.canonicalize(), b.canonicalize()),
            (Ok(a), Ok(b)) if a == b
        )
}

/// Opens the library at the absolute `path` and resolves the function table from it.
fn open(path: &Path) -> Result<Blpapi, SdkLoadError> {
    // SAFETY: opening a library runs its initializers. `path` is the Bloomberg runtime the caller
    // chose, which is the premise of this whole module.
    #[cfg(unix)]
    let library = unsafe {
        Library::open(
            Some(path),
            libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL,
        )
    };
    // SAFETY: as above.
    #[cfg(windows)]
    let library = unsafe {
        Library::load_with_flags(path, libloading::os::windows::LOAD_WITH_ALTERED_SEARCH_PATH)
    };
    let library = library.map_err(|error| SdkLoadError::Open {
        path: path.to_path_buf(),
        message: os_message(&error),
    })?;

    #[cfg(unix)]
    if let Some(mapped) = mapped_image(&library)
        && !is_same_file(&mapped, path)
    {
        // Dropping the library closes the handle this call opened.
        return Err(SdkLoadError::Substituted {
            path: path.to_path_buf(),
            mapped,
        });
    }

    into_table(library, path)
}

/// Opens the library the dynamic loader finds for the bare file name `name`; see [`load_system`].
#[cfg(all(unix, not(target_os = "macos")))]
fn open_system(name: &str) -> Result<(Blpapi, PathBuf), SdkLoadError> {
    // SAFETY: opening a library runs its initializers; the caller asked for the system's
    // Bloomberg runtime by its file name.
    let library = unsafe {
        Library::open(
            Some(name),
            libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL,
        )
    }
    .map_err(|error| SdkLoadError::Open {
        path: PathBuf::from(name),
        message: os_message(&error),
    })?;
    let path = mapped_image(&library).unwrap_or_else(|| PathBuf::from(name));
    let table = into_table(library, &path)?;
    Ok((table, path))
}

/// dyld's default fallback directories, which is where an unresolved `@rpath/` library ended up
/// being looked for when the runtime was linked.
#[cfg(target_os = "macos")]
const SYSTEM_LIBRARY_DIRS: &[&str] = &["/usr/local/lib", "/usr/lib"];

/// Opens `name` from dyld's default fallback directories; see [`load_system`].
#[cfg(target_os = "macos")]
fn open_system(name: &str) -> Result<(Blpapi, PathBuf), SdkLoadError> {
    for dir in SYSTEM_LIBRARY_DIRS {
        let path = Path::new(dir).join(name);
        if path.is_file() {
            return open(&path).map(|table| (table, path));
        }
    }
    Err(SdkLoadError::Open {
        path: PathBuf::from(name),
        message: format!("not in {}", SYSTEM_LIBRARY_DIRS.join(" or ")),
    })
}

/// Opens the library Windows finds for the bare file name `name`; see [`load_system`].
#[cfg(windows)]
fn open_system(name: &str) -> Result<(Blpapi, PathBuf), SdkLoadError> {
    // SAFETY: opening a library runs its initializers; the caller asked for the system's
    // Bloomberg runtime by its file name.
    let library = unsafe {
        Library::load_with_flags(
            name,
            libloading::os::windows::LOAD_LIBRARY_SEARCH_DEFAULT_DIRS,
        )
    }
    .map_err(|error| SdkLoadError::Open {
        path: PathBuf::from(name),
        message: os_message(&error),
    })?;
    let (library, path) = module_path(library);
    let path = path.unwrap_or_else(|| PathBuf::from(name));
    let table = into_table(library, &path)?;
    Ok((table, path))
}

/// Resolves the function table from an open library, naming the functions it lacks.
fn into_table(library: Library, path: &Path) -> Result<Blpapi, SdkLoadError> {
    // The generated loader stops at the first missing function and, on Windows, cannot say which
    // one it was. Looking each one up first costs microseconds and names them all.
    let missing = missing_functions(&library);
    if !missing.is_empty() {
        return Err(SdkLoadError::MissingSymbol {
            path: path.to_path_buf(),
            message: describe_missing(&missing),
        });
    }

    // SAFETY: `Blpapi`'s function pointers carry the signatures of the SDK headers this crate was
    // built against, which is the contract of the BLPAPI C ABI for a runtime of that family. The
    // library handle moves into the table and is never closed.
    unsafe { Blpapi::from_library(library) }.map_err(|error| SdkLoadError::MissingSymbol {
        path: path.to_path_buf(),
        message: os_message(&error),
    })
}

/// The functions of the table that `library` does not export.
fn missing_functions(library: &Library) -> Vec<&'static str> {
    blpapi_sys::FUNCTION_NAMES
        .iter()
        .filter(|name| {
            // SAFETY: the symbol is only looked up; it is never called or read through.
            unsafe { library.get::<*mut c_void>(**name) }.is_err()
        })
        .filter_map(|name| name.to_str().ok())
        .collect()
}

/// `blpapi_A, blpapi_B and 3 more (5 of 157 functions): ...`, for [`SdkLoadError::MissingSymbol`].
fn describe_missing(missing: &[&str]) -> String {
    const SHOWN: usize = 5;
    let total = blpapi_sys::FUNCTION_NAMES.len();
    if missing.len() == total {
        return format!(
            "every one of the {total} BLPAPI functions this build calls: it is not a Bloomberg \
             runtime"
        );
    }
    let mut names = missing
        .iter()
        .take(SHOWN)
        .copied()
        .collect::<Vec<_>>()
        .join(", ");
    if missing.len() > SHOWN {
        names.push_str(&format!(" and {} more", missing.len() - SHOWN));
    }
    format!(
        "{names} ({} of the {total} BLPAPI functions this build calls): it is older than this \
         build needs",
        missing.len()
    )
}

/// What the operating system said. `libloading` prints only which call failed (`dlopen failed`)
/// and keeps the reason as the error's source.
fn os_message(error: &libloading::Error) -> String {
    match std::error::Error::source(error) {
        Some(reason) => reason.to_string().trim().to_owned(),
        None => error.to_string(),
    }
}

/// The file the dynamic loader actually mapped for `library`, from the image that holds one of
/// its functions. `None` when the library has no BLPAPI symbol (the table load then reports it)
/// or the loader cannot say.
#[cfg(unix)]
fn mapped_image(library: &Library) -> Option<PathBuf> {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: looking a symbol up only reads the library's symbol table; its address is only
    // passed to dladdr, never called or dereferenced.
    let symbol = unsafe { library.get::<*mut c_void>(c"blpapi_getVersionInfo") }.ok()?;
    let address = symbol.into_raw();
    let mut info = std::mem::MaybeUninit::<libc::Dl_info>::zeroed();
    // SAFETY: `address` lies in an image this process has loaded; dladdr fills `info` and returns
    // zero when it cannot.
    if unsafe { libc::dladdr(address, info.as_mut_ptr()) } == 0 {
        return None;
    }
    // SAFETY: dladdr returned non-zero, so it initialized `info`.
    let info = unsafe { info.assume_init() };
    if info.dli_fname.is_null() {
        return None;
    }
    // SAFETY: `dli_fname` is a NUL-terminated path owned by the loader for the image's lifetime,
    // and the image stays loaded while `library` is open.
    let name = unsafe { CStr::from_ptr(info.dli_fname) };
    Some(PathBuf::from(OsStr::from_bytes(name.to_bytes())))
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetModuleFileNameW(module: isize, filename: *mut u16, size: u32) -> u32;
}

/// The file Windows loaded `library` from, or `None` when it cannot say.
#[cfg(windows)]
fn module_path(library: Library) -> (Library, Option<PathBuf>) {
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;

    // The longest path Windows supports, in UTF-16 units, with its terminator.
    const CAPACITY: usize = 32_768;
    let handle = library.into_raw();
    let mut buffer = vec![0u16; CAPACITY];
    // SAFETY: `handle` is the open module just taken out of `library`, and `buffer` is writable
    // for `CAPACITY` units.
    let length = unsafe { GetModuleFileNameW(handle, buffer.as_mut_ptr(), CAPACITY as u32) };
    // SAFETY: `handle` came from `into_raw` above and was not freed, so this restores ownership
    // of the same open module.
    let library = unsafe { Library::from_raw(handle) };
    let length = length as usize;
    // Zero is a failure, and a full buffer a truncated path.
    let path = (length > 0 && length < CAPACITY)
        .then(|| PathBuf::from(OsString::from_wide(&buffer[..length])));
    (library, path)
}

/// The loaded runtime's function table.
///
/// This is the hot path: one atomic load and a branch. Every BLPAPI call in this workspace is
/// `sdk().blpapi_…(…)`.
///
/// # Panics
///
/// If no runtime is loaded. Hosts call [`load`] right after their native module loads; code that
/// cannot know whether that happened checks [`loaded_sdk_path`] first.
#[inline]
pub fn sdk() -> &'static Blpapi {
    match SDK.get() {
        Some(loaded) => &loaded.table,
        None => sdk_not_loaded(),
    }
}

#[cold]
#[inline(never)]
#[track_caller]
fn sdk_not_loaded() -> ! {
    panic!(
        "the Bloomberg runtime is not loaded: call xbbg_core::load(<absolute path to the BLPAPI \
         library>) before any other xbbg_core call"
    )
}

/// The path of the loaded Bloomberg runtime, or `None` before [`load`] or [`load_system`] succeeds.
pub fn loaded_sdk_path() -> Option<&'static Path> {
    SDK.get().map(|loaded| loaded.path.as_path())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A library every supported system has and that is no Bloomberg runtime.
    fn system_library() -> (Library, PathBuf) {
        #[cfg(target_os = "macos")]
        let name = "/usr/lib/libSystem.B.dylib";
        #[cfg(all(unix, not(target_os = "macos")))]
        let name = "libc.so.6";
        #[cfg(windows)]
        let name = "kernel32.dll";
        // SAFETY: the C library (or kernel32) is already loaded in every process.
        let library = unsafe { libloading::Library::new(name) }.expect("the system library opens");
        (library.into(), PathBuf::from(name))
    }

    #[test]
    fn relative_paths_are_refused() {
        let error = load("libblpapi3_64.so").unwrap_err();
        assert!(matches!(error, SdkLoadError::RelativePath { .. }));
    }

    #[test]
    fn loading_is_idempotent_and_refuses_another_library() {
        // The test binary's constructor loaded the runtime (see `load_sdk_for_tests`).
        let path = loaded_sdk_path().expect("the constructor loaded a runtime");
        assert!(path.is_absolute());
        // The same path again is fine; so is the same file under another spelling.
        load(path).expect("reloading the same path");
        let respelled = path
            .parent()
            .expect("a library has a directory")
            .join(".")
            .join(path.file_name().expect("a library has a file name"));
        load(&respelled).expect("reloading the same file");
        // A different file is refused, naming both.
        let other = std::env::current_exe().expect("test executable path");
        match load(&other) {
            Err(SdkLoadError::AlreadyLoaded { loaded, requested }) => {
                assert_eq!(loaded, path);
                assert_eq!(requested, other);
            }
            other => panic!("expected AlreadyLoaded, got {other:?}"),
        }
        let (major, ..) = crate::sdk_version();
        assert!(major >= 3);
    }

    #[test]
    fn a_missing_library_reports_its_path_and_the_systems_reason() {
        // `load` only reaches the open step for a path that is not the loaded one; with a runtime
        // already loaded the comparison comes first, so exercise `open` directly.
        let missing = std::env::temp_dir().join("xbbg-no-such-runtime.so");
        match open(&missing) {
            Err(SdkLoadError::Open { path, message }) => {
                assert_eq!(path, missing);
                // The operating system's own words, not only the name of the call that failed.
                assert!(
                    !message.is_empty() && !message.ends_with("failed"),
                    "{message}"
                );
            }
            Err(other) => panic!("expected Open, got {other:?}"),
            Ok(_) => panic!("a missing library cannot open"),
        }
    }

    #[test]
    fn a_library_without_the_functions_is_named_as_no_bloomberg_runtime() {
        let (library, path) = system_library();
        let missing = missing_functions(&library);
        assert_eq!(missing.len(), blpapi_sys::FUNCTION_NAMES.len());
        assert!(missing.contains(&"blpapi_getVersionInfo"));
        match into_table(library, &path) {
            Err(SdkLoadError::MissingSymbol {
                path: reported,
                message,
            }) => {
                assert_eq!(reported, path);
                assert!(message.contains("not a Bloomberg runtime"), "{message}");
            }
            Err(other) => panic!("expected MissingSymbol, got {other:?}"),
            Ok(_) => panic!("the system library is not a Bloomberg runtime"),
        }
    }

    #[test]
    fn missing_functions_are_listed_by_name_up_to_a_handful() {
        let total = blpapi_sys::FUNCTION_NAMES.len();
        assert_eq!(
            describe_missing(&["blpapi_A", "blpapi_B"]),
            format!(
                "blpapi_A, blpapi_B (2 of the {total} BLPAPI functions this build calls): it is \
                 older than this build needs"
            )
        );
        let many = ["a", "b", "c", "d", "e", "f", "g"];
        assert!(describe_missing(&many).starts_with("a, b, c, d, e and 2 more (7 of the "));
    }

    #[test]
    fn the_system_search_takes_a_bare_file_name_and_reports_a_library_it_cannot_find() {
        for name in ["", "lib/libblpapi3_64.so", r"lib\blpapi3_64.dll", ".."] {
            assert!(
                matches!(load_system(name), Err(SdkLoadError::NotAFileName { .. })),
                "{name:?}"
            );
        }
        // A runtime is loaded in this process, so the public function returns it; the search
        // itself is exercised directly.
        assert_eq!(
            load_system("xbbg-no-such-runtime.so").expect("the loaded runtime"),
            loaded_sdk_path().expect("the constructor loaded a runtime")
        );
        match open_system("xbbg-no-such-runtime.so") {
            Err(SdkLoadError::Open { path, message }) => {
                assert_eq!(path, Path::new("xbbg-no-such-runtime.so"));
                assert!(
                    !message.is_empty() && !message.ends_with("failed"),
                    "{message}"
                );
            }
            Err(other) => panic!("expected Open, got {other:?}"),
            Ok(_) => panic!("no such library exists"),
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_reports_the_file_a_module_was_loaded_from() {
        let (library, _) = system_library();
        let (_library, path) = module_path(library);
        let path = path.expect("kernel32 has a file");
        assert!(path.is_absolute(), "{}", path.display());
        assert!(
            path.to_string_lossy()
                .to_ascii_lowercase()
                .ends_with("kernel32.dll"),
            "{}",
            path.display()
        );
    }
}
