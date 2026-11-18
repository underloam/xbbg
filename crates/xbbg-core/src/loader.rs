//! Run-time loading of the Bloomberg runtime.
//!
//! No part of this workspace links `libblpapi3_64`. A host picks one library path by its own search
//! order and calls [`load`] once, right after its own module loads; every BLPAPI call then goes
//! through the function table [`sdk`] returns.
//!
//! The loaded library is process-wide and is never unloaded: the SDK keeps threads and callbacks
//! that point into it. Loading a second, different library is refused.

use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use crate::ffi::Blpapi;

/// The loaded runtime: the resolved function table (which owns the open library) and its path.
struct Loaded {
    table: Blpapi,
    path: PathBuf,
}

/// The one loaded runtime. Set once and never cleared, so `sdk()` is a single atomic load.
static SDK: OnceLock<Loaded> = OnceLock::new();

/// Serializes [`load`], so concurrent callers never open the same library twice.
static LOAD_LOCK: Mutex<()> = Mutex::new(());

/// Why a Bloomberg runtime could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SdkLoadError {
    /// [`load`] takes an absolute path: a relative one would be resolved by the platform's library
    /// search order, which is exactly what this loader exists to avoid.
    #[error("the Bloomberg runtime path must be absolute: {}", path.display())]
    RelativePath {
        /// The path that was given.
        path: PathBuf,
    },
    /// The file could not be opened as a library.
    #[error("cannot open the Bloomberg runtime {}: {message}", path.display())]
    Open {
        /// The library that was opened.
        path: PathBuf,
        /// The loader's message.
        message: String,
    },
    /// The library opened but lacks a function this build calls (for example, it is not a BLPAPI
    /// runtime, or it predates a function in use).
    #[error("the Bloomberg runtime {} lacks a required function: {message}", path.display())]
    MissingSymbol {
        /// The library that was opened.
        path: PathBuf,
        /// The loader's message, naming the function.
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
    // A concurrent `load` may have finished while this one waited for the lock.
    if let Some(loaded) = SDK.get() {
        return same_runtime(loaded, path);
    }

    let table = open(path)?;
    let loaded = Loaded {
        table,
        path: path.to_path_buf(),
    };
    if SDK.set(loaded).is_err() {
        unreachable!("SDK is only set while holding LOAD_LOCK, after the check above");
    }
    Ok(())
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

fn open(path: &Path) -> Result<Blpapi, SdkLoadError> {
    // SAFETY: opening a library runs its initializers. `path` is the Bloomberg runtime the caller
    // chose, which is the premise of this whole module.
    #[cfg(unix)]
    let library = unsafe {
        libloading::os::unix::Library::open(
            Some(path),
            libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL,
        )
    };
    // SAFETY: as above.
    #[cfg(windows)]
    let library = unsafe {
        libloading::os::windows::Library::load_with_flags(
            path,
            libloading::os::windows::LOAD_WITH_ALTERED_SEARCH_PATH,
        )
    };
    let library = library.map_err(|error| SdkLoadError::Open {
        path: path.to_path_buf(),
        message: error.to_string(),
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

    // SAFETY: `Blpapi`'s function pointers carry the signatures of the SDK headers this crate was
    // built against, which is the contract of the BLPAPI C ABI for a runtime of that family. The
    // library handle moves into the table and is never closed.
    unsafe { Blpapi::from_library(library) }.map_err(|error| SdkLoadError::MissingSymbol {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

/// The file the dynamic loader actually mapped for `library`, from the image that holds one of
/// its functions. `None` when the library has no BLPAPI symbol (the table load then reports it)
/// or the loader cannot say.
#[cfg(unix)]
fn mapped_image(library: &libloading::os::unix::Library) -> Option<PathBuf> {
    use std::ffi::{CStr, OsStr};
    use std::os::unix::ffi::OsStrExt;

    // SAFETY: looking a symbol up only reads the library's symbol table; its address is only
    // passed to dladdr, never called or dereferenced.
    let symbol = unsafe { library.get::<*mut libc::c_void>(b"blpapi_getVersionInfo\0") }.ok()?;
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

/// The path of the loaded Bloomberg runtime, or `None` before [`load`] succeeds.
pub fn loaded_sdk_path() -> Option<&'static Path> {
    SDK.get().map(|loaded| loaded.path.as_path())
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn a_missing_library_reports_its_path() {
        // `load` only reaches the open step for a path that is not the loaded one; with a runtime
        // already loaded the comparison comes first, so exercise `open` directly.
        let missing = std::env::temp_dir().join("xbbg-no-such-runtime.so");
        match open(&missing) {
            Err(SdkLoadError::Open { path, .. }) => assert_eq!(path, missing),
            Err(other) => panic!("expected Open, got {other:?}"),
            Ok(_) => panic!("a missing library cannot open"),
        }
    }
}
