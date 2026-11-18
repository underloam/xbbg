//! Load a Bloomberg runtime library by absolute path and keep it loaded.
//!
//! The engine addon links `blpapi3` at load time. A library of the same name that is already in
//! the process satisfies that link, so loading the verified cache copy first lets the engine addon
//! load without any search path. Handles are never closed: the library must outlive the addon.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Libraries loaded so far, so a repeated call for the same path is a no-op.
static LOADED: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// Load `path` (absolute) into the process and keep it for the process lifetime.
///
/// The error text names the path and carries the operating system's own message.
pub(crate) fn preload(path: &Path) -> Result<(), String> {
    if !path.is_absolute() {
        return Err(format!(
            "cannot preload {}: the path must be absolute",
            path.display()
        ));
    }
    // A poisoned lock only means an earlier caller panicked; the list is still valid.
    let mut loaded = LOADED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if loaded.iter().any(|known| known == path) {
        return Ok(());
    }
    open(path).map_err(|reason| format!("cannot preload {}: {reason}", path.display()))?;
    loaded.push(path.to_path_buf());
    Ok(())
}

#[cfg(unix)]
fn open(path: &Path) -> Result<(), String> {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    let name = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| "the path contains a NUL byte".to_owned())?;
    // SAFETY: `name` is a valid NUL-terminated string. The handle is intentionally leaked so the
    // library stays loaded. Callers hold the `LOADED` lock, which serialises dlopen/dlerror here.
    let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_GLOBAL) };
    if !handle.is_null() {
        return Ok(());
    }
    // SAFETY: dlerror returns null or a valid NUL-terminated string owned by the C runtime.
    let message = unsafe {
        let text = libc::dlerror();
        if text.is_null() {
            None
        } else {
            Some(CStr::from_ptr(text).to_string_lossy().into_owned())
        }
    };
    Err(message.unwrap_or_else(|| "dlopen failed without an error message".to_owned()))
}

#[cfg(windows)]
fn open(path: &Path) -> Result<(), String> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::System::LibraryLoader::{
        LOAD_WITH_ALTERED_SEARCH_PATH, LoadLibraryExW,
    };

    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    if wide[..wide.len() - 1].contains(&0) {
        return Err("the path contains a NUL character".to_owned());
    }
    // SAFETY: `wide` is a valid NUL-terminated UTF-16 string. The handle is intentionally leaked
    // so the library stays loaded. LOAD_WITH_ALTERED_SEARCH_PATH resolves the library's own
    // dependencies from its directory, as the full path requires.
    let handle = unsafe {
        LoadLibraryExW(
            wide.as_ptr(),
            std::ptr::null_mut(),
            LOAD_WITH_ALTERED_SEARCH_PATH,
        )
    };
    if handle.is_null() {
        // `io::Error` renders the system message for the last OS error code.
        return Err(std::io::Error::last_os_error().to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::preload;

    #[test]
    fn relative_paths_are_refused() {
        let error = preload(std::path::Path::new("libblpapi3_64.so")).unwrap_err();
        assert!(error.contains("must be absolute"), "{error}");
        assert!(error.contains("libblpapi3_64.so"), "{error}");
    }

    #[test]
    fn a_missing_library_names_the_path_and_the_os_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent-runtime.lib");
        let error = preload(&missing).unwrap_err();
        assert!(
            error.starts_with(&format!("cannot preload {}: ", missing.display())),
            "{error}"
        );
        assert!(
            error.len() > "cannot preload : ".len() + missing.as_os_str().len(),
            "{error}"
        );
    }

    #[test]
    fn a_file_that_is_not_a_library_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("fake-runtime.lib");
        std::fs::write(&fake, b"synthetic bytes, not an executable image").unwrap();
        let error = preload(&fake).unwrap_err();
        assert!(error.contains(&fake.display().to_string()), "{error}");
        assert!(error.starts_with("cannot preload "), "{error}");
    }

    #[test]
    fn a_failed_load_is_not_remembered() {
        let dir = tempfile::tempdir().unwrap();
        let fake = dir.path().join("fake-runtime.lib");
        std::fs::write(&fake, b"not a library").unwrap();
        assert!(preload(&fake).is_err());
        assert!(preload(&fake).is_err());
    }
}
