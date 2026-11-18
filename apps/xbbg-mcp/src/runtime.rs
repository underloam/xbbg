//! Finds and loads the Bloomberg runtime before the server starts.
//!
//! The search order lives in `xbbg_host::sdk` (explicit path, `BLPAPI_LIB_DIR`, `BLPAPI_ROOT`,
//! `XBBG_DEV_SDK_ROOT`, the vendored SDK, hint directories, the Bloomberg Terminal, the operating
//! system's library search path variables, then the xbbg cache). The binary passes only what the
//! launcher knows and the library cannot:
//!
//! | variable | meaning |
//! |---|---|
//! | `XBBG_MCP_LIB_DIR` | explicit runtime directory (or library file); searched first |
//! | `XBBG_MCP_HINT_DIRS` | directories to try after the vendored SDK, as an OS path list (`:` on Unix, `;` on Windows); the launcher passes the Python `blpapi` package directory |
//! | `XBBG_MCP_REPO_ROOT` | a development checkout; enables `<repo>/vendor/blpapi-sdk` and anchors a relative `XBBG_DEV_SDK_ROOT` |

use std::ffi::OsString;
use std::path::PathBuf;

use xbbg_host::sdk::{Hint, LoadedSdk, LocateRequest, SdkNotFound, locate_and_load};

pub(crate) const LIB_DIR_VAR: &str = "XBBG_MCP_LIB_DIR";
pub(crate) const HINT_DIRS_VAR: &str = "XBBG_MCP_HINT_DIRS";
pub(crate) const REPO_ROOT_VAR: &str = "XBBG_MCP_REPO_ROOT";

/// Builds the locate request from the launcher's variables; an empty variable counts as unset.
pub(crate) fn locate_request(get: impl Fn(&str) -> Option<OsString>) -> LocateRequest {
    let get = |name: &str| get(name).filter(|value| !value.is_empty());
    LocateRequest {
        explicit: get(LIB_DIR_VAR).map(PathBuf::from),
        hints: get(HINT_DIRS_VAR)
            .map(|list| {
                std::env::split_paths(&list)
                    .filter(|dir| !dir.as_os_str().is_empty())
                    .map(|dir| Hint {
                        label: HINT_DIRS_VAR.to_owned(),
                        dir,
                    })
                    .collect()
            })
            .unwrap_or_default(),
        repo_root: get(REPO_ROOT_VAR).map(PathBuf::from),
        ..LocateRequest::default()
    }
}

/// Finds and loads the runtime for this process.
pub(crate) fn load() -> Result<LoadedSdk, SdkNotFound> {
    locate_and_load(&locate_request(|name| std::env::var_os(name)))
}

/// The startup failure text: every refusal, then every way to supply a runtime.
pub(crate) fn startup_error(error: &SdkNotFound) -> String {
    format!(
        "xbbg-mcp: {error}\nxbbg-mcp: set {LIB_DIR_VAR}, BLPAPI_LIB_DIR or BLPAPI_ROOT, install \
         Bloomberg's blpapi package for the Python the launcher finds, or use a Bloomberg Terminal \
         (DAPI) install"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn request(vars: &[(&str, OsString)]) -> LocateRequest {
        let vars: HashMap<String, OsString> = vars
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect();
        locate_request(|name| vars.get(name).cloned())
    }

    #[test]
    fn nothing_set_searches_everything_with_the_process_environment() {
        assert_eq!(request(&[]), LocateRequest::default());
        assert_eq!(
            request(&[
                (LIB_DIR_VAR, OsString::new()),
                (HINT_DIRS_VAR, OsString::new()),
                (REPO_ROOT_VAR, OsString::new()),
            ]),
            LocateRequest::default()
        );
    }

    #[test]
    fn the_startup_error_lists_the_refusals_and_the_ways_to_supply_a_runtime() {
        let error = SdkNotFound {
            rejected: Vec::new(),
        };
        let text = startup_error(&error);
        assert!(
            text.starts_with("xbbg-mcp: no Bloomberg runtime library"),
            "{text}"
        );
        assert!(
            text.contains(LIB_DIR_VAR) && text.contains("BLPAPI_ROOT"),
            "{text}"
        );
    }

    #[test]
    fn the_launcher_variables_become_the_request() {
        let hint_list = std::env::join_paths(["wheel", "other"]).unwrap();
        let request = request(&[
            (LIB_DIR_VAR, "explicit".into()),
            (HINT_DIRS_VAR, hint_list),
            (REPO_ROOT_VAR, "checkout".into()),
        ]);
        assert_eq!(request.explicit, Some(PathBuf::from("explicit")));
        assert_eq!(request.repo_root, Some(PathBuf::from("checkout")));
        let hints: Vec<_> = request
            .hints
            .iter()
            .map(|hint| (hint.label.as_str(), hint.dir.clone()))
            .collect();
        assert_eq!(
            hints,
            [
                (HINT_DIRS_VAR, PathBuf::from("wheel")),
                (HINT_DIRS_VAR, PathBuf::from("other"))
            ]
        );
        assert!(request.env.is_none() && request.cache_root.is_none());
    }
}
