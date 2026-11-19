//! `locate_and_load` when no rule names a runtime: the operating system's own search is the last
//! step before the cache.
//!
//! Whether that search finds one depends on the machine (a runtime on `LD_LIBRARY_PATH`, in an
//! `ldconfig` directory, in `/usr/local/lib`, beside the executable), so the test accepts both
//! outcomes and checks what each must look like. It has this process to itself: a loaded runtime
//! is never unloaded.

use std::collections::BTreeMap;
use std::ffi::OsStr;

use xbbg_host::sdk::{LocateRequest, SourceKind, locate_and_load};
use xbbg_sdk::Platform;

#[test]
fn the_operating_systems_search_is_tried_after_the_listed_rules() {
    let platform = Platform::host().expect("a supported test platform");
    let empty = tempfile::tempdir().expect("temp dir");
    // An empty view and an empty cache: no listed rule can find anything.
    let request = LocateRequest {
        env: Some(BTreeMap::new()),
        cache_root: Some(empty.path().to_owned()),
        ..LocateRequest::default()
    };

    match locate_and_load(&request) {
        Ok(loaded) => {
            assert_eq!(loaded.source, SourceKind::System);
            assert!(loaded.path.is_absolute(), "{}", loaded.path.display());
            assert_eq!(xbbg_core::loaded_sdk_path(), Some(loaded.path.as_path()));
            let (major, ..) = xbbg_core::sdk_version();
            assert!(major >= 3, "{}", loaded.version);
        }
        Err(error) => {
            let refused = error
                .rejected
                .last()
                .expect("the system search is reported");
            assert_eq!(refused.source, SourceKind::System);
            // The file name that was searched for, or the file the system found and refused.
            assert_eq!(
                refused.path.file_name(),
                Some(OsStr::new(platform.library_name())),
                "{}",
                refused.path.display()
            );
            assert!(
                !refused.reason.is_empty() && !refused.reason.ends_with("failed"),
                "{}",
                refused.reason
            );
            assert!(xbbg_core::loaded_sdk_path().is_none());

            // Turned off, the search does not appear at all.
            let hermetic = LocateRequest {
                skip_system: true,
                ..request
            };
            let error = locate_and_load(&hermetic).expect_err("nothing to load");
            assert!(error.rejected.is_empty(), "{:?}", error.rejected);
        }
    }
}
