//! `locate_and_load` when nothing loads: every refusal is listed, nothing stays loaded, and an
//! explicit path is never replaced by another rule's runtime.

use std::collections::BTreeMap;
use std::path::Path;

use xbbg_host::sdk::{LocateRequest, SourceKind, locate_and_load, locate_and_load_json};
use xbbg_sdk::Platform;

fn bogus_library(dir: &Path) -> std::path::PathBuf {
    let platform = Platform::host().expect("a supported test platform");
    let bogus = dir.join(platform.library_names()[0]);
    std::fs::write(&bogus, b"this is not a shared library").expect("write bogus library");
    bogus
}

#[test]
fn nothing_loadable_lists_every_rejection() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bogus_dir = dir.path().join("bogus");
    std::fs::create_dir(&bogus_dir).expect("create dir");
    let bogus = bogus_library(&bogus_dir);
    let missing = dir.path().join("missing");

    // A view with only two variables: nothing from the process environment can load. The
    // operating system's own search follows the real environment, so it is left out.
    let request = LocateRequest {
        env: Some(BTreeMap::from([
            ("BLPAPI_ROOT".to_owned(), missing.display().to_string()),
            ("BLPAPI_LIB_DIR".to_owned(), bogus_dir.display().to_string()),
        ])),
        skip_system: true,
        ..LocateRequest::default()
    };
    let error = locate_and_load(&request).expect_err("nothing loads");

    assert_eq!(error.rejected.len(), 2, "{:?}", error.rejected);
    assert_eq!(error.rejected[0].source, SourceKind::BlpapiRoot);
    assert_eq!(error.rejected[0].reason, "does not exist");
    assert_eq!(error.rejected[1].source, SourceKind::BlpapiLibDir);
    assert_eq!(error.rejected[1].path, bogus);
    // The operating system's own words about the file, not only the call that failed.
    assert!(
        !error.rejected[1].reason.ends_with("failed"),
        "{}",
        error.rejected[1].reason
    );

    let text = error.to_string();
    assert!(text.contains(&bogus.display().to_string()), "{text}");
    assert!(text.contains(&missing.display().to_string()), "{text}");
    assert!(text.contains(&error.rejected[1].reason), "{text}");
    assert!(xbbg_core::loaded_sdk_path().is_none());

    // The JSON failure carries the same list.
    let json = serde_json::to_string(&request).expect("request json");
    let failure = locate_and_load_json(&json).expect_err("nothing loads");
    let value: serde_json::Value = serde_json::from_str(&failure).expect("json failure");
    assert_eq!(value["message"], text.as_str());
    assert_eq!(value["rejected"].as_array().map(Vec::len), Some(2));
    assert_eq!(value["rejected"][1]["source"], "blpapi_lib_dir");
}

#[test]
fn an_explicit_path_that_does_not_load_is_not_replaced_by_another_rule() {
    let dir = tempfile::tempdir().expect("temp dir");
    let bogus = bogus_library(dir.path());

    // The process environment (BLPAPI_LIB_DIR / BLPAPI_ROOT) names a runtime that would load.
    let request = LocateRequest {
        explicit: Some(bogus.clone()),
        env: Some(std::env::vars().collect()),
        ..LocateRequest::default()
    };
    let error = locate_and_load(&request).expect_err("the explicit path does not load");

    assert_eq!(error.rejected.len(), 1, "{:?}", error.rejected);
    assert_eq!(error.rejected[0].source, SourceKind::Explicit);
    assert_eq!(error.rejected[0].path, bogus);
    assert!(!error.rejected[0].reason.is_empty());
    assert!(
        xbbg_core::loaded_sdk_path().is_none(),
        "another runtime was loaded in its place"
    );
}

#[test]
fn an_explicit_directory_without_a_library_fails_with_that_reason() {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(dir.path().join("readme.txt"), b"nothing here").expect("write file");
    let request = |explicit: &Path| LocateRequest {
        explicit: Some(explicit.to_path_buf()),
        env: Some(std::env::vars().collect()),
        ..LocateRequest::default()
    };

    let error = locate_and_load(&request(dir.path())).expect_err("no library there");
    assert_eq!(error.rejected.len(), 1, "{:?}", error.rejected);
    assert_eq!(error.rejected[0].source, SourceKind::Explicit);
    assert_eq!(error.rejected[0].path, dir.path());
    assert!(
        error.rejected[0]
            .reason
            .starts_with("holds no Bloomberg runtime library"),
        "{}",
        error.rejected[0].reason
    );
    assert!(
        error
            .to_string()
            .contains(&dir.path().display().to_string())
    );

    let error = locate_and_load(&request(&dir.path().join("missing"))).expect_err("no such path");
    assert_eq!(error.rejected.len(), 1, "{:?}", error.rejected);
    assert_eq!(error.rejected[0].reason, "does not exist");
    assert!(xbbg_core::loaded_sdk_path().is_none());
}
