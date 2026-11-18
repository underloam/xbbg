//! `locate_and_load` against a real Bloomberg runtime named by `BLPAPI_LIB_DIR` / `BLPAPI_ROOT`.
//!
//! The runtime is process-wide and never unloaded, so one test walks the whole story in order.

use std::collections::BTreeMap;

use xbbg_host::sdk::{LocateRequest, SourceKind, locate, locate_and_load, locate_and_load_json};
use xbbg_sdk::Platform;

#[test]
fn a_bogus_first_candidate_is_rejected_and_the_real_runtime_loads_once() {
    let real = locate(&LocateRequest::default())
        .into_first_path()
        .expect("this test needs a Bloomberg runtime: set BLPAPI_LIB_DIR or BLPAPI_ROOT");

    // A file named like the library that is not one, ahead of everything else.
    let platform = Platform::host().expect("a supported test platform");
    let bogus_dir = tempfile::tempdir().expect("temp dir");
    let bogus = bogus_dir.path().join(platform.library_names()[0]);
    std::fs::write(&bogus, b"this is not a shared library").expect("write bogus library");

    let request = LocateRequest {
        explicit: Some(bogus.clone()),
        env: Some(std::env::vars().collect::<BTreeMap<_, _>>()),
        ..LocateRequest::default()
    };
    let loaded = locate_and_load(&request).expect("the real runtime loads after the bogus one");

    assert_eq!(loaded.path, real);
    assert_ne!(loaded.source, SourceKind::Explicit);
    assert!(
        matches!(
            loaded.source,
            SourceKind::BlpapiLibDir | SourceKind::BlpapiRoot
        ),
        "{:?}",
        loaded.source
    );
    let (major, minor, patch, build) = xbbg_core::sdk_version();
    assert_eq!(loaded.version, format!("{major}.{minor}.{patch}.{build}"));
    assert!(major >= 3, "{}", loaded.version);
    assert_eq!(xbbg_core::loaded_sdk_path(), Some(real.as_path()));

    let refused = loaded
        .rejected
        .iter()
        .find(|refused| refused.path == bogus)
        .expect("the bogus library is reported");
    assert_eq!(refused.source, SourceKind::Explicit);
    assert!(
        !refused.reason.is_empty() && !refused.reason.starts_with("cannot open"),
        "the loader's own message, not a repeat of the path: {}",
        refused.reason
    );

    // A second call is idempotent, whatever it asks for.
    assert_eq!(locate_and_load(&request).expect("second call"), loaded);
    assert_eq!(
        locate_and_load(&LocateRequest::default()).expect("third call"),
        loaded
    );

    // The JSON surface reports the same runtime.
    let json = locate_and_load_json("{}").expect("json call");
    let value: serde_json::Value = serde_json::from_str(&json).expect("json result");
    assert_eq!(value["version"], loaded.version.as_str());
    assert_eq!(value["source"], loaded.source.as_str());
    assert_eq!(value["path"], real.to_string_lossy().as_ref());
    assert!(value["rejected"].is_array());

    // A malformed request is an Err with the same shape, whether or not a runtime is loaded.
    let failure = locate_and_load_json(r#"{"cacheDir": "x"}"#).expect_err("unknown field");
    let value: serde_json::Value = serde_json::from_str(&failure).expect("json failure");
    assert!(
        value["message"]
            .as_str()
            .is_some_and(|text| text.contains("invalid locate request"))
    );
    assert_eq!(value["rejected"], serde_json::json!([]));
}
