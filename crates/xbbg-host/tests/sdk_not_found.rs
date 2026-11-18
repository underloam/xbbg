//! `locate_and_load` when nothing loads: every refusal is listed, and nothing stays loaded.

use std::collections::BTreeMap;

use xbbg_host::sdk::{LocateRequest, SourceKind, locate_and_load, locate_and_load_json};
use xbbg_sdk::Platform;

#[test]
fn nothing_loadable_lists_every_rejection() {
    let platform = Platform::host().expect("a supported test platform");
    let dir = tempfile::tempdir().expect("temp dir");
    let bogus = dir.path().join(platform.library_names()[0]);
    std::fs::write(&bogus, b"this is not a shared library").expect("write bogus library");
    let missing = dir.path().join("missing");

    // An empty environment view: nothing from the process environment can load.
    let request = LocateRequest {
        explicit: Some(bogus.clone()),
        env: Some(BTreeMap::from([(
            "BLPAPI_ROOT".to_owned(),
            missing.display().to_string(),
        )])),
        ..LocateRequest::default()
    };
    let error = locate_and_load(&request).expect_err("nothing loads");

    assert_eq!(error.rejected.len(), 2, "{:?}", error.rejected);
    assert_eq!(error.rejected[0].source, SourceKind::BlpapiRoot);
    assert_eq!(error.rejected[0].reason, "does not exist");
    assert_eq!(error.rejected[1].source, SourceKind::Explicit);
    assert_eq!(error.rejected[1].path, bogus);

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
    assert_eq!(value["rejected"][1]["source"], "explicit");
}
