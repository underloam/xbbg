//! conda channels: a `file://` channel and the same channel over HTTP (plain repodata.json).

mod common;

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use common::{CondaPackage, Response, Server, conda_channel, offline, options, sha256};
use xbbg_sdk::{Platform, Provisioner, Source, VersionSpec};

fn channel_url(root: &Path) -> String {
    url::Url::from_directory_path(root).unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn file_channel_picks_newest_version_then_build_and_verifies_paths_json() {
    let root = tempfile::tempdir().unwrap();
    let old = CondaPackage::windows("3.26.8.1", 0, ".tar.bz2");
    let build0 = CondaPackage::windows("3.26.9.1", 0, ".conda");
    let build1 = CondaPackage::windows("3.26.9.1", 1, ".conda");
    let linux = CondaPackage::linux("3.26.9.1", 0, ".tar.bz2");
    conda_channel(
        root.path(),
        &[
            (old.clone(), None),
            (build0, None),
            (build1.clone(), None),
            (linux.clone(), None),
        ],
    );
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::conda(channel_url(root.path()));

    let latest = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(latest.version, "3.26.9.1");
    assert_eq!(
        fs::read(&latest.lib_path).unwrap(),
        build1.lib,
        "build number 1 beats 0"
    );
    assert_eq!(latest.lib_path.file_name().unwrap(), "blpapi3_64.dll");
    assert_eq!(latest.lib_sha256, sha256(&build1.lib));
    let package_bytes = fs::read(root.path().join("win-64").join(build1.filename())).unwrap();
    assert_eq!(latest.artifact_sha256, sha256(&package_bytes));
    assert_eq!(
        latest.source,
        format!("conda:{}", channel_url(root.path()).trim_end_matches('/'))
    );
    assert_eq!(latest.license_paths.len(), 1);
    assert!(latest.license_paths[0].ends_with(Path::new("licenses").join("License.txt")));
    assert!(
        latest
            .lib_path
            .to_string_lossy()
            .contains(&format!("conda-{}", &latest.artifact_sha256[..12]))
    );

    let pinned = provisioner
        .provision(
            &source,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.26.8.1"),
        )
        .await
        .unwrap();
    assert_eq!(pinned.version, "3.26.8.1");
    assert_eq!(
        fs::read(&pinned.lib_path).unwrap(),
        old.lib,
        "tar.bz2 packages work too"
    );

    let on_linux = provisioner
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(fs::read(&on_linux.lib_path).unwrap(), linux.lib);
    assert_eq!(on_linux.lib_path.file_name().unwrap(), "libblpapi3_64.so");

    let missing = provisioner
        .provision(
            &source,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.25.1.1"),
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code(), "NOT_FOUND");
    let none_for_mac = provisioner
        .provision(&source, Platform::MacosAarch64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert!(
        matches!(none_for_mac.code(), "NOT_FOUND" | "NETWORK"),
        "{none_for_mac}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn repodata_hash_mismatch_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let package = CondaPackage::windows("3.26.9.1", 0, ".conda");
    let wrong = "ab".repeat(32);
    conda_channel(root.path(), &[(package, Some(wrong.as_str()))]);
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let err = provisioner
        .provision(
            &Source::conda(channel_url(root.path())),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY");
    assert!(err.to_string().contains(&wrong), "{err}");
    assert!(provisioner.installed(None).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn library_not_matching_paths_json_is_rejected() {
    let root = tempfile::tempdir().unwrap();
    let package = CondaPackage {
        corrupt_paths_json: true,
        ..CondaPackage::windows("3.26.9.1", 0, ".tar.bz2")
    };
    conda_channel(root.path(), &[(package, None)]);
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let err = provisioner
        .provision(
            &Source::conda(channel_url(root.path())),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY");
    assert!(err.to_string().contains("paths.json"), "{err}");
    assert!(provisioner.installed(None).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn http_channel_falls_back_to_repodata_json_and_offline_uses_the_cache() {
    let root = tempfile::tempdir().unwrap();
    let package = CondaPackage::windows("3.26.9.1", 2, ".conda");
    conda_channel(root.path(), &[(package.clone(), None)]);
    let package_path = root.path().join("win-64").join(package.filename());
    let routes = HashMap::from([
        (
            "/channel/win-64/repodata.json".to_string(),
            Response::ok(
                "application/json",
                fs::read(root.path().join("win-64").join("repodata.json")).unwrap(),
            ),
        ),
        (
            format!("/channel/win-64/{}", package.filename()),
            Response::ok("application/octet-stream", fs::read(&package_path).unwrap()),
        ),
    ]);
    let server = Server::start(routes);
    let cache = tempfile::tempdir().unwrap();
    let source = Source::conda(server.url("/channel"));
    let installed = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(fs::read(&installed.lib_path).unwrap(), package.lib);
    assert_eq!(
        installed.source,
        format!("conda:{}", server.url("/channel"))
    );
    assert_eq!(
        server.hits(&format!("/channel/win-64/{}", package.filename())),
        1
    );

    let before = server.total_hits();
    let cached = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(cached.cache_hit);
    assert_eq!(cached.lib_path, installed.lib_path);
    assert_eq!(server.total_hits(), before);

    let resolved = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .resolve(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(
        resolved.version, "3.26.9.1",
        "offline resolution reads the cached repodata"
    );
    assert_eq!(server.total_hits(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_package_names_are_rejected() {
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::CondaChannel {
        channel: "conda-forge".into(),
        package: "  ".into(),
    };
    let err = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENT");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remote_channel_cannot_point_downloads_at_local_files() {
    let root = tempfile::tempdir().unwrap();
    let package = CondaPackage::windows("3.26.9.1", 0, ".conda");
    conda_channel(root.path(), &[(package, None)]);
    let subdir = root.path().join("win-64");
    let mut repodata: serde_json::Value =
        serde_json::from_slice(&fs::read(subdir.join("repodata.json")).unwrap()).unwrap();
    repodata["info"]["base_url"] =
        serde_json::json!(url::Url::from_directory_path(&subdir).unwrap().to_string());
    let server = Server::start(HashMap::from([(
        "/channel/win-64/repodata.json".to_string(),
        Response::ok("application/json", repodata.to_string()),
    )]));
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let error = provisioner
        .provision(
            &Source::conda(server.url("/channel")),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code(), "INVALID_ARTIFACT", "{error}");
    assert!(provisioner.installed(None).unwrap().is_empty());
}
