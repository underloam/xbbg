//! Single archives: local paths, `file:` URLs and HTTP URLs with and without hashes.

mod common;

use std::collections::HashMap;
use std::fs;

use common::{CondaPackage, Response, Server, Wheel, offline, options, sha256};
use xbbg_sdk::{Platform, Provisioner, Source, VersionSpec};

fn wheel_server(wheel: &Wheel) -> Server {
    Server::start(HashMap::from([(
        format!("/files/{}", wheel.filename()),
        Response::ok("application/octet-stream", wheel.bytes()),
    )]))
}

#[tokio::test(flavor = "multi_thread")]
async fn local_wheel_without_hash_installs_and_is_found_again_by_content() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel::windows("3.26.9.1");
    let path = wheel.write_to(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(offline(options(cache.path()))).unwrap();
    let source = Source::artifact(path.to_string_lossy().into_owned());

    let installed = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(installed.version, "3.26.9.1");
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert_eq!(installed.artifact_sha256, sha256(&wheel.bytes()));
    assert_eq!(installed.source, format!("artifact:{}", path.display()));
    assert!(installed.artifact_url.starts_with("file:///"));
    assert!(
        installed
            .lib_path
            .to_string_lossy()
            .contains(&format!("artifact-{}", &installed.artifact_sha256[..12]))
    );

    let file_url = url::Url::from_file_path(&path).unwrap().to_string();
    let again = provisioner
        .provision(
            &Source::artifact(file_url),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert!(
        again.cache_hit,
        "the same archive through a file: URL is the same install"
    );
    assert_eq!(again.lib_path, installed.lib_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_artifacts_need_a_hash_unless_allowed() {
    let wheel = Wheel::linux("3.26.9.1");
    let server = wheel_server(&wheel);
    let url = server.url(&format!("/files/{}", wheel.filename()));
    let cache = tempfile::tempdir().unwrap();

    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::artifact(url.clone()),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENT");
    assert!(err.to_string().contains("#sha256="), "{err}");
    assert_eq!(server.total_hits(), 0);

    let mut relaxed = options(cache.path());
    relaxed.require_artifact_hash = false;
    let installed = Provisioner::new(relaxed)
        .unwrap()
        .provision(
            &Source::artifact(url),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_artifact_hash_fragment_is_verified_and_enables_offline_hits() {
    let wheel = Wheel::macos("3.26.9.1");
    let server = wheel_server(&wheel);
    let hash = sha256(&wheel.bytes());
    let url = server.url(&format!("/files/{}", wheel.filename()));
    let cache = tempfile::tempdir().unwrap();

    let wrong = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::artifact(format!("{url}#sha256={}", "0".repeat(64))),
            Platform::MacosAarch64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(wrong.code(), "INTEGRITY");

    let source = Source::artifact(format!("{url}#sha256={}", hash.to_uppercase()));
    let installed = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&source, Platform::MacosAarch64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(installed.artifact_sha256, hash);
    assert_eq!(installed.artifact_url, url, "no fragment in provenance");
    let downloads = server.total_hits();

    let cached = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .provision(&source, Platform::MacosAarch64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(cached.cache_hit);
    assert_eq!(
        server.total_hits(),
        downloads,
        "known hash + install = no request"
    );

    let conflicting = Source::Artifact {
        location: format!("{url}#sha256={hash}"),
        sha256: Some("1".repeat(64)),
    };
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&conflicting, Platform::MacosAarch64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENT");
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_platform_old_and_unknown_archives_are_rejected_before_any_io() {
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let server = Server::start(HashMap::new());
    let hash = "a".repeat(64);
    for (name, platform, needle) in [
        (
            "blpapi-3.26.9.1-py3-none-manylinux2014_x86_64.whl",
            Platform::WindowsX86_64,
            "not a windows-x86_64 wheel",
        ),
        (
            "blpapi-3.24.6-cp312-cp312-win_amd64.whl",
            Platform::WindowsX86_64,
            "3.24.10",
        ),
        (
            "blpapi-3.26.9.1.tar.gz",
            Platform::WindowsX86_64,
            "expected a .whl, .conda or .tar.bz2",
        ),
    ] {
        let source = Source::artifact(format!(
            "{}#sha256={hash}",
            server.url(&format!("/files/{name}"))
        ));
        let err = provisioner
            .provision(&source, platform, &VersionSpec::Latest)
            .await
            .unwrap_err();
        assert_eq!(err.code(), "INVALID_ARGUMENT", "{name}: {err}");
        assert!(err.to_string().contains(needle), "{name}: {err}");
    }
    assert_eq!(server.total_hits(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn local_conda_packages_install_and_wrong_subdirs_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let package = CondaPackage::windows("3.26.9.1", 0, ".conda");
    let path = package.write_to(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::artifact(path.to_string_lossy().into_owned());

    let installed = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(
        installed.version, "3.26.9.1",
        "version from info/index.json"
    );
    assert_eq!(fs::read(&installed.lib_path).unwrap(), package.lib);

    let resolved = provisioner
        .resolve(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(resolved.version, "3.26.9.1", "version from the file name");
    assert_eq!(resolved.filename, package.filename());

    let err = provisioner
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENT");
    assert!(err.to_string().contains("win-64"), "{err}");

    let tar = CondaPackage::linux("3.26.8.1", 0, ".tar.bz2");
    let tar_path = tar.write_to(dir.path());
    let linux = provisioner
        .provision(
            &Source::artifact(tar_path.to_string_lossy().into_owned()),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(fs::read(&linux.lib_path).unwrap(), tar.lib);
}

#[tokio::test(flavor = "multi_thread")]
async fn tampered_wheel_record_is_rejected_for_artifacts_too() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel {
        corrupt_record: true,
        ..Wheel::windows("3.26.9.1")
    };
    let path = wheel.write_to(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let err = provisioner
        .provision(
            &Source::artifact(path.to_string_lossy().into_owned()),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY");
    assert!(provisioner.installed(None).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_local_file_is_an_io_error() {
    let dir = tempfile::tempdir().unwrap();
    let cache = tempfile::tempdir().unwrap();
    let missing = dir.path().join("blpapi-3.26.9.1-py3-none-win_amd64.whl");
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::artifact(missing.to_string_lossy().into_owned()),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "IO", "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_finds_a_remote_artifact_without_hash_by_its_location() {
    let wheel = Wheel::linux("3.26.9.1");
    let server = wheel_server(&wheel);
    let url = server.url(&format!("/files/{}", wheel.filename()));
    let cache = tempfile::tempdir().unwrap();
    let mut relaxed = options(cache.path());
    relaxed.require_artifact_hash = false;
    let source = Source::artifact(url);
    let installed = Provisioner::new(relaxed.clone())
        .unwrap()
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap();

    let cached = Provisioner::new(offline(relaxed))
        .unwrap()
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(cached.cache_hit);
    assert_eq!(cached.lib_path, installed.lib_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_archive_installed_from_one_location_is_found_offline_from_another() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel::linux("3.26.9.1");
    let path = wheel.write_to(dir.path());
    let server = wheel_server(&wheel);
    let cache = tempfile::tempdir().unwrap();
    let mut relaxed = options(cache.path());
    relaxed.require_artifact_hash = false;
    let local = Provisioner::new(relaxed.clone())
        .unwrap()
        .provision(
            &Source::artifact(path.to_string_lossy().into_owned()),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();

    // Online the remote copy downloads, matches the installed archive and is recorded.
    let remote = Source::artifact(server.url(&format!("/files/{}", wheel.filename())));
    let online = Provisioner::new(relaxed.clone())
        .unwrap()
        .provision(&remote, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(online.cache_hit);
    assert_eq!(online.lib_path, local.lib_path);

    let cached = Provisioner::new(offline(relaxed))
        .unwrap()
        .provision(&remote, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(cached.lib_path, local.lib_path);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_uppercase_hash_fragment_is_verified() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel::windows("3.26.9.1");
    let path = wheel.write_to(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let file_url = url::Url::from_file_path(&path).unwrap();
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::artifact(format!("{file_url}#SHA256={}", "0".repeat(64))),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY", "{err}");

    let installed = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::artifact(format!("{file_url}#SHA256={}", sha256(&wheel.bytes()))),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(installed.artifact_sha256, sha256(&wheel.bytes()));
}
