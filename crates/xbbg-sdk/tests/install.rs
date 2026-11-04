//! Cache behaviour: concurrent installs, incomplete and corrupted installs, listing, cleanup.

mod common;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime};

use common::{Response, Server, Wheel, offline, options, simple_html};
use xbbg_sdk::{Platform, Provisioner, Source, VersionSpec, installed};

fn index(wheels: &[Wheel]) -> Server {
    let files: Vec<(String, Vec<u8>)> = wheels.iter().map(|w| (w.filename(), w.bytes())).collect();
    serve_index(&files)
}

/// A PEP 503 index serving exactly these archive bytes.
fn serve_index(files: &[(String, Vec<u8>)]) -> Server {
    let mut routes: HashMap<String, Response> = files
        .iter()
        .map(|(name, bytes)| {
            (
                format!("/files/{name}"),
                Response::ok("application/octet-stream", bytes.clone()),
            )
        })
        .collect();
    routes.insert(
        "/simple/blpapi/".into(),
        Response::ok("text/html", simple_html(files, &[], &[])),
    );
    Server::start(routes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_provisioners_share_one_download_and_one_install() {
    let wheel = Wheel::windows("3.26.9.1");
    let server = index(std::slice::from_ref(&wheel));
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let (cache, source) = (cache.path().to_path_buf(), source.clone());
            tokio::spawn(async move {
                Provisioner::new(options(&cache))
                    .unwrap()
                    .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
                    .await
            })
        })
        .collect();
    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.unwrap().unwrap());
    }
    assert_eq!(
        results.iter().filter(|r| !r.cache_hit).count(),
        1,
        "exactly one installer"
    );
    assert!(results.iter().all(|r| r.lib_path == results[0].lib_path));
    assert_eq!(
        server.hits(&format!("/files/{}", wheel.filename())),
        1,
        "one download"
    );
    assert_eq!(installed(cache.path(), None).unwrap().len(), 1);
    let leftovers: Vec<_> = fs::read_dir(cache.path().join("tmp")).unwrap().collect();
    assert!(leftovers.is_empty(), "staging and downloads are cleaned up");
}

#[tokio::test(flavor = "multi_thread")]
async fn incomplete_and_corrupted_installs_are_ignored_and_replaced() {
    let wheel = Wheel::windows("3.26.9.1");
    let server = index(std::slice::from_ref(&wheel));
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let first = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    let install_dir = first
        .lib_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    // A crash between staging and commit leaves no provenance.json.
    fs::remove_file(install_dir.join("provenance.json")).unwrap();
    assert!(
        installed(cache.path(), None).unwrap().is_empty(),
        "incomplete installs are invisible"
    );
    let repaired = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(!repaired.cache_hit);
    assert_eq!(repaired.lib_path, first.lib_path);
    assert!(install_dir.join("provenance.json").is_file());

    // A modified library no longer matches its provenance.
    fs::write(&first.lib_path, b"tampered").unwrap();
    assert!(
        installed(cache.path(), None).unwrap().is_empty(),
        "corrupt installs are invisible"
    );
    let reinstalled = provisioner
        .provision(
            &source,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.26.9.1"),
        )
        .await
        .unwrap();
    assert!(!reinstalled.cache_hit);
    assert_eq!(fs::read(&reinstalled.lib_path).unwrap(), wheel.lib);
}

#[tokio::test(flavor = "multi_thread")]
async fn listing_is_newest_first_and_filters_by_platform() {
    let wheels = [
        Wheel::windows("3.26.8.1"),
        Wheel::windows("3.26.10.1"),
        Wheel::windows("3.26.9.1"),
        Wheel::linux("3.26.9.1"),
    ];
    let server = index(&wheels);
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    for wheel in &wheels {
        let platform = if wheel.tag.contains("win") {
            Platform::WindowsX86_64
        } else {
            Platform::LinuxX86_64
        };
        provisioner
            .provision(&source, platform, &VersionSpec::exact(&wheel.version))
            .await
            .unwrap();
    }
    let all = installed(cache.path(), None).unwrap();
    let listed: Vec<_> = all
        .iter()
        .map(|i| (i.version.as_str(), i.platform))
        .collect();
    assert_eq!(
        listed,
        [
            ("3.26.10.1", Platform::WindowsX86_64),
            ("3.26.9.1", Platform::WindowsX86_64),
            ("3.26.9.1", Platform::LinuxX86_64),
            ("3.26.8.1", Platform::WindowsX86_64),
        ]
    );
    assert!(all.iter().all(|i| i.cache_hit));
    let linux = installed(cache.path(), Some(Platform::LinuxX86_64)).unwrap();
    assert_eq!(linux.len(), 1);
    assert!(
        installed(&cache.path().join("missing"), None)
            .unwrap()
            .is_empty()
    );
}

/// Sets `path`'s modification time three days back (files and directories).
fn make_stale(path: &Path) {
    let mut file = fs::File::options();
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_BACKUP_SEMANTICS: needed to open a directory.
        file.write(true).custom_flags(0x0200_0000);
    }
    #[cfg(not(windows))]
    file.read(true);
    file.open(path)
        .unwrap()
        .set_modified(SystemTime::now() - Duration::from_secs(3 * 24 * 60 * 60))
        .unwrap();
}

#[test]
fn stale_temporaries_are_removed_and_everything_else_kept() {
    let cache = tempfile::tempdir().unwrap();
    let tmp = cache.path().join("tmp");
    let crashed_staging = tmp.join("staging-crashed");
    let user_dir = tmp.join("project-data");
    fs::create_dir_all(crashed_staging.join("lib")).unwrap();
    fs::create_dir_all(&user_dir).unwrap();
    fs::write(user_dir.join("results.csv"), b"user data").unwrap();
    let stale = tmp.join("dl-crashed.whl");
    let fresh = tmp.join("dl-running.whl");
    let user_file = tmp.join("notes.txt");
    for file in [&stale, &fresh, &user_file] {
        fs::write(file, b"partial").unwrap();
    }
    for old in [&stale, &user_file, &crashed_staging, &user_dir] {
        make_stale(old);
    }

    Provisioner::new(options(cache.path())).unwrap();
    assert!(!stale.exists());
    assert!(!crashed_staging.exists());
    assert!(fresh.exists());
    assert!(
        user_file.exists() && user_dir.join("results.csv").exists(),
        "a cache directory's tmp/ may hold the user's own files"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_same_archive_from_another_index_is_found_offline_from_either() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    let (primary, mirror) = (serve_index(&files), serve_index(&files));
    let cache = tempfile::tempdir().unwrap();
    let from_primary = Source::pypi(primary.url("/simple/"));
    let from_mirror = Source::pypi(mirror.url("/simple/"));
    let first = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&from_primary, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();

    let online = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&from_mirror, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(online.cache_hit, "one archive, one install");
    assert_eq!(online.lib_path, first.lib_path);
    assert_eq!(mirror.hits(&format!("/files/{}", wheel.filename())), 0);

    let before = mirror.total_hits();
    let offline_provisioner = Provisioner::new(offline(options(cache.path()))).unwrap();
    for source in [&from_mirror, &from_primary] {
        let cached = offline_provisioner
            .provision(source, Platform::WindowsX86_64, &VersionSpec::Latest)
            .await
            .unwrap();
        assert_eq!(cached.lib_path, first.lib_path);
    }
    let pinned = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &from_mirror,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.26.9.1"),
        )
        .await
        .unwrap();
    assert!(pinned.cache_hit);
    assert_eq!(
        mirror.total_hits(),
        before,
        "the mirror's exact pin is served from the cache"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_install_of_another_archive_with_the_same_hash_prefix_is_not_a_cache_hit() {
    let wheel = Wheel::windows("3.26.9.1");
    let server = index(std::slice::from_ref(&wheel));
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let first = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();

    // Another archive whose sha256 shares the 12 digits of the directory name.
    let provenance_path = first
        .lib_path
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("provenance.json");
    let other = format!("{}{}", &first.artifact_sha256[..12], "0".repeat(52));
    let provenance = fs::read_to_string(&provenance_path)
        .unwrap()
        .replace(&first.artifact_sha256, &other);
    fs::write(&provenance_path, provenance).unwrap();

    let error = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "INTEGRITY", "{error}");
    assert!(error.to_string().contains(&other), "{error}");
}

#[test]
fn blocking_facade_provisions_and_lists() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel::linux("3.26.9.1");
    let path = wheel.write_to(dir.path());
    let cache = tempfile::tempdir().unwrap();
    let provisioner = xbbg_sdk::blocking::Provisioner::new(options(cache.path())).unwrap();
    let source = Source::artifact(path.to_string_lossy().into_owned());
    let installed = provisioner
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .unwrap();
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert_eq!(
        provisioner.installed(None).unwrap(),
        vec![xbbg_sdk::Provisioned {
            cache_hit: true,
            ..installed
        }]
    );
}
