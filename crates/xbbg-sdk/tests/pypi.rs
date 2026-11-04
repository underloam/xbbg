//! PyPI simple indexes served over HTTP (PEP 503 HTML like Bloomberg's, PEP 691 JSON).

mod common;

use std::collections::HashMap;
use std::fs;

use common::{Response, Server, Wheel, offline, options, sha256, simple_html, simple_json};
use xbbg_sdk::{Platform, Provisioner, Source, VersionSpec};

const HTML: &str = "text/html";
const JSON: &str = "application/vnd.pypi.simple.v1+json";

/// An index with the three platforms of 3.26.9.1, an older release, one below the minimum,
/// a yanked newer release, a pre-release and a source distribution.
fn bloomberg_like_index() -> (Server, HashMap<String, Vec<u8>>) {
    let wheels = [
        Wheel::windows("3.26.9.1"),
        Wheel::linux("3.26.9.1"),
        Wheel::macos("3.26.9.1"),
        Wheel::windows("3.26.8.1"),
        Wheel::new("3.24.6", "cp312-cp312-win_amd64", "blpapi/blpapi3_64.dll"),
        Wheel::windows("3.27.0.1"),
        Wheel::windows("3.28.0.1rc1"),
    ];
    let mut files: Vec<(String, Vec<u8>)> =
        wheels.iter().map(|w| (w.filename(), w.bytes())).collect();
    files.push(("blpapi-3.26.9.1.tar.gz".into(), b"sdist".to_vec()));
    let html = simple_html(&files, &["blpapi-3.27.0.1-py3-none-win_amd64.whl"], &[]);
    let mut routes: HashMap<String, Response> = files
        .iter()
        .map(|(name, bytes)| {
            (
                format!("/files/{name}"),
                Response::ok("application/octet-stream", bytes.clone()),
            )
        })
        .collect();
    routes.insert("/simple/blpapi/".into(), Response::ok(HTML, html));
    let bytes = files.into_iter().collect();
    (Server::start(routes), bytes)
}

#[tokio::test(flavor = "multi_thread")]
async fn html_index_installs_newest_stable_wheel_with_verified_library_and_licences() {
    let (server, files) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::pypi(server.url("/simple/"));

    let installed = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();

    let wheel = Wheel::windows("3.26.9.1");
    assert_eq!(
        installed.version, "3.26.9.1",
        "yanked 3.27 and the 3.28 rc are skipped"
    );
    assert!(!installed.cache_hit);
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert_eq!(installed.lib_sha256, sha256(&wheel.lib));
    assert_eq!(installed.artifact_sha256, sha256(&files[&wheel.filename()]));
    assert_eq!(installed.source, format!("pypi:{}", server.url("/simple/")));
    assert_eq!(
        installed.artifact_url,
        server.url(&format!("/files/{}", wheel.filename()))
    );
    let expected_dir = cache
        .path()
        .join("sdk")
        .join("3.26.9.1")
        .join("windows-x86_64")
        .join(format!("pypi-{}", &installed.artifact_sha256[..12]));
    assert_eq!(
        installed.lib_path,
        expected_dir.join("lib").join("blpapi3_64.dll")
    );
    let licenses: Vec<_> = installed
        .license_paths
        .iter()
        .map(|path| {
            path.strip_prefix(&expected_dir)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/")
        })
        .collect();
    assert_eq!(
        licenses,
        [
            "licenses/License.txt",
            "licenses/cpp-licenses/LICENSE.openssl"
        ]
    );

    let provenance: serde_json::Value =
        serde_json::from_slice(&fs::read(expected_dir.join("provenance.json")).unwrap()).unwrap();
    assert_eq!(provenance["version"], "3.26.9.1");
    assert_eq!(provenance["platform"], "windows-x86_64");
    assert_eq!(provenance["lib"], "lib/blpapi3_64.dll");
    assert_eq!(provenance["libSha256"], installed.lib_sha256.as_str());
    assert_eq!(
        provenance["artifactSha256"],
        installed.artifact_sha256.as_str()
    );
    assert_eq!(provenance["licenses"][0], "licenses/License.txt");

    let again = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert!(again.cache_hit);
    assert_eq!(again.lib_path, installed.lib_path);
    assert_eq!(
        server.hits(&format!("/files/{}", wheel.filename())),
        1,
        "the wheel is downloaded once"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn each_platform_gets_its_own_wheel_and_library_name() {
    let (server, _) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::pypi(server.url("/simple/"));
    for (platform, wheel, lib) in [
        (
            Platform::LinuxX86_64,
            Wheel::linux("3.26.9.1"),
            "libblpapi3_64.so",
        ),
        (
            Platform::MacosAarch64,
            Wheel::macos("3.26.9.1"),
            "libblpapi3_64.so",
        ),
    ] {
        let installed = provisioner
            .provision(&source, platform, &VersionSpec::Latest)
            .await
            .unwrap();
        assert_eq!(installed.platform, platform);
        assert_eq!(installed.lib_path.file_name().unwrap(), lib);
        assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
        assert!(
            installed
                .lib_path
                .to_string_lossy()
                .contains(platform.as_str())
        );
    }
    let all = provisioner.installed(None).unwrap();
    assert_eq!(all.len(), 2);
    assert_eq!(
        provisioner
            .installed(Some(Platform::LinuxX86_64))
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pep691_json_index_with_absolute_urls() {
    let wheel = Wheel::linux("3.26.5.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    // Absolute URLs need the port, which the server only knows after binding: serve a page
    // whose URLs point at a second server holding the files.
    let file_server = Server::start(
        files
            .iter()
            .map(|(name, bytes)| {
                (
                    format!("/files/{name}"),
                    Response::ok("application/octet-stream", bytes.clone()),
                )
            })
            .collect(),
    );
    let index = Server::start(HashMap::from([(
        "/simple/blpapi/".to_string(),
        Response::ok(JSON, simple_json(&file_server.base, &files)),
    )]));
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let installed = provisioner
        .provision(
            &Source::pypi(index.url("/simple")),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(installed.version, "3.26.5.1");
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert_eq!(
        installed.source,
        format!("pypi:{}", index.url("/simple/")),
        "index URL gains a slash"
    );
    assert_eq!(file_server.hits(&format!("/files/{}", wheel.filename())), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn exact_pin_is_resolved_and_then_served_from_the_cache_without_network() {
    let (server, _) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let pin = VersionSpec::exact("3.26.8.1");
    let first = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &pin)
        .await
        .unwrap();
    assert_eq!(first.version, "3.26.8.1");
    let before = server.total_hits();

    let second = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &pin)
        .await
        .unwrap();
    assert!(second.cache_hit);
    assert_eq!(second.lib_path, first.lib_path);
    assert_eq!(
        server.total_hits(),
        before,
        "a cached exact pin makes no request"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn versions_below_the_minimum_are_rejected_or_skipped() {
    let (server, _) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::pypi(server.url("/simple/"));

    let err = provisioner
        .provision(
            &source,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.24.6"),
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INVALID_ARGUMENT");
    assert!(err.to_string().contains("3.24.10"), "{err}");
    assert_eq!(server.total_hits(), 0, "rejected before any request");

    let only_old = Wheel::new("3.24.6", "cp312-cp312-win_amd64", "blpapi/blpapi3_64.dll");
    let files = vec![(only_old.filename(), only_old.bytes())];
    let old_index = Server::start(HashMap::from([(
        "/simple/blpapi/".to_string(),
        Response::ok(HTML, simple_html(&files, &[], &[])),
    )]));
    let err = provisioner
        .provision(
            &Source::pypi(old_index.url("/simple/")),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_FOUND");
    assert!(
        err.to_string().contains("oldest accepted release: 3.24.10"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn platform_without_a_wheel_is_not_found() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    let server = Server::start(HashMap::from([(
        "/simple/blpapi/".to_string(),
        Response::ok(HTML, simple_html(&files, &[], &[])),
    )]));
    let cache = tempfile::tempdir().unwrap();
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::pypi(server.url("/simple/")),
            Platform::MacosAarch64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_FOUND");
    assert!(err.to_string().contains("macos-aarch64"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_project_is_not_found() {
    let server = Server::start(HashMap::new());
    let cache = tempfile::tempdir().unwrap();
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::pypi(server.url("/simple/")),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NOT_FOUND", "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn archive_hash_mismatch_with_the_index_is_rejected_and_nothing_is_installed() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    let server = Server::start(HashMap::from([
        (
            "/simple/blpapi/".to_string(),
            Response::ok(HTML, simple_html(&files, &[], &[&wheel.filename()])),
        ),
        (
            format!("/files/{}", wheel.filename()),
            Response::ok("application/octet-stream", files[0].1.clone()),
        ),
    ]));
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let err = provisioner
        .provision(
            &Source::pypi(server.url("/simple/")),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY");
    assert!(err.to_string().contains("published sha256"), "{err}");
    assert!(provisioner.installed(None).unwrap().is_empty());
    assert_eq!(
        fs::read_dir(cache.path().join("tmp")).unwrap().count(),
        0,
        "no leftovers"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn record_mismatch_is_rejected_and_nothing_is_installed() {
    let wheel = Wheel {
        corrupt_record: true,
        ..Wheel::windows("3.26.9.1")
    };
    let files = vec![(wheel.filename(), wheel.bytes())];
    let server = Server::start(HashMap::from([
        (
            "/simple/blpapi/".to_string(),
            Response::ok(HTML, simple_html(&files, &[], &[])),
        ),
        (
            format!("/files/{}", wheel.filename()),
            Response::ok("application/octet-stream", files[0].1.clone()),
        ),
    ]));
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let err = provisioner
        .provision(
            &Source::pypi(server.url("/simple/")),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), "INTEGRITY");
    assert!(err.to_string().contains("RECORD"), "{err}");
    assert!(provisioner.installed(None).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_in_the_index_url_authenticate_every_request_and_never_leak() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    let server = Server::start_with_auth(
        HashMap::from([
            (
                "/simple/blpapi/".to_string(),
                Response::ok(HTML, simple_html(&files, &[], &[])),
            ),
            (
                format!("/files/{}", wheel.filename()),
                Response::ok("application/octet-stream", files[0].1.clone()),
            ),
        ]),
        Some("svc@corp:p:ss"),
    );
    let cache = tempfile::tempdir().unwrap();
    let url = server
        .url("/simple/")
        .replace("http://", "http://svc%40corp:p%3Ass@");
    let installed = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::pypi(url),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(installed.version, "3.26.9.1");
    assert_eq!(installed.source, format!("pypi:{}", server.url("/simple/")));
    assert!(!installed.artifact_url.contains("svc"));
    let provenance = fs::read_to_string(
        installed
            .lib_path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("provenance.json"),
    )
    .unwrap();
    assert!(
        !provenance.contains("p:ss") && !provenance.contains("svc"),
        "{provenance}"
    );
    assert!(server.authorization_headers().iter().all(Option::is_some));
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_credentials_are_sent_and_missing_ones_fail_with_a_network_error() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    let routes = HashMap::from([
        (
            "/simple/blpapi/".to_string(),
            Response::ok(HTML, simple_html(&files, &[], &[])),
        ),
        (
            format!("/files/{}", wheel.filename()),
            Response::ok("application/octet-stream", files[0].1.clone()),
        ),
    ]);
    let server = Server::start_with_auth(routes, Some("user:secret"));
    let source = Source::pypi(server.url("/simple/"));

    let cache = tempfile::tempdir().unwrap();
    let err = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "NETWORK", "{err}");
    assert!(err.to_string().contains("HTTP 401"), "{err}");
    assert!(
        err.to_string()
            .contains("check the credentials for 127.0.0.1"),
        "{err}"
    );

    let mut with_credentials = options(cache.path());
    with_credentials.network.credentials.push((
        "127.0.0.1".into(),
        xbbg_sdk::Credential::Basic {
            username: "user".into(),
            password: "secret".into(),
        },
    ));
    let installed = Provisioner::new(with_credentials)
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(installed.version, "3.26.9.1");
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_serves_the_newest_cached_install_and_fails_cleanly_without_one() {
    let (server, _) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));

    let err = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(err.code(), "OFFLINE");
    assert_eq!(server.total_hits(), 0);

    let online = Provisioner::new(options(cache.path())).unwrap();
    for version in ["3.26.8.1", "3.26.9.1"] {
        online
            .provision(
                &source,
                Platform::WindowsX86_64,
                &VersionSpec::exact(version),
            )
            .await
            .unwrap();
    }
    let before = server.total_hits();
    let offline_provisioner = Provisioner::new(offline(options(cache.path()))).unwrap();
    let newest = offline_provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(newest.version, "3.26.9.1");
    assert!(newest.cache_hit);
    let pinned = offline_provisioner
        .provision(
            &source,
            Platform::WindowsX86_64,
            &VersionSpec::exact("3.26.8.1"),
        )
        .await
        .unwrap();
    assert_eq!(pinned.version, "3.26.8.1");
    let missing = offline_provisioner
        .provision(&source, Platform::LinuxX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(missing.code(), "OFFLINE");
    assert_eq!(
        server.total_hits(),
        before,
        "offline never touches the network"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resolve_reports_the_choice_without_downloading() {
    let (server, files) = bloomberg_like_index();
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let resolved = provisioner
        .resolve(
            &Source::pypi(server.url("/simple/")),
            Platform::LinuxX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    let wheel = Wheel::linux("3.26.9.1");
    assert_eq!(resolved.version, "3.26.9.1");
    assert_eq!(resolved.filename, wheel.filename());
    assert_eq!(
        resolved.sha256.as_deref(),
        Some(sha256(&files[&wheel.filename()]).as_str())
    );
    assert_eq!(server.hits(&format!("/files/{}", wheel.filename())), 0);
    assert!(provisioner.installed(None).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn local_directory_index_works_offline() {
    let root = tempfile::tempdir().unwrap();
    let wheel = Wheel::windows("3.26.9.1");
    wheel.write_to(&root.path().join("files"));
    let page = simple_html(&[(wheel.filename(), wheel.bytes())], &[], &[]);
    let project = root.path().join("simple").join("blpapi");
    fs::create_dir_all(&project).unwrap();
    fs::write(project.join("index.html"), page).unwrap();

    let cache = tempfile::tempdir().unwrap();
    let installed = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .provision(
            &Source::pypi(root.path().join("simple").to_string_lossy().into_owned()),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(installed.version, "3.26.9.1");
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert!(
        installed.artifact_url.starts_with("file:///"),
        "{}",
        installed.artifact_url
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_remote_index_cannot_point_downloads_at_local_files() {
    let dir = tempfile::tempdir().unwrap();
    let wheel = Wheel::windows("3.26.9.1");
    let path = wheel.write_to(dir.path());
    let page = serde_json::json!({
        "meta": {"api-version": "1.1"},
        "name": "blpapi",
        "files": [{
            "filename": wheel.filename(),
            "url": url::Url::from_file_path(&path).unwrap().to_string(),
            "hashes": {"sha256": sha256(&wheel.bytes())},
            "requires-python": ">=3.8",
            "yanked": false,
        }],
    })
    .to_string();
    let server = Server::start(HashMap::from([(
        "/simple/blpapi/".to_string(),
        Response::ok(JSON, page),
    )]));
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(options(cache.path())).unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let error = provisioner
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "INVALID_ARTIFACT", "{error}");
    assert!(
        error.to_string().contains("only link http or https"),
        "{error}"
    );
    assert!(provisioner.installed(None).unwrap().is_empty());
    let error = provisioner
        .resolve(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "INVALID_ARTIFACT", "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn index_credentials_never_reach_another_plain_http_origin() {
    let wheel = Wheel::windows("3.26.9.1");
    let files = vec![(wheel.filename(), wheel.bytes())];
    // Same host, another port: host-keyed credential stores would match it.
    let file_server = Server::start(
        files
            .iter()
            .map(|(name, bytes)| {
                (
                    format!("/files/{name}"),
                    Response::ok("application/octet-stream", bytes.clone()),
                )
            })
            .collect(),
    );
    let index = Server::start_with_auth(
        HashMap::from([(
            "/simple/blpapi/".to_string(),
            Response::ok(JSON, simple_json(&file_server.base, &files)),
        )]),
        Some("user:secret"),
    );
    let cache = tempfile::tempdir().unwrap();
    let url = index
        .url("/simple/")
        .replace("http://", "http://user:secret@");
    let installed = Provisioner::new(options(cache.path()))
        .unwrap()
        .provision(
            &Source::pypi(url),
            Platform::WindowsX86_64,
            &VersionSpec::Latest,
        )
        .await
        .unwrap();
    assert_eq!(fs::read(&installed.lib_path).unwrap(), wheel.lib);
    assert!(index.authorization_headers().iter().all(Option::is_some));
    let sent = file_server.authorization_headers();
    assert!(
        !sent.is_empty() && sent.iter().all(Option::is_none),
        "credentials leaked to {}: {sent:?}",
        file_server.base
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn offline_latest_never_returns_an_install_below_the_minimum() {
    let old = Wheel::new("3.24.6", "cp312-cp312-win_amd64", "blpapi/blpapi3_64.dll");
    let files = vec![(old.filename(), old.bytes())];
    let server = Server::start(HashMap::from([
        (
            "/simple/blpapi/".to_string(),
            Response::ok(HTML, simple_html(&files, &[], &[])),
        ),
        (
            format!("/files/{}", old.filename()),
            Response::ok("application/octet-stream", files[0].1.clone()),
        ),
    ]));
    let cache = tempfile::tempdir().unwrap();
    let source = Source::pypi(server.url("/simple/"));
    let mut permissive = options(cache.path());
    permissive.min_pypi_version = None;
    let installed = Provisioner::new(permissive)
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap();
    assert_eq!(installed.version, "3.24.6");

    let error = Provisioner::new(offline(options(cache.path())))
        .unwrap()
        .provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest)
        .await
        .unwrap_err();
    assert_eq!(error.code(), "OFFLINE", "{error}");
}
