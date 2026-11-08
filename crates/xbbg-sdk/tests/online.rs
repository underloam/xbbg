//! Opt-in checks against Bloomberg's public index and conda-forge:
//! `cargo test -p xbbg-sdk --test online -- --ignored --nocapture`.
//! Uses the system proxy settings and a temporary cache.

use std::time::Instant;

use xbbg_sdk::{Platform, ProvisionOptions, Provisioner, Source, VersionSpec, installed};

fn check(installed: &xbbg_sdk::Provisioned) {
    let bytes = std::fs::read(&installed.lib_path).unwrap();
    let sha = hex::encode(rattler_digest::compute_bytes_digest::<rattler_digest::Sha256>(&bytes));
    assert_eq!(sha, installed.lib_sha256);
    assert!(!installed.license_paths.is_empty());
    assert!(installed.license_paths.iter().all(|path| path.is_file()));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network: downloads from Bloomberg's index and conda-forge"]
async fn bloomberg_index_and_conda_forge_for_every_platform() {
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(ProvisionOptions::new(cache.path())).unwrap();
    for source in [Source::bloomberg_pypi(), Source::conda("conda-forge")] {
        for platform in Platform::ALL {
            let started = Instant::now();
            let first = provisioner
                .provision(&source, platform, &VersionSpec::Latest)
                .await
                .unwrap();
            println!(
                "{} {platform}: {} in {:.2?} (cache_hit={})\n  lib {} sha256 {}\n  artifact {} sha256 {}\n  licenses {}",
                first.source,
                first.version,
                started.elapsed(),
                first.cache_hit,
                first.lib_path.display(),
                first.lib_sha256,
                first.artifact_url,
                first.artifact_sha256,
                first.license_paths.len()
            );
            assert!(!first.cache_hit);
            assert_eq!(first.platform, platform);
            check(&first);
            let started = Instant::now();
            let again = provisioner
                .provision(&source, platform, &VersionSpec::Latest)
                .await
                .unwrap();
            println!(
                "  warm rerun: cache_hit={} in {:.2?}",
                again.cache_hit,
                started.elapsed()
            );
            assert!(again.cache_hit);
            assert_eq!(again.lib_path, first.lib_path);
        }
    }

    let mut offline = ProvisionOptions::new(cache.path());
    offline.network.offline = true;
    let offline = Provisioner::new(offline).unwrap();
    for platform in Platform::ALL {
        let hit = offline
            .provision(&Source::bloomberg_pypi(), platform, &VersionSpec::Latest)
            .await
            .unwrap();
        assert!(hit.cache_hit);
    }
    let listed = installed(cache.path(), None).unwrap();
    println!("installed: {} runtime(s)", listed.len());
    for install in &listed {
        println!(
            "  {} {} {}",
            install.version, install.platform, install.source
        );
    }
    assert_eq!(listed.len(), 6);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network: downloads one wheel from Bloomberg's index"]
async fn bloomberg_wheel_as_a_hashed_artifact_and_an_exact_pin() {
    let cache = tempfile::tempdir().unwrap();
    let provisioner = Provisioner::new(ProvisionOptions::new(cache.path())).unwrap();
    let platform = Platform::WindowsX86_64;
    let resolved = provisioner
        .resolve(
            &Source::bloomberg_pypi(),
            platform,
            &VersionSpec::exact("3.24.11"),
        )
        .await
        .unwrap();
    println!(
        "resolved {} -> {} sha256 {:?}",
        resolved.version, resolved.artifact_url, resolved.sha256
    );
    let source = Source::artifact(format!(
        "{}#sha256={}",
        resolved.artifact_url,
        resolved.sha256.unwrap()
    ));
    let installed = provisioner
        .provision(&source, platform, &VersionSpec::Latest)
        .await
        .unwrap();
    println!(
        "artifact {} -> {} lib sha256 {}",
        installed.source,
        installed.lib_path.display(),
        installed.lib_sha256
    );
    assert_eq!(installed.version, "3.24.11");
    check(&installed);
}
