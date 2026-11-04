//! conda channels through rattler's gateway (sharded repodata where the channel offers it) and
//! `info/paths.json`-verified staging of extracted packages.

use std::collections::HashMap;
use std::fs;
use std::path::{Component, Path, PathBuf};

use rattler_conda_types::package::{IndexJson, PackageFile, PathsJson};
use rattler_conda_types::{
    Channel, ChannelConfig as CondaChannelConfig, MatchSpec, ParseMatchSpecOptions, RepoDataRecord,
};
use rattler_repodata_gateway::fetch::CacheAction;
use rattler_repodata_gateway::{
    ChannelConfig as GatewayChannelConfig, ChannelRelationsMode, Gateway, SourceConfig,
};

use crate::error::{Error, Result, chain};
use crate::http::Http;
use crate::install::rename;
use crate::platform::Platform;
use crate::wheel::{Extracted, safe_relative};

/// The gateway and channel-name resolution of one provisioner.
pub(crate) struct Conda {
    gateway: Gateway,
    channels: CondaChannelConfig,
}

impl Conda {
    pub(crate) fn new(http: &Http, cache_dir: PathBuf, root_dir: PathBuf) -> Self {
        let source = SourceConfig {
            sharded_enabled: true,
            cache_action: if http.offline {
                CacheAction::ForceCacheOnly
            } else {
                CacheAction::CacheOrFetch
            },
            ..SourceConfig::default()
        };
        let gateway = Gateway::builder()
            .with_client(http.client.clone())
            .with_cache_dir(cache_dir)
            .with_channel_config(GatewayChannelConfig {
                default: source,
                per_channel: HashMap::new(),
            })
            .with_max_concurrent_requests(8)
            .finish();
        Self {
            gateway,
            channels: CondaChannelConfig::default_with_root_dir(root_dir),
        }
    }

    /// A channel name (resolved against `https://conda.anaconda.org/`), URL or local directory.
    pub(crate) fn channel(&self, channel: &str) -> Result<Channel> {
        Channel::from_str(channel, &self.channels).map_err(|err| {
            Error::InvalidArgument(format!("invalid conda channel `{channel}`: {err}"))
        })
    }

    /// The newest record of `package` (or exactly `version`) for `platform`: highest version,
    /// then build number, then timestamp.
    pub(crate) async fn resolve(
        &self,
        channel: &Channel,
        package: &str,
        platform: Platform,
        version: Option<&str>,
    ) -> Result<RepoDataRecord> {
        let spec = match version {
            None => package.to_string(),
            Some(version) => format!("{package} =={version}"),
        };
        let spec = MatchSpec::from_str(&spec, ParseMatchSpecOptions::lenient()).map_err(|err| {
            Error::InvalidArgument(format!("invalid conda package request `{spec}`: {err}"))
        })?;
        let output = self
            .gateway
            .query([channel.clone()], [platform.conda_subdir()], [spec])
            .recursive(false)
            .channel_relations(ChannelRelationsMode::Disabled)
            .execute()
            .await
            .map_err(|err| {
                Error::Network(format!(
                    "reading {} repodata of {} failed: {}",
                    platform.conda_subdir().as_str(),
                    channel.canonical_name(),
                    chain(&err)
                ))
            })?;
        output
            .iter()
            .flat_map(|repodata| repodata.iter())
            .max_by(|a, b| {
                let (a, b) = (&a.package_record, &b.package_record);
                a.version
                    .cmp(&b.version)
                    .then(a.build_number.cmp(&b.build_number))
                    .then(a.timestamp.cmp(&b.timestamp))
            })
            .cloned()
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "{} has no {package}{} for {}",
                    channel.canonical_name(),
                    version.map(|v| format!(" {v}")).unwrap_or_default(),
                    platform.conda_subdir().as_str()
                ))
            })
    }
}

/// Unpacks a `.conda` or `.tar.bz2` archive into `dest`. Blocking.
pub(crate) fn unpack(archive: &Path, dest: &Path) -> Result<()> {
    rattler_package_streaming::fs::extract(archive, dest)
        .map(drop)
        .map_err(|err| {
            Error::InvalidArtifact(format!("unpacking {}: {}", archive.display(), chain(&err)))
        })
}

/// `info/index.json` of an unpacked package.
pub(crate) fn read_index(package: &Path) -> Result<IndexJson> {
    IndexJson::from_package_directory(package)
        .map_err(|err| Error::InvalidArtifact(format!("reading info/index.json: {err}")))
}

/// Moves the library (verified against `info/paths.json`) and `info/licenses/**` from an
/// unpacked package into `staging/{lib,licenses}`.
///
/// rattler recreates a package's symbolic links on Unix, with whatever target the package names.
/// The library must therefore be a regular file (a link could point anywhere and would be hashed
/// and later loaded through), a linked `info/licenses` is skipped, and links inside it are
/// dropped.
pub(crate) fn stage(package: &Path, platform: Platform, staging: &Path) -> Result<Extracted> {
    let paths = PathsJson::from_package_directory_with_deprecated_fallback(package)
        .map_err(|err| Error::InvalidArtifact(format!("reading info/paths.json: {err}")))?;
    let entry = platform
        .library_names()
        .iter()
        .flat_map(|name| {
            platform
                .conda_library_dirs()
                .iter()
                .map(move |dir| Path::new(dir).join(name))
        })
        .find_map(|candidate| {
            paths
                .paths
                .iter()
                .find(|entry| entry.relative_path == candidate)
        })
        .ok_or_else(|| {
            Error::InvalidArtifact(format!(
                "the package contains no {} library in {}",
                platform.library_names().join(" or "),
                platform.conda_library_dirs().join(" or ")
            ))
        })?;
    let source = package.join(&entry.relative_path);
    let shown = entry.relative_path.display();
    let metadata = fs::symlink_metadata(&source)
        .map_err(|err| Error::io(format!("reading {}", source.display()), err))?;
    if !metadata.is_file() {
        return Err(Error::InvalidArtifact(format!(
            "{shown} is not a regular file in the package"
        )));
    }
    let expected = entry
        .sha256
        .ok_or_else(|| Error::Integrity(format!("info/paths.json has no sha256 for {shown}")))?;
    let actual = rattler_digest::compute_file_digest::<rattler_digest::Sha256>(&source)
        .map_err(|err| Error::io(format!("reading {}", source.display()), err))?;
    if actual != expected {
        return Err(Error::Integrity(format!(
            "{shown} does not match info/paths.json: expected sha256 {}, got {}",
            hex::encode(expected),
            hex::encode(actual)
        )));
    }
    if let Some(size) = entry.size_in_bytes
        && metadata.len() != size
    {
        return Err(Error::Integrity(format!(
            "{shown} does not match info/paths.json: expected {size} bytes, got {}",
            metadata.len()
        )));
    }

    let file_name = entry
        .relative_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .ok_or_else(|| Error::InvalidArtifact(format!("library entry {shown} has no file name")))?;
    let lib = format!("lib/{file_name}");
    move_into(&source, &staging.join(&lib))?;

    let mut licenses = Vec::new();
    let package_licenses = package.join("info").join("licenses");
    if fs::symlink_metadata(&package_licenses).is_ok_and(|meta| meta.is_dir()) {
        let staged_licenses = staging.join("licenses");
        move_into(&package_licenses, &staged_licenses)?;
        collect_files(&staged_licenses, &staged_licenses, &mut licenses)?;
    }
    licenses.sort();
    Ok(Extracted {
        lib,
        lib_sha256: hex::encode(actual),
        licenses,
    })
}

/// Renames within the cache's `tmp/` directory (same volume).
fn move_into(from: &Path, to: &Path) -> Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| Error::io(format!("creating {}", parent.display()), err))?;
    }
    rename(from, to).map_err(|err| {
        Error::io(
            format!("moving {} to {}", from.display(), to.display()),
            err,
        )
    })
}

/// `licenses/<relative>` for every regular file under `dir`; anything else that is not a
/// directory (a link) is removed from the staging directory.
fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<()> {
    let entries =
        fs::read_dir(dir).map_err(|err| Error::io(format!("listing {}", dir.display()), err))?;
    for entry in entries {
        let entry = entry.map_err(|err| Error::io(format!("listing {}", dir.display()), err))?;
        let path = entry.path();
        // Not followed: a link reports itself.
        let kind = entry
            .file_type()
            .map_err(|err| Error::io(format!("reading {}", path.display()), err))?;
        if kind.is_dir() {
            collect_files(root, &path, out)?;
        } else if kind.is_file() {
            let relative = path.strip_prefix(root).map_err(|_| {
                Error::Internal(format!("{} escaped {}", path.display(), root.display()))
            })?;
            out.push(format!("licenses/{}", license_path(relative)?));
        } else {
            // A directory link is removed with `remove_dir` on Windows; neither follows it.
            fs::remove_file(&path)
                .or_else(|_| fs::remove_dir(&path))
                .map_err(|err| Error::io(format!("removing {}", path.display()), err))?;
        }
    }
    Ok(())
}

/// The `/`-separated form of a licence file's path below `licenses/`. Every component must be
/// UTF-8 without `\`, so that the recorded path names exactly the file on disk.
fn license_path(relative: &Path) -> Result<String> {
    let unsupported = || {
        Error::InvalidArtifact(format!(
            "unsupported licence file name {}",
            relative.display()
        ))
    };
    let mut parts = Vec::new();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            return Err(unsupported());
        };
        let part = part
            .to_str()
            .filter(|part| !part.contains('\\'))
            .ok_or_else(unsupported)?;
        parts.push(part);
    }
    safe_relative(&parts.join("/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIB: &str = "lib/libblpapi3_64.so";

    /// An unpacked Linux package `root/name` holding the library `LIB` (listed in `paths.json`)
    /// and an empty `info/licenses`.
    fn package(root: &Path, name: &str) -> PathBuf {
        let package = root.join(name);
        fs::create_dir_all(package.join("info").join("licenses")).unwrap();
        fs::create_dir_all(package.join("lib")).unwrap();
        fs::write(package.join(LIB), b"library").unwrap();
        let sha256 =
            hex::encode(rattler_digest::compute_bytes_digest::<rattler_digest::Sha256>(b"library"));
        fs::write(
            package.join("info").join("paths.json"),
            serde_json::json!({
                "paths": [{"_path": LIB, "path_type": "hardlink", "sha256": sha256,
                           "size_in_bytes": 7}],
                "paths_version": 1,
            })
            .to_string(),
        )
        .unwrap();
        package
    }

    /// Creates a symbolic link, or returns `false` where the platform refuses (Windows without
    /// the symlink privilege).
    fn symlink(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        let made = std::os::unix::fs::symlink(target, link);
        #[cfg(windows)]
        let made = if target.is_dir() {
            std::os::windows::fs::symlink_dir(target, link)
        } else {
            std::os::windows::fs::symlink_file(target, link)
        };
        made.is_ok()
    }

    #[test]
    fn licence_files_are_recorded_with_slashes() {
        let root = tempfile::tempdir().unwrap();
        let package = package(root.path(), "package");
        let licenses = package.join("info").join("licenses");
        fs::create_dir_all(licenses.join("cpp")).unwrap();
        fs::write(licenses.join("License.txt"), b"Bloomberg").unwrap();
        fs::write(licenses.join("cpp").join("LICENSE.openssl"), b"OpenSSL").unwrap();
        let staging = root.path().join("staging");
        let extracted = stage(&package, Platform::LinuxX86_64, &staging).unwrap();
        assert_eq!(extracted.lib, LIB);
        assert_eq!(
            extracted.licenses,
            ["licenses/License.txt", "licenses/cpp/LICENSE.openssl"]
        );
        assert_eq!(fs::read(staging.join(LIB)).unwrap(), b"library");
    }

    #[test]
    fn a_linked_library_is_rejected() {
        let root = tempfile::tempdir().unwrap();
        let outside = root.path().join("outside.so");
        fs::write(&outside, b"library").unwrap();
        let package = package(root.path(), "package");
        fs::remove_file(package.join(LIB)).unwrap();
        if !symlink(&outside, &package.join(LIB)) {
            return;
        }
        let error = stage(
            &package,
            Platform::LinuxX86_64,
            &root.path().join("staging"),
        )
        .expect_err("a link is not the library");
        assert_eq!(error.code(), "INVALID_ARTIFACT", "{error}");
        assert!(error.to_string().contains("not a regular file"), "{error}");
    }

    #[test]
    fn linked_licences_are_never_followed() {
        let root = tempfile::tempdir().unwrap();
        let secrets = root.path().join("secrets");
        fs::create_dir_all(&secrets).unwrap();
        fs::write(secrets.join("id_rsa"), b"private key").unwrap();

        // `info/licenses` itself is a link: skipped.
        let linked = package(root.path(), "linked");
        let licenses = linked.join("info").join("licenses");
        fs::remove_dir(&licenses).unwrap();
        if !symlink(&secrets, &licenses) {
            return;
        }
        let staging = root.path().join("staging-linked");
        let extracted = stage(&linked, Platform::LinuxX86_64, &staging).unwrap();
        assert!(extracted.licenses.is_empty());
        assert!(fs::symlink_metadata(staging.join("licenses")).is_err());

        // Links inside a real `info/licenses`: dropped, their targets untouched.
        let inside = package(root.path(), "inside");
        let licenses = inside.join("info").join("licenses");
        fs::write(licenses.join("License.txt"), b"Bloomberg").unwrap();
        assert!(symlink(&secrets.join("id_rsa"), &licenses.join("key")));
        assert!(symlink(&secrets, &licenses.join("dir")));
        let staging = root.path().join("staging-inside");
        let extracted = stage(&inside, Platform::LinuxX86_64, &staging).unwrap();
        assert_eq!(extracted.licenses, ["licenses/License.txt"]);
        assert!(fs::symlink_metadata(staging.join("licenses").join("key")).is_err());
        assert!(fs::symlink_metadata(staging.join("licenses").join("dir")).is_err());
        assert_eq!(fs::read(secrets.join("id_rsa")).unwrap(), b"private key");
    }

    #[test]
    fn licence_names_must_round_trip_through_the_provenance() {
        assert_eq!(
            license_path(&Path::new("cpp").join("LICENSE")).unwrap(),
            "cpp/LICENSE"
        );
        // On Unix `\` is part of a file name, which the provenance would read as a separator.
        let names: &[&str] = if cfg!(unix) {
            &["a\\b", "..", "x:y"]
        } else {
            &["..", "x:y"]
        };
        for name in names {
            let error = license_path(Path::new(name)).expect_err(name);
            assert_eq!(error.code(), "INVALID_ARTIFACT", "{name}");
        }
    }
}
