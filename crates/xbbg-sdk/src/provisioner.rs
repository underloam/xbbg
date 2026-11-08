//! Resolve, download, verify and install.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rattler_conda_types::Channel;
use rattler_conda_types::package::CondaArchiveIdentifier;
use url::Url;
use uv_distribution_filename::WheelFilename;
use uv_normalize::PackageName;
use uv_pep440::Version;

use crate::conda::{self, Conda};
use crate::error::{Error, Result, redact};
use crate::http::{self, Fetched, Http};
use crate::install::{
    self, Layout, Provenance, Provisioned, add_source, commit, existing_install, find_by_archive,
    find_by_source,
};
use crate::options::{Credential, ProvisionOptions};
use crate::platform::{Platform, VersionSpec};
use crate::pypi::{UvIndex, UvSettings};
use crate::source::{DEFAULT_CONDA_PACKAGE, DEFAULT_PYPI_PROJECT, Source, SourceKind};
use crate::wheel::{self, Extracted};

/// Downloads older than this in `<cache>/tmp` belong to crashed runs.
const STALE_TEMPORARIES: Duration = Duration::from_secs(24 * 60 * 60);

/// What a source offers for a request, before anything is downloaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    /// Version that would be installed. For artifacts it comes from the file name; the
    /// package's own metadata decides at install time.
    pub version: String,
    /// Target platform.
    pub platform: Platform,
    /// Source label (see [`Provisioned::source`]).
    pub source: String,
    /// Archive file name.
    pub filename: String,
    /// Archive URL without credentials.
    pub artifact_url: String,
    /// Expected archive SHA-256 (always present for indexes and channels).
    pub sha256: Option<String>,
}

/// Provisions Bloomberg runtimes into one cache.
///
/// Every method is safe to call concurrently, also from several processes sharing the cache:
/// each install directory is written under an exclusive file lock and committed by a rename.
/// The futures are `Send`; uv's index client runs on a dedicated thread. Dropping a future
/// (cancelling) never interrupts file-system work half-way: the blocking steps own the lock,
/// the download and the staging directories until they finish.
pub struct Provisioner {
    layout: Layout,
    http: Http,
    conda: Conda,
    uv: Mutex<Option<Arc<UvIndex>>>,
    require_artifact_hash: bool,
    min_pypi_version: Option<Version>,
}

/// A source after validation, with credentials moved out of its URL.
enum Target {
    Pypi {
        index: Url,
        project: PackageName,
        label: String,
        local: bool,
    },
    Conda {
        channel: Channel,
        package: String,
        label: String,
        local: bool,
    },
    Artifact(Artifact),
}

struct Artifact {
    url: Url,
    filename: String,
    format: ArchiveFormat,
    sha256: Option<String>,
    label: String,
    local: bool,
}

enum ArchiveFormat {
    Wheel(WheelFilename),
    Conda,
}

/// An archive chosen from an index or channel.
struct Candidate {
    version: String,
    url: Url,
    filename: String,
    sha256: String,
}

impl Provisioner {
    /// Creates the cache directories and the HTTP clients. No network access happens here.
    pub fn new(options: ProvisionOptions) -> Result<Self> {
        let layout = Layout::new(&options.cache_dir)?;
        layout.remove_stale_temporaries(STALE_TEMPORARIES);
        let http = Http::new(&options.network)?;
        let root_dir = std::env::current_dir().unwrap_or_else(|_| layout.root().to_path_buf());
        let conda = Conda::new(&http, layout.http_dir("rattler"), root_dir);
        let min_pypi_version = options
            .min_pypi_version
            .as_deref()
            .map(|version| {
                Version::from_str(version).map_err(|err| {
                    Error::InvalidArgument(format!(
                        "invalid minimum PyPI version `{version}`: {err}"
                    ))
                })
            })
            .transpose()?;
        Ok(Self {
            layout,
            http,
            conda,
            uv: Mutex::new(None),
            require_artifact_hash: options.require_artifact_hash,
            min_pypi_version,
        })
    }

    /// The absolute cache root.
    pub fn cache_root(&self) -> &Path {
        self.layout.root()
    }

    /// What `provision` would install, without downloading archives. Offline, this reads the
    /// cached index pages and repodata.
    pub async fn resolve(
        &self,
        source: &Source,
        platform: Platform,
        version: &VersionSpec,
    ) -> Result<Resolved> {
        match self.prepare(source, platform)? {
            Target::Pypi {
                index,
                project,
                label,
                local,
            } => {
                let candidate = self
                    .choose_wheel(&index, &project, platform, version, local)
                    .await?;
                Ok(Resolved {
                    version: candidate.version,
                    platform,
                    source: label,
                    filename: candidate.filename,
                    artifact_url: redact(&candidate.url),
                    sha256: Some(candidate.sha256),
                })
            }
            Target::Conda {
                channel,
                package,
                label,
                local,
            } => {
                let candidate = self
                    .choose_package(&channel, &package, platform, version, local)
                    .await?;
                Ok(Resolved {
                    version: candidate.version,
                    platform,
                    source: label,
                    filename: candidate.filename,
                    artifact_url: redact(&candidate.url),
                    sha256: Some(candidate.sha256),
                })
            }
            Target::Artifact(artifact) => {
                let version = match &artifact.format {
                    ArchiveFormat::Wheel(wheel) => wheel.version.to_string(),
                    ArchiveFormat::Conda => CondaArchiveIdentifier::try_from_filename(
                        &artifact.filename,
                    )
                    .map(|archive| archive.identifier.version)
                    .ok_or_else(|| {
                        Error::InvalidArgument(format!(
                            "{} is not a conda package file name (name-version-build.conda)",
                            artifact.filename
                        ))
                    })?,
                };
                Ok(Resolved {
                    version,
                    platform,
                    source: artifact.label,
                    filename: artifact.filename,
                    artifact_url: redact(&artifact.url),
                    sha256: artifact.sha256,
                })
            }
        }
    }

    /// Installs the requested runtime (or returns the existing install) and returns it.
    ///
    /// An exact version that is already installed from the same source is returned without
    /// network access. Offline, remote sources only return installs from the cache: the newest
    /// one from that source for [`VersionSpec::Latest`] (never older than the minimum PyPI
    /// version for PyPI sources), and for an artifact without a hash the one installed from the
    /// same location. Local files and directories work as usual.
    pub async fn provision(
        &self,
        source: &Source,
        platform: Platform,
        version: &VersionSpec,
    ) -> Result<Provisioned> {
        match self.prepare(source, platform)? {
            Target::Pypi {
                index,
                project,
                label,
                local,
            } => {
                if let VersionSpec::Exact(pin) = version {
                    let pin = parse_pep440(pin)?;
                    wheel::check_minimum(project.as_ref(), &pin, self.min_pypi_version.as_ref())?;
                }
                let minimum = self.min_pypi_version.as_ref();
                if let Some(hit) = self
                    .cached(&label, platform, version, local, minimum)
                    .await?
                {
                    return Ok(hit);
                }
                let candidate = self
                    .choose_wheel(&index, &project, platform, version, local)
                    .await?;
                self.install(SourceKind::Pypi, &label, platform, candidate, Unpack::Wheel)
                    .await
            }
            Target::Conda {
                channel,
                package,
                label,
                local,
            } => {
                if let Some(hit) = self.cached(&label, platform, version, local, None).await? {
                    return Ok(hit);
                }
                let candidate = self
                    .choose_package(&channel, &package, platform, version, local)
                    .await?;
                self.install(
                    SourceKind::Conda,
                    &label,
                    platform,
                    candidate,
                    Unpack::Conda,
                )
                .await
            }
            Target::Artifact(artifact) => self.install_artifact(artifact, platform).await,
        }
    }

    /// Every valid install in this cache (optionally for one platform), newest version first.
    pub fn installed(&self, platform: Option<Platform>) -> Result<Vec<Provisioned>> {
        install::installed(self.layout.root(), platform)
    }

    /// Exact pins hit the cache even online; offline, remote sources must be served from it.
    async fn cached(
        &self,
        label: &str,
        platform: Platform,
        version: &VersionSpec,
        local: bool,
        minimum: Option<&Version>,
    ) -> Result<Option<Provisioned>> {
        let pin = match version {
            VersionSpec::Exact(pin) => Some(pin.clone()),
            VersionSpec::Latest => None,
        };
        let offline = self.http.offline && !local;
        if pin.is_none() && !offline {
            return Ok(None);
        }
        let root = self.layout.root().to_path_buf();
        let (source, wanted, minimum) = (label.to_string(), pin.clone(), minimum.cloned());
        let hit = blocking(move || {
            find_by_source(
                &root,
                &source,
                platform,
                wanted.as_deref(),
                minimum.as_ref(),
            )
        })
        .await?;
        if hit.is_some() {
            return Ok(hit);
        }
        if offline {
            return Err(Error::Offline(format!(
                "offline mode: no {platform} runtime{} from {label} in the cache {}",
                pin.map(|pin| format!(" {pin}")).unwrap_or_default(),
                self.layout.root().display()
            )));
        }
        Ok(None)
    }

    async fn choose_wheel(
        &self,
        index: &Url,
        project: &PackageName,
        platform: Platform,
        version: &VersionSpec,
        local: bool,
    ) -> Result<Candidate> {
        let wanted = match version {
            VersionSpec::Exact(pin) => {
                let pin = parse_pep440(pin)?;
                wheel::check_minimum(project.as_ref(), &pin, self.min_pypi_version.as_ref())?;
                Some(pin)
            }
            VersionSpec::Latest => None,
        };
        let files = self.uv()?.list(index, project.as_ref()).await?;
        let (chosen, file) = wheel::choose_wheel(
            &files,
            project,
            wanted.as_ref(),
            self.min_pypi_version.as_ref(),
            platform,
        )
        .ok_or_else(|| {
            let minimum = self
                .min_pypi_version
                .as_ref()
                .map(|minimum| format!(" (oldest accepted release: {minimum})"))
                .unwrap_or_default();
            Error::NotFound(format!(
                "{} has no {project} {version} wheel for {platform}{minimum}",
                redact(index)
            ))
        })?;
        let sha256 = file.sha256.clone().ok_or_else(|| {
            Error::Integrity(format!(
                "{} publishes no sha256 for {}; refusing an unverifiable download",
                redact(index),
                file.filename
            ))
        })?;
        check_download(&file.url, local, &redact(index))?;
        Ok(Candidate {
            version: chosen.to_string(),
            url: file.url.clone(),
            filename: file.filename.clone(),
            sha256,
        })
    }

    async fn choose_package(
        &self,
        channel: &Channel,
        package: &str,
        platform: Platform,
        version: &VersionSpec,
        local: bool,
    ) -> Result<Candidate> {
        let pin = match version {
            VersionSpec::Exact(pin) => Some(pin.as_str()),
            VersionSpec::Latest => None,
        };
        let record = self.conda.resolve(channel, package, platform, pin).await?;
        let filename = record.identifier.to_string();
        let sha256 = record
            .package_record
            .sha256
            .map(hex::encode)
            .ok_or_else(|| {
                Error::Integrity(format!(
                    "{} publishes no sha256 for {filename}; refusing an unverifiable download",
                    channel.canonical_name()
                ))
            })?;
        check_download(&record.url, local, &channel.canonical_name())?;
        Ok(Candidate {
            version: record.package_record.version.to_string(),
            url: record.url.clone(),
            filename,
            sha256,
        })
    }

    /// Lock, re-check, download, verify the archive, extract with verification, commit.
    async fn install(
        &self,
        kind: SourceKind,
        label: &str,
        platform: Platform,
        candidate: Candidate,
        unpack: Unpack,
    ) -> Result<Provisioned> {
        let final_dir =
            self.layout
                .install_dir(&candidate.version, platform, kind, &candidate.sha256);
        // Shared with each blocking step, so a cancelled provision cannot release the lock while
        // one of them still works on the install directory.
        let lock = Arc::new(self.layout.lock(&final_dir).await?);
        let existing = {
            let (lock, dir) = (Arc::clone(&lock), final_dir.clone());
            let (sha256, source) = (candidate.sha256.clone(), label.to_string());
            blocking(move || {
                let _lock = lock;
                existing_install(&dir, &sha256, &source)
            })
            .await?
        };
        if let Some(existing) = existing {
            return Ok(existing);
        }
        let temp = self.layout.temp_file(&candidate.filename)?;
        let fetched = http::fetch(&self.http, &candidate.url, temp).await?;
        if fetched.sha256 != candidate.sha256 {
            return Err(Error::Integrity(format!(
                "{} does not match its published sha256: expected {}, got {}",
                redact(&candidate.url),
                candidate.sha256,
                fetched.sha256
            )));
        }
        let (staging, staged) = self.extract(fetched, platform, unpack).await?;
        let provenance = Provenance {
            version: candidate.version,
            platform,
            source: label.to_string(),
            artifact_url: redact(&candidate.url),
            artifact_sha256: candidate.sha256,
            lib: staged.extracted.lib,
            lib_sha256: staged.extracted.lib_sha256,
            licenses: staged.extracted.licenses,
        };
        blocking(move || {
            let _lock = lock;
            commit(staging, &final_dir, &provenance)
        })
        .await
    }

    async fn install_artifact(
        &self,
        artifact: Artifact,
        platform: Platform,
    ) -> Result<Provisioned> {
        if let Some(expected) = &artifact.sha256
            && let Some(hit) = self
                .installed_archive(platform, expected, &artifact.label)
                .await?
        {
            return Ok(hit);
        }
        if self.http.offline && !artifact.local {
            // Without a hash only the location identifies the archive.
            if artifact.sha256.is_none() {
                let (root, label) = (self.layout.root().to_path_buf(), artifact.label.clone());
                let hit =
                    blocking(move || find_by_source(&root, &label, platform, None, None)).await?;
                if let Some(hit) = hit {
                    return Ok(hit);
                }
            }
            return Err(Error::Offline(format!(
                "offline mode: {} is not in the cache {}",
                artifact.label,
                self.layout.root().display()
            )));
        }
        let temp = self.layout.temp_file(&artifact.filename)?;
        let fetched = http::fetch(&self.http, &artifact.url, temp).await?;
        if let Some(expected) = &artifact.sha256
            && *expected != fetched.sha256
        {
            return Err(Error::Integrity(format!(
                "{} does not match the expected sha256: expected {expected}, got {}",
                artifact.label, fetched.sha256
            )));
        }
        let sha256 = fetched.sha256.clone();
        if let Some(hit) = self
            .installed_archive(platform, &sha256, &artifact.label)
            .await?
        {
            return Ok(hit);
        }
        let unpack = match &artifact.format {
            ArchiveFormat::Wheel(_) => Unpack::Wheel,
            ArchiveFormat::Conda => Unpack::CondaArtifact,
        };
        let (staging, staged) = self.extract(fetched, platform, unpack).await?;
        let version = match (&artifact.format, staged.version) {
            (ArchiveFormat::Wheel(wheel), _) => wheel.version.to_string(),
            (ArchiveFormat::Conda, Some(version)) => version,
            (ArchiveFormat::Conda, None) => {
                return Err(Error::Internal(
                    "conda artifact unpacked without a version".into(),
                ));
            }
        };
        let final_dir = self
            .layout
            .install_dir(&version, platform, SourceKind::Artifact, &sha256);
        let lock = self.layout.lock(&final_dir).await?;
        let provenance = Provenance {
            version,
            platform,
            source: artifact.label,
            artifact_url: redact(&artifact.url),
            artifact_sha256: sha256,
            lib: staged.extracted.lib,
            lib_sha256: staged.extracted.lib_sha256,
            licenses: staged.extracted.licenses,
        };
        blocking(move || {
            let _lock = lock;
            commit(staging, &final_dir, &provenance)
        })
        .await
    }

    /// A valid artifact install of the archive with `sha256`, with `label` recorded as one of
    /// its sources (so that the label finds it offline, also without a hash).
    async fn installed_archive(
        &self,
        platform: Platform,
        sha256: &str,
        label: &str,
    ) -> Result<Option<Provisioned>> {
        let root = self.layout.root().to_path_buf();
        let wanted = sha256.to_string();
        let found =
            blocking(move || find_by_archive(&root, SourceKind::Artifact, platform, &wanted))
                .await?;
        let Some((dir, hit)) = found else {
            return Ok(None);
        };
        if hit.source == label {
            return Ok(Some(hit));
        }
        let lock = self.layout.lock(&dir).await?;
        let label = label.to_string();
        blocking(move || {
            let _lock = lock;
            add_source(&dir, &hit.source, &label);
            Ok(Some(hit))
        })
        .await
    }

    /// Extracts the library and licences into a fresh staging directory. The blocking task owns
    /// the archive and every directory it writes, so cancelling the provision never deletes
    /// them under it; they are cleaned up when it ends.
    async fn extract(
        &self,
        fetched: Fetched,
        platform: Platform,
        unpack: Unpack,
    ) -> Result<(tempfile::TempDir, Staged)> {
        let staging = self.layout.staging()?;
        let unpacked = match unpack {
            Unpack::Wheel => None,
            Unpack::Conda | Unpack::CondaArtifact => Some(self.layout.staging()?),
        };
        blocking(move || {
            let staged = match &unpacked {
                None => Staged {
                    extracted: wheel::extract(&fetched.path, platform, staging.path())?,
                    version: None,
                },
                Some(unpacked) => {
                    let package = unpacked.path();
                    conda::unpack(&fetched.path, package)?;
                    let index = conda::read_index(package)?;
                    if matches!(unpack, Unpack::CondaArtifact) {
                        let subdir = index.subdir.clone().unwrap_or_default();
                        if subdir != platform.conda_subdir().as_str() {
                            return Err(Error::InvalidArgument(format!(
                                "the package is built for {}, not {} ({platform})",
                                if subdir.is_empty() {
                                    "an unknown subdir"
                                } else {
                                    &subdir
                                },
                                platform.conda_subdir().as_str()
                            )));
                        }
                    }
                    Staged {
                        extracted: conda::stage(package, platform, staging.path())?,
                        version: Some(index.version.to_string()),
                    }
                }
            };
            drop((fetched, unpacked));
            Ok((staging, staged))
        })
        .await
    }

    fn uv(&self) -> Result<Arc<UvIndex>> {
        let mut uv = self
            .uv
            .lock()
            .map_err(|_| Error::Internal("PyPI index state poisoned".into()))?;
        if let Some(index) = uv.as_ref() {
            return Ok(index.clone());
        }
        let index = Arc::new(UvIndex::start(UvSettings {
            cache_dir: self.layout.http_dir("uv"),
            client: self.http.uv_client.clone(),
            auth: self.http.auth_middleware(),
            offline: self.http.offline,
            retries: self.http.retries,
            connect_timeout: self.http.connect_timeout,
            read_timeout: self.http.read_timeout,
        })?);
        *uv = Some(index.clone());
        Ok(index)
    }

    /// Validates a source and moves URL credentials into the credential store.
    fn prepare(&self, source: &Source, platform: Platform) -> Result<Target> {
        match source {
            Source::PypiIndex { url, project } => {
                let mut index = self.location_url(url, "PyPI index")?;
                if !index.path().ends_with('/') {
                    let path = format!("{}/", index.path());
                    index.set_path(&path);
                }
                index.set_fragment(None);
                let project = PackageName::from_str(project.trim()).map_err(|err| {
                    Error::InvalidArgument(format!("invalid PyPI project `{project}`: {err}"))
                })?;
                let mut label = format!("pypi:{}", redact(&index));
                if project.as_ref() != DEFAULT_PYPI_PROJECT {
                    label.push('#');
                    label.push_str(project.as_ref());
                }
                Ok(Target::Pypi {
                    local: index.scheme() == "file",
                    index,
                    project,
                    label,
                })
            }
            Source::CondaChannel { channel, package } => {
                let package = package.trim();
                if package.is_empty() {
                    return Err(Error::InvalidArgument(
                        "conda package name must not be empty".into(),
                    ));
                }
                let channel = channel.trim();
                let channel = if channel.contains("://") {
                    let url = self.location_url(channel, "conda channel")?;
                    self.conda.channel(url.as_str())?
                } else {
                    self.conda.channel(channel)?
                };
                let mut label = format!("conda:{}", channel.canonical_name().trim_end_matches('/'));
                if package != DEFAULT_CONDA_PACKAGE {
                    label.push('#');
                    label.push_str(package);
                }
                Ok(Target::Conda {
                    local: channel.base_url.url().scheme() == "file",
                    channel,
                    package: package.to_string(),
                    label,
                })
            }
            Source::Artifact { location, sha256 } => {
                self.prepare_artifact(location, sha256.as_deref(), platform)
            }
        }
    }

    fn prepare_artifact(
        &self,
        location: &str,
        sha256: Option<&str>,
        platform: Platform,
    ) -> Result<Target> {
        let mut url = self.location_url(location, "artifact")?;
        // `#sha256=<hex>`, the key in any case (as the Java API reads it).
        let fragment = url.fragment().and_then(|fragment| {
            let (key, value) = fragment.split_once('=')?;
            key.eq_ignore_ascii_case("sha256")
                .then(|| value.to_string())
        });
        url.set_fragment(None);
        let sha256 = match (
            sha256.map(sha256_hex).transpose()?,
            fragment.as_deref().map(sha256_hex).transpose()?,
        ) {
            (Some(given), Some(fragment)) if given != fragment => {
                return Err(Error::InvalidArgument(format!(
                    "conflicting artifact hashes: {given} and #sha256={fragment}"
                )));
            }
            (given, fragment) => given.or(fragment),
        };
        let local = url.scheme() == "file";
        let filename = if local {
            url.to_file_path().ok().and_then(|path| {
                path.file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
        } else {
            url.path_segments()
                .and_then(|mut segments| segments.next_back())
                .map(str::to_string)
        }
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            Error::InvalidArgument(format!("artifact {} has no file name", redact(&url)))
        })?;
        let format = if filename.ends_with(".whl") {
            let wheel = WheelFilename::from_str(&filename).map_err(|err| {
                Error::InvalidArgument(format!("{filename} is not a valid wheel name: {err}"))
            })?;
            if wheel::platform_rank(&wheel, platform).is_none() {
                return Err(Error::InvalidArgument(format!(
                    "{filename} is not a {platform} wheel"
                )));
            }
            wheel::check_minimum(
                wheel.name.as_ref(),
                &wheel.version,
                self.min_pypi_version.as_ref(),
            )?;
            ArchiveFormat::Wheel(wheel)
        } else if filename.ends_with(".conda") || filename.ends_with(".tar.bz2") {
            ArchiveFormat::Conda
        } else {
            return Err(Error::InvalidArgument(format!(
                "unsupported artifact {filename}: expected a .whl, .conda or .tar.bz2 file"
            )));
        };
        if !local && sha256.is_none() && self.require_artifact_hash {
            return Err(Error::InvalidArgument(format!(
                "remote artifact {} needs its sha256 (append #sha256=<hex>) or allow unverified artifacts",
                redact(&url)
            )));
        }
        let label = if local {
            match url.to_file_path() {
                Ok(path) => format!("artifact:{}", path.display()),
                Err(()) => format!("artifact:{}", redact(&url)),
            }
        } else {
            format!("artifact:{}", redact(&url))
        };
        Ok(Target::Artifact(Artifact {
            url,
            filename,
            format,
            sha256,
            label,
            local,
        }))
    }

    /// Parses an `http(s)`/`file` URL or a local path; credentials in the URL are stored for
    /// its host and removed from it. An `http` location may receive credentials over plain
    /// HTTP (see [`Http::allow_plain_http`]); other schemes are rejected.
    fn location_url(&self, location: &str, what: &str) -> Result<Url> {
        let location = location.trim();
        if location.is_empty() {
            return Err(Error::InvalidArgument(format!(
                "{what} location must not be empty"
            )));
        }
        let mut url = match Url::parse(location) {
            Ok(url) if matches!(url.scheme(), "http" | "https" | "file") => url,
            // A scheme this provisioner cannot fetch (`ftp:`, `s3:`, `oci:`, …).
            Ok(url) if url.scheme().len() > 1 => {
                return Err(Error::InvalidArgument(format!(
                    "unsupported {what} URL {}: use an http, https or file URL, or a local path",
                    redact(&url)
                )));
            }
            // A path; `C:\…` parses as a URL with the one-letter scheme `c`.
            _ => {
                let path = std::path::absolute(PathBuf::from(location))
                    .map_err(|err| Error::io(format!("resolving {location}"), err))?;
                let as_url = if path.is_dir() {
                    Url::from_directory_path(&path)
                } else {
                    Url::from_file_path(&path)
                };
                as_url.map_err(|()| {
                    Error::InvalidArgument(format!("invalid {what} path {location}"))
                })?
            }
        };
        if !url.username().is_empty() || url.password().is_some() {
            let host = url
                .host_str()
                .ok_or_else(|| {
                    Error::InvalidArgument(format!("{what} URL with credentials has no host"))
                })?
                .to_string();
            let credential = Credential::Basic {
                username: percent_decode(url.username()),
                password: percent_decode(url.password().unwrap_or_default()),
            };
            self.http.add_credential(&host, &credential)?;
            let _ = url.set_username("");
            let _ = url.set_password(None);
        }
        self.http.allow_plain_http(&url);
        Ok(url)
    }
}

/// A remote index or channel may only send downloads to `http(s)`: [`http::fetch`] reads
/// `file:` URLs in place, and on Windows `file://host/share/…` opens an SMB connection that
/// sends the user's credentials. Local indexes and channels may link local files.
fn check_download(url: &Url, local: bool, source: &str) -> Result<()> {
    if local || matches!(url.scheme(), "http" | "https") {
        Ok(())
    } else {
        Err(Error::InvalidArtifact(format!(
            "{source} points a download at {}; a remote source may only link http or https URLs",
            redact(url)
        )))
    }
}

#[derive(Clone, Copy)]
enum Unpack {
    Wheel,
    Conda,
    CondaArtifact,
}

struct Staged {
    extracted: Extracted,
    /// From `info/index.json` for conda packages.
    version: Option<String>,
}

fn parse_pep440(version: &str) -> Result<Version> {
    Version::from_str(version.trim())
        .map_err(|err| Error::InvalidArgument(format!("invalid version `{version}`: {err}")))
}

fn sha256_hex(text: &str) -> Result<String> {
    let hex = text.trim().to_ascii_lowercase();
    if hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(hex)
    } else {
        Err(Error::InvalidArgument(format!(
            "not a sha256 hex digest: `{text}`"
        )))
    }
}

/// Decodes `%XX` escapes (URL user info); invalid escapes are kept verbatim.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2]))
        {
            out.push(hi * 16 + lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Runs file-system work off the async runtime.
async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|err| Error::Internal(format!("file-system task failed: {err}")))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_decoding_handles_escapes_and_garbage() {
        assert_eq!(percent_decode("user%40corp"), "user@corp");
        assert_eq!(percent_decode("p%3Ass%2F"), "p:ss/");
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }

    #[test]
    fn sha256_hex_normalizes_and_rejects() {
        let upper = "AB".repeat(32);
        assert_eq!(sha256_hex(&upper).unwrap(), "ab".repeat(32));
        assert_eq!(sha256_hex("abc").unwrap_err().code(), "INVALID_ARGUMENT");
    }

    #[test]
    fn provision_future_is_send() {
        fn assert_send<T: Send>(_: &T) {}
        let dir = tempfile::tempdir().unwrap();
        let provisioner = Provisioner::new(ProvisionOptions::new(dir.path())).unwrap();
        let source = Source::bloomberg_pypi();
        let future = provisioner.provision(&source, Platform::WindowsX86_64, &VersionSpec::Latest);
        assert_send(&future);
        let resolve = provisioner.resolve(&source, Platform::WindowsX86_64, &VersionSpec::Latest);
        assert_send(&resolve);
    }

    #[test]
    fn remote_sources_only_download_over_http() {
        let remote = Url::parse("https://repo.example/files/blpapi.whl").unwrap();
        assert!(check_download(&remote, false, "index").is_ok());
        for link in [
            "file:///etc/passwd",
            "file://server/share/blpapi.whl",
            "ftp://repo.example/blpapi.whl",
        ] {
            let link = Url::parse(link).unwrap();
            let error = check_download(&link, false, "https://repo.example/simple/")
                .expect_err(link.as_str());
            assert_eq!(error.code(), "INVALID_ARTIFACT");
            assert!(check_download(&link, true, "a local index").is_ok());
        }
    }

    #[test]
    fn unknown_url_schemes_are_rejected_with_their_credentials_hidden() {
        let dir = tempfile::tempdir().unwrap();
        let provisioner = Provisioner::new(ProvisionOptions::new(dir.path())).unwrap();
        for location in [
            "ftp://svc:hunter2@files.example/blpapi-3.26.9.1-py3-none-win_amd64.whl",
            "s3://bucket/channel",
            "oci://ghcr.io/channel",
        ] {
            let Err(error) = provisioner.location_url(location, "artifact") else {
                panic!("{location} is not fetchable");
            };
            assert_eq!(error.code(), "INVALID_ARGUMENT", "{location}");
            let message = error.to_string();
            assert!(message.contains("unsupported artifact URL"), "{message}");
            assert!(
                !message.contains("hunter2") && !message.contains("svc"),
                "{message}"
            );
        }
        // One-letter schemes are drive letters, and those are paths.
        let path = dir.path().join("blpapi-3.26.9.1-py3-none-win_amd64.whl");
        let url = provisioner
            .location_url(&path.to_string_lossy(), "artifact")
            .unwrap();
        assert_eq!(url.scheme(), "file");
        assert_eq!(url.to_file_path().unwrap(), path);
    }
}
