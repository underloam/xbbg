//! Where runtimes come from.

/// Bloomberg's public PyPI-compatible index (PEP 503 HTML only).
pub const BLOOMBERG_PYPI_INDEX: &str =
    "https://blpapi.bloomberg.com/repository/releases/python/simple/";
/// PyPI project that ships the runtime inside its wheels.
pub const DEFAULT_PYPI_PROJECT: &str = "blpapi";
/// conda package that ships the runtime.
pub const DEFAULT_CONDA_PACKAGE: &str = "libblpapi";
/// Oldest `blpapi` wheel release whose library keeps its standard name.
///
/// Older cp3x wheels were repaired with auditwheel/delocate, which renamed the Linux library
/// (`blpapi.libs/libblpapi3_64-<hash>.so` with a matching SONAME) and gave the macOS one an
/// absolute install name, so a binding linked against `libblpapi3_64.so` cannot bind to them.
pub const MIN_PYPI_VERSION: &str = "3.24.10";

/// A place that offers Bloomberg runtimes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// A PEP 503 (HTML) or PEP 691 (JSON) simple index: an `http(s)` or `file` URL, or a local
    /// directory. Credentials may be embedded (`https://user:token@host/simple/`).
    PypiIndex {
        /// Index root, e.g. [`BLOOMBERG_PYPI_INDEX`].
        url: String,
        /// Project on that index, normally [`DEFAULT_PYPI_PROJECT`].
        project: String,
    },
    /// A conda channel: a name resolved against `https://conda.anaconda.org/` (`conda-forge`),
    /// an `http(s)`/`file` URL, or a local directory.
    CondaChannel {
        /// Channel name or URL.
        channel: String,
        /// Package in that channel, normally [`DEFAULT_CONDA_PACKAGE`].
        package: String,
    },
    /// One `.whl`, `.conda` or `.tar.bz2` archive: an `http(s)`/`file` URL or a local path.
    /// A URL may carry the expected hash as `#sha256=<hex>`.
    Artifact {
        /// URL or path.
        location: String,
        /// Expected SHA-256 of the archive (lowercase or uppercase hex).
        sha256: Option<String>,
    },
}

impl Source {
    /// A PyPI index offering [`DEFAULT_PYPI_PROJECT`].
    pub fn pypi(url: impl Into<String>) -> Self {
        Self::PypiIndex {
            url: url.into(),
            project: DEFAULT_PYPI_PROJECT.to_string(),
        }
    }

    /// Bloomberg's own index.
    pub fn bloomberg_pypi() -> Self {
        Self::pypi(BLOOMBERG_PYPI_INDEX)
    }

    /// A conda channel offering [`DEFAULT_CONDA_PACKAGE`].
    pub fn conda(channel: impl Into<String>) -> Self {
        Self::CondaChannel {
            channel: channel.into(),
            package: DEFAULT_CONDA_PACKAGE.to_string(),
        }
    }

    /// A single archive without an expected hash (see [`Source::Artifact`]).
    pub fn artifact(location: impl Into<String>) -> Self {
        Self::Artifact {
            location: location.into(),
            sha256: None,
        }
    }

    /// Which kind of source this is.
    pub fn kind(&self) -> SourceKind {
        match self {
            Self::PypiIndex { .. } => SourceKind::Pypi,
            Self::CondaChannel { .. } => SourceKind::Conda,
            Self::Artifact { .. } => SourceKind::Artifact,
        }
    }
}

/// The kind of a [`Source`]; also the prefix of install directories and source labels.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SourceKind {
    /// A PyPI index.
    Pypi,
    /// A conda channel.
    Conda,
    /// A single archive.
    Artifact,
}

impl SourceKind {
    /// `pypi`, `conda` or `artifact`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pypi => "pypi",
            Self::Conda => "conda",
            Self::Artifact => "artifact",
        }
    }
}
