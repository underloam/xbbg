//! Version requests.

use std::fmt;

/// Which version to provision.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum VersionSpec {
    /// The newest stable release the source offers for the platform.
    #[default]
    Latest,
    /// Exactly this version: PEP 440 for PyPI and wheels, a conda version for channels.
    Exact(String),
}

impl VersionSpec {
    /// An exact pin.
    pub fn exact(version: impl Into<String>) -> Self {
        Self::Exact(version.into())
    }
}

impl fmt::Display for VersionSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Latest => f.write_str("latest"),
            Self::Exact(version) => f.write_str(version),
        }
    }
}
