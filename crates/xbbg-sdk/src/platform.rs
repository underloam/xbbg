//! Target platforms and version requests.

use std::fmt;
use std::str::FromStr;

use rattler_conda_types::Subdir;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::Error;

/// A platform the xbbg native bindings ship for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Platform {
    /// 64-bit Windows on x86-64 (`windows-x86_64`).
    WindowsX86_64,
    /// 64-bit Linux on x86-64 (`linux-x86_64`).
    LinuxX86_64,
    /// macOS on Apple silicon (`macos-aarch64`).
    MacosAarch64,
}

impl Platform {
    /// Every supported platform.
    pub const ALL: [Self; 3] = [Self::WindowsX86_64, Self::LinuxX86_64, Self::MacosAarch64];

    /// The canonical name, e.g. `windows-x86_64` (also the cache directory name).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WindowsX86_64 => "windows-x86_64",
            Self::LinuxX86_64 => "linux-x86_64",
            Self::MacosAarch64 => "macos-aarch64",
        }
    }

    /// The platform this process runs on, if it is supported.
    pub fn host() -> Option<Self> {
        if cfg!(all(target_os = "windows", target_arch = "x86_64")) {
            Some(Self::WindowsX86_64)
        } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
            Some(Self::LinuxX86_64)
        } else if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
            Some(Self::MacosAarch64)
        } else {
            None
        }
    }

    /// File names of the Bloomberg library, most preferred first. Bloomberg ships the macOS
    /// library with a `.so` suffix.
    pub fn library_names(self) -> &'static [&'static str] {
        match self {
            Self::WindowsX86_64 => &["blpapi3_64.dll"],
            Self::LinuxX86_64 => &["libblpapi3_64.so"],
            Self::MacosAarch64 => &["libblpapi3_64.so", "libblpapi3_64.dylib"],
        }
    }

    pub(crate) fn conda_subdir(self) -> Subdir {
        match self {
            Self::WindowsX86_64 => Subdir::Win64,
            Self::LinuxX86_64 => Subdir::Linux64,
            Self::MacosAarch64 => Subdir::OsxArm64,
        }
    }

    /// Directories, relative to a conda package root, that hold shared libraries.
    pub(crate) fn conda_library_dirs(self) -> &'static [&'static str] {
        match self {
            Self::WindowsX86_64 => &["Library/bin"],
            Self::LinuxX86_64 | Self::MacosAarch64 => &["lib"],
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Platform {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::ALL
            .into_iter()
            .find(|platform| platform.as_str() == s)
            .ok_or_else(|| Error::UnsupportedPlatform(s.to_string()))
    }
}

impl Serialize for Platform {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Platform {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        name.parse().map_err(serde::de::Error::custom)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_unknown_names_are_rejected() {
        for platform in Platform::ALL {
            assert_eq!(platform.as_str().parse::<Platform>().unwrap(), platform);
        }
        let err = "linux-aarch64".parse::<Platform>().unwrap_err();
        assert_eq!(err.code(), "UNSUPPORTED_PLATFORM");
        assert!(err.to_string().contains("linux-aarch64"));
    }

    #[test]
    fn serde_uses_canonical_names() {
        let json = serde_json::to_string(&Platform::MacosAarch64).unwrap();
        assert_eq!(json, "\"macos-aarch64\"");
        let back: Platform = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Platform::MacosAarch64);
        assert!(serde_json::from_str::<Platform>("\"MacosAarch64\"").is_err());
    }
}
