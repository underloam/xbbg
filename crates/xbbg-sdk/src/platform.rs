//! Target platforms.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

    /// File names of the Bloomberg library, most preferred first.
    ///
    /// This is the union of what the xbbg hosts accepted before they shared one search. Bloomberg
    /// ships the macOS library with a `.so` suffix; `libblpapi3.*` are the unversioned-name
    /// spellings of packaged SDKs. `blpapi3_32.dll` is absent: a 32-bit library cannot load into
    /// the 64-bit processes xbbg ships for.
    pub fn library_names(self) -> &'static [&'static str] {
        match self {
            Self::WindowsX86_64 => &["blpapi3_64.dll"],
            Self::LinuxX86_64 => &["libblpapi3_64.so", "libblpapi3.so"],
            Self::MacosAarch64 => &[
                "libblpapi3_64.so",
                "libblpapi3_64.dylib",
                "libblpapi3.so",
                "libblpapi3.dylib",
            ],
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The text is not the canonical name of a [`Platform`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsePlatformError(String);

impl ParsePlatformError {
    /// The rejected text.
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ParsePlatformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unsupported platform `{}` (supported: ", self.0)?;
        for (index, platform) in Platform::ALL.into_iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            f.write_str(platform.as_str())?;
        }
        f.write_str(")")
    }
}

impl std::error::Error for ParsePlatformError {}

impl FromStr for Platform {
    type Err = ParsePlatformError;

    fn from_str(s: &str) -> Result<Self, ParsePlatformError> {
        Self::ALL
            .into_iter()
            .find(|platform| platform.as_str() == s)
            .ok_or_else(|| ParsePlatformError(s.to_string()))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip_and_unknown_names_are_rejected() {
        for platform in Platform::ALL {
            assert_eq!(platform.as_str().parse::<Platform>().unwrap(), platform);
            assert_eq!(platform.to_string(), platform.as_str());
        }
        let err = "linux-aarch64".parse::<Platform>().unwrap_err();
        assert_eq!(err.name(), "linux-aarch64");
        assert_eq!(
            err.to_string(),
            "unsupported platform `linux-aarch64` (supported: windows-x86_64, linux-x86_64, \
             macos-aarch64)"
        );
    }

    #[test]
    fn serde_uses_canonical_names() {
        let json = serde_json::to_string(&Platform::MacosAarch64).unwrap();
        assert_eq!(json, "\"macos-aarch64\"");
        let back: Platform = serde_json::from_str(&json).unwrap();
        assert_eq!(back, Platform::MacosAarch64);
        assert!(serde_json::from_str::<Platform>("\"MacosAarch64\"").is_err());
    }

    #[test]
    fn libraries_are_named_per_platform() {
        assert_eq!(Platform::WindowsX86_64.library_names(), ["blpapi3_64.dll"]);
        assert_eq!(
            Platform::LinuxX86_64.library_names(),
            ["libblpapi3_64.so", "libblpapi3.so"]
        );
        assert_eq!(
            Platform::MacosAarch64.library_names(),
            [
                "libblpapi3_64.so",
                "libblpapi3_64.dylib",
                "libblpapi3.so",
                "libblpapi3.dylib"
            ]
        );
    }
}
