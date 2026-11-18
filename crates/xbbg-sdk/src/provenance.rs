//! `provenance.json`: the record an install carries, and the path rule its entries obey.

use serde::{Deserialize, Serialize};

use crate::Platform;

/// File name of the provenance record inside an install directory. It is written last, so a
/// directory without it is an incomplete install.
pub const PROVENANCE_FILE: &str = "provenance.json";

/// `provenance.json`: the contract between the provisioner (the only writer) and every loader of
/// the cache. Serialized with camelCase keys (`libSha256`, `artifactUrl`, ...).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Provenance {
    /// Runtime version, e.g. `3.26.9.1`.
    pub version: String,
    /// Platform the library is built for.
    pub platform: Platform,
    /// Source label: `pypi:<index>[#project]`, `conda:<channel url>[#package]` or
    /// `artifact:<url or path>` (never with credentials).
    pub source: String,
    /// URL of the archive (`file:` for local files), without credentials.
    pub artifact_url: String,
    /// SHA-256 of the archive (lowercase hex).
    pub artifact_sha256: String,
    /// The library, relative to the install directory and `/`-separated.
    pub lib: String,
    /// SHA-256 of the library (lowercase hex).
    pub lib_sha256: String,
    /// The licence files, relative to the install directory and `/`-separated.
    pub licenses: Vec<String>,
}

/// A relative path as `/`-separated plain segments (zip-slip guard); `None` when it is unsafe.
///
/// `\` separates segments on every platform, as in zip's `enclosed_name`: a Unix host would
/// otherwise accept `x\..\..\.bashrc` as one harmless name and then use it through a path that,
/// once normalized, climbs out of its directory. Empty, `.` and `..` segments (so also roots and
/// UNC paths) and segments with `:` (drive prefixes, alternate data streams) are rejected.
pub fn safe_relative(relative: &str) -> Option<String> {
    let normalized = relative.replace('\\', "/");
    let plain = !normalized.is_empty()
        && normalized
            .split('/')
            .all(|segment| !matches!(segment, "" | "." | "..") && !segment.contains(':'));
    plain.then_some(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backslashes_separate_segments_on_every_platform() {
        for (relative, expected) in [
            ("License.txt", "License.txt"),
            (
                "cpp-licenses/LICENSE.openssl",
                "cpp-licenses/LICENSE.openssl",
            ),
            (
                "cpp-licenses\\LICENSE.openssl",
                "cpp-licenses/LICENSE.openssl",
            ),
        ] {
            assert_eq!(safe_relative(relative).as_deref(), Some(expected));
        }
        for relative in [
            "",
            "..",
            "a/../b",
            "a\\..\\..\\x",
            "x\\..\\..\\..\\..\\..\\..\\.bashrc",
            "./a",
            "a//b",
            "a/",
            "dir\\",
            "/etc/passwd",
            "\\\\server\\share\\x",
            "C:\\x",
            "C:x",
            "notes.txt:stream",
        ] {
            assert_eq!(safe_relative(relative), None, "{relative}");
        }
    }

    #[test]
    fn keys_are_camel_case() {
        let provenance = Provenance {
            version: "3.26.9.1".into(),
            platform: Platform::LinuxX86_64,
            source: "artifact:x".into(),
            artifact_url: "file:x".into(),
            artifact_sha256: "a".into(),
            lib: "lib/libblpapi3_64.so".into(),
            lib_sha256: "b".into(),
            licenses: vec!["licenses/L".into()],
        };
        let value = serde_json::to_value(&provenance).unwrap();
        for key in [
            "version",
            "platform",
            "source",
            "artifactUrl",
            "artifactSha256",
            "lib",
            "libSha256",
            "licenses",
        ] {
            assert!(value.get(key).is_some(), "{key}");
        }
        assert_eq!(
            serde_json::from_value::<Provenance>(value).unwrap(),
            provenance
        );
    }
}
