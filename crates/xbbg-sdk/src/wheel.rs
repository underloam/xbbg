//! Wheel selection (uv filename, tag and version semantics) and `RECORD`-verified extraction.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rattler_digest::digest::Digest;
use uv_distribution_filename::WheelFilename;
use uv_normalize::PackageName;
use uv_pep440::Version;
use uv_platform_tags::{AbiTag, Arch, BinaryFormat, LanguageTag, PlatformTag};

use crate::error::{Error, Result};
use crate::platform::Platform;
use crate::pypi::IndexFile;

/// Platform rank of a wheel (lower is better); `None` if it does not run on `platform`.
pub(crate) fn platform_rank(wheel: &WheelFilename, platform: Platform) -> Option<u32> {
    wheel
        .platform_tags()
        .iter()
        .filter_map(|tag| match (platform, tag) {
            (Platform::WindowsX86_64, PlatformTag::WinAmd64) => Some(0),
            (
                Platform::LinuxX86_64,
                PlatformTag::Manylinux {
                    arch: Arch::X86_64, ..
                }
                | PlatformTag::Manylinux2014 { arch: Arch::X86_64 }
                | PlatformTag::Manylinux2010 { arch: Arch::X86_64 }
                | PlatformTag::Manylinux1 { arch: Arch::X86_64 },
            ) => Some(0),
            // Bloomberg tagged 3.24.10 and 3.24.11 plain `linux_x86_64`.
            (Platform::LinuxX86_64, PlatformTag::Linux { arch: Arch::X86_64 }) => Some(1),
            // Older minimum macOS first: it runs on more machines.
            (
                Platform::MacosAarch64,
                PlatformTag::Macos {
                    major,
                    minor,
                    binary_format: BinaryFormat::Arm64,
                },
            ) => Some(u32::from(*major) * 100 + u32::from(*minor)),
            (
                Platform::MacosAarch64,
                PlatformTag::Macos {
                    binary_format: BinaryFormat::Universal2,
                    ..
                },
            ) => Some(100_000),
            _ => None,
        })
        .min()
}

/// Python rank (lower is better): `py3-none` first, then CPython wheels newest first.
fn python_rank(wheel: &WheelFilename) -> Option<u32> {
    let pure = wheel
        .python_tags()
        .iter()
        .any(|tag| matches!(tag, LanguageTag::Python { major: 3, .. }))
        && wheel
            .abi_tags()
            .iter()
            .any(|abi| matches!(abi, AbiTag::None));
    if pure {
        return Some(0);
    }
    wheel
        .python_tags()
        .iter()
        .filter_map(|tag| match tag {
            LanguageTag::CPython {
                python_version: (3, minor),
            } => Some(1000 - u32::from(*minor)),
            _ => None,
        })
        .min()
}

/// Parses a wheel file name, checking that it belongs to `project`.
pub(crate) fn parse_wheel_name(filename: &str, project: &PackageName) -> Option<WheelFilename> {
    WheelFilename::from_str(filename)
        .ok()
        .filter(|wheel| wheel.name == *project)
}

/// Rejects wheels older than `minimum` (see [`crate::MIN_PYPI_VERSION`]).
pub(crate) fn check_minimum(
    project: &str,
    version: &Version,
    minimum: Option<&Version>,
) -> Result<()> {
    match minimum {
        Some(minimum) if version < minimum => Err(Error::InvalidArgument(format!(
            "{project} {version} is older than {minimum}, the oldest wheel release whose native \
             library keeps its standard name; use {minimum} or newer"
        ))),
        _ => Ok(()),
    }
}

/// The version and best wheel for `platform`: an exact pin, or the newest stable version that
/// has a compatible wheel. Yanked files and versions below `minimum` are skipped.
pub(crate) fn choose_wheel<'a>(
    files: &'a [IndexFile],
    project: &PackageName,
    wanted: Option<&Version>,
    minimum: Option<&Version>,
    platform: Platform,
) -> Option<(Version, &'a IndexFile)> {
    let mut best: HashMap<Version, ((u32, u32), &IndexFile)> = HashMap::new();
    for file in files.iter().filter(|file| !file.yanked) {
        let Some(wheel) = parse_wheel_name(&file.filename, project) else {
            continue;
        };
        if minimum.is_some_and(|minimum| wheel.version < *minimum) {
            continue;
        }
        if wanted.is_some_and(|wanted| wheel.version != *wanted) {
            continue;
        }
        let (Some(python), Some(rank)) = (python_rank(&wheel), platform_rank(&wheel, platform))
        else {
            continue;
        };
        let key = (python, rank);
        best.entry(wheel.version.clone())
            .and_modify(|current| {
                if key < current.0 {
                    *current = (key, file);
                }
            })
            .or_insert((key, file));
    }
    best.into_iter()
        .filter(|(version, _)| wanted.is_some() || version.is_stable())
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(version, (_, file))| (version, file))
}

/// What [`extract`] put into the staging directory.
#[derive(Debug)]
pub(crate) struct Extracted {
    /// Relative, `/`-separated, e.g. `lib/blpapi3_64.dll`.
    pub(crate) lib: String,
    pub(crate) lib_sha256: String,
    pub(crate) licenses: Vec<String>,
}

/// Extracts the native library and the licence files from `wheel` into
/// `staging/{lib,licenses}`, verifying each against the wheel's `RECORD`.
pub(crate) fn extract(wheel: &Path, platform: Platform, staging: &Path) -> Result<Extracted> {
    let file = fs::File::open(wheel)
        .map_err(|err| Error::io(format!("opening {}", wheel.display()), err))?;
    let mut archive = zip::ZipArchive::new(file).map_err(|err| {
        Error::InvalidArtifact(format!("{} is not a valid wheel: {err}", wheel.display()))
    })?;
    let names: Vec<String> = archive
        .file_names()
        .map(|name| name.map(|name| name.into_owned()))
        .collect::<Result<_, _>>()
        .map_err(|err| Error::InvalidArtifact(format!("reading {}: {err}", wheel.display())))?;

    let records: Vec<&String> = names
        .iter()
        .filter(|name| name.matches('/').count() == 1 && name.ends_with(".dist-info/RECORD"))
        .collect();
    let [record_name] = records.as_slice() else {
        return Err(Error::InvalidArtifact(format!(
            "{} must contain exactly one *.dist-info/RECORD, found {}",
            wheel.display(),
            records.len()
        )));
    };
    let dist_info = record_name.trim_end_matches("RECORD").to_string();
    let record = parse_record(&read_member(&mut archive, record_name)?)?;

    // `blpapi/<lib>` in Bloomberg's wheels; any other directory (delocate's `.dylibs`) after it.
    let lib_member = platform
        .library_names()
        .iter()
        .find_map(|lib| {
            let preferred = format!("blpapi/{lib}");
            if names.contains(&preferred) {
                return Some(preferred);
            }
            names
                .iter()
                .find(|name| name.rsplit('/').next() == Some(*lib))
                .cloned()
        })
        .ok_or_else(|| {
            Error::InvalidArtifact(format!(
                "{} contains no {} library",
                wheel.display(),
                platform.library_names().join(" or ")
            ))
        })?;

    // PEP 639 `<dist-info>/licenses/**`, else legacy LICENSE*/COPYING*/NOTICE* in the dist-info.
    let licenses_prefix = format!("{dist_info}licenses/");
    let mut license_members: Vec<(String, String)> = names
        .iter()
        .filter(|name| name.starts_with(&licenses_prefix) && !name.ends_with('/'))
        .map(|name| (name.clone(), name[licenses_prefix.len()..].to_string()))
        .collect();
    if license_members.is_empty() {
        license_members = names
            .iter()
            .filter_map(|name| {
                let rest = name.strip_prefix(&dist_info)?;
                let upper = rest.to_ascii_uppercase();
                (!rest.contains('/')
                    && ["LICENSE", "LICENCE", "COPYING", "NOTICE"]
                        .iter()
                        .any(|prefix| upper.starts_with(prefix)))
                .then(|| (name.clone(), rest.to_string()))
            })
            .collect();
    }

    let lib_name = lib_member.rsplit('/').next().unwrap_or(&lib_member);
    let lib = format!("lib/{lib_name}");
    let lib_sha256 = extract_verified(&mut archive, &lib_member, &record, &staging.join(&lib))?;
    let mut licenses = Vec::with_capacity(license_members.len());
    for (member, relative) in &license_members {
        let relative = format!("licenses/{}", safe_relative(relative)?);
        extract_verified(&mut archive, member, &record, &staging.join(&relative))?;
        licenses.push(relative);
    }
    licenses.sort();
    Ok(Extracted {
        lib,
        lib_sha256,
        licenses,
    })
}

/// `RECORD` rows: `path,sha256=<urlsafe base64, no padding>,size`.
fn parse_record(bytes: &[u8]) -> Result<HashMap<String, (String, Option<u64>)>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(bytes);
    let mut rows = HashMap::new();
    for row in reader.records() {
        let row = row.map_err(|err| Error::InvalidArtifact(format!("malformed RECORD: {err}")))?;
        let (Some(path), Some(hash)) = (row.get(0), row.get(1)) else {
            continue;
        };
        if hash.is_empty() {
            // RECORD itself and signature files carry no hash.
            continue;
        }
        let size = row.get(2).and_then(|size| size.parse().ok());
        rows.insert(path.to_string(), (hash.to_string(), size));
    }
    Ok(rows)
}

fn read_member(archive: &mut zip::ZipArchive<fs::File>, name: &str) -> Result<Vec<u8>> {
    let mut member = archive
        .by_name(name)
        .map_err(|err| Error::InvalidArtifact(format!("reading {name}: {err}")))?;
    let mut bytes = Vec::new();
    member
        .read_to_end(&mut bytes)
        .map_err(|err| Error::InvalidArtifact(format!("reading {name}: {err}")))?;
    Ok(bytes)
}

/// Streams one member to `dest` through SHA-256; on a `RECORD` mismatch the file is deleted.
fn extract_verified(
    archive: &mut zip::ZipArchive<fs::File>,
    member: &str,
    record: &HashMap<String, (String, Option<u64>)>,
    dest: &Path,
) -> Result<String> {
    let (hash, size) = record
        .get(member)
        .ok_or_else(|| Error::Integrity(format!("{member} is not listed in the wheel RECORD")))?;
    let expected = hash.strip_prefix("sha256=").ok_or_else(|| {
        Error::Integrity(format!("RECORD hash of {member} is not sha256: {hash}"))
    })?;
    let expected = URL_SAFE_NO_PAD
        .decode(expected.trim_end_matches('='))
        .map_err(|err| Error::Integrity(format!("RECORD hash of {member} is not base64: {err}")))?;

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .map_err(|err| Error::io(format!("creating {}", parent.display()), err))?;
    }
    let mut zipped = archive
        .by_name(member)
        .map_err(|err| Error::InvalidArtifact(format!("reading {member}: {err}")))?;
    let mut out = fs::File::create(dest)
        .map_err(|err| Error::io(format!("creating {}", dest.display()), err))?;
    let mut hasher = rattler_digest::Sha256::new();
    let mut buffer = vec![0u8; 256 * 1024];
    let mut written = 0u64;
    loop {
        let read = zipped
            .read(&mut buffer)
            .map_err(|err| Error::InvalidArtifact(format!("decompressing {member}: {err}")))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        out.write_all(&buffer[..read])
            .map_err(|err| Error::io(format!("writing {}", dest.display()), err))?;
        written += read as u64;
    }
    out.sync_all()
        .map_err(|err| Error::io(format!("writing {}", dest.display()), err))?;
    drop(out);
    let actual = hasher.finalize();
    if actual.as_slice() != expected.as_slice() || size.is_some_and(|size| size != written) {
        let _ = fs::remove_file(dest);
        return Err(Error::Integrity(format!(
            "{member} does not match the wheel RECORD: expected sha256 {} ({} bytes), got {} ({written} bytes)",
            hex::encode(&expected),
            size.map_or_else(|| "?".to_string(), |size| size.to_string()),
            hex::encode(actual)
        )));
    }
    Ok(hex::encode(actual))
}

/// A member-derived relative path as `/`-separated plain segments (zip-slip guard).
///
/// `\` separates segments on every platform, as in zip's `enclosed_name`: a Unix host would
/// otherwise accept `x\..\..\.bashrc` as one harmless name and then write it through a path that,
/// once normalized, climbs out of the staging directory. Empty, `.` and `..` segments (so also
/// roots and UNC paths) and segments with `:` (drive prefixes, alternate data streams) are
/// rejected.
pub(crate) fn safe_relative(relative: &str) -> Result<String> {
    let normalized = relative.replace('\\', "/");
    let plain = !normalized.is_empty()
        && normalized
            .split('/')
            .all(|segment| !matches!(segment, "" | "." | "..") && !segment.contains(':'));
    if plain {
        Ok(normalized)
    } else {
        Err(Error::InvalidArtifact(format!(
            "unsafe archive path {relative}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;

    use super::*;

    const DIST_INFO: &str = "blpapi-3.26.9.1.dist-info";

    /// A Linux wheel holding the library and `members`, each listed in its RECORD.
    fn wheel(dir: &Path, members: &[(&str, &[u8])]) -> PathBuf {
        let mut all: Vec<(String, Vec<u8>)> =
            vec![("blpapi/libblpapi3_64.so".to_string(), b"library".to_vec())];
        all.extend(
            members
                .iter()
                .map(|(name, data)| (name.to_string(), data.to_vec())),
        );
        let mut record = String::new();
        for (name, data) in &all {
            let digest = rattler_digest::compute_bytes_digest::<rattler_digest::Sha256>(data);
            record.push_str(&format!(
                "{name},sha256={},{}\n",
                URL_SAFE_NO_PAD.encode(digest),
                data.len()
            ));
        }
        let record_name = format!("{DIST_INFO}/RECORD");
        record.push_str(&format!("{record_name},,\n"));
        all.push((record_name, record.into_bytes()));
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Stored);
        for (name, data) in &all {
            writer.start_file(name.as_str(), options).unwrap();
            writer.write_all(data).unwrap();
        }
        let path = dir.join("blpapi-3.26.9.1-py3-none-manylinux2014_x86_64.whl");
        fs::write(&path, writer.finish().unwrap().into_inner()).unwrap();
        path
    }

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
            assert_eq!(safe_relative(relative).unwrap(), expected);
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
            let error = safe_relative(relative).expect_err(relative);
            assert_eq!(error.code(), "INVALID_ARTIFACT", "{relative}");
        }
    }

    #[test]
    fn licence_members_cannot_escape_the_staging_directory() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("tmp").join("staging-x");
        for member in [
            format!("{DIST_INFO}/licenses/x\\..\\..\\..\\..\\escaped"),
            // No `licenses/` directory: legacy licence files directly in the dist-info.
            format!("{DIST_INFO}/LICENSE\\..\\..\\..\\..\\escaped"),
        ] {
            let path = wheel(dir.path(), &[(member.as_str(), b"payload")]);
            let names: Vec<String> = zip::ZipArchive::new(fs::File::open(&path).unwrap())
                .unwrap()
                .file_names()
                .map(|name| name.unwrap().into_owned())
                .collect();
            assert!(names.contains(&member), "the archive keeps the raw name");

            let error = extract(&path, Platform::LinuxX86_64, &staging).expect_err(&member);
            assert_eq!(error.code(), "INVALID_ARTIFACT", "{member}: {error}");
            assert!(!dir.path().join("escaped").exists(), "{member} escaped");
            fs::remove_dir_all(&staging).unwrap();
        }
    }

    #[test]
    fn backslash_licence_names_are_extracted_where_they_are_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging");
        let member = format!("{DIST_INFO}/licenses/cpp\\LICENSE.openssl");
        let path = wheel(dir.path(), &[(member.as_str(), b"OpenSSL licence")]);
        let extracted = extract(&path, Platform::LinuxX86_64, &staging).unwrap();
        assert_eq!(extracted.licenses, ["licenses/cpp/LICENSE.openssl"]);
        assert_eq!(
            fs::read(staging.join("licenses").join("cpp").join("LICENSE.openssl")).unwrap(),
            b"OpenSSL licence"
        );
    }
}
