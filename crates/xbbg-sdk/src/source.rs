//! Where runtimes come from.

/// Environment variable carrying a default provisioning source specification.
pub const SOURCE_ENV: &str = "XBBG_SDK_SOURCE";
/// Environment variable carrying an optional default provisioning version.
pub const VERSION_ENV: &str = "XBBG_SDK_VERSION";
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

    /// Parses a source specification shared across every xbbg host.
    ///
    /// Accepted forms:
    /// * `pypi:<index url>[#<project>]`
    /// * `conda:<channel>[#<package>]`
    /// * `artifact:<url or path>[#sha256=<hex>]`
    /// * a bare URL or path, classified by its shape: names ending in `.whl`, `.conda` or
    ///   `.tar.bz2` are artifacts, locations containing `/simple` are PyPI indexes, and anything
    ///   else is a conda channel.
    ///
    /// A one-letter prefix (`C:\...`) is treated as a Windows drive path, never a source kind.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let text = required(spec, "BLPAPI source spec")?;
        let colon = text.find(':');
        let (prefix, rest) = match colon {
            Some(index) if index > 1 => {
                (text[..index].to_ascii_lowercase(), text[index + 1..].trim())
            }
            _ => (String::new(), ""),
        };
        match prefix.as_str() {
            "pypi" => {
                if let Some((url, project)) = rest.split_once('#') {
                    Ok(Self::PypiIndex {
                        url: required(url, "index URL")?.to_owned(),
                        project: required(project, "project")?.to_owned(),
                    })
                } else {
                    Ok(Self::pypi(required(rest, "index URL")?))
                }
            }
            "conda" => {
                if let Some((channel, package)) = rest.split_once('#') {
                    Ok(Self::CondaChannel {
                        channel: required(channel, "channel")?.to_owned(),
                        package: required(package, "package")?.to_owned(),
                    })
                } else {
                    Ok(Self::conda(required(rest, "channel")?))
                }
            }
            "artifact" => parse_artifact(rest),
            _ => {
                let shape = text.to_ascii_lowercase().replace('\\', "/");
                let path = shape
                    .find(['#', '?'])
                    .map_or(shape.as_str(), |end| &shape[..end]);
                if path.ends_with(".whl") || path.ends_with(".conda") || path.ends_with(".tar.bz2")
                {
                    parse_artifact(text)
                } else if path.contains("/simple") {
                    Ok(Self::pypi(text))
                } else {
                    Ok(Self::conda(text))
                }
            }
        }
    }

    /// Renders this source in the syntax accepted by [`Source::parse`].
    pub fn spec(&self) -> String {
        match self {
            Self::PypiIndex { url, project } => {
                if project == DEFAULT_PYPI_PROJECT {
                    format!("pypi:{url}")
                } else {
                    format!("pypi:{url}#{project}")
                }
            }
            Self::CondaChannel { channel, package } => {
                if package == DEFAULT_CONDA_PACKAGE {
                    format!("conda:{channel}")
                } else {
                    format!("conda:{channel}#{package}")
                }
            }
            Self::Artifact { location, sha256 } => match sha256 {
                Some(hash) => format!("artifact:{location}#sha256={hash}"),
                None => format!("artifact:{location}"),
            },
        }
    }
}

fn required<'a>(value: &'a str, name: &str) -> Result<&'a str, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(format!("{name} must not be blank"))
    } else {
        Ok(trimmed)
    }
}

fn parse_artifact(raw: &str) -> Result<Source, String> {
    const SHA256_FRAGMENT: &str = "#sha256=";
    let location = required(raw, "artifact location")?;
    let lower = location.to_ascii_lowercase();
    if let Some(fragment) = lower.rfind(SHA256_FRAGMENT) {
        let hash = &location[fragment + SHA256_FRAGMENT.len()..];
        let location = required(&location[..fragment], "artifact location")?;
        return Ok(Source::Artifact {
            location: location.to_owned(),
            sha256: Some(validate_sha256_hex(hash)?),
        });
    }
    Ok(Source::artifact(location))
}

/// Validates a 64-hex-digit SHA-256 string and returns it lowercased.
pub fn validate_sha256_hex(value: &str) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.len() != 64 || !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("sha256 must be 64 hex digits".into());
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// Masks embedded URL credentials (`scheme://user[:pass]@host`) and conda `/t/<token>/` segments
/// in arbitrary user input so error messages never echo secrets.
pub fn redact_user_info(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(scheme_end) = rest.find("://") {
        let after_scheme = scheme_end + 3;
        out.push_str(&rest[..after_scheme]);
        rest = &rest[after_scheme..];
        let authority_end = rest
            .find(['/', '?', '#', ' ', '\t', '\n', '\r'])
            .unwrap_or(rest.len());
        let authority = &rest[..authority_end];
        if let Some(at) = authority.rfind('@') {
            out.push_str("***@");
            out.push_str(&authority[at + 1..]);
        } else {
            out.push_str(authority);
        }
        rest = &rest[authority_end..];
    }
    out.push_str(rest);
    // Also mask conda channel tokens `/t/<token>/`.
    let mut masked = String::with_capacity(out.len());
    let mut tail = out.as_str();
    while let Some(pos) = tail.find("/t/") {
        masked.push_str(&tail[..pos + 3]);
        let after = &tail[pos + 3..];
        if let Some(slash) = after.find('/') {
            masked.push_str("***");
            tail = &after[slash..];
        } else {
            masked.push_str("***");
            tail = "";
            break;
        }
    }
    masked.push_str(tail);
    masked
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_roundtrip_all_source_forms() {
        let pypi = Source::parse(BLOOMBERG_PYPI_INDEX).unwrap();
        assert_eq!(pypi, Source::bloomberg_pypi());
        assert_eq!(Source::parse(&pypi.spec()).unwrap(), pypi);

        let custom_pypi = Source::parse("pypi:https://idx.example/simple/#custom-blpapi").unwrap();
        assert_eq!(
            custom_pypi,
            Source::PypiIndex {
                url: "https://idx.example/simple/".into(),
                project: "custom-blpapi".into(),
            }
        );
        assert_eq!(Source::parse(&custom_pypi.spec()).unwrap(), custom_pypi);

        let conda = Source::parse("conda-forge").unwrap();
        assert_eq!(conda, Source::conda("conda-forge"));
        assert_eq!(Source::parse(&conda.spec()).unwrap(), conda);

        let custom_conda = Source::parse("conda:https://mirror.example/cf#blp-rt").unwrap();
        assert_eq!(
            custom_conda,
            Source::CondaChannel {
                channel: "https://mirror.example/cf".into(),
                package: "blp-rt".into(),
            }
        );

        let hash = "a".repeat(64);
        let artifact = Source::parse(&format!(
            r"C:\tmp\blpapi.whl#SHA256={}",
            hash.to_uppercase()
        ))
        .unwrap();
        assert_eq!(
            artifact,
            Source::Artifact {
                location: r"C:\tmp\blpapi.whl".into(),
                sha256: Some(hash),
            }
        );
        assert_eq!(Source::parse(&artifact.spec()).unwrap(), artifact);
    }

    #[test]
    fn parse_rejects_blank_and_malformed_hashes() {
        assert!(Source::parse("   ").is_err());
        assert!(Source::parse("pypi:").is_err());
        assert!(Source::parse("conda:conda-forge#").is_err());
        assert!(Source::parse("artifact:foo.whl#sha256=not-hex").is_err());
    }

    #[test]
    fn redact_user_info_hides_credentials_and_conda_tokens() {
        assert_eq!(
            redact_user_info("pypi:https://alice:secret@idx.example/simple/"),
            "pypi:https://***@idx.example/simple/"
        );
        assert_eq!(
            redact_user_info("https://conda.example/t/tok-12345/conda-forge"),
            "https://conda.example/t/***/conda-forge"
        );
        assert_eq!(
            redact_user_info("https://idx.example/simple/"),
            "https://idx.example/simple/"
        );
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
