//! Provisioning errors with stable codes for host bindings.

use std::error::Error as StdError;
use std::io;

use rattler_redaction::{DEFAULT_REDACTION_STR, redact_url_for_display};
use reqwest_retry::RetryError;
use url::Url;

/// A provisioning failure.
///
/// Bindings map on [`Error::code`], which is stable across releases; messages are for humans and
/// never contain credentials.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Caller input is invalid: a malformed URL, version, hash or option combination, or a
    /// version older than the supported minimum.
    #[error("{0}")]
    InvalidArgument(String),
    /// The requested platform has no Bloomberg runtime build.
    #[error("unsupported platform `{0}` (supported: windows-x86_64, linux-x86_64, macos-aarch64)")]
    UnsupportedPlatform(String),
    /// The source has no runtime matching the request.
    #[error("{0}")]
    NotFound(String),
    /// Network access is disabled and the cache cannot satisfy the request.
    #[error("{0}")]
    Offline(String),
    /// An HTTP request or connection failed.
    #[error("{0}")]
    Network(String),
    /// A hash or size does not match what the index, repodata, `RECORD` or `paths.json`
    /// promised.
    #[error("{0}")]
    Integrity(String),
    /// The archive is malformed or does not contain the Bloomberg library, or a remote source
    /// points a download somewhere it may not.
    #[error("{0}")]
    InvalidArtifact(String),
    /// A local file-system operation failed.
    #[error("{context}: {source}")]
    Io {
        /// What was being done, including the path.
        context: String,
        /// The underlying failure.
        #[source]
        source: io::Error,
    },
    /// The operation was cancelled.
    #[error("provisioning was cancelled")]
    Cancelled,
    /// An unexpected failure inside the provisioner or one of its dependencies.
    #[error("{0}")]
    Internal(String),
}

/// Result alias for this crate.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Stable machine-readable code: `INVALID_ARGUMENT`, `UNSUPPORTED_PLATFORM`, `NOT_FOUND`,
    /// `OFFLINE`, `NETWORK`, `INTEGRITY`, `INVALID_ARTIFACT`, `IO`, `CANCELLED` or `INTERNAL`.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::UnsupportedPlatform(_) => "UNSUPPORTED_PLATFORM",
            Self::NotFound(_) => "NOT_FOUND",
            Self::Offline(_) => "OFFLINE",
            Self::Network(_) => "NETWORK",
            Self::Integrity(_) => "INTEGRITY",
            Self::InvalidArtifact(_) => "INVALID_ARTIFACT",
            Self::Io { .. } => "IO",
            Self::Cancelled => "CANCELLED",
            Self::Internal(_) => "INTERNAL",
        }
    }

    pub(crate) fn io(context: impl Into<String>, source: io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }
}

/// `error: cause: cause…`, skipping causes whose text is already part of the message.
pub(crate) fn chain(error: &(dyn StdError + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let message = cause.to_string();
        if !message.is_empty() && !text.contains(&message) {
            text.push_str(": ");
            text.push_str(&message);
        }
        source = cause.source();
    }
    text
}

/// `url` without user name, password, query or fragment, and with a conda `/t/<token>/` channel
/// token masked (as rattler shows channel names), for messages and provenance.
pub(crate) fn redact(url: &Url) -> String {
    let mut shown = url.clone();
    // Both fail only for URLs that cannot have credentials, which then have none to hide.
    let _ = shown.set_username("");
    let _ = shown.set_password(None);
    shown.set_query(None);
    shown.set_fragment(None);
    redact_url_for_display(&shown, DEFAULT_REDACTION_STR)
        .unwrap_or(shown)
        .to_string()
}

/// A failed HTTP exchange with `url`.
pub(crate) fn from_reqwest(url: &Url, error: reqwest::Error) -> Error {
    let status = error.status();
    let error = error.without_url();
    let text = chain(&error);
    match status {
        Some(status) if status == reqwest::StatusCode::NOT_FOUND => {
            Error::NotFound(format!("{} not found (HTTP 404)", redact(url)))
        }
        _ => Error::Network(format!("GET {} failed: {text}", redact(url))),
    }
}

/// A failed request through the middleware stack (retry, auth, offline guard).
pub(crate) fn from_middleware(url: &Url, error: reqwest_middleware::Error) -> Error {
    match error {
        reqwest_middleware::Error::Reqwest(error) => from_reqwest(url, error),
        reqwest_middleware::Error::Middleware(error) => match error.downcast::<RetryError>() {
            // The retry layer wraps the last attempt's error, whose text quotes the full request
            // URL: classify that error itself.
            Ok(RetryError::Error(inner)) => from_middleware(url, inner),
            Ok(RetryError::WithRetries { retries, err }) => match from_middleware(url, err) {
                Error::Network(message) => {
                    Error::Network(format!("{message} (after {retries} retries)"))
                }
                other => other,
            },
            Err(error) => {
                if error
                    .downcast_ref::<rattler_networking::OfflineError>()
                    .is_some()
                {
                    Error::Offline(format!(
                        "offline mode: {} is not available without network access",
                        redact(url)
                    ))
                } else {
                    Error::Network(format!(
                        "GET {} failed: {}",
                        redact(url),
                        chain(error.as_ref())
                    ))
                }
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_strips_credentials_query_and_fragment() {
        let url = Url::parse("https://user:secret@repo.example/simple/blpapi/?token=x#sha256=ab")
            .unwrap();
        assert_eq!(redact(&url), "https://repo.example/simple/blpapi/");
    }

    #[test]
    fn redact_masks_conda_channel_tokens() {
        let url = Url::parse(
            "https://conda.anaconda.org/t/tk-123-secret/private/win-64/libblpapi-3.26.9.1-0.conda",
        )
        .unwrap();
        assert_eq!(
            redact(&url),
            "https://conda.anaconda.org/t/********/private/win-64/libblpapi-3.26.9.1-0.conda"
        );
        let channel = Url::parse("https://conda.anaconda.org/t/tk-123-secret/private/").unwrap();
        assert_eq!(
            redact(&channel),
            "https://conda.anaconda.org/t/********/private/"
        );
    }

    #[test]
    fn chain_skips_repeated_causes() {
        let inner = io::Error::other("disk full");
        let outer = Error::io("writing lib/blpapi3_64.dll", inner);
        assert_eq!(chain(&outer), "writing lib/blpapi3_64.dll: disk full");
        assert_eq!(outer.code(), "IO");
    }

    #[tokio::test]
    async fn retried_failures_are_classified_without_the_request_url() {
        // Nothing listens on a port that was just released.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let url = Url::parse(&format!(
            "http://127.0.0.1:{port}/files/x.whl?X-Amz-Signature=topsecret"
        ))
        .unwrap();
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let refused = client.get(url.clone()).send().await.unwrap_err();
        assert!(
            refused.to_string().contains("topsecret"),
            "reqwest quotes the URL: {refused}"
        );

        let error = from_middleware(
            &url,
            retried(RetryError::WithRetries {
                retries: 2,
                err: reqwest_middleware::Error::Reqwest(refused),
            }),
        );
        assert_eq!(error.code(), "NETWORK");
        let message = error.to_string();
        assert!(!message.contains("topsecret"), "{message}");
        assert!(
            message.starts_with(&format!("GET http://127.0.0.1:{port}/files/x.whl failed")),
            "{message}"
        );
        assert!(message.ends_with("(after 2 retries)"), "{message}");

        let status = reqwest::Response::from(
            ::http::Response::builder()
                .status(404)
                .body(Vec::<u8>::new())
                .unwrap(),
        )
        .error_for_status()
        .unwrap_err();
        let error = from_middleware(
            &url,
            retried(RetryError::Error(reqwest_middleware::Error::Reqwest(
                status,
            ))),
        );
        assert_eq!(error.code(), "NOT_FOUND", "{error}");
        assert_eq!(
            error.to_string(),
            format!("http://127.0.0.1:{port}/files/x.whl not found (HTTP 404)")
        );
    }

    /// `error` as the retry middleware reports it.
    fn retried(error: RetryError) -> reqwest_middleware::Error {
        reqwest_middleware::Error::Middleware(error.into())
    }
}
