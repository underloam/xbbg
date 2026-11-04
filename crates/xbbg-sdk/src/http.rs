//! One HTTP stack for PyPI, conda and artifacts: reqwest (rustls, operating-system trust store)
//! with retry, explicit credential storage and an offline guard.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures_util::StreamExt;
use rattler_digest::digest::Digest;
use rattler_networking::authentication_storage::backends::memory::MemoryStorage;
use rattler_networking::authentication_storage::backends::netrc::NetRcStorage;
use rattler_networking::{
    Authentication, AuthenticationMiddleware, AuthenticationStorage, OfflineMiddleware,
};
use reqwest_middleware::{ClientWithMiddleware, Middleware, Next};
use reqwest_retry::RetryTransientMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::error::{Error, Result, from_middleware, from_reqwest};
use crate::options::{Credential, NetworkOptions, ProxyConfig, TlsConfig};

/// Clients built from one set of [`NetworkOptions`].
pub(crate) struct Http {
    /// Follows redirects itself; used for conda repodata, packages and every archive download.
    pub(crate) client: ClientWithMiddleware,
    /// Same settings without redirect handling: uv follows redirects itself so that its
    /// middleware (and ours) runs on every hop.
    pub(crate) uv_client: reqwest::Client,
    /// Credentials shared by both stacks; URL-embedded credentials are added at run time.
    auth: AuthenticationStorage,
    /// The authentication middleware of both stacks.
    scoped_auth: ScopedAuth,
    pub(crate) offline: bool,
    pub(crate) retries: u32,
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
}

impl Http {
    pub(crate) fn new(net: &NetworkOptions) -> Result<Self> {
        let auth = auth_storage(net)?;
        let scoped_auth = ScopedAuth::new(auth.clone());
        let raw = reqwest_client(net, true)?;
        let mut builder = reqwest_middleware::ClientBuilder::new(raw);
        if net.offline {
            builder = builder.with(OfflineMiddleware);
        } else if net.retries > 0 {
            builder = builder.with(RetryTransientMiddleware::new_with_policy(
                ExponentialBackoff::builder().build_with_max_retries(net.retries),
            ));
        }
        let client = builder.with(scoped_auth.clone()).build();
        Ok(Self {
            client,
            uv_client: reqwest_client(net, false)?,
            auth,
            scoped_auth,
            offline: net.offline,
            retries: net.retries,
            connect_timeout: net.connect_timeout,
            read_timeout: net.read_timeout,
        })
    }

    /// Middleware that authenticates requests from [`Http::auth`] (for uv).
    pub(crate) fn auth_middleware(&self) -> ScopedAuth {
        self.scoped_auth.clone()
    }

    /// Registers credentials for `host` for every later request.
    pub(crate) fn add_credential(&self, host: &str, credential: &Credential) -> Result<()> {
        self.auth
            .store(host, &authentication(credential))
            .map_err(|err| Error::Internal(format!("storing credentials for {host}: {err}")))
    }

    /// Lets credentials go to `url`'s origin even over plain `http`: the caller configured that
    /// location. A no-op for other schemes.
    pub(crate) fn allow_plain_http(&self, url: &Url) {
        if url.scheme() == "http" {
            self.scoped_auth
                .plain_http_origins()
                .insert(url.origin().ascii_serialization());
        }
    }
}

/// Sends stored credentials only where they cannot travel in clear text: over `https`, or to
/// an `http` origin (scheme, host and port) the caller configured as a source.
///
/// The credential stores are keyed by host alone, and uv runs this middleware again on every
/// redirect hop; unscoped, an HTTPS-to-HTTP redirect on the same host, or an `http://` file link
/// on an HTTPS index, would carry them unencrypted. (Redirects that reqwest follows itself drop
/// the `Authorization` header whenever the origin changes.)
#[derive(Clone)]
pub(crate) struct ScopedAuth {
    inner: AuthenticationMiddleware,
    plain_http: Arc<Mutex<HashSet<String>>>,
}

impl ScopedAuth {
    fn new(storage: AuthenticationStorage) -> Self {
        Self {
            inner: AuthenticationMiddleware::from_auth_storage(storage),
            plain_http: Arc::default(),
        }
    }

    fn plain_http_origins(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.plain_http
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether a request to `url` may carry credentials.
    fn covers(&self, url: &Url) -> bool {
        match url.scheme() {
            "https" => true,
            "http" => self
                .plain_http_origins()
                .contains(&url.origin().ascii_serialization()),
            _ => false,
        }
    }
}

#[async_trait::async_trait]
impl Middleware for ScopedAuth {
    async fn handle(
        &self,
        request: reqwest::Request,
        extensions: &mut ::http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        if self.covers(request.url()) {
            self.inner.handle(request, extensions, next).await
        } else {
            next.run(request, extensions).await
        }
    }
}

fn authentication(credential: &Credential) -> Authentication {
    match credential {
        Credential::Basic { username, password } => Authentication::BasicHTTP {
            username: username.clone(),
            password: password.clone(),
        },
        Credential::Bearer { token } => Authentication::BearerToken(token.clone()),
    }
}

/// Credential store with only explicitly configured backends (no keyring, no implicit files).
fn auth_storage(net: &NetworkOptions) -> Result<AuthenticationStorage> {
    let mut storage = AuthenticationStorage::empty();
    storage.add_backend(Arc::new(MemoryStorage::with_name("xbbg-sdk")));
    if let Some(netrc) = &net.netrc {
        let backend = NetRcStorage::from_path(netrc).map_err(|err| {
            Error::InvalidArgument(format!("cannot read netrc file {}: {err}", netrc.display()))
        })?;
        storage.add_backend(Arc::new(backend));
    }
    for (host, credential) in &net.credentials {
        if host.trim().is_empty() {
            return Err(Error::InvalidArgument(
                "credential host must not be empty".into(),
            ));
        }
        storage
            .store(host, &authentication(credential))
            .map_err(|err| Error::Internal(format!("storing credentials for {host}: {err}")))?;
    }
    Ok(storage)
}

fn reqwest_client(net: &NetworkOptions, follow_redirects: bool) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .user_agent(&net.user_agent)
        .connect_timeout(net.connect_timeout)
        .read_timeout(net.read_timeout)
        .tls_backend_rustls()
        .redirect(if follow_redirects {
            reqwest::redirect::Policy::limited(10)
        } else {
            reqwest::redirect::Policy::none()
        });
    builder = match &net.proxy {
        // reqwest reads the environment and the OS proxy settings when no proxy is added.
        ProxyConfig::System => builder,
        ProxyConfig::Direct => builder.no_proxy(),
        ProxyConfig::Explicit { url, no_proxy } => {
            let mut proxy = reqwest::Proxy::all(url.as_str()).map_err(|err| {
                Error::InvalidArgument(format!("invalid proxy URL: {}", crate::error::chain(&err)))
            })?;
            if !no_proxy.is_empty() {
                proxy = proxy.no_proxy(reqwest::NoProxy::from_string(&no_proxy.join(",")));
            }
            builder.proxy(proxy)
        }
    };
    builder = match &net.tls {
        TlsConfig::System { extra_ca_pem: None } => builder,
        TlsConfig::System {
            extra_ca_pem: Some(pem),
        } => builder.tls_certs_merge(read_pem_bundle(pem)?),
        TlsConfig::Exclusive { ca_pem } => builder.tls_certs_only(read_pem_bundle(ca_pem)?),
    };
    builder.build().map_err(|err| {
        Error::Internal(format!(
            "building the HTTP client: {}",
            crate::error::chain(&err)
        ))
    })
}

fn read_pem_bundle(path: &Path) -> Result<Vec<reqwest::Certificate>> {
    let bytes = std::fs::read(path)
        .map_err(|err| Error::io(format!("reading CA bundle {}", path.display()), err))?;
    let certificates = reqwest::Certificate::from_pem_bundle(&bytes).map_err(|err| {
        Error::InvalidArgument(format!(
            "invalid CA bundle {}: {}",
            path.display(),
            crate::error::chain(&err)
        ))
    })?;
    if certificates.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "CA bundle {} contains no PEM certificates",
            path.display()
        )));
    }
    Ok(certificates)
}

/// A downloaded or local archive with its measured SHA-256.
pub(crate) struct Fetched {
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
    /// Deletes a downloaded temporary file on drop; `None` for local files.
    pub(crate) _temp: Option<tempfile::TempPath>,
}

/// Downloads `url` (or reads a `file:` URL in place), hashing it on the way.
///
/// Request-level failures are retried by the middleware; a body interrupted mid-stream is
/// downloaded again here, up to `retries` times.
pub(crate) async fn fetch(http: &Http, url: &Url, temp: tempfile::TempPath) -> Result<Fetched> {
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|()| Error::InvalidArgument(format!("not a local file URL: {url}")))?;
        drop(temp);
        let sha256 = hash_file(path.clone()).await?;
        return Ok(Fetched {
            path,
            sha256,
            _temp: None,
        });
    }
    let mut attempt = 0u32;
    loop {
        let response = http
            .client
            .get(url.clone())
            .send()
            .await
            .map_err(|err| from_middleware(url, err))?
            .error_for_status()
            .map_err(|err| from_reqwest(url, err))?;
        match write_body(response, &temp).await {
            Ok(sha256) => {
                return Ok(Fetched {
                    path: temp.to_path_buf(),
                    sha256,
                    _temp: Some(temp),
                });
            }
            Err(BodyError::Stream(err)) if attempt < http.retries => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(250 * u64::from(attempt))).await;
                drop(err);
            }
            Err(BodyError::Stream(err)) => return Err(from_reqwest(url, err)),
            Err(BodyError::Io(err)) => {
                return Err(Error::io(format!("writing {}", temp.display()), err));
            }
        }
    }
}

enum BodyError {
    Stream(reqwest::Error),
    Io(std::io::Error),
}

async fn write_body(response: reqwest::Response, dest: &Path) -> Result<String, BodyError> {
    let file = tokio::fs::File::create(dest).await.map_err(BodyError::Io)?;
    let mut out = tokio::io::BufWriter::with_capacity(256 * 1024, file);
    let mut hasher = rattler_digest::Sha256::new();
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(BodyError::Stream)?;
        hasher.update(&chunk);
        out.write_all(&chunk).await.map_err(BodyError::Io)?;
    }
    out.flush().await.map_err(BodyError::Io)?;
    out.into_inner().sync_all().await.map_err(BodyError::Io)?;
    Ok(hex::encode(hasher.finalize()))
}

/// SHA-256 of a local file, off the async runtime.
pub(crate) async fn hash_file(path: PathBuf) -> Result<String> {
    tokio::task::spawn_blocking(move || {
        rattler_digest::compute_file_digest::<rattler_digest::Sha256>(&path)
            .map(hex::encode)
            .map_err(|err| Error::io(format!("reading {}", path.display()), err))
    })
    .await
    .map_err(|err| Error::Internal(format!("hashing task failed: {err}")))?
}
