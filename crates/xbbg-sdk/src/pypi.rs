//! PyPI simple indexes through uv's `RegistryClient`.
//!
//! uv's index futures are not `Send` (they box local futures), so every uv call runs on one
//! dedicated thread with a current-thread runtime; [`UvIndex::list`] is a `Send` handle to it.
//! The thread ends when the [`UvIndex`] is dropped.

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use reqwest_middleware::{Middleware, Next};
use tokio::sync::{mpsc, oneshot};
use url::Url;
use uv_cache::Cache;
use uv_client::{
    AuthIntegration, BaseClientBuilder, Connectivity, ErrorKind, ExtraMiddleware, MetadataFormat,
    OwnedArchive, RegistryClient, RegistryClientBuilder,
};
use uv_distribution_types::{Index, IndexCapabilities, IndexLocations, IndexUrl};
use uv_normalize::PackageName;

use crate::error::{Error, Result, chain, redact};
use crate::http::ScopedAuth;

/// One file listed by an index (wheels and source distributions alike).
#[derive(Clone, Debug)]
pub(crate) struct IndexFile {
    pub(crate) filename: String,
    /// Absolute URL without fragment.
    pub(crate) url: Url,
    /// Lowercase hex SHA-256 advertised by the index.
    pub(crate) sha256: Option<String>,
    pub(crate) yanked: bool,
}

/// Settings for the uv clients, captured once per provisioner.
pub(crate) struct UvSettings {
    pub(crate) cache_dir: PathBuf,
    pub(crate) client: reqwest::Client,
    pub(crate) auth: ScopedAuth,
    pub(crate) offline: bool,
    pub(crate) retries: u32,
    pub(crate) connect_timeout: Duration,
    pub(crate) read_timeout: Duration,
}

struct Job {
    index: Url,
    project: String,
    reply: oneshot::Sender<Result<Vec<IndexFile>>>,
}

/// Handle to the uv thread.
pub(crate) struct UvIndex {
    jobs: mpsc::UnboundedSender<Job>,
}

impl UvIndex {
    pub(crate) fn start(settings: UvSettings) -> Result<Self> {
        let (jobs, receiver) = mpsc::unbounded_channel();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Error::Internal(format!("starting the PyPI index runtime: {err}")))?;
        std::thread::Builder::new()
            .name("xbbg-sdk-uv".into())
            .spawn(move || run(runtime, settings, receiver))
            .map_err(|err| Error::Internal(format!("starting the PyPI index thread: {err}")))?;
        Ok(Self { jobs })
    }

    /// Every file of `project` on the index rooted at `index`.
    pub(crate) async fn list(&self, index: &Url, project: &str) -> Result<Vec<IndexFile>> {
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Job {
                index: index.clone(),
                project: project.to_string(),
                reply,
            })
            .map_err(|_| Error::Internal("the PyPI index thread has stopped".into()))?;
        answer
            .await
            .map_err(|_| Error::Internal("the PyPI index thread dropped a request".into()))?
    }
}

fn run(
    runtime: tokio::runtime::Runtime,
    settings: UvSettings,
    mut jobs: mpsc::UnboundedReceiver<Job>,
) {
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        // Holds a shared lock on `<cache>/.lock` for as long as the thread lives.
        let cache = match Cache::from_path(&settings.cache_dir).init().await {
            Ok(cache) => cache,
            Err(err) => {
                let message = format!(
                    "initializing the PyPI index cache {}: {}",
                    settings.cache_dir.display(),
                    chain(&err)
                );
                while let Some(job) = jobs.recv().await {
                    let _ = job.reply.send(Err(Error::Internal(message.clone())));
                }
                return;
            }
        };
        let refused = RefusedPages::default();
        let mut clients: HashMap<Url, RegistryClient> = HashMap::new();
        while let Some(Job {
            index,
            project,
            mut reply,
        }) = jobs.recv().await
        {
            let client = match clients.get(&index) {
                Some(client) => client.clone(),
                None => match registry_client(&settings, &cache, &index, &refused) {
                    Ok(client) => {
                        clients.insert(index.clone(), client.clone());
                        client
                    }
                    Err(err) => {
                        let _ = reply.send(Err(err));
                        continue;
                    }
                },
            };
            let refused = refused.clone();
            tokio::task::spawn_local(async move {
                let listed = tokio::select! {
                    // The caller went away (cancelled): stop fetching.
                    () = reply.closed() => None,
                    result = list_files(&client, &index, &project, &refused) => Some(result),
                };
                if let Some(result) = listed {
                    let _ = reply.send(result);
                }
            });
        }
    });
}

/// Statuses of refused (401/403) requests. uv reports a refused project page as "not found"
/// (it would move on to the next index); with one index the refusal is the useful answer.
#[derive(Clone, Default)]
struct RefusedPages(Arc<Mutex<HashMap<Url, u16>>>);

impl RefusedPages {
    /// A refusal recorded for a page under `index`.
    fn under(&self, index: &Url) -> Option<u16> {
        let pages = self.0.lock().ok()?;
        pages
            .iter()
            .find(|(url, _)| url.as_str().starts_with(index.as_str()))
            .map(|(_, status)| *status)
    }
}

#[async_trait::async_trait]
impl Middleware for RefusedPages {
    async fn handle(
        &self,
        request: reqwest::Request,
        extensions: &mut http::Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<reqwest::Response> {
        let url = request.url().clone();
        let response = next.run(request, extensions).await?;
        let status = response.status().as_u16();
        if matches!(status, 401 | 403)
            && let Ok(mut pages) = self.0.lock()
        {
            pages.insert(url, status);
        }
        Ok(response)
    }
}

fn registry_client(
    settings: &UvSettings,
    cache: &Cache,
    index: &Url,
    refused: &RefusedPages,
) -> Result<RegistryClient> {
    let index_url = IndexUrl::from_str(index.as_str()).map_err(|err| {
        Error::InvalidArgument(format!("invalid PyPI index URL {}: {err}", redact(index)))
    })?;
    let locations = IndexLocations::new(vec![Index::from_index_url(index_url)], Vec::new(), false);
    let base = BaseClientBuilder::default()
        // Our client: our user agent, proxy and TLS roots (uv's default is bundled roots only).
        .custom_client(settings.client.clone())
        .connectivity(if settings.offline {
            Connectivity::Offline
        } else {
            Connectivity::Online
        })
        .retries(settings.retries)
        .read_timeout(settings.read_timeout)
        .connect_timeout(settings.connect_timeout)
        // Skip uv-auth (implicit netrc, uv credentials.toml, keyring, cloud signers) and use the
        // explicit credential store instead.
        .auth_integration(AuthIntegration::NoAuthMiddleware)
        .extra_middleware(ExtraMiddleware(vec![
            Arc::new(refused.clone()),
            Arc::new(settings.auth.clone()),
        ]));
    RegistryClientBuilder::new(base, cache.clone())
        .index_locations(locations)
        .build()
        .map_err(|err| Error::Internal(format!("building the PyPI index client: {}", chain(&err))))
}

async fn list_files(
    client: &RegistryClient,
    index: &Url,
    project: &str,
    refused: &RefusedPages,
) -> Result<Vec<IndexFile>> {
    let name = PackageName::from_str(project).map_err(|err| {
        Error::InvalidArgument(format!("invalid PyPI project name `{project}`: {err}"))
    })?;
    let concurrency = tokio::sync::Semaphore::new(1);
    let results = client
        .simple_detail(&name, None, &IndexCapabilities::default(), &concurrency)
        .await
        .map_err(|err| match (index_error(index, project, &err), refused.under(index)) {
            (Error::NotFound(_), Some(status)) => Error::Network(format!(
                "{} refused access to the {project} page (HTTP {status}); check the credentials for {}",
                redact(index),
                index.host_str().unwrap_or("the index host")
            )),
            (error, _) => error,
        })?;
    let mut files = Vec::new();
    for (_, format) in results {
        let MetadataFormat::Simple(archive) = format else {
            return Err(Error::Internal(format!(
                "{} is a flat index, not a simple index",
                redact(index)
            )));
        };
        for datum in OwnedArchive::deserialize(&archive) {
            for (_, file) in datum.files.all(&name) {
                let mut url = file
                    .url
                    .to_url()
                    .map_err(|err| {
                        Error::Network(format!(
                            "{} lists an invalid file URL: {err}",
                            redact(index)
                        ))
                    })?
                    .into_unsafe_url();
                url.set_fragment(None);
                let sha256 = file
                    .hashes
                    .iter()
                    .find(|digest| digest.algorithm().to_string() == "sha256")
                    .map(|digest| digest.digest().to_ascii_lowercase());
                files.push(IndexFile {
                    filename: file.filename.to_string(),
                    url,
                    sha256,
                    yanked: file
                        .yanked
                        .as_ref()
                        .is_some_and(|yanked| yanked.is_yanked()),
                });
            }
        }
    }
    Ok(files)
}

fn index_error(index: &Url, project: &str, err: &uv_client::Error) -> Error {
    match err.kind() {
        ErrorKind::Offline(_) => Error::Offline(format!(
            "offline mode: the {project} page of {} is not in the cache",
            redact(index)
        )),
        ErrorKind::RemotePackageNotFound(_)
        | ErrorKind::LocalPackageNotFound(_)
        | ErrorKind::LocalIndexNotFound(_) => {
            Error::NotFound(format!("{} has no project `{project}`", redact(index)))
        }
        _ => Error::Network(format!(
            "reading the {project} page of {} failed: {}",
            redact(index),
            chain(err)
        )),
    }
}
