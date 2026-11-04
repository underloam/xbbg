//! A synchronous [`Provisioner`](crate::Provisioner) for callers without an async runtime.

use crate::error::{Error, Result};
use crate::install::Provisioned;
use crate::options::ProvisionOptions;
use crate::platform::{Platform, VersionSpec};
use crate::provisioner::Resolved;
use crate::source::Source;

/// Blocking facade over [`crate::Provisioner`] with its own current-thread runtime.
///
/// Do not call it from inside an async runtime (tokio refuses to block a runtime thread).
pub struct Provisioner {
    inner: crate::Provisioner,
    runtime: tokio::runtime::Runtime,
}

impl Provisioner {
    /// See [`crate::Provisioner::new`].
    pub fn new(options: ProvisionOptions) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| Error::Internal(format!("starting a runtime: {err}")))?;
        Ok(Self {
            inner: crate::Provisioner::new(options)?,
            runtime,
        })
    }

    /// See [`crate::Provisioner::resolve`].
    pub fn resolve(
        &self,
        source: &Source,
        platform: Platform,
        version: &VersionSpec,
    ) -> Result<Resolved> {
        self.runtime
            .block_on(self.inner.resolve(source, platform, version))
    }

    /// See [`crate::Provisioner::provision`].
    pub fn provision(
        &self,
        source: &Source,
        platform: Platform,
        version: &VersionSpec,
    ) -> Result<Provisioned> {
        self.runtime
            .block_on(self.inner.provision(source, platform, version))
    }

    /// See [`crate::Provisioner::installed`].
    pub fn installed(&self, platform: Option<Platform>) -> Result<Vec<Provisioned>> {
        self.inner.installed(platform)
    }
}
