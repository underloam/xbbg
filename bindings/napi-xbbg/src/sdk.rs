//! Bloomberg runtime loading and cache provisioning for Node.
//!
//! The engine addon links no Bloomberg library. The JavaScript entry module calls [`load_sdk`]
//! once, right after the addon loads, and everything that talks to BLPAPI works afterwards. The
//! search order and provisioner live in `xbbg-sdk`, reached through `xbbg-host`; this module
//! forwards requests (including Node's `process.env` view) and results as JSON.

use std::collections::HashMap;
use std::path::Path;

use napi::bindgen_prelude::{Error, Status};
use napi_derive::napi;
use serde_json::Value;
use xbbg_host::sdk::provision::{
    bloomberg_pypi_index, cli_run, default_sdk_cache_root as host_default_cache_root,
    installed_json, locate_and_load_or_provision_json, provision_json_async,
};
use xbbg_host::sdk::{LocateRequest, locate};

use crate::errors::host_error_to_napi;

fn failure(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}

/// The text of a `locate_and_load_or_provision_json` failure, `{"message": ..., "rejected": [...]}`;
/// the message already lists every refusal. Anything else is passed through unchanged.
fn failure_message(failure_json: &str) -> String {
    serde_json::from_str::<Value>(failure_json)
        .ok()
        .and_then(|value| value.get("message")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| failure_json.to_owned())
}

/// Find a Bloomberg runtime by the shared search order and load it into this process.
///
/// `request` is a `LocateRequest` in camelCase JSON (plus optional `sdkSource` / `sdkVersion`).
/// When no local or cached runtime loads and no explicit path was given, provisions into the
/// shared xbbg cache if `sdkSource` or `XBBG_SDK_SOURCE` is set. Calling it again after a
/// runtime is loaded returns that runtime.
#[napi]
pub fn load_sdk(request: Option<Value>) -> napi::Result<Value> {
    let request = request.unwrap_or_else(|| Value::Object(Default::default()));
    let loaded = locate_and_load_or_provision_json(&request.to_string())
        .map_err(|failure_json| failure(failure_message(&failure_json)))?;
    serde_json::from_str(&loaded).map_err(|error| failure(error.to_string()))
}

/// List the candidates the shared search order finds and the cache installs it refused, in order,
/// without loading anything.
#[napi]
pub fn locate_sdk(request: Option<Value>) -> napi::Result<Value> {
    let request: LocateRequest = match request {
        Some(value) => serde_json::from_value(value)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?,
        None => LocateRequest::default(),
    };
    serde_json::to_value(locate(&request)).map_err(|error| failure(error.to_string()))
}

/// The runtime this process loaded, in the shape [`load_sdk`] returns, or `null` before one is
/// loaded. It never loads anything.
#[napi]
pub fn loaded_sdk() -> napi::Result<Option<Value>> {
    if xbbg_core::loaded_sdk_path().is_none() {
        return Ok(None);
    }
    // A runtime is loaded, so this returns it without searching again.
    load_sdk(None).map(Some)
}

/// The loaded runtime's version as `major.minor.patch.build`, or `null` before one is loaded
/// (the function table does not exist yet, and reading it then would abort the process).
#[napi]
pub fn sdk_version() -> Option<String> {
    xbbg_core::loaded_sdk_path()?;
    let (major, minor, patch, build) = xbbg_core::sdk_version();
    Some(format!("{major}.{minor}.{patch}.{build}"))
}

/// Provision a Bloomberg runtime into the shared xbbg cache (works without any runtime loaded).
#[napi]
pub async fn provision_sdk(options_json: String) -> napi::Result<Value> {
    let json = provision_json_async(&options_json)
        .await
        .map_err(host_error_to_napi)?;
    serde_json::from_str(&json).map_err(|error| failure(error.to_string()))
}

/// List verified provisioned runtimes in `cache_dir` (defaults to the shared xbbg cache).
#[napi]
pub fn installed_sdks(cache_dir: Option<String>, platform: Option<String>) -> napi::Result<Value> {
    let json = installed_json(cache_dir.as_deref().map(Path::new), platform.as_deref())
        .map_err(host_error_to_napi)?;
    serde_json::from_str(&json).map_err(|error| failure(error.to_string()))
}

/// Default shared xbbg cache directory, or `null` when no home directory is known.
#[napi]
pub fn default_sdk_cache_dir() -> Option<String> {
    host_default_cache_root().map(|path| path.to_string_lossy().into_owned())
}

/// Bloomberg's public PyPI-compatible simple index URL.
#[napi]
pub fn sdk_bloomberg_pypi_index() -> &'static str {
    bloomberg_pypi_index()
}

/// Run the shared `xbbg-sdk` CLI command, returning exit code, stdout, and stderr.
#[napi(object)]
pub struct SdkCliOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[napi]
pub fn run_sdk_cli(
    prog: String,
    args: Vec<String>,
    env: Option<HashMap<String, String>>,
) -> SdkCliOutput {
    let mut out = Vec::new();
    let mut err = Vec::new();
    let exit_code = match env {
        Some(map) => cli_run(
            &prog,
            &args,
            &|name| {
                map.get(name).cloned().or_else(|| {
                    if cfg!(windows) {
                        map.iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(name))
                            .map(|(_, v)| v.clone())
                    } else {
                        None
                    }
                })
            },
            &mut out,
            &mut err,
        ),
        None => cli_run(
            &prog,
            &args,
            &|name| std::env::var(name).ok(),
            &mut out,
            &mut err,
        ),
    };
    SdkCliOutput {
        exit_code,
        stdout: String::from_utf8_lossy(&out).into_owned(),
        stderr: String::from_utf8_lossy(&err).into_owned(),
    }
}
