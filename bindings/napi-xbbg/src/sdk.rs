//! Bloomberg runtime loading for Node.
//!
//! The engine addon links no Bloomberg library. The JavaScript entry module calls [`load_sdk`]
//! once, right after the addon loads, and everything that talks to BLPAPI works afterwards. The
//! search order lives in `xbbg-sdk`, reached through `xbbg-host`; this module only forwards the
//! host's request (its `env` view, its repository root) and the result as JSON.

use napi::bindgen_prelude::{Error, Status};
use napi_derive::napi;
use serde_json::Value;
use xbbg_host::sdk::{LocateRequest, locate};

fn failure(message: impl Into<String>) -> Error {
    Error::new(Status::GenericFailure, message.into())
}

/// The text of a `locate_and_load_json` failure, `{"message": ..., "rejected": [...]}`; the message
/// already lists every refusal. Anything else is passed through unchanged.
fn failure_message(failure_json: &str) -> String {
    serde_json::from_str::<Value>(failure_json)
        .ok()
        .and_then(|value| value.get("message")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| failure_json.to_owned())
}

/// Find a Bloomberg runtime by the shared search order and load it into this process.
///
/// `request` is a `LocateRequest` in camelCase JSON; every field is optional. Returns the loaded
/// runtime (`path`, `source`, `detail`, `version`, and the candidates that were refused before
/// it). Throws with every refusal listed when no candidate loads. Calling it again after a
/// runtime is loaded returns that runtime.
#[napi]
pub fn load_sdk(request: Option<Value>) -> napi::Result<Value> {
    let request = request.unwrap_or_else(|| Value::Object(Default::default()));
    let loaded = xbbg_host::sdk::locate_and_load_json(&request.to_string())
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
