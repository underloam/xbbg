//! JNI bindings for Bloomberg SDK provisioning (`xbbg_sdk_jni`).
//!
//! The JVM loads this library before any Bloomberg runtime is available, so it
//! must never link blpapi.
//!
//! Natives of `io.github.underloam.xbbg.internal.NativeSdk`:
//! `abiVersion()`, `provision(optionsJson, token)` (async: completes the token through
//! `NativeBridge` with the result JSON, see [`request`]), `installed(cacheDir, platform)` (sync:
//! a JSON array) and `cancel(token)`.

mod bridge;
mod request;

use std::ffi::c_void;
use std::path::PathBuf;

use jni::objects::{JClass, JString};
use jni::strings::JNIString;
use jni::sys::{JNI_ERR, JNI_OK, JNI_VERSION_1_8, jint, jlong};
use jni::{Env, EnvUnowned, JavaVM, jni_str};
use xbbg_sdk::{Platform, Provisioner};

use crate::bridge::{NativeError, SdkError, ThrowSdk, spawn_call};

/// Version of the Java/native contract; `NativeSdk.ABI` must match.
const ABI_VERSION: jint = 1;

/// Initializes the JavaVM singleton and the cached `NativeBridge` methods. A missing bridge
/// class fails the load with `UnsatisfiedLinkError`.
///
/// # Safety
///
/// Called by the JVM only, with its own `JavaVM` pointer.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnLoad(
    vm: *mut jni::sys::JavaVM,
    _reserved: *mut c_void,
) -> jint {
    if vm.is_null() {
        return JNI_ERR;
    }
    // SAFETY: the JVM passes its own live, non-null JavaVM pointer.
    unsafe { JavaVM::from_raw(vm) };
    let mut raw_env: *mut c_void = std::ptr::null_mut();
    // SAFETY: JNI_OnLoad runs on an attached Java thread of this VM.
    let status = unsafe { ((*(*vm)).v1_2.GetEnv)(vm, &mut raw_env, JNI_VERSION_1_8) };
    if status != JNI_OK || raw_env.is_null() {
        return JNI_ERR;
    }
    // SAFETY: `raw_env` is this thread's JNIEnv for the duration of JNI_OnLoad.
    let mut env = unsafe { EnvUnowned::from_raw(raw_env.cast()) };
    let loaded = env
        .with_env(|env| -> Result<bool, jni::errors::Error> {
            match bridge::init(env) {
                Ok(()) => Ok(true),
                Err(error) => {
                    if !env.exception_check() {
                        let message = JNIString::from(format!(
                            "xbbg_sdk_jni cannot resolve io.github.underloam.xbbg.internal.NativeBridge: {error}"
                        ));
                        let _ = env.throw_new(jni_str!("java/lang/UnsatisfiedLinkError"), message);
                    }
                    Ok(false)
                }
            }
        })
        .resolve::<jni::errors::ThrowRuntimeExAndDefault>();
    if loaded { JNI_VERSION_1_8 } else { JNI_ERR }
}

fn string_arg(env: &Env<'_>, value: &JString<'_>, name: &str) -> Result<String, NativeError> {
    if value.is_null() {
        return Err(SdkError::argument(format!("{name} must not be null")).into());
    }
    Ok(value.try_to_string(env)?)
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeSdk_abiVersion<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> jint {
    env.with_env(|_env| Ok::<_, NativeError>(ABI_VERSION))
        .resolve::<ThrowSdk>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeSdk_provision<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    options: JString<'local>,
    token: jlong,
) {
    env.with_env(|env| -> Result<(), NativeError> {
        // Async natives never throw: even a null or unreadable request fails the token.
        let json = match string_arg(env, &options, "options") {
            Ok(json) => json,
            Err(NativeError::Sdk(error)) => {
                spawn_call(token, async move { Err(error) });
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        spawn_call(token, provision(json));
        Ok(())
    })
    .resolve::<ThrowSdk>()
}

async fn provision(json: String) -> Result<String, SdkError> {
    let request = request::parse(&json).map_err(SdkError::argument)?;
    let provisioner = Provisioner::new(request.options)?;
    let installed = provisioner
        .provision(&request.source, request.platform, &request.version)
        .await?;
    Ok(request::result_json(&installed))
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeSdk_installed<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    cache_dir: JString<'local>,
    platform: JString<'local>,
) -> JString<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let cache_dir = PathBuf::from(string_arg(env, &cache_dir, "cacheDir")?);
        let platform = if platform.is_null() {
            None
        } else {
            let name = platform.try_to_string(env)?;
            Some(name.parse::<Platform>().map_err(SdkError::from)?)
        };
        let installed = xbbg_sdk::installed(&cache_dir, platform).map_err(SdkError::from)?;
        Ok(JString::new(env, request::results_json(&installed))?)
    })
    .resolve::<ThrowSdk>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeSdk_cancel<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    token: jlong,
) {
    env.with_env(|_env| -> Result<(), NativeError> {
        bridge::cancel(token);
        Ok(())
    })
    .resolve::<ThrowSdk>()
}
