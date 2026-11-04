//! JNI bindings for the xbbg engine (`xbbg_jni`), loaded by `io.github.underloam:xbbg`.
//!
//! Every native of `io.github.underloam.xbbg.internal.NativeXbbg` is defined
//! here. Sync natives run on the calling Java thread and throw the Java
//! exception `NativeBridge.error` builds for their [`HostError`]; async natives
//! only read their arguments, spawn the call on the bridge runtime and return:
//! the result (or any failure, argument errors included) reaches Java through
//! `NativeBridge.complete` / `NativeBridge.fail` with the caller's token.
//! A Rust panic is caught at every entry point and in every spawned call and
//! reported as INTERNAL `native panic: <message>`.

#[cfg(target_endian = "big")]
compile_error!("the JVM table transport sends little-endian buffers");

mod bridge;
mod engine;
mod ext;
mod handles;
mod payload;
mod recipes;
mod requests;
#[cfg(feature = "test-support")]
mod subscription_fixture;
mod subscriptions;
mod table;
#[cfg(feature = "test-support")]
mod test_support;

use std::ffi::c_void;
use std::time::Duration;

use jni::objects::{JByteArray, JClass, JObject, JString};
use jni::strings::JNIString;
use jni::sys::{JNI_ERR, JNI_OK, JNI_VERSION_1_8, jint, jlong};
use jni::{Env, EnvUnowned, JavaVM, jni_str};
use xbbg_host::errors::HostError;

use crate::bridge::{NativeError, ThrowNative, spawn_call};
use crate::payload::{Payload, ResultKind};

/// Version of the Java/native call contract; `NativeLibraries.ABI` must match.
pub(crate) const ABI_VERSION: jint = 1;

/// Initialize logging, the JavaVM singleton and the cached `NativeBridge`
/// methods. A missing bridge class fails the load with `UnsatisfiedLinkError`.
///
/// # Safety
///
/// Called by the JVM only, with its own `JavaVM` pointer.
#[unsafe(no_mangle)]
pub unsafe extern "system" fn JNI_OnLoad(
    vm: *mut jni::sys::JavaVM,
    _reserved: *mut c_void,
) -> jint {
    xbbg_log::init();
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
                            "xbbg_jni cannot resolve io.github.underloam.xbbg.internal.NativeBridge: {error}"
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

/// A non-null Java string argument.
fn string_arg(env: &Env<'_>, value: &JString<'_>, name: &str) -> Result<String, NativeError> {
    if value.is_null() {
        return Err(HostError::argument(format!("{name} must not be null")).into());
    }
    Ok(value.try_to_string(env)?)
}

/// A JSON argument object; Java `null` means `{}`.
fn args_arg(env: &Env<'_>, value: &JString<'_>) -> Result<String, NativeError> {
    if value.is_null() {
        return Ok("{}".to_owned());
    }
    Ok(value.try_to_string(env)?)
}

/// Spawn an async call whose arguments were already read on the caller thread.
/// Argument errors, including a bad result kind, fail the token too.
fn spawn_checked<F>(token: jlong, kind: jint, call: impl FnOnce(ResultKind) -> F)
where
    F: Future<Output = Result<Payload, HostError>> + Send + 'static,
{
    match ResultKind::from_code(kind) {
        Ok(kind) => spawn_call(token, kind, call(kind)),
        Err(error) => spawn_call(token, ResultKind::Void, async move { Err(error) }),
    }
}

/// Read async-call arguments; a null or unreadable string fails the token.
fn read_or_fail<T>(
    env: &Env<'_>,
    token: jlong,
    read: impl FnOnce(&Env<'_>) -> Result<T, NativeError>,
) -> Result<Option<T>, NativeError> {
    match read(env) {
        Ok(value) => Ok(Some(value)),
        Err(NativeError::Host(error)) => {
            spawn_call(token, ResultKind::Void, async move { Err(error) });
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_abiVersion<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> jint {
    env.with_env(|_env| Ok::<_, NativeError>(ABI_VERSION))
        .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_nativeVersion<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> JString<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        Ok(JString::new(env, env!("CARGO_PKG_VERSION"))?)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_sdkVersion<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> JString<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let (major, minor, patch, build) = xbbg_core::sdk_version();
        Ok(JString::new(
            env,
            format!("{major}.{minor}.{patch}.{build}"),
        )?)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_setLogLevel<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    level: JString<'local>,
) {
    env.with_env(|env| -> Result<_, NativeError> {
        let level = string_arg(env, &level, "level")?;
        let parsed = xbbg_log::parse_level(&level).ok_or_else(|| {
            HostError::argument(format!(
                "Invalid log level '{level}'. Expected: trace, debug, info, warn, error"
            ))
        })?;
        xbbg_log::set_level(parsed);
        Ok(())
    })
    .resolve::<ThrowNative>()
}

/// The Rust log level as napi names it.
fn log_level_name(level: xbbg_log::Level) -> &'static str {
    match level {
        xbbg_log::Level::TRACE => "trace",
        xbbg_log::Level::DEBUG => "debug",
        xbbg_log::Level::INFO => "info",
        xbbg_log::Level::WARN => "warn",
        xbbg_log::Level::ERROR => "error",
    }
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_getLogLevel<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
) -> JString<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        Ok(JString::new(
            env,
            log_level_name(xbbg_log::current_level()),
        )?)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_connect<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    config_json: JString<'local>,
    token: jlong,
) {
    env.with_env(|env| -> Result<_, NativeError> {
        if let Some(config) = read_or_fail(env, token, |env| args_arg(env, &config_json))? {
            spawn_call(token, ResultKind::Handle, engine::connect(config));
        }
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_engineClose<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    engine: jlong,
) {
    env.with_env(|_env| -> Result<_, NativeError> {
        engine::close(engine);
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_engineRelease<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    engine: jlong,
) {
    env.with_env(|_env| -> Result<_, NativeError> {
        engine::release(engine);
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_engineCall<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    engine: jlong,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
    token: jlong,
) {
    env.with_env(|env| -> Result<_, NativeError> {
        let Some((method, args)) = read_or_fail(env, token, |env| {
            Ok((string_arg(env, &method, "method")?, args_arg(env, &args)?))
        })?
        else {
            return Ok(());
        };
        spawn_checked(token, result_kind, move |kind| async move {
            let entry = engine::engine_entry(engine)?;
            engine::dispatch(entry, &method, &args, kind).await
        });
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_engineCallSync<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    engine: jlong,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
) -> JObject<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let method = string_arg(env, &method, "method")?;
        let args = args_arg(env, &args)?;
        let kind = ResultKind::from_code(result_kind)?;
        let entry = engine::engine_entry(engine)?;
        let payload = engine::dispatch_sync(&entry, &method, &args, kind)?;
        payload.into_java(env, kind)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_subscriptionCall<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    subscription: jlong,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
    token: jlong,
) {
    env.with_env(|env| -> Result<_, NativeError> {
        let Some((method, args)) = read_or_fail(env, token, |env| {
            Ok((string_arg(env, &method, "method")?, args_arg(env, &args)?))
        })?
        else {
            return Ok(());
        };
        let subscription = handles::handle_id(subscription);
        spawn_checked(token, result_kind, move |kind| async move {
            subscriptions::subscription_call(subscription, &method, &args, kind).await
        });
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_subscriptionCallSync<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    subscription: jlong,
    method: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
) -> JObject<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let method = string_arg(env, &method, "method")?;
        let args = args_arg(env, &args)?;
        let kind = ResultKind::from_code(result_kind)?;
        let payload = subscriptions::subscription_call_sync(
            handles::handle_id(subscription),
            &method,
            &args,
            kind,
        )?;
        payload.into_java(env, kind)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_subscriptionRelease<
    'local,
>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    subscription: jlong,
) {
    env.with_env(|_env| -> Result<_, NativeError> {
        subscriptions::subscription_release(handles::handle_id(subscription));
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_extCall<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    function: JString<'local>,
    args: JString<'local>,
    result_kind: jint,
) -> JObject<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let function = string_arg(env, &function, "fn")?;
        let args = args_arg(env, &args)?;
        let kind = ResultKind::from_code(result_kind)?;
        let payload = ext::call(&function, &args, kind)?;
        payload.into_java(env, kind)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_extCallBytes<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    function: JString<'local>,
    input: JByteArray<'local>,
) -> JByteArray<'local> {
    env.with_env(|env| -> Result<_, NativeError> {
        let function = string_arg(env, &function, "fn")?;
        if input.is_null() {
            return Err(HostError::argument("input must not be null").into());
        }
        let input = env.convert_byte_array(&input)?;
        let output = ext::call_bytes(&function, &input)?;
        Ok(env.byte_array_from_slice(&output)?)
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_cancel<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    token: jlong,
) {
    env.with_env(|_env| -> Result<_, NativeError> {
        bridge::cancel(token);
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_io_github_underloam_xbbg_internal_NativeXbbg_shutdown<'local>(
    mut env: EnvUnowned<'local>,
    _class: JClass<'local>,
    timeout_millis: jlong,
) {
    env.with_env(|_env| -> Result<_, NativeError> {
        let timeout = Duration::from_millis(u64::try_from(timeout_millis).unwrap_or(0));
        if !engine::shutdown(timeout) {
            xbbg_log::warn!(
                timeout_ms = timeout_millis,
                "engine teardown still running when the shutdown wait ended"
            );
        }
        Ok(())
    })
    .resolve::<ThrowNative>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_names_round_trip_through_the_parser() {
        for name in ["trace", "debug", "info", "warn", "error"] {
            let level = xbbg_log::parse_level(name).unwrap();
            assert_eq!(log_level_name(level), name);
        }
        assert_eq!(
            log_level_name(xbbg_log::parse_level("WARNING").unwrap()),
            "warn"
        );
        assert_eq!(log_level_name(xbbg_log::parse_level("0").unwrap()), "trace");
        assert!(xbbg_log::parse_level("verbose").is_none());
    }
}
