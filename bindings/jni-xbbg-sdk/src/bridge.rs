//! Upcalls into `io.github.underloam.xbbg.internal.NativeBridge`, the provisioning runtime and
//! error classification.
//!
//! Threads: one lazily built Tokio runtime with two `xbbg-sdk-jni` workers runs every
//! provisioning call as its own task (cancellation aborts it; a panic is isolated by
//! `JoinError`). Completions are rare, so the delivering worker attaches to the JVM only for
//! the upcall and detaches again: no native thread stays attached and none can keep the JVM
//! alive at exit.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex, OnceLock};

use jni::errors::{Error as JniError, ErrorPolicy};
use jni::ids::JStaticMethodID;
use jni::objects::{Global, JClass, JObject, JString, JThrowable};
use jni::signature::{JavaType, Primitive};
use jni::sys::jvalue;
use jni::{Env, JavaVM, jni_sig, jni_str};
use tokio::runtime::Runtime;
use tokio::task::AbortHandle;

/// `NativeBridge` error kinds (contract §4.6).
const KIND_INTERNAL: i32 = 7;
const KIND_ARGUMENT: i32 = 8;
const KIND_CANCELLED: i32 = 10;
const KIND_SDK_NOT_FOUND: i32 = 11;

/// A classified failure for Java: `NativeBridge.error(kind, message, {"code": …})`.
#[derive(Debug)]
pub(crate) struct SdkError {
    kind: i32,
    message: String,
    code: Option<String>,
}

impl SdkError {
    pub(crate) fn argument(message: impl Into<String>) -> Self {
        Self {
            kind: KIND_ARGUMENT,
            message: message.into(),
            code: None,
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: KIND_INTERNAL,
            message: message.into(),
            code: None,
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self::from(xbbg_sdk::Error::Cancelled)
    }

    pub(crate) fn panic(payload: &(dyn std::any::Any + Send)) -> Self {
        let message = payload
            .downcast_ref::<&str>()
            .map(|text| (*text).to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        Self::internal(format!("native panic: {message}"))
    }

    fn context_json(&self) -> Option<String> {
        self.code
            .as_ref()
            .map(|code| serde_json::json!({ "code": code }).to_string())
    }
}

impl From<xbbg_sdk::Error> for SdkError {
    fn from(error: xbbg_sdk::Error) -> Self {
        let kind = match &error {
            xbbg_sdk::Error::InvalidArgument(_) | xbbg_sdk::Error::UnsupportedPlatform(_) => {
                KIND_ARGUMENT
            }
            xbbg_sdk::Error::NotFound(_) | xbbg_sdk::Error::Offline(_) => KIND_SDK_NOT_FOUND,
            xbbg_sdk::Error::Cancelled => KIND_CANCELLED,
            _ => KIND_INTERNAL,
        };
        Self {
            kind,
            message: error.to_string(),
            code: Some(format!("SDK_{}", error.code())),
        }
    }
}

impl std::fmt::Display for SdkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// A failure inside a native body: classified, or a JNI failure (`JavaException` means a Java
/// exception is pending and becomes the error).
#[derive(Debug)]
pub(crate) enum NativeError {
    Sdk(SdkError),
    Jni(JniError),
}

impl From<SdkError> for NativeError {
    fn from(error: SdkError) -> Self {
        Self::Sdk(error)
    }
}

impl From<JniError> for NativeError {
    fn from(error: JniError) -> Self {
        Self::Jni(error)
    }
}

impl std::fmt::Display for NativeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sdk(error) => write!(f, "{error}"),
            Self::Jni(error) => write!(f, "JNI error: {error}"),
        }
    }
}

impl std::error::Error for NativeError {}

/// `NativeBridge` methods, resolved once in `JNI_OnLoad` (whose class loader sees them).
struct JavaRefs {
    bridge: Global<JClass<'static>>,
    complete: JStaticMethodID,
    fail: JStaticMethodID,
    error: JStaticMethodID,
}

static JAVA: OnceLock<JavaRefs> = OnceLock::new();

/// Resolves and caches the `NativeBridge` methods.
pub(crate) fn init(env: &mut Env<'_>) -> Result<(), JniError> {
    if JAVA.get().is_some() {
        return Ok(());
    }
    let bridge = env.find_class(jni_str!("io/github/underloam/xbbg/internal/NativeBridge"))?;
    let complete = env.get_static_method_id(
        &bridge,
        jni_str!("complete"),
        jni_sig!("(JLjava/lang/Object;)V"),
    )?;
    let fail = env.get_static_method_id(
        &bridge,
        jni_str!("fail"),
        jni_sig!("(JLjava/lang/Throwable;)V"),
    )?;
    let error = env.get_static_method_id(
        &bridge,
        jni_str!("error"),
        jni_sig!("(ILjava/lang/String;Ljava/lang/String;)Ljava/lang/Throwable;"),
    )?;
    let refs = JavaRefs {
        bridge: env.new_global_ref(bridge)?,
        complete,
        fail,
        error,
    };
    // A concurrent first load keeps whichever set won; both are equivalent.
    let _ = JAVA.set(refs);
    Ok(())
}

fn java() -> Result<&'static JavaRefs, SdkError> {
    JAVA.get().ok_or_else(|| {
        SdkError::internal("xbbg_sdk_jni is not initialized (JNI_OnLoad did not run)")
    })
}

impl JavaRefs {
    fn complete(&self, env: &mut Env<'_>, token: i64, value: &JObject<'_>) -> Result<(), JniError> {
        let args = [jvalue { j: token }, jvalue { l: value.as_raw() }];
        // SAFETY: resolved as `(JLjava/lang/Object;)V`; the arguments match.
        unsafe {
            env.call_static_method_unchecked(
                &self.bridge,
                self.complete,
                JavaType::Primitive(Primitive::Void),
                &args,
            )
        }?;
        Ok(())
    }

    fn fail(&self, env: &mut Env<'_>, token: i64, error: &JThrowable<'_>) -> Result<(), JniError> {
        let args = [jvalue { j: token }, jvalue { l: error.as_raw() }];
        // SAFETY: resolved as `(JLjava/lang/Throwable;)V`; the arguments match.
        unsafe {
            env.call_static_method_unchecked(
                &self.bridge,
                self.fail,
                JavaType::Primitive(Primitive::Void),
                &args,
            )
        }?;
        Ok(())
    }

    fn throwable<'local>(
        &self,
        env: &mut Env<'local>,
        error: &SdkError,
    ) -> Result<JThrowable<'local>, JniError> {
        let message = JString::new(env, &error.message)?;
        let context = match error.context_json() {
            Some(json) => JString::new(env, json)?,
            None => JString::null(),
        };
        let args = [
            jvalue { i: error.kind },
            jvalue {
                l: message.as_raw(),
            },
            jvalue {
                l: context.as_raw(),
            },
        ];
        // SAFETY: resolved as `(ILjava/lang/String;Ljava/lang/String;)Ljava/lang/Throwable;`.
        let throwable = unsafe {
            env.call_static_method_unchecked(&self.bridge, self.error, JavaType::Object, &args)
        }?
        .l()?;
        // SAFETY: the method's declared return type is java.lang.Throwable.
        Ok(unsafe { JThrowable::from_raw(env, throwable.into_raw()) })
    }
}

/// The Java exception for `error`: a pending exception wins, else `NativeBridge.error` picks
/// the class.
fn throwable<'local>(
    env: &mut Env<'local>,
    error: NativeError,
) -> Result<JThrowable<'local>, JniError> {
    let error = match error {
        NativeError::Sdk(error) => error,
        NativeError::Jni(error) => {
            if let Some(pending) = env.exception_occurred() {
                env.exception_clear();
                return Ok(pending);
            }
            SdkError::internal(format!("JNI error: {error}"))
        }
    };
    match java() {
        Ok(java) => java.throwable(env, &error),
        Err(missing) => {
            let message = JString::new(env, format!("{}: {}", missing.message, error.message))?;
            let class = env.find_class(jni_str!("java/lang/IllegalStateException"))?;
            let constructor = env.get_method_id(
                &class,
                jni_str!("<init>"),
                jni_sig!("(Ljava/lang/String;)V"),
            )?;
            // SAFETY: `constructor` is IllegalStateException(String).
            let object = unsafe {
                env.new_object_unchecked(
                    &class,
                    constructor,
                    &[jvalue {
                        l: message.as_raw(),
                    }],
                )
            }?;
            // SAFETY: an IllegalStateException is a Throwable.
            Ok(unsafe { JThrowable::from_raw(env, object.into_raw()) })
        }
    }
}

/// Error policy of the sync natives: throw the error's Java exception (a pending Java
/// exception wins) and return the type's default.
pub(crate) struct ThrowSdk;

impl<T: Default> ErrorPolicy<T, NativeError> for ThrowSdk {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _captures: &mut (),
        error: NativeError,
    ) -> jni::errors::Result<T> {
        if matches!(error, NativeError::Jni(_)) && env.exception_check() {
            return Ok(T::default());
        }
        let throwable = throwable(env, error)?;
        // `throw` reports the now-pending exception as an error; it propagates to Java.
        let _ = env.throw(throwable);
        Ok(T::default())
    }

    fn on_panic<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _captures: &mut (),
        payload: Box<dyn std::any::Any + Send + 'static>,
    ) -> jni::errors::Result<T> {
        if env.exception_check() {
            env.exception_clear();
        }
        let throwable = throwable(env, SdkError::panic(&*payload).into())?;
        let _ = env.throw(throwable);
        Ok(T::default())
    }
}

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("xbbg-sdk-jni")
        .enable_all()
        .build()
        .expect("create the xbbg_sdk_jni runtime")
});

/// Abort handles of running calls, by Java token.
static CALLS: LazyLock<Mutex<HashMap<i64, AbortHandle>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Runs `call` and completes Java `token` with its JSON text; every failure, including a panic
/// or cancellation, fails the token.
pub(crate) fn spawn_call<F>(token: i64, call: F)
where
    F: Future<Output = Result<String, SdkError>> + Send + 'static,
{
    let task = RUNTIME.spawn(call);
    if let Ok(mut calls) = CALLS.lock() {
        calls.insert(token, task.abort_handle());
    }
    // Spawned after the insert, so the removal below always follows it.
    RUNTIME.spawn(async move {
        let outcome = match task.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_panic() => Err(SdkError::panic(&*error.into_panic())),
            Err(_) => Err(SdkError::cancelled()),
        };
        if let Ok(mut calls) = CALLS.lock() {
            calls.remove(&token);
        }
        deliver(token, outcome);
    });
}

/// Aborts the call behind `token` if it is still running; it then fails with CANCELLED.
pub(crate) fn cancel(token: i64) {
    let handle = CALLS.lock().ok().and_then(|mut calls| calls.remove(&token));
    if let Some(handle) = handle {
        handle.abort();
    }
}

/// Completes `token` from this runtime thread, attached to the JVM for this upcall only.
fn deliver(token: i64, outcome: Result<String, SdkError>) {
    let (Ok(java), Ok(vm)) = (java(), JavaVM::singleton()) else {
        return;
    };
    let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        vm.attach_current_thread_for_scope(|env| -> Result<(), JniError> {
            match &outcome {
                Ok(json) => {
                    let value = JString::new(env, json)?;
                    java.complete(env, token, &value)
                }
                Err(error) => {
                    let throwable = java.throwable(env, error)?;
                    java.fail(env, token, &throwable)
                }
            }
        })
    }));
    if !matches!(delivered, Ok(Ok(()))) {
        // Last resort: the Java future must not hang. Report the delivery failure itself.
        let fallback =
            SdkError::internal("xbbg_sdk_jni could not deliver a provisioning result to Java");
        let _ = vm.attach_current_thread_for_scope(|env| -> Result<(), JniError> {
            if env.exception_check() {
                env.exception_clear();
            }
            let throwable = java.throwable(env, &fallback)?;
            java.fail(env, token, &throwable)
        });
    }
}
