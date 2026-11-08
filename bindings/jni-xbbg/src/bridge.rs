//! The JVM side of the bridge: cached `NativeBridge` methods, the bridge
//! runtime, async call completion and Java exceptions.
//!
//! Threads: one lazily built Tokio runtime with two workers named
//! `xbbg-jni-bridge` polls every async native call. A call runs as its own task
//! (so cancellation can abort it and a panic is isolated by `JoinError`); a
//! second task awaits it and completes the Java token. Only that completion
//! touches the JVM: it attaches its worker permanently as a daemon thread (a
//! JVM can exit while bridge workers live), converts the result to Java and
//! calls `NativeBridge.complete` / `NativeBridge.fail`, which hand off to the
//! Java callback executor. No Rust lock is held across an upcall.

use std::cell::Cell;
use std::collections::HashMap;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{LazyLock, OnceLock};

use jni::errors::{Error as JniError, ErrorPolicy};
use jni::ids::JStaticMethodID;
use jni::objects::{Global, JClass, JObject, JString, JThrowable};
use jni::refs::Reference;
use jni::signature::{JavaType, Primitive};
use jni::sys::{JNI_OK, JNI_VERSION_1_8, JavaVMAttachArgs, jvalue};
use jni::{Env, JavaVM, jni_sig, jni_str};
use parking_lot::Mutex;
use tokio::runtime::Runtime;
use tokio::task::AbortHandle;
use xbbg_host::errors::HostError;

use crate::payload::{Payload, ResultKind};

/// A failure inside a native body: a classified error, or a JNI failure
/// (`JavaException` means a Java exception is pending and becomes the error).
#[derive(Debug)]
pub(crate) enum NativeError {
    Host(HostError),
    Jni(JniError),
}

impl From<HostError> for NativeError {
    fn from(error: HostError) -> Self {
        Self::Host(error)
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
            Self::Host(error) => write!(f, "{error}"),
            Self::Jni(error) => write!(f, "JNI error: {error}"),
        }
    }
}

impl std::error::Error for NativeError {}

/// `NativeBridge` and boxing methods, resolved once in `JNI_OnLoad`.
pub(crate) struct JavaRefs {
    pub(crate) bridge: Global<JClass<'static>>,
    complete: JStaticMethodID,
    fail: JStaticMethodID,
    error: JStaticMethodID,
    pub(crate) table: JStaticMethodID,
    pub(crate) table_class: Global<JClass<'static>>,
    long_class: Global<JClass<'static>>,
    long_value_of: JStaticMethodID,
    boolean_class: Global<JClass<'static>>,
    boolean_value_of: JStaticMethodID,
}

static JAVA: OnceLock<JavaRefs> = OnceLock::new();

/// The cached Java references; INTERNAL before `JNI_OnLoad` succeeded.
pub(crate) fn java() -> Result<&'static JavaRefs, HostError> {
    JAVA.get()
        .ok_or_else(|| HostError::internal("xbbg_jni is not initialized (JNI_OnLoad did not run)"))
}

/// Resolve and cache every class and method the bridge calls.
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
    let table = env.get_static_method_id(
        &bridge,
        jni_str!("table"),
        jni_sig!(
            "([Ljava/lang/String;[B[Ljava/lang/String;[ZI[I[[B[[B[[B[Ljava/lang/String;[Ljava/lang/String;Ljava/lang/String;)Lio/github/underloam/xbbg/table/Table;"
        ),
    )?;
    let table_class = env.find_class(jni_str!("io/github/underloam/xbbg/table/Table"))?;
    let long_class = env.find_class(jni_str!("java/lang/Long"))?;
    let long_value_of = env.get_static_method_id(
        &long_class,
        jni_str!("valueOf"),
        jni_sig!("(J)Ljava/lang/Long;"),
    )?;
    let boolean_class = env.find_class(jni_str!("java/lang/Boolean"))?;
    let boolean_value_of = env.get_static_method_id(
        &boolean_class,
        jni_str!("valueOf"),
        jni_sig!("(Z)Ljava/lang/Boolean;"),
    )?;
    let refs = JavaRefs {
        bridge: env.new_global_ref(bridge)?,
        complete,
        fail,
        error,
        table,
        table_class: env.new_global_ref(table_class)?,
        long_class: env.new_global_ref(long_class)?,
        long_value_of,
        boolean_class: env.new_global_ref(boolean_class)?,
        boolean_value_of,
    };
    // A concurrent first load keeps whichever set won; both are equivalent.
    let _ = JAVA.set(refs);
    Ok(())
}

impl JavaRefs {
    /// `NativeBridge.complete(token, value)`.
    fn complete(&self, env: &mut Env<'_>, token: i64, value: &JObject<'_>) -> Result<(), JniError> {
        let args = [jvalue { j: token }, jvalue { l: value.as_raw() }];
        // SAFETY: resolved as `(JLjava/lang/Object;)V`; arguments match.
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

    /// `NativeBridge.fail(token, error)`.
    fn fail(&self, env: &mut Env<'_>, token: i64, error: &JThrowable<'_>) -> Result<(), JniError> {
        let args = [jvalue { j: token }, jvalue { l: error.as_raw() }];
        // SAFETY: resolved as `(JLjava/lang/Throwable;)V`; arguments match.
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

    /// The Java exception for `error`, built by `NativeBridge.error`.
    pub(crate) fn throwable<'local>(
        &self,
        env: &mut Env<'local>,
        error: &HostError,
    ) -> Result<JThrowable<'local>, JniError> {
        let message = JString::new(env, &error.message)?;
        let context = match error.context_json() {
            Some(json) => JString::new(env, json)?,
            None => JString::null(),
        };
        let args = [
            jvalue {
                i: error.kind.code(),
            },
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

    /// `Long.valueOf(value)`.
    pub(crate) fn boxed_long<'local>(
        &self,
        env: &mut Env<'local>,
        value: i64,
    ) -> Result<JObject<'local>, NativeError> {
        // SAFETY: resolved as `(J)Ljava/lang/Long;`.
        Ok(unsafe {
            env.call_static_method_unchecked(
                &self.long_class,
                self.long_value_of,
                JavaType::Object,
                &[jvalue { j: value }],
            )
        }?
        .l()?)
    }

    /// `Boolean.valueOf(value)`.
    pub(crate) fn boxed_bool<'local>(
        &self,
        env: &mut Env<'local>,
        value: bool,
    ) -> Result<JObject<'local>, NativeError> {
        // SAFETY: resolved as `(Z)Ljava/lang/Boolean;`.
        Ok(unsafe {
            env.call_static_method_unchecked(
                &self.boolean_class,
                self.boolean_value_of,
                JavaType::Object,
                &[jvalue { z: value }],
            )
        }?
        .l()?)
    }
}

/// The Java exception for `error`: the pending exception of a failed JNI
/// call, or the class `NativeBridge.error` picks for a classified error.
pub(crate) fn throwable<'local>(
    env: &mut Env<'local>,
    error: NativeError,
) -> Result<JThrowable<'local>, JniError> {
    let host = match error {
        NativeError::Host(host) => host,
        NativeError::Jni(error) => {
            if let Some(pending) = env.exception_occurred() {
                env.exception_clear();
                return Ok(pending);
            }
            HostError::internal(format!("JNI error: {error}"))
        }
    };
    match java() {
        Ok(java) => java.throwable(env, &host),
        Err(_) => {
            // Without NativeBridge the closest standard class carries the text.
            let message = JString::new(env, format!("{}: {}", host.kind.name(), host.message))?;
            let class = env.find_class(jni_str!("java/lang/IllegalStateException"))?;
            let ctor = env.get_method_id(
                &class,
                jni_str!("<init>"),
                jni_sig!("(Ljava/lang/String;)V"),
            )?;
            // SAFETY: `ctor` is IllegalStateException(String).
            let object = unsafe {
                env.new_object_unchecked(
                    &class,
                    ctor,
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

/// Error policy of every sync native: throw the error's Java exception (a
/// pending Java exception wins) and return the type's default.
pub(crate) struct ThrowNative;

impl<T: Default> ErrorPolicy<T, NativeError> for ThrowNative {
    type Captures<'unowned_env_local: 'native_method, 'native_method> = ();

    fn on_error<'unowned_env_local: 'native_method, 'native_method>(
        env: &mut Env<'unowned_env_local>,
        _captures: &mut (),
        error: NativeError,
    ) -> jni::errors::Result<T> {
        raise(env, error);
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
        raise(env, HostError::native_panic(&*payload).into());
        Ok(T::default())
    }
}

/// Leave `error`'s Java exception pending (a pending Java exception wins).
/// If the exception itself cannot be built, a `RuntimeException` carries the
/// text instead, so a failing native never returns silently.
fn raise(env: &mut Env<'_>, error: NativeError) {
    if let NativeError::Jni(_) = error
        && env.exception_check()
    {
        return;
    }
    let fallback = error.to_string();
    match throwable(env, error) {
        // `throw` reports the now-pending exception as an error; it propagates.
        Ok(throwable) => {
            let _ = env.throw(throwable);
        }
        Err(failure) => {
            if !env.exception_check() {
                let _ = env.throw(format!(
                    "{fallback} (its Java exception could not be built: {failure})"
                ));
            }
        }
    }
}

const BRIDGE_THREAD_NAME: &str = "xbbg-jni-bridge";

thread_local! {
    /// Whether this thread attached itself (as a daemon) and must detach.
    static DAEMON_ATTACHED: Cell<bool> = const { Cell::new(false) };
}

/// Attach the current native thread as a JVM daemon, once per thread.
/// A thread the JVM already knows (a Java caller) is left alone.
fn attach_daemon(vm: &JavaVM) -> Result<(), JniError> {
    if DAEMON_ATTACHED.get() {
        return Ok(());
    }
    let raw = vm.get_raw();
    let mut env: *mut c_void = std::ptr::null_mut();
    // SAFETY: `raw` is the live JavaVM; GetEnv only inspects this thread.
    let attached = unsafe { ((*(*raw)).v1_2.GetEnv)(raw, &mut env, JNI_VERSION_1_8) } == JNI_OK;
    if attached {
        return Ok(());
    }
    let mut args = JavaVMAttachArgs {
        version: JNI_VERSION_1_8,
        name: c"xbbg-jni-bridge".as_ptr().cast_mut(),
        group: std::ptr::null_mut(),
    };
    // SAFETY: `args` outlives the call; the env pointer is not retained here.
    let status = unsafe {
        ((*(*raw)).v1_4.AttachCurrentThreadAsDaemon)(
            raw,
            &mut env,
            (&raw mut args).cast::<c_void>(),
        )
    };
    jni::errors::jni_error_code_to_result(status)?;
    DAEMON_ATTACHED.set(true);
    Ok(())
}

/// Detach a thread this module attached; called when a runtime thread stops.
fn detach_daemon() {
    if !DAEMON_ATTACHED.get() {
        return;
    }
    DAEMON_ATTACHED.set(false);
    if let Ok(vm) = JavaVM::singleton() {
        let raw = vm.get_raw();
        // SAFETY: this thread attached itself and holds no JNI frame here.
        unsafe { ((*(*raw)).v1_1.DetachCurrentThread)(raw) };
    }
}

/// The process-wide bridge runtime (two `xbbg-jni-bridge` workers), built on
/// first use.
static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name(BRIDGE_THREAD_NAME)
        .on_thread_stop(detach_daemon)
        .enable_all()
        .build()
        .expect("create the xbbg JNI bridge runtime")
});

/// The bridge runtime.
pub(crate) fn runtime() -> &'static Runtime {
    &RUNTIME
}

/// Abort handles of in-flight async calls, by Java token.
static CALLS: LazyLock<Mutex<HashMap<i64, AbortHandle>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Run `call` on the bridge runtime and complete Java `token` with its result
/// as `kind`. Every failure, including a panic or cancellation, fails the token.
pub(crate) fn spawn_call<F>(token: i64, kind: ResultKind, call: F)
where
    F: Future<Output = Result<Payload, HostError>> + Send + 'static,
{
    let runtime = runtime();
    let task = runtime.spawn(call);
    CALLS.lock().insert(token, task.abort_handle());
    runtime.spawn(async move {
        let outcome = match task.await {
            Ok(outcome) => outcome,
            Err(error) if error.is_panic() => Err(HostError::native_panic(&*error.into_panic())),
            Err(_) => Err(HostError::cancelled()),
        };
        CALLS.lock().remove(&token);
        deliver(token, kind, outcome);
    });
}

/// Abort the call behind `token`, if it is still running; it then fails with
/// CANCELLED. Unknown tokens are ignored.
pub(crate) fn cancel(token: i64) {
    let handle = CALLS.lock().remove(&token);
    if let Some(handle) = handle {
        handle.abort();
    }
}

/// Number of async calls still running.
#[cfg(feature = "test-support")]
pub(crate) fn calls_in_flight() -> usize {
    CALLS.lock().len()
}

/// Complete `token` on this (bridge) thread: convert, then upcall.
fn deliver(token: i64, kind: ResultKind, outcome: Result<Payload, HostError>) {
    let Ok(java) = java() else {
        xbbg_log::error!(token, "cannot complete a JNI call before JNI_OnLoad");
        return;
    };
    let Ok(vm) = JavaVM::singleton() else {
        xbbg_log::error!(token, "cannot complete a JNI call without a JavaVM");
        return;
    };
    if let Err(error) = attach_daemon(&vm) {
        xbbg_log::error!(token, error = %error, "cannot attach a bridge thread to the JVM");
        return;
    }
    let delivered = catch_unwind(AssertUnwindSafe(|| {
        vm.attach_current_thread(|env| -> Result<(), JniError> {
            let converted = outcome
                .map_err(NativeError::from)
                .and_then(|payload| payload.into_java(env, kind));
            match converted {
                Ok(value) => java.complete(env, token, &value),
                Err(error) => {
                    let throwable = match throwable(env, error) {
                        Ok(throwable) => throwable,
                        // Building the exception threw (a NativeBridge bug):
                        // that exception still completes the call.
                        Err(failure) => match env.exception_occurred() {
                            Some(pending) => {
                                env.exception_clear();
                                pending
                            }
                            None => return Err(failure),
                        },
                    };
                    java.fail(env, token, &throwable)
                }
            }
        })
    }));
    match delivered {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            xbbg_log::error!(token, error = %error, "completing a JNI call failed");
        }
        Err(payload) => {
            let error = HostError::native_panic(&*payload);
            let failed = vm.attach_current_thread(|env| -> Result<(), JniError> {
                let throwable = java.throwable(env, &error)?;
                java.fail(env, token, &throwable)
            });
            if let Err(failure) = failed {
                xbbg_log::error!(token, error = %failure, "failing a panicked JNI call failed");
            }
        }
    }
}
