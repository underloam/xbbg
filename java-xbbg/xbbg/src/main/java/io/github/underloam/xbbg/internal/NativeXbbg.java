package io.github.underloam.xbbg.internal;

/**
 * Native methods of {@code xbbg_jni}. Only {@link NativeLibraries} and {@link NativeBridge} call
 * them, after the library is loaded.
 *
 * <p>Async methods complete through {@link NativeBridge#complete(long, Object)} or {@link
 * NativeBridge#fail(long, Throwable)} and never throw; sync methods throw exceptions built by
 * {@link NativeBridge#error(int, String, String)}.
 */
final class NativeXbbg {
  private NativeXbbg() {}

  static native int abiVersion();

  static native String nativeVersion();

  static native String sdkVersion();

  static native void setLogLevel(String level);

  static native String getLogLevel();

  static native void connect(String configJson, long token);

  /** Signals shutdown (subscriptions end) and drops the handle; idempotent. */
  static native void engineClose(long engine);

  /**
   * Drops the handle without signalling shutdown: the engine is torn down when its last holder (a
   * subscription or an in-flight call) lets go. Idempotent.
   */
  static native void engineRelease(long engine);

  static native void engineCall(
      long engine, String method, String argsJson, int resultKind, long token);

  static native Object engineCallSync(long engine, String method, String argsJson, int resultKind);

  static native void subscriptionCall(
      long sub, String method, String argsJson, int resultKind, long token);

  static native Object subscriptionCallSync(
      long sub, String method, String argsJson, int resultKind);

  static native void subscriptionRelease(long sub);

  static native Object extCall(String fn, String argsJson, int resultKind);

  static native byte[] extCallBytes(String fn, byte[] input);

  static native void cancel(long token);

  static native void shutdown(long timeoutMillis);
}
