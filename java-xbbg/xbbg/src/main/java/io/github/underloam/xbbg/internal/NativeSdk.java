package io.github.underloam.xbbg.internal;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * JNI entry points of {@code xbbg_sdk_jni}, the Bloomberg runtime provisioner.
 *
 * <p>The library never links blpapi, so it loads on machines without any Bloomberg runtime. It is
 * extracted from the jar and loaded on first use only; a failed load is not remembered, so a later
 * call can succeed.
 */
public final class NativeSdk {
  /** ABI version this class expects from {@code xbbg_sdk_jni}. */
  public static final int ABI = 1;

  private static final String LIBRARY = "xbbg_sdk_jni";
  private static volatile boolean loaded;

  private NativeSdk() {}

  static native int abiVersion();

  static native void provision(String optionsJson, long token);

  static native String installed(String cacheDir, String platform);

  static native void cancel(long token);

  /**
   * Loads {@code xbbg_sdk_jni} if it is not loaded yet.
   *
   * @throws UnsatisfiedLinkError if the jar has no provisioner for this platform, the library
   *     cannot be loaded, or it is a stale build
   */
  public static void ensureLoaded() {
    if (loaded) {
      return;
    }
    synchronized (NativeSdk.class) {
      if (loaded) {
        return;
      }
      Path library = NativeLibraries.extractBundled(LIBRARY);
      System.load(library.toString());
      int abi = abiVersion();
      if (abi != ABI) {
        throw new UnsatisfiedLinkError(
            "stale "
                + LIBRARY
                + " at "
                + library
                + ": ABI "
                + abi
                + ", expected "
                + ABI
                + "; use an xbbg jar whose natives match its classes");
      }
      loaded = true;
    }
  }

  /**
   * Starts provisioning; the future completes with the result object as JSON text. Cancelling the
   * future cancels the native work.
   *
   * @param optionsJson the request (see {@code bindings/jni-xbbg-sdk/src/request.rs})
   * @return the pending result
   */
  public static CompletableFuture<Object> provisionAsync(String optionsJson) {
    try {
      ensureLoaded();
    } catch (UnsatisfiedLinkError | RuntimeException loadFailure) {
      return Futures.failed(loadFailure);
    }
    return NativeBridge.startAsync(token -> provision(optionsJson, token), NativeSdk::cancel);
  }

  /**
   * Lists the valid installs under a cache root.
   *
   * @param cacheDir the cache root
   * @param platform platform filter, or {@code null} for every platform
   * @return a JSON array of result objects, newest version first
   */
  public static String installedJson(Path cacheDir, String platform) {
    ensureLoaded();
    return installed(cacheDir.toAbsolutePath().toString(), platform);
  }
}
