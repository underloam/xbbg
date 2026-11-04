package io.github.underloam.xbbg.internal;

import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.Assumptions;

/**
 * Test-only natives of a {@code --features test-support} build of {@code xbbg_jni}: fixture methods
 * routed by prefix ({@code core.*}, {@code requests.*}, {@code recipes.*}, {@code fixture.*}).
 *
 * <p>Native tests call {@link #assumeAvailable()} first: without staged test-support natives they
 * are skipped, unless {@code -Dxbbg.requireNatives=true}, which turns the skip into a failure.
 */
public final class NativeTestSupport {
  /** System property that makes missing natives a test failure instead of a skip. */
  public static final String REQUIRE_PROPERTY = "xbbg.requireNatives";

  private static volatile Throwable unavailable;
  private static volatile boolean probed;

  private NativeTestSupport() {}

  static native Object call(String method, String argsJson, int resultKind);

  static native void callAsync(String method, String argsJson, int resultKind, long token);

  /**
   * Whether test-support natives are loaded and answer a probe call.
   *
   * @return {@code true} when fixture calls can run
   */
  public static boolean available() {
    return unavailableReason() == null;
  }

  /**
   * Skips the calling test when test-support natives are unavailable, or fails it when {@code
   * -Dxbbg.requireNatives=true}.
   */
  public static void assumeAvailable() {
    Throwable reason = unavailableReason();
    if (reason == null) {
      return;
    }
    if (Boolean.getBoolean(REQUIRE_PROPERTY)) {
      throw new AssertionError(
          "test-support natives are required but unavailable: " + reason, reason);
    }
    Assumptions.assumeTrue(false, "test-support natives unavailable: " + reason);
  }

  /**
   * Calls a sync fixture method.
   *
   * @param method the method name, for example {@code core.echo}
   * @param args the argument object
   * @param kind the result kind
   * @return the result
   */
  public static Object invoke(String method, JsonValue args, ResultKind kind) {
    NativeLibraries.ensureLoaded();
    return NativeResults.check(kind, call(method, args.text(), kind.code()));
  }

  /**
   * Calls an async fixture method through the pending-call registry.
   *
   * @param method the method name, for example {@code core.sleep}
   * @param args the argument object
   * @param kind the result kind
   * @return the future result; cancelling it cancels the native call
   */
  public static CompletableFuture<Object> invokeAsync(
      final String method, final JsonValue args, final ResultKind kind) {
    NativeLibraries.ensureLoaded();
    CompletableFuture<Object> call =
        NativeBridge.startAsync(
            new LongConsumer() {
              @Override
              public void accept(long token) {
                callAsync(method, args.text(), kind.code(), token);
              }
            },
            new LongConsumer() {
              @Override
              public void accept(long token) {
                NativeXbbg.cancel(token);
              }
            });
    return Futures.map(call, value -> NativeResults.check(kind, value));
  }

  @SuppressWarnings("StaticAssignmentOfThrowable")
  private static Throwable unavailableReason() {
    if (!probed) {
      synchronized (NativeTestSupport.class) {
        if (!probed) {
          try {
            NativeLibraries.ensureLoaded();
            call("core.echo", "{}", ResultKind.VOID.code());
          } catch (Throwable error) {
            // Suppressed: the probe failure is cached on purpose. Its stack trace shows where
            // loading failed, and NativeLibraries does not cache failures, so probing again would
            // repeat the whole native load for every test.
            unavailable = error;
          }
          probed = true;
        }
      }
    }
    return unavailable;
  }
}
