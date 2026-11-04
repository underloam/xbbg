package io.github.underloam.xbbg.internal;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.table.Table;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Java side of the native call protocol: the upcalls the JNI libraries make ({@link #complete},
 * {@link #fail}, {@link #error}, {@link #table}), the registry of pending async calls, and the
 * entry points the rest of the library uses to call {@code xbbg_jni}.
 *
 * <p>Async calls get a token from a counter starting at 1 and a pending entry. A completion runs on
 * {@link Callbacks#executor()} (on the default executor for calls started inside {@link
 * Callbacks#withInternalCompletions}); a completion for an unknown token (cancelled or already
 * settled) is dropped. Cancelling the returned future removes the entry and asks the native side to
 * cancel.
 *
 * <p>Internal API: not covered by compatibility guarantees.
 */
public final class NativeBridge {
  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");
  private static final AtomicLong NEXT_TOKEN = new AtomicLong();
  private static final ConcurrentHashMap<Long, PendingCall> PENDING =
      new ConcurrentHashMap<Long, PendingCall>();
  private static final LongConsumer CANCEL_XBBG =
      new LongConsumer() {
        @Override
        public void accept(long token) {
          NativeXbbg.cancel(token);
        }
      };

  private NativeBridge() {}

  // ---------------------------------------------------------------- upcalls (native -> Java)

  /**
   * Completes a pending call. Never throws.
   *
   * @param token the call token
   * @param value the result
   */
  static void complete(long token, final Object value) {
    try {
      final PendingCall call = PENDING.remove(token);
      if (call == null) {
        return;
      }
      Callbacks.dispatch(
          new Runnable() {
            @Override
            public void run() {
              call.settle(value);
            }
          },
          call.completionExecutor());
    } catch (Throwable unexpected) {
      LOG.log(Level.SEVERE, "xbbg could not complete native call " + token, unexpected);
    }
  }

  /**
   * Fails a pending call. Never throws.
   *
   * @param token the call token
   * @param error the failure
   */
  static void fail(long token, Throwable error) {
    try {
      final PendingCall call = PENDING.remove(token);
      if (call == null) {
        return;
      }
      final Throwable failure =
          error != null ? error : new BlpInternalException("native call failed without an error");
      Callbacks.dispatch(
          new Runnable() {
            @Override
            public void run() {
              call.completeExceptionally(failure);
            }
          },
          call.completionExecutor());
    } catch (Throwable unexpected) {
      LOG.log(Level.SEVERE, "xbbg could not fail native call " + token, unexpected);
    }
  }

  /**
   * Builds the Java exception for a native error. Never throws.
   *
   * @param kind the native error kind (1 to 11)
   * @param message the message
   * @param contextJson optional context JSON ({@code service}, {@code operation}, {@code
   *     requestId}, {@code bloombergRequestIds}, {@code topic}, {@code detail}, {@code element},
   *     {@code suggestion}, {@code code}), or {@code null}
   * @return the exception
   */
  static Throwable error(int kind, String message, String contextJson) {
    try {
      return NativeErrors.build(kind, message, contextJson);
    } catch (Throwable unexpected) {
      BlpInternalException error = new BlpInternalException(String.valueOf(message));
      error.addSuppressed(unexpected);
      return error;
    }
  }

  /**
   * Builds a table from canonical Arrow buffers; see {@link Table#fromArrowBuffers}.
   *
   * @return the table
   * @throws BlpInternalException if the buffers are inconsistent (the native side clears the
   *     pending exception and fails the call with it)
   */
  static Table table(
      String[] names,
      byte[] types,
      String[] timezones,
      boolean[] nullable,
      int numRows,
      int[] nullCounts,
      byte[][] values,
      byte[][] offsets,
      byte[][] validity,
      String[] metadataKeys,
      String[] metadataValues,
      String decodedMetadataJson) {
    return Table.fromArrowBuffers(
        names,
        types,
        timezones,
        nullable,
        numRows,
        nullCounts,
        values,
        offsets,
        validity,
        metadataKeys,
        metadataValues,
        decodedMetadataJson);
  }

  // ---------------------------------------------------------------- pending calls

  /**
   * Registers a pending call and starts it. {@code start} receives the token and must hand it to an
   * async native method, which later completes the call through {@link #complete} or {@link #fail}.
   * If {@code start} throws, the call fails with that exception. Cancelling the returned future
   * removes the pending entry and calls {@code onCancel} with the token.
   *
   * @param start starts the native work
   * @param onCancel best-effort native cancellation, or {@code null}
   * @return the future of the call's result
   */
  public static CompletableFuture<Object> startAsync(LongConsumer start, LongConsumer onCancel) {
    return startAsync(start, onCancel, null);
  }

  /**
   * Like {@link #startAsync(LongConsumer, LongConsumer)}, for calls whose result owns a native
   * resource: after cancellation the pending entry stays registered until the native side settles
   * the call, and a value that arrives after cancellation goes to {@code onLateValue} (which
   * releases it) instead of being dropped.
   *
   * @param start starts the native work
   * @param onCancel best-effort native cancellation, or {@code null}
   * @param onLateValue releases a result that arrives after cancellation, or {@code null}
   * @return the future of the call's result
   */
  public static CompletableFuture<Object> startAsync(
      LongConsumer start, LongConsumer onCancel, Consumer<Object> onLateValue) {
    return startAsync(start, onCancel, onLateValue, null);
  }

  /**
   * Like {@link #startAsync(LongConsumer, LongConsumer, Consumer)}, and keeps {@code owner}
   * strongly reachable until the call settles: the pending call (reachable from the registry) holds
   * a stage that references it. An {@code Engine} passes itself, so its cleaner cannot release the
   * native engine before the native side has looked it up for this call.
   *
   * @param start starts the native work
   * @param onCancel best-effort native cancellation, or {@code null}
   * @param onLateValue releases a result that arrives after cancellation, or {@code null}
   * @param owner the object to keep reachable, or {@code null}
   * @return the future of the call's result
   */
  static CompletableFuture<Object> startAsync(
      LongConsumer start, LongConsumer onCancel, Consumer<Object> onLateValue, Object owner) {
    long token = NEXT_TOKEN.incrementAndGet();
    PendingCall call = new PendingCall(token, onCancel, onLateValue);
    if (owner != null) {
      // The stage is popped (and owner let go) once the call settles, success or not.
      Futures.onComplete(call, (value, error) -> Objects.requireNonNull(owner));
    }
    PENDING.put(token, call);
    try {
      start.accept(token);
    } catch (Throwable error) {
      PENDING.remove(token, call);
      call.completeExceptionally(error);
    }
    return call;
  }

  static int pendingCount() {
    return PENDING.size();
  }

  // ---------------------------------------------------------------- xbbg_jni entry points

  /**
   * Starts an engine.
   *
   * @param config the engine configuration JSON
   * @return the future engine handle
   */
  public static CompletableFuture<Long> connect(final JsonValue config) {
    try {
      NativeLibraries.ensureLoaded();
    } catch (Throwable loadFailure) {
      return Futures.failed(loadFailure);
    }
    CompletableFuture<Object> call =
        startAsync(
            new LongConsumer() {
              @Override
              public void accept(long token) {
                NativeXbbg.connect(config.text(), token);
              }
            },
            CANCEL_XBBG,
            new Consumer<Object>() {
              @Override
              public void accept(Object lateHandle) {
                if (lateHandle instanceof Long) {
                  NativeXbbg.engineClose((Long) lateHandle);
                }
              }
            });
    return engineHandle(
        call,
        new LongConsumer() {
          @Override
          public void accept(long unwanted) {
            NativeXbbg.engineClose(unwanted);
          }
        });
  }

  /**
   * The engine handle of a {@code connect} call. Cancelling the result cancels the call; a handle
   * that arrives after the result was cancelled (or completed otherwise) goes to {@code release},
   * so a cancelled connect never leaks its engine.
   *
   * @param call the native call
   * @param release closes an engine nobody receives
   * @return the handle
   */
  static CompletableFuture<Long> engineHandle(
      final CompletableFuture<Object> call, final LongConsumer release) {
    final Futures.CancellableFuture<Long> result =
        new Futures.CancellableFuture<Long>(
            new Runnable() {
              @Override
              public void run() {
                call.cancel(false);
              }
            });
    Futures.onComplete(
        call,
        (value, error) -> {
          if (error != null) {
            result.completeExceptionally(Futures.unwrap(error));
            return;
          }
          Long handle;
          try {
            handle = (Long) NativeResults.check(ResultKind.HANDLE, value);
          } catch (Throwable mappingError) {
            result.completeExceptionally(mappingError);
            return;
          }
          if (!result.complete(handle)) {
            release.accept(handle);
          }
        });
    return result;
  }

  /**
   * Closes an engine: shuts it down (its subscriptions end) and releases the handle. Non-blocking
   * and idempotent.
   *
   * @param engine the engine handle
   */
  public static void engineClose(long engine) {
    NativeLibraries.ensureLoaded();
    NativeXbbg.engineClose(engine);
  }

  /**
   * Releases the handle of an engine nobody references any more, without shutting it down: the
   * engine is torn down once its subscriptions and in-flight calls let go of it. Non-blocking and
   * idempotent.
   *
   * @param engine the engine handle
   */
  public static void engineRelease(long engine) {
    NativeLibraries.ensureLoaded();
    NativeXbbg.engineRelease(engine);
  }

  /**
   * Calls an async engine method. {@code owner} stays reachable until the call settles, so the
   * engine's cleaner cannot release the handle before the native side has looked it up.
   *
   * @param owner the Java engine object
   * @param engine the engine handle
   * @param method the method name
   * @param args the argument object
   * @param kind the requested result kind
   * @return the future result
   */
  public static CompletableFuture<Object> engineCall(
      Object owner,
      final long engine,
      final String method,
      final JsonValue args,
      final ResultKind kind) {
    CompletableFuture<Object> call =
        startXbbg(
            token -> NativeXbbg.engineCall(engine, method, args.text(), kind.code(), token),
            null,
            Objects.requireNonNull(owner, "owner"));
    return Futures.map(call, value -> NativeResults.check(kind, value));
  }

  /**
   * Calls a sync engine method on the calling thread.
   *
   * @param engine the engine handle
   * @param method the method name
   * @param args the argument object
   * @param kind the requested result kind
   * @return the result
   */
  public static Object engineCallSync(long engine, String method, JsonValue args, ResultKind kind) {
    NativeLibraries.ensureLoaded();
    return NativeResults.check(
        kind, NativeXbbg.engineCallSync(engine, method, args.text(), kind.code()));
  }

  /**
   * Starts an async {@code xbbg_jni} call, loading the natives first; cancelling the returned
   * future cancels the native call. With {@code onLateValue}, a result that arrives after
   * cancellation goes to it instead of being dropped (see {@link #startAsync(LongConsumer,
   * LongConsumer, Consumer)}).
   *
   * @param start hands the token to the native method
   * @param onLateValue releases a result that arrives after cancellation, or {@code null}
   * @return the future result
   */
  static CompletableFuture<Object> startXbbg(LongConsumer start, Consumer<Object> onLateValue) {
    return startXbbg(start, onLateValue, null);
  }

  private static CompletableFuture<Object> startXbbg(
      LongConsumer start, Consumer<Object> onLateValue, Object owner) {
    try {
      NativeLibraries.ensureLoaded();
    } catch (Throwable loadFailure) {
      return Futures.failed(loadFailure);
    }
    return startAsync(start, CANCEL_XBBG, onLateValue, owner);
  }

  /**
   * Calls a sync subscription method on the calling thread.
   *
   * @param subscription the subscription handle
   * @param method the method name
   * @param args the argument object
   * @param kind the requested result kind
   * @return the result
   */
  public static Object subscriptionCallSync(
      long subscription, String method, JsonValue args, ResultKind kind) {
    NativeLibraries.ensureLoaded();
    return NativeResults.check(
        kind, NativeXbbg.subscriptionCallSync(subscription, method, args.text(), kind.code()));
  }

  /**
   * Releases a subscription handle. Idempotent.
   *
   * @param subscription the subscription handle
   */
  public static void subscriptionRelease(long subscription) {
    NativeLibraries.ensureLoaded();
    NativeXbbg.subscriptionRelease(subscription);
  }

  /**
   * Calls a sync engine-free function.
   *
   * @param function the function name
   * @param args the argument object
   * @param kind the requested result kind
   * @return the result
   */
  public static Object extCall(String function, JsonValue args, ResultKind kind) {
    NativeLibraries.ensureLoaded();
    return NativeResults.check(kind, NativeXbbg.extCall(function, args.text(), kind.code()));
  }

  /**
   * Calls a sync engine-free function that takes and returns Arrow IPC bytes.
   *
   * @param function the function name
   * @param input the input bytes
   * @return the output bytes
   */
  public static byte[] extCallBytes(String function, byte[] input) {
    NativeLibraries.ensureLoaded();
    byte[] result = NativeXbbg.extCallBytes(function, input);
    if (result == null) {
      throw new BlpInternalException("native " + function + " returned no bytes");
    }
    return result;
  }

  /**
   * Returns the crate version of the loaded {@code xbbg_jni}.
   *
   * @return the version
   */
  public static String nativeVersion() {
    NativeLibraries.ensureLoaded();
    return NativeXbbg.nativeVersion();
  }

  /**
   * Returns the version of the loaded Bloomberg runtime.
   *
   * @return {@code major.minor.patch.build}
   */
  public static String sdkVersion() {
    NativeLibraries.ensureLoaded();
    return NativeXbbg.sdkVersion();
  }

  /**
   * Sets the native log level.
   *
   * @param level the level name
   */
  public static void setLogLevel(String level) {
    NativeLibraries.ensureLoaded();
    NativeXbbg.setLogLevel(level);
  }

  /**
   * Returns the native log level.
   *
   * @return the level name
   */
  public static String getLogLevel() {
    NativeLibraries.ensureLoaded();
    return NativeXbbg.getLogLevel();
  }

  /**
   * A pending call: removed from the registry on completion, failure or cancellation (or, with a
   * late-value handler, when the native side settles a cancelled call).
   */
  private static final class PendingCall extends CompletableFuture<Object> {
    private final long token;
    private final LongConsumer onCancel;
    private final Consumer<Object> onLateValue;

    /** Started inside {@link Callbacks#withInternalCompletions}: completes on the default pool. */
    private final boolean internal;

    PendingCall(long token, LongConsumer onCancel, Consumer<Object> onLateValue) {
      this.token = token;
      this.onCancel = onCancel;
      this.onLateValue = onLateValue;
      this.internal = Callbacks.internalCompletions();
    }

    /** The executor that settles this call. */
    Executor completionExecutor() {
      return internal ? Callbacks.defaultExecutor() : Callbacks.executor();
    }

    void settle(Object value) {
      if (!complete(value) && onLateValue != null) {
        try {
          onLateValue.accept(value);
        } catch (Throwable error) {
          LOG.log(Level.WARNING, "xbbg could not release the late result of call " + token, error);
        }
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (!cancelled) {
        return false;
      }
      boolean pending =
          onLateValue != null ? PENDING.get(token) == this : PENDING.remove(token, this);
      if (pending && onCancel != null) {
        try {
          onCancel.accept(token);
        } catch (Throwable error) {
          LOG.log(Level.WARNING, "xbbg native cancel of call " + token + " failed", error);
        }
      }
      return true;
    }
  }
}
