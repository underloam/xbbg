package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.Xbbg;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

/** Simulates the native side by calling the upcalls the JNI library makes. */
@Isolated
class NativeBridgeTest {
  private final AtomicLong lastToken = new AtomicLong();
  private final List<Long> cancelled = Collections.synchronizedList(new ArrayList<Long>());

  private final LongConsumer recordToken =
      new LongConsumer() {
        @Override
        public void accept(long token) {
          lastToken.set(token);
        }
      };
  private final LongConsumer recordCancel =
      new LongConsumer() {
        @Override
        public void accept(long token) {
          cancelled.add(token);
        }
      };

  @AfterEach
  void restoreExecutor() {
    Xbbg.setCallbackExecutor(null);
  }

  @Test
  void completionRunsOnTheCallbackExecutor() throws Exception {
    final List<Runnable> queued = new ArrayList<Runnable>();
    Xbbg.setCallbackExecutor(
        new Executor() {
          @Override
          public void execute(Runnable command) {
            queued.add(command);
          }
        });
    CompletableFuture<Object> future = NativeBridge.startAsync(recordToken, recordCancel);
    long token = lastToken.get();
    int pendingBefore = NativeBridge.pendingCount();
    NativeBridge.complete(token, "value");
    assertEquals(pendingBefore - 1, NativeBridge.pendingCount());
    assertFalse(future.isDone(), "completion waits for the executor");
    assertEquals(1, queued.size());
    queued.get(0).run();
    assertEquals("value", future.get(0, TimeUnit.SECONDS));
  }

  @Test
  void tokensIncreaseFromOne() throws Exception {
    CompletableFuture<Object> firstCall = NativeBridge.startAsync(recordToken, null);
    long first = lastToken.get();
    CompletableFuture<Object> secondCall = NativeBridge.startAsync(recordToken, null);
    assertTrue(first >= 1);
    assertEquals(first + 1, lastToken.get());
    RuntimeException firstError = new RuntimeException("first");
    RuntimeException secondError = new RuntimeException("second");
    NativeBridge.fail(first, firstError);
    NativeBridge.fail(first + 1, secondError);
    ExecutionException firstFailure =
        assertThrows(ExecutionException.class, () -> firstCall.get(5, TimeUnit.SECONDS));
    assertSame(firstError, firstFailure.getCause());
    ExecutionException secondFailure =
        assertThrows(ExecutionException.class, () -> secondCall.get(5, TimeUnit.SECONDS));
    assertSame(secondError, secondFailure.getCause());
  }

  @Test
  void defaultExecutorUsesNamedDaemonThreads() throws Exception {
    CompletableFuture<Object> future = NativeBridge.startAsync(recordToken, null);
    final AtomicReference<Thread> thread = new AtomicReference<Thread>();
    CompletableFuture<Object> observed =
        future.whenComplete((value, error) -> thread.set(Thread.currentThread()));
    NativeBridge.complete(lastToken.get(), 1L);
    assertEquals(1L, observed.get(5, TimeUnit.SECONDS));
    assertTrue(thread.get().getName().startsWith("xbbg-callback-"), thread.get().getName());
    assertTrue(thread.get().isDaemon());
  }

  @Test
  void failureCompletesExceptionally() throws Exception {
    CompletableFuture<Object> future = NativeBridge.startAsync(recordToken, null);
    BlpInternalException error = new BlpInternalException("boom");
    NativeBridge.fail(lastToken.get(), error);
    ExecutionException thrown =
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertSame(error, thrown.getCause());

    CompletableFuture<Object> noError = NativeBridge.startAsync(recordToken, null);
    NativeBridge.fail(lastToken.get(), null);
    ExecutionException missing =
        assertThrows(ExecutionException.class, () -> noError.get(5, TimeUnit.SECONDS));
    assertInstanceOf(BlpInternalException.class, missing.getCause());
  }

  @Test
  void cancellationRemovesTheCallAndCancelsNatively() {
    CompletableFuture<Object> future = NativeBridge.startAsync(recordToken, recordCancel);
    long token = lastToken.get();
    assertTrue(future.cancel(true));
    assertEquals(Collections.singletonList(token), cancelled);
    NativeBridge.complete(token, "late");
    assertTrue(future.isCancelled());
    assertThrows(CancellationException.class, future::join);
    assertTrue(future.cancel(true), "an already cancelled future reports cancelled");
    assertEquals(1, cancelled.size(), "native cancel runs once");
  }

  @Test
  void lateValuesOfCancelledCallsAreReleased() throws Exception {
    final CompletableFuture<Object> released = new CompletableFuture<Object>();
    CompletableFuture<Object> future =
        NativeBridge.startAsync(
            recordToken,
            recordCancel,
            new Consumer<Object>() {
              @Override
              public void accept(Object value) {
                released.complete(value);
              }
            });
    long token = lastToken.get();
    int pending = NativeBridge.pendingCount();
    future.cancel(false);
    assertEquals(pending, NativeBridge.pendingCount(), "entry stays until the native side settles");
    assertEquals(Collections.singletonList(token), cancelled);
    NativeBridge.complete(token, 42L);
    assertEquals(42L, released.get(5, TimeUnit.SECONDS));
    assertEquals(pending - 1, NativeBridge.pendingCount());
  }

  @Test
  void aThrowingStartFailsTheCall() {
    final IllegalArgumentException startError = new IllegalArgumentException("bad args");
    int pending = NativeBridge.pendingCount();
    CompletableFuture<Object> future =
        NativeBridge.startAsync(
            new LongConsumer() {
              @Override
              public void accept(long token) {
                throw startError;
              }
            },
            null);
    assertEquals(pending, NativeBridge.pendingCount());
    assertTrue(future.isCompletedExceptionally());
    ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
    assertSame(startError, thrown.getCause());
  }

  @Test
  void rejectedCompletionsFallBackToTheDefaultExecutor() throws Exception {
    Xbbg.setCallbackExecutor(
        new Executor() {
          @Override
          public void execute(Runnable command) {
            throw new RejectedExecutionException("shut down");
          }
        });
    CompletableFuture<Object> future = NativeBridge.startAsync(recordToken, null);
    NativeBridge.complete(lastToken.get(), "ok");
    assertEquals("ok", future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void unknownTokensAreIgnored() {
    int pending = NativeBridge.pendingCount();
    NativeBridge.complete(Long.MAX_VALUE, "nobody");
    NativeBridge.fail(Long.MAX_VALUE - 1, new RuntimeException("nobody"));
    assertEquals(pending, NativeBridge.pendingCount());
  }

  /** Starts a call owned by a fresh object and returns only a weak reference to the owner. */
  private WeakReference<Object> startOwnedCall() {
    Object owner = new Object();
    CompletableFuture<Object> unused = NativeBridge.startAsync(recordToken, null, null, owner);
    return new WeakReference<Object>(owner);
  }

  private static void collect(WeakReference<?> ref, int rounds) throws InterruptedException {
    for (int i = 0; i < rounds && ref.get() != null; i++) {
      System.gc();
      Thread.sleep(10);
    }
  }

  @Test
  void aPendingCallKeepsItsOwnerReachableUntilItSettles() throws Exception {
    WeakReference<Object> owner = startOwnedCall();
    long token = lastToken.get();
    collect(owner, 20);
    assertNotNull(owner.get(), "a pending call keeps its owner (an Engine) reachable");
    NativeBridge.complete(token, "done");
    collect(owner, 500);
    assertNull(owner.get(), "a settled call lets its owner go");
  }

  @Test
  void internalCallsCompleteOnTheDefaultExecutor() throws Exception {
    final List<Runnable> queued = Collections.synchronizedList(new ArrayList<Runnable>());
    Xbbg.setCallbackExecutor(queued::add);
    CompletableFuture<Object> user = NativeBridge.startAsync(recordToken, null);
    long userToken = lastToken.get();
    CompletableFuture<Object> internal =
        Callbacks.withInternalCompletions(() -> NativeBridge.startAsync(recordToken, null));
    long internalToken = lastToken.get();
    NativeBridge.complete(internalToken, "internal");
    NativeBridge.complete(userToken, "user");
    // The user's executor never runs anything here, yet the internal call completes.
    assertEquals("internal", internal.get(5, TimeUnit.SECONDS));
    assertFalse(user.isDone());
    assertEquals(1, queued.size());
    queued.get(0).run();
    assertEquals("user", user.get(0, TimeUnit.SECONDS));
  }

  @Test
  void aConnectCancelledAfterTheEngineStartedReleasesIt() throws Exception {
    CompletableFuture<Object> call = new CompletableFuture<Object>();
    final List<Long> released = Collections.synchronizedList(new ArrayList<Long>());
    final CompletableFuture<Long> handle = NativeBridge.engineHandle(call, released::add);
    // Registered after engineHandle's own stage, so it runs first: the result is cancelled after
    // the call completed but before the handle reached it, the race a late cancel can hit.
    CompletableFuture<Void> cancelFirst = call.thenRun(() -> handle.cancel(false));
    call.complete(42L);
    cancelFirst.get(5, TimeUnit.SECONDS);
    assertTrue(handle.isCancelled());
    assertEquals(Collections.singletonList(42L), released);

    CompletableFuture<Object> delivered = new CompletableFuture<Object>();
    CompletableFuture<Long> kept = NativeBridge.engineHandle(delivered, released::add);
    delivered.complete(7L);
    assertEquals(Long.valueOf(7L), kept.get(5, TimeUnit.SECONDS));
    assertEquals(Collections.singletonList(42L), released, "a delivered handle is not released");
  }
}
