package io.github.underloam.xbbg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class XbbgTest {
  /** Refused at once; no Bloomberg process is needed for the connection to fail. */
  static final EngineConfig UNREACHABLE =
      EngineConfig.builder()
          .host("127.0.0.1")
          .port(1)
          .numStartAttempts(1)
          .autoRestartOnDisconnection(false)
          .requestPoolSize(1)
          .subscriptionPoolSize(0)
          .build();

  @AfterEach
  void reset() {
    Xbbg.resetConfiguration();
  }

  @Test
  void awaitReturnsValuesAndRethrowsFailuresUnwrapped() {
    assertEquals("v", Xbbg.await(CompletableFuture.completedFuture("v")));

    BlpValidationException validation = new BlpValidationException("bad", "rows");
    CompletableFuture<String> failed = new CompletableFuture<String>();
    failed.completeExceptionally(validation);
    assertSame(validation, assertThrows(BlpValidationException.class, () -> Xbbg.await(failed)));

    CompletableFuture<String> wrapped = new CompletableFuture<String>();
    wrapped.completeExceptionally(new CompletionException(validation));
    assertSame(validation, assertThrows(BlpValidationException.class, () -> Xbbg.await(wrapped)));

    CompletableFuture<String> dependent = failed.thenApply(value -> value + "!");
    assertSame(validation, assertThrows(BlpValidationException.class, () -> Xbbg.await(dependent)));

    final IOException checked = new IOException("io");
    CompletableFuture<String> checkedFailure = new CompletableFuture<String>();
    checkedFailure.completeExceptionally(checked);
    CompletionException rewrapped =
        assertThrows(CompletionException.class, () -> Xbbg.await(checkedFailure));
    assertSame(checked, rewrapped.getCause());

    CompletableFuture<String> cancelled = new CompletableFuture<String>();
    cancelled.cancel(true);
    assertThrows(CancellationException.class, () -> Xbbg.await(cancelled));
  }

  @Test
  void awaitRestoresTheInterruptFlag() {
    CompletableFuture<String> never = new CompletableFuture<String>();
    Thread.currentThread().interrupt();
    try {
      CancellationException error =
          assertThrows(CancellationException.class, () -> Xbbg.await(never));
      assertInstanceOf(InterruptedException.class, error.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void configureStoresTheConfigAndTheSharedEngineRetriesAfterFailure() throws Exception {
    assertSame(UNREACHABLE, Xbbg.configure(UNREACHABLE));
    CompletableFuture<Engine> first = Xbbg.engine();
    ExecutionException firstError =
        assertThrows(ExecutionException.class, () -> first.get(60, TimeUnit.SECONDS));
    CompletableFuture<Engine> second = Xbbg.engine();
    assertNotSame(first, second);
    ExecutionException secondError =
        assertThrows(ExecutionException.class, () -> second.get(60, TimeUnit.SECONDS));
    assertEquals(firstError.getCause().getClass(), secondError.getCause().getClass());
  }

  @Test
  void cancellingACallersCopyLeavesTheSharedConnectionAlone() throws Exception {
    Xbbg.configure(UNREACHABLE);
    CompletableFuture<Engine> cancelled = Xbbg.engine();
    CompletableFuture<Engine> other = Xbbg.engine();
    cancelled.cancel(true);
    ExecutionException error =
        assertThrows(ExecutionException.class, () -> other.get(60, TimeUnit.SECONDS));
    assertTrue(
        error.getCause() instanceof BlpException || error.getCause() instanceof LinkageError,
        String.valueOf(error.getCause()));
  }

  @Test
  void configureWithHostAndPortBuildsTheConfig() {
    EngineConfig config = Xbbg.configure("example-host", 8196);
    assertEquals("example-host", config.host().get());
    assertEquals(8196, config.port().getAsInt());
  }

  @Test
  void versionComesFromTheBuild() {
    assertTrue(Xbbg.version().matches("\\d+\\.\\d+\\.\\d+.*"), Xbbg.version());
  }
}
