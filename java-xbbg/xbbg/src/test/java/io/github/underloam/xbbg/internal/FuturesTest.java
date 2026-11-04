package io.github.underloam.xbbg.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;

class FuturesTest {
  @Test
  void mapConvertsValuesAndPropagatesFailures() throws Exception {
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    CompletableFuture<String> mapped = Futures.map(source, value -> "v" + value);
    source.complete(4);
    assertEquals("v4", mapped.get());

    CompletableFuture<Integer> failing = new CompletableFuture<Integer>();
    CompletableFuture<String> mappedFailure = Futures.map(failing, value -> "never");
    IllegalStateException error = new IllegalStateException("x");
    failing.completeExceptionally(error);
    ExecutionException thrown = assertThrows(ExecutionException.class, mappedFailure::get);
    assertSame(error, thrown.getCause());

    CompletableFuture<Integer> throwingMapper = new CompletableFuture<Integer>();
    CompletableFuture<String> mapperError =
        Futures.map(
            throwingMapper,
            value -> {
              throw new IllegalArgumentException("mapper");
            });
    throwingMapper.complete(1);
    assertEquals(
        "mapper", assertThrows(ExecutionException.class, mapperError::get).getCause().getMessage());
  }

  @Test
  void cancellingAMappedFutureCancelsItsSource() {
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    CompletableFuture<String> mapped = Futures.map(source, String::valueOf);
    assertTrue(mapped.cancel(true));
    assertTrue(source.isCancelled());
  }

  @Test
  void composeCancelsTheSourceAndTheInnerStage() {
    CompletableFuture<Integer> source = new CompletableFuture<Integer>();
    final CompletableFuture<String> inner = new CompletableFuture<String>();
    CompletableFuture<String> composed = Futures.compose(source, value -> inner);
    source.complete(1);
    assertFalse(inner.isDone());
    composed.cancel(true);
    assertTrue(inner.isCancelled());

    CompletableFuture<Integer> pendingSource = new CompletableFuture<Integer>();
    Futures.compose(pendingSource, value -> inner).cancel(true);
    assertTrue(pendingSource.isCancelled());
  }

  @Test
  void composeUnwrapsCompletionExceptions() {
    CompletableFuture<Integer> source = CompletableFuture.completedFuture(1);
    IllegalStateException error = new IllegalStateException("inner");
    CompletableFuture<String> composed =
        Futures.compose(
            source,
            value -> {
              CompletableFuture<String> failed = new CompletableFuture<String>();
              failed.completeExceptionally(new CompletionException(error));
              return failed;
            });
    ExecutionException thrown = assertThrows(ExecutionException.class, composed::get);
    assertSame(error, thrown.getCause());
  }

  @Test
  void copiesAreIndependentOfTheSharedFuture() throws Exception {
    CompletableFuture<String> shared = new CompletableFuture<String>();
    CompletableFuture<String> first = Futures.copy(shared);
    CompletableFuture<String> second = Futures.copy(shared);
    first.cancel(true);
    assertFalse(shared.isCancelled());
    shared.complete("engine");
    assertEquals("engine", second.get());
  }

  @Test
  void unwrapStripsNestedWrappers() {
    IllegalStateException root = new IllegalStateException("root");
    assertSame(root, Futures.unwrap(new CompletionException(new ExecutionException(root))));
    assertSame(root, Futures.unwrap(root));
  }
}
