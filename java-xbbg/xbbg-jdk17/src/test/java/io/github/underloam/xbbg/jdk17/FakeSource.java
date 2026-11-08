package io.github.underloam.xbbg.jdk17;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/** A {@link FlowSource} whose reads the test completes by hand, tracking overlapping reads. */
final class FakeSource<T> implements FlowSource<T> {
  private final List<CompletableFuture<Optional<T>>> reads = new ArrayList<>();
  final AtomicInteger closes = new AtomicInteger();
  private final AtomicInteger inFlight = new AtomicInteger();
  final AtomicInteger maxInFlight = new AtomicInteger();

  @Override
  public synchronized CompletableFuture<Optional<T>> next() {
    int now = inFlight.incrementAndGet();
    maxInFlight.accumulateAndGet(now, Math::max);
    CompletableFuture<Optional<T>> read = new CompletableFuture<>();
    reads.add(read);
    return read;
  }

  @Override
  public CompletableFuture<?> close() {
    closes.incrementAndGet();
    return CompletableFuture.completedFuture(List.of());
  }

  synchronized int readCount() {
    return reads.size();
  }

  /** Completes the latest read with a value. */
  void emit(T value) {
    latest().complete(Optional.of(value));
  }

  /** Completes the latest read with the end of the stream. */
  void end() {
    latest().complete(Optional.empty());
  }

  /** Fails the latest read. */
  void fail(Throwable error) {
    latest().completeExceptionally(error);
  }

  private CompletableFuture<Optional<T>> latest() {
    CompletableFuture<Optional<T>> read;
    synchronized (this) {
      read = reads.get(reads.size() - 1);
    }
    if (read.isDone()) {
      throw new AssertionError("no read in flight");
    }
    inFlight.decrementAndGet();
    return read;
  }
}
