package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.Xbbg;
import io.github.underloam.xbbg.internal.Futures;
import java.time.Duration;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Reading and closing shared by {@link Subscription}, {@link ArrowSubscription} and {@link
 * DeliveryStream}.
 *
 * <p>Reads are serialized across every view of a subscription, and one subscription is read in one
 * format: the first read (or draining unsubscribe) fixes scalar ticks, Arrow tables or marked
 * deliveries, and reading another format fails with {@link IllegalStateException} without stealing
 * data. Closing any view closes the subscription for every view.
 *
 * <p>User code that runs inside a read of this subscription (a backend conversion, or a dependent
 * stage of a {@link #nextAsync()} future that runs on the completing thread) cannot wait for this
 * subscription: blocking reads there throw {@link IllegalStateException} and {@link #close()} only
 * starts the close. Use {@link #nextAsync()} or an async stage instead.
 *
 * @param <T> the value type
 */
abstract class SubscriptionView<T> implements Iterable<T>, AutoCloseable {
  private static final Comparator<Read<?>> OLDEST_FIRST =
      Comparator.comparingLong(read -> read.order);

  final ViewIterator<?, T> reader;

  /**
   * Reads whose blocking wait timed out or was interrupted, oldest first: later reads hand them out
   * before starting new ones, so no value is lost or overtaken. Guarded by {@code this}.
   */
  private final PriorityQueue<Read<T>> carried = new PriorityQueue<Read<T>>(4, OLDEST_FIRST);

  /** Reads started through this view; guarded by {@code this}. */
  private long started;

  SubscriptionView(ViewIterator<?, T> reader) {
    this.reader = reader;
  }

  /** A read of this view, numbered in start order. */
  private static final class Read<V> {
    final long order;
    final CompletableFuture<Optional<V>> future;

    Read(long order, CompletableFuture<Optional<V>> future) {
      this.order = order;
      this.future = future;
    }
  }

  /**
   * Reads the next value. The future completes empty at the end of the stream and fails with the
   * read's error (a terminal error closes the subscription first). Cancelling the future closes the
   * whole subscription.
   *
   * @return the next value, or empty at the end of the stream
   */
  public CompletableFuture<Optional<T>> nextAsync() {
    return takeRead().future;
  }

  /** The oldest carried read, else a new one. */
  private Read<T> takeRead() {
    synchronized (this) {
      Read<T> read = carried.poll();
      if (read != null) {
        return read;
      }
      // Numbered and started under the monitor, so numbers follow the start order of this view.
      return new Read<T>(++started, reader.next());
    }
  }

  /**
   * Waits up to {@code timeout} for the next value. A timeout leaves the read in flight: the next
   * call to {@code next}, {@link #nextAsync()} or the iterator receives its result, so no value is
   * lost; reads left in flight by several timed-out calls are handed out oldest first. A negative
   * timeout waits not at all; one too long to count in nanoseconds (about 292 years) is capped.
   * Interrupting the waiting thread also leaves the read in flight.
   *
   * @param timeout the longest time to wait
   * @return the next value, or empty at the end of the stream
   * @throws TimeoutException if no value arrived in time
   * @throws CancellationException if the thread was interrupted (the interrupt flag is restored)
   * @throws IllegalStateException if called from user code inside one of this subscription's reads
   *     (a backend conversion or a read callback), where the read would wait for itself; use {@link
   *     #nextAsync()} there
   */
  public Optional<T> next(Duration timeout) throws TimeoutException {
    long nanos = timeoutNanos(Objects.requireNonNull(timeout, "timeout"));
    ensureBlockingAllowed();
    return await(takeRead(), nanos);
  }

  /** A wait in nanoseconds: at least 0, saturated at {@link Long#MAX_VALUE}. */
  static long timeoutNanos(Duration timeout) {
    if (timeout.isNegative()) {
      return 0;
    }
    try {
      return timeout.toNanos();
    } catch (ArithmeticException tooLong) {
      return Long.MAX_VALUE;
    }
  }

  private void ensureBlockingAllowed() {
    if (reader.coordinator().insideReadOnThisThread()) {
      throw new IllegalStateException(
          "a blocking read inside this subscription's backend conversion or read callback would"
              + " wait for itself; use nextAsync()");
    }
  }

  /**
   * Waits for a read (forever when {@code nanos < 0}), carrying it over on timeout or interrupt.
   */
  private Optional<T> await(Read<T> read, long nanos) throws TimeoutException {
    try {
      return nanos < 0 ? read.future.get() : read.future.get(nanos, TimeUnit.NANOSECONDS);
    } catch (TimeoutException timedOut) {
      carry(read);
      throw timedOut;
    } catch (InterruptedException interrupted) {
      carry(read);
      Thread.currentThread().interrupt();
      CancellationException cancelled =
          new CancellationException("interrupted while waiting for a subscription value");
      cancelled.initCause(interrupted);
      throw cancelled;
    } catch (ExecutionException failed) {
      Throwable cause = failed.getCause();
      Futures.throwIfUnchecked(cause);
      throw new CompletionException(cause);
    }
  }

  private void carry(Read<T> read) {
    synchronized (this) {
      carried.add(read);
    }
  }

  /**
   * Returns a blocking iterator: {@code hasNext()} waits for the next value or the end of the
   * stream and throws read errors unchecked. The iterator shares the subscription's reads; an
   * interrupted {@code hasNext()} throws {@link CancellationException} and leaves the read in
   * flight for the next call. Inside user code that runs within one of this subscription's reads,
   * {@code hasNext()} throws {@link IllegalStateException} instead of waiting for itself.
   *
   * @return the iterator
   */
  @Override
  public Iterator<T> iterator() {
    return new Iterator<T>() {
      // The value read ahead by hasNext(); null before it reads.
      private Optional<T> buffered;

      private Optional<T> peek() {
        Optional<T> current = buffered;
        if (current == null) {
          ensureBlockingAllowed();
          try {
            current = await(takeRead(), -1);
          } catch (TimeoutException impossible) {
            throw new IllegalStateException(impossible);
          }
          buffered = current;
        }
        return current;
      }

      @Override
      public boolean hasNext() {
        return peek().isPresent();
      }

      @Override
      public T next() {
        Optional<T> current = peek();
        if (!current.isPresent()) {
          throw new NoSuchElementException("the subscription has ended");
        }
        buffered = null;
        return current.get();
      }
    };
  }

  /**
   * Returns a sequential stream over {@link #iterator()}; closing the stream closes the
   * subscription.
   *
   * @return the stream
   */
  public Stream<T> stream() {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                iterator(), Spliterator.ORDERED | Spliterator.NONNULL),
            false)
        .onClose(this::close);
  }

  /**
   * Closes the subscription. With {@code drain}, returns the unread values in this view's form:
   * values already read from the native batch, then any in-flight batch, then the native drain.
   * Without it, unread values are discarded. Every view shares one close; once it completed,
   * further closes complete with an empty list. An unread failure (for example data loss) is
   * reported instead of partial results.
   *
   * @param drain whether to return the unread values
   * @return the drained values (empty without {@code drain})
   */
  public CompletableFuture<List<T>> unsubscribe(boolean drain) {
    return reader.startClose(drain, false);
  }

  /**
   * Closes the subscription and discards unread values; same as {@code unsubscribe(false)}.
   *
   * @return completion
   */
  public CompletableFuture<List<T>> unsubscribe() {
    return unsubscribe(false);
  }

  /**
   * Closes the subscription and waits for the close, discarding unread values. Called from user
   * code inside one of this subscription's reads (its own backend conversion, or a read callback
   * that runs on the completing thread, such as {@code nextAsync().thenAccept(t -> sub.close())}),
   * it starts the close without waiting: the close completes once that code returns.
   *
   * @throws RuntimeException the close's failure, such as an unread read error
   */
  @Override
  public void close() {
    CompletableFuture<List<T>> closing = unsubscribe(false);
    if (reader.coordinator().insideReadOnThisThread()) {
      return;
    }
    Xbbg.await(closing);
  }
}
