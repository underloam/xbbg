package io.github.underloam.xbbg.jdk17;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A single-subscriber {@link Flow.Publisher} over a {@link FlowSource}: one read in flight at a
 * time and only while the subscriber has outstanding demand.
 *
 * @param <T> the value type
 */
final class ViewPublisher<T> implements Flow.Publisher<T> {
  private final FlowSource<T> source;
  private final AtomicBoolean subscribed = new AtomicBoolean();

  ViewPublisher(FlowSource<T> source) {
    this.source = Objects.requireNonNull(source, "source");
  }

  @Override
  public void subscribe(Flow.Subscriber<? super T> subscriber) {
    Objects.requireNonNull(subscriber, "subscriber");
    if (!subscribed.compareAndSet(false, true)) {
      subscriber.onSubscribe(Rejected.INSTANCE);
      subscriber.onError(
          new IllegalStateException(
              "this publisher supports one subscriber; the subscription view is already published"));
      return;
    }
    new Reader<T>(source, subscriber).start();
  }

  /** The subscription handed to a rejected second subscriber. */
  private enum Rejected implements Flow.Subscription {
    INSTANCE;

    @Override
    public void request(long n) {}

    @Override
    public void cancel() {}
  }

  /** A failed read. */
  private record Failure(Throwable error) {}

  /**
   * One subscription. Every signal to the subscriber happens inside the serialized drain loop
   * ({@code wip} counts pending drain requests), so signals never overlap and a {@code request}
   * from inside {@code onNext} only queues another pass.
   */
  private static final class Reader<T> implements Flow.Subscription {
    private final FlowSource<T> source;
    // Starts at 1: onSubscribe runs as the first drain pass, so no signal can overlap it.
    private final AtomicInteger wip = new AtomicInteger(1);
    private final AtomicLong requested = new AtomicLong();
    private final AtomicReference<Long> invalidRequest = new AtomicReference<>();
    private volatile boolean cancelled;
    private volatile Object result;

    // Only touched inside the drain loop, which wip serializes; done is also read on entry to
    // every drain pass, so it is volatile for the threads that start one.
    private Flow.Subscriber<? super T> downstream;
    private boolean reading;
    private volatile boolean done;

    Reader(FlowSource<T> source, Flow.Subscriber<? super T> downstream) {
      this.source = source;
      this.downstream = downstream;
    }

    void start() {
      downstream.onSubscribe(this);
      drainLoop(1);
    }

    @Override
    public void request(long n) {
      if (n <= 0) {
        invalidRequest.compareAndSet(null, n);
      } else {
        long current;
        long next;
        do {
          current = requested.get();
          if (current == Long.MAX_VALUE) {
            break;
          }
          next = current + n;
          if (next < 0) {
            next = Long.MAX_VALUE;
          }
        } while (!requested.compareAndSet(current, next));
      }
      drain();
    }

    @Override
    public void cancel() {
      cancelled = true;
      drain();
    }

    private void drain() {
      if (wip.getAndIncrement() == 0) {
        drainLoop(1);
      }
    }

    private void drainLoop(int initialMissed) {
      int missed = initialMissed;
      while (true) {
        if (!done) {
          step();
        }
        missed = wip.addAndGet(-missed);
        if (missed == 0) {
          return;
        }
      }
    }

    // Unchecked: result holds either a Failure or the Optional<T> that source.next() completed.
    @SuppressWarnings("unchecked")
    private void step() {
      while (true) {
        Flow.Subscriber<? super T> subscriber = downstream;
        if (cancelled) {
          terminate();
          return;
        }
        Long invalid = invalidRequest.get();
        if (invalid != null) {
          terminate();
          subscriber.onError(
              new IllegalArgumentException(
                  "Reactive Streams rule 3.9: request(" + invalid + ") must be positive"));
          return;
        }
        Object completed = result;
        if (completed != null) {
          result = null;
          reading = false;
          if (completed instanceof Failure failure) {
            terminate();
            subscriber.onError(failure.error());
            return;
          }
          Optional<T> value = (Optional<T>) completed;
          if (value.isEmpty()) {
            terminate();
            subscriber.onComplete();
            return;
          }
          if (requested.get() != Long.MAX_VALUE) {
            requested.decrementAndGet();
          }
          try {
            subscriber.onNext(value.get());
          } catch (RuntimeException | Error thrown) {
            // Rule 2.13: a throwing subscriber is treated as having cancelled.
            terminate();
            throw thrown;
          }
          continue;
        }
        if (!reading && requested.get() > 0) {
          reading = true;
          startRead();
          continue;
        }
        return;
      }
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private void startRead() {
      CompletableFuture<Optional<T>> read;
      try {
        read = source.next();
      } catch (RuntimeException failed) {
        result = new Failure(failed);
        return;
      }
      // Suppressed: the callback stores the outcome in result and drains; nothing reads the
      // stage whenComplete returns.
      read.whenComplete((value, error) -> {
        if (error != null) {
          result = new Failure(unwrap(error));
        } else if (value == null) {
          result = new Failure(new NullPointerException("subscription read returned null"));
        } else {
          result = value;
        }
        drain();
      });
    }

    /** Ends the subscription: no further signals, the view is closed and references dropped. */
    @SuppressWarnings("FutureReturnValueIgnored")
    private void terminate() {
      done = true;
      downstream = null;
      result = null;
      try {
        // Suppressed: a close failure (for example an unread error) cannot be signalled after a
        // terminal signal or a cancel (rules 1.7 and 3.6); the subscription is closed either way.
        source.close();
      } catch (RuntimeException ignored) {
        // see above
      }
    }

    private static Throwable unwrap(Throwable error) {
      Throwable current = error;
      while ((current instanceof CompletionException || current instanceof ExecutionException)
          && current.getCause() != null) {
        current = current.getCause();
      }
      return current;
    }
  }
}
