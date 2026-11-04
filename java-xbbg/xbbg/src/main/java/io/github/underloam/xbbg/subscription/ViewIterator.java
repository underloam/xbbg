package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.subscription.SubscriptionCoordinator.BeginClose;
import io.github.underloam.xbbg.subscription.SubscriptionCoordinator.ReadMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * One view's reads and close over the shared coordinator (the JavaScript {@code
 * SubscriptionIterator}): values buffered from a native batch are handed out one at a time, reads
 * are serialized across views, a close is shared by every view and drains compose the buffered
 * values, an in-flight batch and the native drain.
 *
 * <p>All state is guarded by the coordinator's lock; futures are completed outside it.
 *
 * @param <B> the native batch type
 * @param <V> the value type
 */
final class ViewIterator<B, V> {
  private enum Phase {
    OPEN,
    CLOSING,
    CLOSED
  }

  /** A serialized read's result: end, a value, or a publication to commit at the boundary. */
  private static final class ReadResult<B, V> {
    final V value;
    final B publication;

    ReadResult(V value, B publication) {
      this.value = value;
      this.publication = publication;
    }
  }

  private final SubscriptionCoordinator coordinator;
  private final Object lock;
  private final Supplier<CompletableFuture<B>> readBatch;
  private final Function<Boolean, CompletableFuture<List<B>>> closeNative;
  private final Projection<B, V> projection;
  private final ReadMode readMode;
  private final Object owner = new Object();
  private final ArrayDeque<V> pending = new ArrayDeque<V>();
  private final List<V> closingPending = new ArrayList<V>();
  private Phase phase = Phase.OPEN;
  private boolean closeDrain;
  private CompletableFuture<List<V>> closeInFlight;

  ViewIterator(
      SubscriptionCoordinator coordinator,
      Supplier<CompletableFuture<B>> readBatch,
      Function<Boolean, CompletableFuture<List<B>>> closeNative,
      Projection<B, V> projection,
      ReadMode readMode) {
    this.coordinator = coordinator;
    this.lock = coordinator;
    this.readBatch = readBatch;
    this.closeNative = closeNative;
    this.projection = projection;
    this.readMode = readMode;
    synchronized (lock) {
      coordinator.observeClose(closingOwner -> {
        if (owner.equals(closingOwner)) {
          return;
        }
        phase = Phase.CLOSING;
        closeDrain = false;
        pending.clear();
        closingPending.clear();
      });
    }
  }

  SubscriptionCoordinator coordinator() {
    return coordinator;
  }

  private ReadResult<B, V> done() {
    return new ReadResult<B, V>(null, null);
  }

  /**
   * The next value; empty at the end. Cancelling the returned future closes the whole subscription
   * (the JavaScript abort). Its dependent stages that run on the completing thread run inside the
   * read (see {@link SubscriptionCoordinator#insideRead}), where blocking calls on this
   * subscription would wait for themselves.
   */
  CompletableFuture<Optional<V>> next() {
    final ReadFuture<V> user = new ReadFuture<V>(this);
    try {
      coordinator.ensureRowDelivery();
      synchronized (lock) {
        if (phase != Phase.OPEN) {
          user.complete(Optional.<V>empty());
          return user;
        }
        coordinator.claimReadMode(readMode);
      }
    } catch (RuntimeException error) {
      user.completeExceptionally(error);
      return user;
    }
    Futures.onComplete(
        coordinator.enqueue(this::nextSerialized),
        (result, error) -> {
          if (error != null) {
            fail(user, Futures.unwrap(error));
            return;
          }
          Optional<V> published;
          try {
            published = publishRead(result);
          } catch (Throwable failure) {
            fail(user, failure);
            return;
          }
          settle(user, published);
        });
    return user;
  }

  /**
   * Completes a read future inside the read: the queue tail settles only after its callbacks
   * return, so a callback that closed the subscription or iterated it and waited would wait for
   * itself.
   */
  private void settle(final CompletableFuture<Optional<V>> user, final Optional<V> value) {
    coordinator.insideRead(() -> user.complete(value));
  }

  /** Fails a read future inside the read, like {@link #settle}. */
  private void settleFailure(final CompletableFuture<Optional<V>> user, final Throwable error) {
    coordinator.insideRead(() -> user.completeExceptionally(error));
  }

  private void fail(final CompletableFuture<Optional<V>> user, final Throwable error) {
    if (error instanceof ProjectionError) {
      boolean open;
      synchronized (lock) {
        open = phase == Phase.OPEN;
      }
      if (open) {
        settleFailure(user, ((ProjectionError) error).original());
      } else {
        settle(user, Optional.<V>empty());
      }
      return;
    }
    Futures.onComplete(
        rejectAfterCleanup(error, startClose(false, false)),
        (ignored, failure) -> settleFailure(user, Futures.unwrap(failure)));
  }

  private Optional<V> publishRead(ReadResult<B, V> result) {
    if (result.publication == null) {
      return Optional.ofNullable(result.value);
    }
    B batch = result.publication;
    // Under the lock no close can start between the check and the native commit.
    synchronized (lock) {
      if (phase != Phase.OPEN) {
        projection.rollback(batch);
        return Optional.empty();
      }
      boolean published = false;
      try {
        Optional<V> value = projection.publish(batch);
        published = true;
        return value;
      } finally {
        if (!published) {
          // A failed publication (a terminal error or a native failure) goes back to the source.
          projection.rollback(batch);
        }
      }
    }
  }

  private CompletableFuture<ReadResult<B, V>> nextSerialized() {
    synchronized (lock) {
      if (phase != Phase.OPEN) {
        return CompletableFuture.completedFuture(done());
      }
      V value = pending.poll();
      if (value != null) {
        return CompletableFuture.completedFuture(new ReadResult<B, V>(value, null));
      }
    }
    return readLoop();
  }

  private CompletableFuture<ReadResult<B, V>> readLoop() {
    synchronized (lock) {
      if (phase != Phase.OPEN) {
        return CompletableFuture.completedFuture(done());
      }
    }
    CompletableFuture<B> read;
    try {
      read = readBatch.get();
    } catch (Throwable error) {
      read = Futures.failed(error);
    }
    return Futures.handleCompose(read, this::afterRead);
  }

  private CompletableFuture<ReadResult<B, V>> afterRead(B batch, Throwable readFailure) {
    if (readFailure != null) {
      Throwable error = Futures.unwrap(readFailure);
      // Preserve the read failure over diagnostic reporting.
      coordinator.emitWarningsQuietly();
      synchronized (lock) {
        if (phase != Phase.OPEN) {
          // A conversion failure already restored its source for the closing owner.
          if (!(error instanceof ProjectionError)) {
            coordinator.recordCloseReadError(error);
          }
          return CompletableFuture.completedFuture(done());
        }
      }
      return Futures.failed(error);
    }
    try {
      coordinator.emitWarnings();
    } catch (RuntimeException error) {
      if (batch == null || projection.kind != Projection.Kind.PUBLICATION) {
        return Futures.failed(error);
      }
      if (!projection.hasError(batch)) {
        projection.rollback(batch);
        return Futures.failed(new ProjectionError(error));
      }
      // A terminal receipt must still reach the final publication boundary.
    }
    CompletableFuture<Throwable> naturalClose;
    synchronized (lock) {
      if (phase != Phase.OPEN) {
        if (batch != null && projection.kind == Projection.Kind.PUBLICATION) {
          projection.rollback(batch);
        } else if (batch != null && closeDrain) {
          try {
            appendBatch(closingPending, batch);
          } catch (RuntimeException error) {
            coordinator.recordCloseReadError(error);
          }
        }
        return CompletableFuture.completedFuture(done());
      }
      if (batch != null) {
        try {
          switch (projection.kind) {
            case PUBLICATION:
              return CompletableFuture.completedFuture(new ReadResult<B, V>(null, batch));
            case ONE:
              return CompletableFuture.completedFuture(
                  new ReadResult<B, V>(projection.one(batch), null));
            default:
              pending.addAll(projection.many(batch));
              V value = pending.poll();
              if (value != null) {
                return CompletableFuture.completedFuture(new ReadResult<B, V>(value, null));
              }
          }
        } catch (RuntimeException error) {
          return Futures.failed(error);
        }
        naturalClose = null;
      } else {
        phase = Phase.CLOSED;
        pending.clear();
        naturalClose = coordinator.finishNaturalClose(owner);
      }
    }
    if (batch == null) {
      if (naturalClose != null) {
        naturalClose.complete(null);
      }
      return CompletableFuture.completedFuture(done());
    }
    // An empty batch: keep reading.
    return readLoop();
  }

  private void appendBatch(List<V> target, B batch) {
    switch (projection.kind) {
      case PUBLICATION:
        Optional<V> value = projection.publish(batch);
        if (value.isPresent()) {
          target.add(value.get());
        }
        break;
      case ONE:
        target.add(projection.one(batch));
        break;
      default:
        target.addAll(projection.many(batch));
    }
  }

  /** Closes the subscription; with {@code drain}, returns the unread values in this view's form. */
  CompletableFuture<List<V>> startClose(boolean drain, boolean markedGuard) {
    if (drain && !markedGuard) {
      RuntimeException deliveryError = coordinator.readDeliveryError(readMode);
      if (deliveryError != null) {
        // A format error never discards an unread SDK failure: drain through the marked API,
        // then report the validation error only if that succeeded.
        final RuntimeException formatError = deliveryError;
        return startClose(true, true).thenCompose(ignored -> Futures.<List<V>>failed(formatError));
      }
      RuntimeException mismatch = coordinator.readModeMismatch(readMode);
      if (mismatch != null) {
        CompletableFuture<List<V>> cleanup =
            coordinator.canClose() ? startClose(false, false) : waitForSharedClose();
        return rejectAfterCleanup(mismatch, cleanup);
      }
    }
    final CompletableFuture<List<V>> closePromise = new CompletableFuture<List<V>>();
    final CompletableFuture<Void> readBarrier;
    synchronized (lock) {
      if (drain && !markedGuard) {
        try {
          coordinator.claimReadMode(readMode);
        } catch (RuntimeException error) {
          return Futures.failed(error);
        }
      }
      if (closeInFlight != null) {
        return closeInFlight;
      }
      BeginClose begin = null;
      if (coordinator.canClose()) {
        phase = Phase.CLOSING;
        closeDrain = drain;
        if (drain) {
          closingPending.addAll(pending);
        }
        pending.clear();
        begin = coordinator.beginClose(owner);
      }
      if (begin == null || !begin.started) {
        readBarrier = null;
      } else {
        readBarrier = begin.barrier;
        closeInFlight = closePromise;
      }
    }
    if (readBarrier == null) {
      return waitForSharedClose();
    }
    CompletableFuture<List<B>> nativeClose;
    try {
      nativeClose =
          markedGuard
              ? coordinator.drainMarkedForGuard().thenApply(ignored -> (List<B>) null)
              : closeNative.apply(drain);
    } catch (Throwable error) {
      nativeClose = Futures.failed(error);
    }
    final boolean draining = drain;
    Futures.onComplete(
        nativeClose,
        (batches, nativeFailure) ->
            Futures.onComplete(
                readBarrier,
                (ignored, ignoredError) ->
                    completeClose(closePromise, batches, nativeFailure, draining)));
    Futures.onComplete(closePromise, (ignored, ignoredError) -> forgetClose(closePromise));
    return closePromise;
  }

  /** Forgets a finished close, unless a newer close has replaced it. */
  @SuppressWarnings("ReferenceEquality")
  private void forgetClose(CompletableFuture<List<V>> closePromise) {
    synchronized (lock) {
      // Suppressed: identity on purpose. Futures have no value equality (Error Prone's
      // UndefinedEquals); the question is whether this very close is still in flight.
      if (closeInFlight == closePromise) {
        closeInFlight = null;
      }
    }
  }

  private void completeClose(
      CompletableFuture<List<V>> closePromise,
      List<B> nativeBatches,
      Throwable nativeFailure,
      boolean drain) {
    Throwable nativeError = null;
    boolean retryableDrain = false;
    if (nativeFailure != null) {
      Throwable failure = Futures.unwrap(nativeFailure);
      retryableDrain = drain && failure instanceof ProjectionError;
      nativeError =
          failure instanceof ProjectionError ? ((ProjectionError) failure).original() : failure;
    }
    Throwable readError = coordinator.closeReadError();
    if (readError != null) {
      retryableDrain = false;
    }
    Throwable closeError = null;
    List<V> result = Collections.emptyList();
    if (nativeError != null && readError != null) {
      // withSuppressed returns the read error unchanged when it is also the native error.
      closeError = SubscriptionCoordinator.withSuppressed(readError, nativeError);
    } else if (nativeError != null) {
      closeError = nativeError;
    } else if (readError != null) {
      closeError = readError;
    } else if (drain) {
      try {
        List<V> drained;
        synchronized (lock) {
          drained = new ArrayList<V>(closingPending);
        }
        if (nativeBatches != null) {
          for (B batch : nativeBatches) {
            appendBatch(drained, batch);
          }
        }
        result = Collections.unmodifiableList(drained);
      } catch (Throwable error) {
        closeError = error;
      }
    }
    CompletableFuture<Throwable> closed;
    synchronized (lock) {
      phase = Phase.CLOSED;
      closeDrain = false;
      pending.clear();
      closingPending.clear();
      closed = coordinator.finishClose(retryableDrain);
    }
    if (closed != null) {
      closed.complete(closeError);
    }
    coordinator.emitWarningsQuietly();
    if (closeError != null) {
      closePromise.completeExceptionally(closeError);
    } else {
      closePromise.complete(result);
    }
  }

  private CompletableFuture<List<V>> waitForSharedClose() {
    final CompletableFuture<List<V>> result = new CompletableFuture<List<V>>();
    Futures.onComplete(
        coordinator.whenClosed(),
        (ignored, error) -> {
          synchronized (lock) {
            phase = Phase.CLOSED;
            closeDrain = false;
            pending.clear();
            closingPending.clear();
          }
          if (error != null) {
            result.completeExceptionally(Futures.unwrap(error));
          } else {
            result.complete(Collections.<V>emptyList());
          }
        });
    return result;
  }

  /**
   * Fails with {@code primary} after {@code cleanup} settled, carrying its failure as suppressed.
   */
  private static <T> CompletableFuture<T> rejectAfterCleanup(
      final Throwable primary, CompletableFuture<?> cleanup) {
    final CompletableFuture<T> result = new CompletableFuture<T>();
    Futures.onComplete(
        cleanup,
        (ignored, error) -> {
          if (error != null) {
            SubscriptionCoordinator.withSuppressed(primary, Futures.unwrap(error));
          }
          result.completeExceptionally(primary);
        });
    return result;
  }

  /** A read whose cancellation closes the subscription (the JavaScript abort). */
  private static final class ReadFuture<V> extends CompletableFuture<Optional<V>> {
    private final ViewIterator<?, V> iterator;

    ReadFuture(ViewIterator<?, V> iterator) {
      this.iterator = iterator;
    }

    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public boolean cancel(boolean mayInterruptIfRunning) {
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (cancelled) {
        // Suppressed: nothing waits on this close. Its failures surface through the shared close
        // to later closers.
        iterator.startClose(false, false);
      }
      return cancelled;
    }
  }
}
