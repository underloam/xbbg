package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.BlpValidationException;
import io.github.underloam.xbbg.XbbgWarning;
import io.github.underloam.xbbg.XbbgWarnings;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.JsonFields;
import io.github.underloam.xbbg.internal.NativeDrain;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.ResultKind;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * State shared by every view over one native subscription (the JavaScript {@code
 * SubscriptionCoordinator}): the serialized read queue, the read-format claim, the close phase,
 * close observers and the late read error.
 *
 * <p>The coordinator's monitor guards this state and the state of every view. No user code and no
 * future completion runs while it is held. The coordinator is never exposed outside this package.
 */
final class SubscriptionCoordinator {
  private static final Logger LOG = Logger.getLogger("io.github.underloam.xbbg");

  enum Phase {
    OPEN,
    CLOSING,
    CLOSED,
    DRAIN_PENDING
  }

  enum ReadMode {
    SCALAR,
    ARROW,
    DELIVERY;

    String label() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  /** Told when another view starts closing (the owner is the closing view, or null). */
  interface CloseObserver {
    void closing(Object owner);
  }

  static final class BeginClose {
    final CompletableFuture<Void> barrier;
    final boolean started;

    BeginClose(CompletableFuture<Void> barrier, boolean started) {
      this.barrier = barrier;
      this.started = started;
    }
  }

  final NativeSubscription inner;
  private final boolean deliversRows;
  private final boolean requiresDeliveries;
  private Phase phase = Phase.OPEN;
  private ReadMode readMode;
  private final List<CloseObserver> closeObservers = new ArrayList<CloseObserver>();
  private CompletableFuture<Throwable> closed = new CompletableFuture<Throwable>();
  private Throwable lateReadError;
  private CompletableFuture<Void> readTail = CompletableFuture.completedFuture(null);

  SubscriptionCoordinator(NativeSubscription inner) {
    this.inner = inner;
    this.deliversRows = inner.deliversRows();
    this.requiresDeliveries = inner.requiresDeliveries();
  }

  /** The subscription running user code inside one of its reads on the current thread. */
  private static final ThreadLocal<SubscriptionCoordinator> IN_READ =
      new ThreadLocal<SubscriptionCoordinator>();

  /**
   * Runs user code that one of this subscription's reads is waiting on: a backend conversion, or
   * the completion of a read future, whose non-async dependent stages run right there. The thread
   * is marked so that blocking calls on this subscription from inside that code fail or return
   * early instead of waiting for the read that runs them.
   */
  <T> T insideRead(Supplier<T> userCode) {
    SubscriptionCoordinator outer = IN_READ.get();
    IN_READ.set(this);
    try {
      return userCode.get();
    } finally {
      if (outer == null) {
        IN_READ.remove();
      } else {
        IN_READ.set(outer);
      }
    }
  }

  /** Whether the current thread runs user code inside one of this subscription's reads. */
  boolean insideReadOnThisThread() {
    return IN_READ.get() == this;
  }

  /**
   * Runs {@code read} after every earlier read settled (success or failure); reads across all views
   * are serialized through this queue.
   */
  <T> CompletableFuture<T> enqueue(final Supplier<CompletableFuture<T>> read) {
    final CompletableFuture<T> result = new CompletableFuture<T>();
    CompletableFuture<Void> previous;
    synchronized (this) {
      previous = readTail;
      readTail = result.handle((value, error) -> null);
    }
    Futures.onComplete(
        previous,
        (ignored, ignoredError) -> {
          CompletableFuture<T> started;
          try {
            started = read.get();
          } catch (Throwable error) {
            started = Futures.failed(error);
          }
          Futures.onComplete(
              started,
              (value, error) -> {
                if (error != null) {
                  result.completeExceptionally(Futures.unwrap(error));
                } else {
                  result.complete(value);
                }
              });
        });
    return result;
  }

  /** Delivers the new delayed-data and rejected-field events as {@link XbbgWarning}s. */
  void emitWarnings() {
    Object events = JsonFields.parse(inner.takeWarnings(), "warnings");
    for (Object item : JsonFields.array(events, "warnings")) {
      Map<String, Object> event = JsonFields.object(item, "warning");
      String messageType = JsonFields.string(event, "messageType");
      String topic = JsonFields.optString(event, "topic");
      String detail = JsonFields.optString(event, "detail");
      boolean delayed = "DelayedStream".equals(messageType);
      XbbgWarnings.emit(
          new XbbgWarning(
              delayed ? XbbgWarnings.DELAYED_DATA_TYPE : "BlpFieldWarning",
              delayed ? "XBBG_DELAYED_STREAM" : "XBBG_FIELD_EXCEPTION",
              topic == null ? messageType : messageType + ": " + topic,
              detail));
    }
  }

  /** {@link #emitWarnings()} where a diagnostic failure must not replace the real outcome. */
  void emitWarningsQuietly() {
    try {
      emitWarnings();
    } catch (RuntimeException error) {
      LOG.log(Level.FINE, "subscription warnings could not be read", error);
    }
  }

  void ensureRowDelivery() {
    if (!deliversRows) {
      throw new BlpValidationException(
          "Subscription has rows=false; use latest() instead of reading rows", "rows");
    }
  }

  BlpValidationException readDeliveryError(ReadMode mode) {
    if (mode != ReadMode.DELIVERY && requiresDeliveries) {
      return new BlpValidationException(
          "This subscription requires deliveries(); plain row reads and drains would lose"
              + " delivery metadata",
          "delivery");
    }
    return null;
  }

  IllegalStateException readModeMismatch(ReadMode mode) {
    synchronized (this) {
      if (readMode != null && readMode != mode) {
        return new IllegalStateException(
            "subscription is already being read as "
                + readMode.label()
                + "; cannot also read as "
                + mode.label());
      }
      return null;
    }
  }

  /** Fixes the read format; call with the coordinator's monitor held. */
  synchronized void claimReadMode(ReadMode mode) {
    BlpValidationException deliveryError = readDeliveryError(mode);
    if (deliveryError != null) {
      throw deliveryError;
    }
    IllegalStateException mismatch = readModeMismatch(mode);
    if (mismatch != null) {
      throw mismatch;
    }
    readMode = mode;
  }

  boolean canClose() {
    synchronized (this) {
      return phase == Phase.OPEN || phase == Phase.DRAIN_PENDING;
    }
  }

  /** Drains through the marked native API (results discarded), for the plain-drain guard. */
  CompletableFuture<Void> drainMarkedForGuard() {
    return inner
        .unsubscribeDeliveries(true, ResultKind.TABLE)
        .thenApply((NativeDrain drain) -> {
          if (drain.conversionFailure() != null) {
            throw new ProjectionError(drain.conversionFailure());
          }
          if (drain.receipt() != 0) {
            inner.commitDrain(drain.receipt());
          }
          return null;
        });
  }

  Throwable closeReadError() {
    synchronized (this) {
      return lateReadError;
    }
  }

  void recordCloseReadError(Throwable error) {
    synchronized (this) {
      if (lateReadError == null) {
        lateReadError = error;
      }
    }
  }

  /** Registers a view's close observer; call with the coordinator's monitor held. */
  synchronized void observeClose(CloseObserver observer) {
    if (phase == Phase.OPEN) {
      closeObservers.add(observer);
    } else {
      observer.closing(null);
    }
  }

  /** Starts the shared close for {@code owner}; call with the coordinator's monitor held. */
  synchronized BeginClose beginClose(Object owner) {
    if (!(phase == Phase.OPEN || phase == Phase.DRAIN_PENDING)) {
      return new BeginClose(readTail, false);
    }
    if (phase == Phase.DRAIN_PENDING) {
      // The retained closed source is retried; no live reader can publish.
      closed = new CompletableFuture<Throwable>();
    }
    if (phase == Phase.OPEN) {
      inner.signalClosing();
    }
    phase = Phase.CLOSING;
    for (CloseObserver observer : closeObservers) {
      observer.closing(owner);
    }
    closeObservers.clear();
    return new BeginClose(readTail, true);
  }

  /**
   * Records a natural end of stream; call with the coordinator's monitor held and complete the
   * returned barrier (if any) with {@code null} after releasing it.
   */
  synchronized CompletableFuture<Throwable> finishNaturalClose(Object owner) {
    if (phase != Phase.OPEN) {
      return null;
    }
    phase = Phase.CLOSED;
    for (CloseObserver observer : closeObservers) {
      observer.closing(owner);
    }
    closeObservers.clear();
    return closed;
  }

  /**
   * Records the end of a close; call with the coordinator's monitor held and complete the returned
   * barrier (if any) with the close error after releasing it.
   */
  synchronized CompletableFuture<Throwable> finishClose(boolean retryableDrain) {
    if (phase == Phase.CLOSED) {
      return null;
    }
    phase = retryableDrain ? Phase.DRAIN_PENDING : Phase.CLOSED;
    lateReadError = null;
    closeObservers.clear();
    return closed;
  }

  /** Completes when the shared close completed; fails with its error. */
  CompletableFuture<Void> whenClosed() {
    CompletableFuture<Throwable> barrier;
    synchronized (this) {
      barrier = closed;
    }
    final CompletableFuture<Void> result = new CompletableFuture<Void>();
    Futures.onComplete(
        barrier,
        (error, unexpected) -> {
          Throwable failure = unexpected != null ? Futures.unwrap(unexpected) : error;
          if (failure != null) {
            result.completeExceptionally(failure);
          } else {
            result.complete(null);
          }
        });
    return result;
  }

  /** Attaches {@code secondary} to {@code primary} once (the JavaScript {@code AggregateError}). */
  @SuppressWarnings("ReferenceEquality")
  static Throwable withSuppressed(Throwable primary, Throwable secondary) {
    // Suppressed: identity on purpose, as in Throwable.addSuppressed. "Once" means this very
    // instance; an exception type that overrides equals must still be attached.
    if (secondary != null && secondary != primary) {
      for (Throwable existing : primary.getSuppressed()) {
        if (existing == secondary) {
          return primary;
        }
      }
      primary.addSuppressed(secondary);
    }
    return primary;
  }
}
