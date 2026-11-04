package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.Backend;
import io.github.underloam.xbbg.BlpInternalException;
import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import io.github.underloam.xbbg.internal.Backends;
import io.github.underloam.xbbg.internal.Futures;
import io.github.underloam.xbbg.internal.NativeCleaner;
import io.github.underloam.xbbg.internal.NativeSubscription;
import io.github.underloam.xbbg.internal.SubscriptionBridge;
import io.github.underloam.xbbg.internal.TickBatch;
import io.github.underloam.xbbg.subscription.SubscriptionCoordinator.ReadMode;
import io.github.underloam.xbbg.table.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A live subscription, read as scalar {@link Tick}s (this view), Arrow tables ({@link #arrow()}) or
 * marked deliveries ({@link #deliveries(Backend)}).
 *
 * <p>Each read returns the updates already queued, up to {@code flushThreshold} per native read,
 * and never waits for more to arrive. One subscription is read in one format; reads are serialized
 * across views and closing any view closes every view. A terminal stream failure (data loss,
 * session termination) is delivered after the data received before it, then the stream ends.
 * Cancelling a pending {@link #nextAsync()} closes the subscription.
 *
 * <p>Membership changes ({@link #add}, {@link #addFields}, {@link #remove}) and {@link #latest()}
 * do not depend on the read format. Delayed-data and rejected-field events are reported through
 * {@link io.github.underloam.xbbg.XbbgWarnings} after every native read, control call and close.
 *
 * <p>Close the subscription when done ({@link #close()} or {@link #unsubscribe(boolean)}); an
 * unreachable subscription releases its native resources.
 */
public final class Subscription extends SubscriptionView<Tick> {
  static {
    SubscriptionBridge.installViews(Subscription::new);
  }

  private final SubscriptionCoordinator coordinator;
  private final NativeSubscription inner;
  private ArrowSubscription arrowView;
  private final Map<Backend<?>, DeliveryStream<?>> deliveryViews =
      new HashMap<Backend<?>, DeliveryStream<?>>();

  Subscription(NativeSubscription inner) {
    this(new SubscriptionCoordinator(inner), new ScalarProjection());
  }

  private Subscription(SubscriptionCoordinator coordinator, ScalarProjection projection) {
    super(
        new ViewIterator<TickBatch, Tick>(
            coordinator,
            coordinator.inner::nextTicks,
            coordinator.inner::unsubscribeTicks,
            projection,
            ReadMode.SCALAR));
    this.coordinator = coordinator;
    this.inner = coordinator.inner;
    // Every view holds the coordinator, so the handle lives while any view is reachable.
    NativeCleaner.register(coordinator, inner::release);
    coordinator.emitWarnings();
  }

  /** Decodes native batches into ticks, caching each layout by version. */
  private static final class ScalarProjection extends Projection<TickBatch, Tick> {
    private final Map<Integer, TickLayout> layouts = new ConcurrentHashMap<Integer, TickLayout>();

    ScalarProjection() {
      super(Kind.MANY);
    }

    @Override
    List<Tick> many(TickBatch batch) {
      try {
        batch.validate();
      } catch (IllegalStateException malformed) {
        throw new BlpInternalException(malformed.getMessage());
      }
      if (batch.hasLayout()) {
        layouts.put(
            batch.layoutVersion(),
            new TickLayout(batch.layoutVersion(), batch.layoutFields(), batch.layoutKinds()));
      }
      int rows = batch.rowCount();
      if (rows == 0) {
        return new ArrayList<Tick>(0);
      }
      TickLayout layout = layouts.get(batch.layoutVersion());
      if (layout == null) {
        throw new BlpInternalException(
            "subscription layout " + batch.layoutVersion() + " was not supplied by native");
      }
      List<Tick> ticks = new ArrayList<Tick>(rows);
      for (int row = 0; row < rows; row++) {
        ticks.add(new Tick(batch, layout, row));
      }
      return ticks;
    }
  }

  /**
   * Returns a reusable handle for field lookups on {@link Tick}s.
   *
   * @param name the field name
   * @return a new handle
   */
  public FieldHandle field(String name) {
    return FieldHandle.of(name);
  }

  /**
   * Returns the Arrow view of this subscription (one instance per subscription).
   *
   * @return the Arrow view
   */
  public synchronized ArrowSubscription arrow() {
    if (arrowView == null) {
      arrowView = new ArrowSubscription(coordinator);
    }
    return arrowView;
  }

  /**
   * Returns the marked-delivery view with {@link Table} data.
   *
   * @return the delivery view
   */
  public DeliveryStream<Table> deliveries() {
    return deliveries(Backend.TABLE);
  }

  /**
   * Returns the marked-delivery view for a backend (one instance per backend). Views with different
   * backends share the {@code delivery} read format and the same deliveries.
   *
   * @param backend the data representation
   * @param <T> the data type
   * @return the delivery view
   */
  // Unchecked: deliveryViews maps each backend to the DeliveryStream built for that very backend
  // below, so the view for a Backend<T> is a DeliveryStream<T>.
  @SuppressWarnings("unchecked")
  public synchronized <T> DeliveryStream<T> deliveries(Backend<T> backend) {
    if (backend == null) {
      throw new NullPointerException("backend");
    }
    DeliveryStream<?> view = deliveryViews.get(backend);
    if (view == null) {
      view = new DeliveryStream<T>(coordinator, backend);
      deliveryViews.put(backend, view);
    }
    return (DeliveryStream<T>) view;
  }

  /**
   * Adds upstream topics.
   *
   * @param tickers the topics
   * @return completion
   */
  public CompletableFuture<Void> add(Collection<String> tickers) {
    return add(tickers, null);
  }

  /**
   * Adds upstream topics with consumer labels. For an auction subscription, add venue topics with
   * aliases.
   *
   * @param tickers the topics
   * @param aliases topic to label, or {@code null}
   * @return completion; fails with {@link io.github.underloam.xbbg.BlpValidationException} when the
   *     subscription is closed or an alias is invalid
   */
  public CompletableFuture<Void> add(Collection<String> tickers, Map<String, String> aliases) {
    List<String> topics;
    Map<String, String> labels;
    try {
      topics = copy(tickers, "tickers");
      labels =
          aliases == null
              ? Collections.<String, String>emptyMap()
              : StreamOptions.copyAliases(aliases);
    } catch (RuntimeException error) {
      return Futures.failed(error);
    }
    return withWarnings(inner.add(topics, labels));
  }

  /**
   * Grows this consumer's field projection and the shared feed's field union (the union never
   * shrinks while the feed lives).
   *
   * @param fields the fields
   * @return completion
   */
  public CompletableFuture<Void> addFields(Collection<String> fields) {
    List<String> copy;
    try {
      copy = copy(fields, "fields");
    } catch (RuntimeException error) {
      return Futures.failed(error);
    }
    return withWarnings(inner.addFields(copy));
  }

  /**
   * Removes topics by consumer label; siblings sharing the feed are unaffected.
   *
   * @param tickers the labels
   * @return completion
   */
  public CompletableFuture<Void> remove(Collection<String> tickers) {
    List<String> labels;
    try {
      labels = copy(tickers, "tickers");
    } catch (RuntimeException error) {
      return Futures.failed(error);
    }
    return withWarnings(inner.remove(labels));
  }

  private static List<String> copy(Collection<String> values, String what) {
    if (values == null) {
      throw new NullPointerException(what);
    }
    return Collections.unmodifiableList(new ArrayList<String>(values));
  }

  private CompletableFuture<Void> withWarnings(CompletableFuture<Void> call) {
    final CompletableFuture<Void> result = new CompletableFuture<Void>();
    Futures.onComplete(
        call,
        (ignored, error) -> {
          coordinator.emitWarningsQuietly();
          if (error != null) {
            result.completeExceptionally(Futures.unwrap(error));
          } else {
            result.complete(null);
          }
        });
    return result;
  }

  /**
   * Returns the latest image of every active label (the last known values; it consumes nothing and
   * does not fix the read format).
   *
   * @return a table with {@code topic}, {@code last_update}, {@code live}, {@code delayed}, then
   *     the fields; unknown values and clears are null
   */
  public Table latest() {
    return latest(Backend.TABLE);
  }

  /**
   * Returns the latest image of every active label in a backend's representation.
   *
   * @param backend the representation
   * @param <T> the result type
   * @return the latest images
   * @throws io.github.underloam.xbbg.BlpValidationException if the subscription is closed
   */
  public <T> T latest(Backend<T> backend) {
    try {
      return Backends.fromNative(backend, inner.latest(Backends.nativeKind(backend)));
    } finally {
      coordinator.emitWarningsQuietly();
    }
  }

  /**
   * Returns the delivery mode.
   *
   * @return {@code deltas} or {@code state}
   */
  public SubscriptionDeliveryMode delivery() {
    return inner.delivery();
  }

  /**
   * Returns the consumer labels.
   *
   * @return the labels
   */
  public List<String> tickers() {
    return StatusJson.strings(inner.snapshot("tickers"), "tickers");
  }

  /**
   * Returns the live field projection.
   *
   * @return the fields (empty after close)
   */
  public List<String> fields() {
    return StatusJson.strings(inner.snapshot("fields"), "fields");
  }

  /**
   * Whether the subscription is open, streaming and has at least one active topic.
   *
   * @return the flag
   */
  public boolean isActive() {
    return inner.isActive();
  }

  /**
   * Returns delivery totals over every topic.
   *
   * @return the stats
   */
  public SubscriptionStats stats() {
    return StatusJson.stats(inner.snapshot("stats"));
  }

  /**
   * Returns every diagnostic as one consistent snapshot.
   *
   * @return the status
   */
  public SubscriptionStatus status() {
    return StatusJson.status(inner.snapshot("status"));
  }

  /**
   * Returns the status history (newest 128).
   *
   * @return the events
   */
  public List<SubscriptionEvent> events() {
    return StatusJson.events(inner.snapshot("events"));
  }

  /**
   * Returns the failed or terminated topics.
   *
   * @return the failures
   */
  public List<SubscriptionFailure> failures() {
    return StatusJson.failures(inner.snapshot("failures"));
  }

  /**
   * Returns the labels of {@link #failures()}.
   *
   * @return the failed labels
   */
  public List<String> failedTickers() {
    return StatusJson.strings(inner.snapshot("failedTickers"), "failedTickers");
  }

  /**
   * Returns the lifecycle of every consumer label.
   *
   * @return label to state
   */
  public Map<String, TopicState> topicStates() {
    return StatusJson.topicStates(inner.snapshot("topicStates"));
  }

  /**
   * Returns the fields Bloomberg rejected.
   *
   * @return label to field to error category
   */
  public Map<String, Map<String, String>> fieldErrors() {
    return StatusJson.fieldErrors(inner.snapshot("fieldErrors"));
  }

  /**
   * Returns the session state.
   *
   * @return the session status
   */
  public SessionStatus sessionStatus() {
    return StatusJson.session(inner.snapshot("sessionStatus"));
  }

  /**
   * Returns the availability of each service used.
   *
   * @return service URI to status
   */
  public Map<String, ServiceStatus> serviceStatus() {
    return StatusJson.services(inner.snapshot("serviceStatus"));
  }

  /**
   * Returns the slow-consumer and data-loss counters.
   *
   * @return the admin status
   */
  public AdminStatus adminStatus() {
    return StatusJson.admin(inner.snapshot("adminStatus"));
  }
}
