package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.subscription.ArrowSubscription;
import io.github.underloam.xbbg.subscription.DeliveryStream;
import io.github.underloam.xbbg.subscription.Subscription;
import io.github.underloam.xbbg.subscription.SubscriptionDelivery;
import io.github.underloam.xbbg.subscription.Tick;
import io.github.underloam.xbbg.table.Table;
import java.util.Objects;
import java.util.concurrent.Flow;

/**
 * {@link Flow.Publisher} adapters for subscription views, for use with any Reactive Streams library
 * ({@code FlowAdapters}, Reactor, RxJava, Mutiny, ...).
 *
 * <p>Each publisher is demand-driven: it reads the view with {@code nextAsync()} only while the
 * subscriber has outstanding demand, and never has more than one read in flight, so the view's
 * native queue (not the publisher) buffers updates the subscriber has not asked for yet. Signals
 * are serialized and run on the thread that completed the read (the callback executor, see {@link
 * io.github.underloam.xbbg.Xbbg#setCallbackExecutor}) or the thread that called {@code request}.
 *
 * <ul>
 *   <li>The end of the stream (after {@code unsubscribe}) signals {@code onComplete}.
 *   <li>A read failure signals {@code onError} with the original exception (for example a {@link
 *       io.github.underloam.xbbg.BlpSubscriptionDataLossException}), unwrapped from {@link
 *       java.util.concurrent.CompletionException}, and closes the subscription.
 *   <li>{@code cancel()} closes the subscription, like aborting a JavaScript async iterator; unread
 *       values are discarded.
 *   <li>Each publisher accepts one subscriber, since reading consumes the view; a second subscriber
 *       receives {@code onError(IllegalStateException)}.
 * </ul>
 *
 * <p>The publisher shares the view with its other readers: reading the same view elsewhere at the
 * same time splits the values between them, and closing the view completes the publisher.
 */
public final class XbbgFlow {
  private XbbgFlow() {}

  /**
   * Publishes a subscription's scalar ticks.
   *
   * @param subscription the subscription
   * @return a single-subscriber publisher
   */
  public static Flow.Publisher<Tick> ticks(Subscription subscription) {
    Objects.requireNonNull(subscription, "subscription");
    return publisher(FlowSource.of(subscription::nextAsync, subscription::unsubscribe));
  }

  /**
   * Publishes a subscription's Arrow batches ({@link Subscription#arrow()}).
   *
   * @param subscription the Arrow view
   * @return a single-subscriber publisher
   */
  public static Flow.Publisher<Table> arrow(ArrowSubscription subscription) {
    Objects.requireNonNull(subscription, "subscription");
    return publisher(FlowSource.of(subscription::nextAsync, subscription::unsubscribe));
  }

  /**
   * Publishes a subscription's marked deliveries ({@link Subscription#deliveries}). Map them with
   * {@link DeliveryView#of} for exhaustive pattern matching.
   *
   * @param deliveries the delivery view
   * @param <T> the data representation
   * @return a single-subscriber publisher
   */
  public static <T> Flow.Publisher<SubscriptionDelivery<T>> deliveries(
      DeliveryStream<T> deliveries) {
    Objects.requireNonNull(deliveries, "deliveries");
    return publisher(FlowSource.of(deliveries::nextAsync, deliveries::unsubscribe));
  }

  static <T> Flow.Publisher<T> publisher(FlowSource<T> source) {
    return new ViewPublisher<T>(source);
  }
}
