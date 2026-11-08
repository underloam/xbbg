package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.subscription.SubscriptionDelivery;
import io.github.underloam.xbbg.subscription.SubscriptionGap;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link SubscriptionDelivery} as a sealed type, for exhaustive pattern matching:
 *
 * <pre>{@code
 * switch (DeliveryView.of(delivery)) {
 *   case DeliveryView.Deltas<Table> d -> apply(d.data(), d.gapBefore());
 *   case DeliveryView.State<Table> s -> replace(s.data());
 * }
 * }</pre>
 *
 * (pattern matching for {@code switch} needs Java 21; on 17 use {@code instanceof} patterns).
 *
 * @param <T> the data representation
 */
public sealed interface DeliveryView<T> permits DeliveryView.Deltas, DeliveryView.State {
  /**
   * Returns the delivery's rows.
   *
   * @return the data
   */
  T data();

  /**
   * Converts a delivery.
   *
   * @param delivery the delivery
   * @param <T> the data representation
   * @return {@link Deltas} or {@link State}, by {@link SubscriptionDelivery#kind()}
   */
  static <T> DeliveryView<T> of(SubscriptionDelivery<T> delivery) {
    Objects.requireNonNull(delivery, "delivery");
    return switch (delivery.kind()) {
      case DELTAS -> new Deltas<T>(delivery.data(), delivery.gapBefore());
      case STATE -> new State<T>(delivery.data());
    };
  }

  /**
   * A batch of sparse deltas (columns {@code timestamp}, {@code topic}, the fields, then {@code
   * __xbbg_present}).
   *
   * @param data the delivery's rows
   * @param gapBefore the updates dropped just before this delivery ({@code drop_oldest} only), or
   *     empty when the stream is continuous
   * @param <T> the data representation
   */
  record Deltas<T>(T data, Optional<SubscriptionGap> gapBefore) implements DeliveryView<T> {
    /**
     * Validates the components.
     *
     * @param data the delivery's rows
     * @param gapBefore the gap, or empty
     */
    public Deltas {
      Objects.requireNonNull(data, "data");
      Objects.requireNonNull(gapBefore, "gapBefore");
    }
  }

  /**
   * A batch of current-state images for dirty topics (columns {@code topic}, {@code last_update},
   * {@code live}, {@code delayed}, {@code valid}, then the fields). State deliveries never report
   * gaps.
   *
   * @param data the delivery's rows
   * @param <T> the data representation
   */
  record State<T>(T data) implements DeliveryView<T> {
    /**
     * Validates the component.
     *
     * @param data the delivery's rows
     */
    public State {
      Objects.requireNonNull(data, "data");
    }
  }
}
