package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.SubscriptionDeliveryMode;
import java.util.Optional;

/**
 * One marked delivery: a batch of sparse deltas or current-state images, in the representation of
 * the {@link io.github.underloam.xbbg.Backend} it was read with, plus the continuity gap that
 * precedes it.
 *
 * <p>Delta data has the columns {@code timestamp}, {@code topic}, the data fields, then a binary
 * {@code __xbbg_present} column whose bit {@code i} (least significant first) marks schema field
 * {@code i + 2} as present; a present null is a clear. State data has {@code topic}, {@code
 * last_update}, {@code live}, {@code delayed}, {@code valid}, then the fields.
 *
 * @param <T> the data representation
 */
public final class SubscriptionDelivery<T> {
  private final SubscriptionDeliveryMode kind;
  private final T data;
  private final SubscriptionGap gapBefore;

  SubscriptionDelivery(SubscriptionDeliveryMode kind, T data, SubscriptionGap gapBefore) {
    this.kind = kind;
    this.data = data;
    this.gapBefore = gapBefore;
  }

  /**
   * Returns whether this delivery holds deltas or state images.
   *
   * @return the delivery kind
   */
  public SubscriptionDeliveryMode kind() {
    return kind;
  }

  /**
   * Returns the delivery's rows.
   *
   * @return the data
   */
  public T data() {
    return data;
  }

  /**
   * Returns the updates dropped just before this delivery ({@code drop_oldest} only; state
   * deliveries never report gaps).
   *
   * @return the gap, or empty when the stream is continuous
   */
  public Optional<SubscriptionGap> gapBefore() {
    return Optional.ofNullable(gapBefore);
  }

  @Override
  public String toString() {
    return "SubscriptionDelivery{kind="
        + kind
        + (gapBefore == null ? "" : ", gapBefore=" + gapBefore)
        + ", data="
        + data
        + "}";
  }
}
