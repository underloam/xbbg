package io.github.underloam.xbbg.subscription;

/**
 * Updates intentionally evicted from this consumer's queue before a delivery ({@code drop_oldest}
 * overflow). The gap is never part of the delivery's data or metadata.
 */
public final class SubscriptionGap {
  /** The only cause today: the consumer's local queue overflowed. */
  public static final String LOCAL_OVERFLOW = "local_overflow";

  private final String cause;
  private final long droppedUpdates;

  SubscriptionGap(String cause, long droppedUpdates) {
    this.cause = cause;
    this.droppedUpdates = droppedUpdates;
  }

  /**
   * Returns why updates were dropped.
   *
   * @return {@code "local_overflow"}
   */
  public String cause() {
    return cause;
  }

  /**
   * Returns the exact number of evicted source updates (not rows or batches). The native counter is
   * an unsigned 64-bit integer; use {@link Long#toUnsignedString(long)} for values above {@link
   * Long#MAX_VALUE}.
   *
   * @return the count, as an unsigned 64-bit value
   */
  public long droppedUpdates() {
    return droppedUpdates;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SubscriptionGap)) {
      return false;
    }
    SubscriptionGap that = (SubscriptionGap) other;
    return cause.equals(that.cause) && droppedUpdates == that.droppedUpdates;
  }

  @Override
  public int hashCode() {
    return 31 * cause.hashCode() + Long.hashCode(droppedUpdates);
  }

  @Override
  public String toString() {
    return "SubscriptionGap{cause="
        + cause
        + ", droppedUpdates="
        + Long.toUnsignedString(droppedUpdates)
        + "}";
  }
}
