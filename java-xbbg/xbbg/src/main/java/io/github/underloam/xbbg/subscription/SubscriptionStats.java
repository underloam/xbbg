package io.github.underloam.xbbg.subscription;

/** Totals over every topic's delivery metrics, as one snapshot. */
public final class SubscriptionStats {
  private final long messagesReceived;
  private final long droppedBatches;
  private final long batchesSent;
  private final boolean slowConsumer;

  SubscriptionStats(
      long messagesReceived, long droppedBatches, long batchesSent, boolean slowConsumer) {
    this.messagesReceived = messagesReceived;
    this.droppedBatches = droppedBatches;
    this.batchesSent = batchesSent;
    this.slowConsumer = slowConsumer;
  }

  /**
   * Returns the Bloomberg messages received.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long messagesReceived() {
    return messagesReceived;
  }

  /**
   * Returns the updates dropped by overflow handling (the historical name counts updates).
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long droppedBatches() {
    return droppedBatches;
  }

  /**
   * Returns the updates handed to this consumer's queue.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long batchesSent() {
    return batchesSent;
  }

  /**
   * Whether any topic is flagged as a slow consumer.
   *
   * @return the flag
   */
  public boolean slowConsumer() {
    return slowConsumer;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SubscriptionStats)) {
      return false;
    }
    SubscriptionStats that = (SubscriptionStats) other;
    return messagesReceived == that.messagesReceived
        && droppedBatches == that.droppedBatches
        && batchesSent == that.batchesSent
        && slowConsumer == that.slowConsumer;
  }

  @Override
  public int hashCode() {
    int hash = Long.hashCode(messagesReceived);
    hash = 31 * hash + Long.hashCode(droppedBatches);
    hash = 31 * hash + Long.hashCode(batchesSent);
    return 31 * hash + Boolean.hashCode(slowConsumer);
  }

  @Override
  public String toString() {
    return "SubscriptionStats{messagesReceived="
        + messagesReceived
        + ", droppedBatches="
        + droppedBatches
        + ", batchesSent="
        + batchesSent
        + ", slowConsumer="
        + slowConsumer
        + "}";
  }
}
