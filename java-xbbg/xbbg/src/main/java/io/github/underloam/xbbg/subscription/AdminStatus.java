package io.github.underloam.xbbg.subscription;

import java.util.Objects;
import java.util.OptionalLong;

/** Slow-consumer and data-loss counters from Bloomberg admin messages. */
public final class AdminStatus {
  private final boolean slowConsumerWarningActive;
  private final long slowConsumerWarningCount;
  private final long slowConsumerClearedCount;
  private final long dataLossCount;
  private final Long lastWarningUs;
  private final Long lastClearedUs;
  private final Long lastDataLossUs;

  AdminStatus(
      boolean slowConsumerWarningActive,
      long slowConsumerWarningCount,
      long slowConsumerClearedCount,
      long dataLossCount,
      Long lastWarningUs,
      Long lastClearedUs,
      Long lastDataLossUs) {
    this.slowConsumerWarningActive = slowConsumerWarningActive;
    this.slowConsumerWarningCount = slowConsumerWarningCount;
    this.slowConsumerClearedCount = slowConsumerClearedCount;
    this.dataLossCount = dataLossCount;
    this.lastWarningUs = lastWarningUs;
    this.lastClearedUs = lastClearedUs;
    this.lastDataLossUs = lastDataLossUs;
  }

  /**
   * Whether a slow-consumer warning is active.
   *
   * @return the flag
   */
  public boolean slowConsumerWarningActive() {
    return slowConsumerWarningActive;
  }

  /**
   * Returns the slow-consumer warnings received.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long slowConsumerWarningCount() {
    return slowConsumerWarningCount;
  }

  /**
   * Returns the slow-consumer warnings cleared.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long slowConsumerClearedCount() {
    return slowConsumerClearedCount;
  }

  /**
   * Returns the data-loss messages received.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long dataLossCount() {
    return dataLossCount;
  }

  /**
   * Returns when the last slow-consumer warning arrived.
   *
   * @return microseconds since the epoch, or empty
   */
  public OptionalLong lastWarningUs() {
    return optional(lastWarningUs);
  }

  /**
   * Returns when the last slow-consumer warning cleared.
   *
   * @return microseconds since the epoch, or empty
   */
  public OptionalLong lastClearedUs() {
    return optional(lastClearedUs);
  }

  /**
   * Returns when the last data loss was reported.
   *
   * @return microseconds since the epoch, or empty
   */
  public OptionalLong lastDataLossUs() {
    return optional(lastDataLossUs);
  }

  private static OptionalLong optional(Long value) {
    return value == null ? OptionalLong.empty() : OptionalLong.of(value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof AdminStatus)) {
      return false;
    }
    AdminStatus that = (AdminStatus) other;
    return slowConsumerWarningActive == that.slowConsumerWarningActive
        && slowConsumerWarningCount == that.slowConsumerWarningCount
        && slowConsumerClearedCount == that.slowConsumerClearedCount
        && dataLossCount == that.dataLossCount
        && Objects.equals(lastWarningUs, that.lastWarningUs)
        && Objects.equals(lastClearedUs, that.lastClearedUs)
        && Objects.equals(lastDataLossUs, that.lastDataLossUs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        slowConsumerWarningActive,
        slowConsumerWarningCount,
        slowConsumerClearedCount,
        dataLossCount,
        lastWarningUs,
        lastClearedUs,
        lastDataLossUs);
  }

  @Override
  public String toString() {
    return "AdminStatus{slowConsumerWarningActive="
        + slowConsumerWarningActive
        + ", slowConsumerWarningCount="
        + slowConsumerWarningCount
        + ", slowConsumerClearedCount="
        + slowConsumerClearedCount
        + ", dataLossCount="
        + dataLossCount
        + (lastWarningUs == null ? "" : ", lastWarningUs=" + lastWarningUs)
        + (lastClearedUs == null ? "" : ", lastClearedUs=" + lastClearedUs)
        + (lastDataLossUs == null ? "" : ", lastDataLossUs=" + lastDataLossUs)
        + "}";
  }
}
