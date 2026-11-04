package io.github.underloam.xbbg.table;

import java.util.OptionalInt;

/** Per-security delayed-data flag from the {@code xbbg.delayed} result metadata. */
public final class DelayedDataInfo {
  private final boolean delayed;
  private final Integer delayMinutes;

  /**
   * Creates a value.
   *
   * @param delayed Bloomberg's {@code IS_DELAYED_STREAM} for the requesting session
   * @param delayMinutes minutes behind real time, or {@code null} when real-time or unknown
   */
  public DelayedDataInfo(boolean delayed, Integer delayMinutes) {
    this.delayed = delayed;
    this.delayMinutes = delayMinutes;
  }

  /**
   * Whether Bloomberg delivers delayed data for the security.
   *
   * @return Bloomberg's {@code IS_DELAYED_STREAM}
   */
  public boolean delayed() {
    return delayed;
  }

  /**
   * Returns how far behind real time the data is, from {@code EXCHANGE_DELAY}.
   *
   * @return the delay in minutes, or empty when real-time or unparseable
   */
  public OptionalInt delayMinutes() {
    return delayMinutes == null ? OptionalInt.empty() : OptionalInt.of(delayMinutes.intValue());
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof DelayedDataInfo)) {
      return false;
    }
    DelayedDataInfo that = (DelayedDataInfo) other;
    return delayed == that.delayed
        && (delayMinutes == null
            ? that.delayMinutes == null
            : delayMinutes.equals(that.delayMinutes));
  }

  @Override
  public int hashCode() {
    return 31 * Boolean.valueOf(delayed).hashCode() + (delayMinutes == null ? 0 : delayMinutes);
  }

  @Override
  public String toString() {
    return "DelayedDataInfo{delayed=" + delayed + ", delayMinutes=" + delayMinutes + "}";
  }
}
