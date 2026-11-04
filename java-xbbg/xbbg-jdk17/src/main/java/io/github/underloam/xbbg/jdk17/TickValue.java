package io.github.underloam.xbbg.jdk17;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

/**
 * A present field of a {@link io.github.underloam.xbbg.subscription.Tick}, typed by its native
 * layout kind, for exhaustive pattern matching. Read one with {@link TickValues#get}; an absent
 * field has no {@code TickValue} at all, and an explicit clear is {@link Cleared}.
 */
public sealed interface TickValue
    permits TickValue.BoolValue,
        TickValue.IntValue,
        TickValue.LongValue,
        TickValue.DoubleValue,
        TickValue.StringValue,
        TickValue.DateValue,
        TickValue.TimeValue,
        TickValue.TimestampValue,
        TickValue.Cleared {

  /**
   * A {@code bool} field.
   *
   * @param value the value
   */
  record BoolValue(boolean value) implements TickValue {}

  /**
   * An {@code i32} field.
   *
   * @param value the value
   */
  record IntValue(int value) implements TickValue {}

  /**
   * An {@code i64} field (or a 64-bit integer in a field of unknown kind).
   *
   * @param value the value
   */
  record LongValue(long value) implements TickValue {}

  /**
   * An {@code f64} field.
   *
   * @param value the value
   */
  record DoubleValue(double value) implements TickValue {}

  /**
   * A {@code str} field.
   *
   * @param value the value
   */
  record StringValue(String value) implements TickValue {
    /**
     * Validates the component.
     *
     * @param value the value
     */
    public StringValue {
      Objects.requireNonNull(value, "value");
    }
  }

  /**
   * A {@code date32} field.
   *
   * @param value the date
   */
  record DateValue(LocalDate value) implements TickValue {
    /**
     * Validates the component.
     *
     * @param value the date
     */
    public DateValue {
      Objects.requireNonNull(value, "value");
    }
  }

  /**
   * A {@code time64_us} field.
   *
   * @param micros microseconds since midnight
   */
  record TimeValue(long micros) implements TickValue {
    /**
     * Returns the time of day.
     *
     * @return the time
     * @throws DateTimeException if {@code micros} is outside one day
     */
    public LocalTime toLocalTime() {
      if (micros < 0 || micros >= 86_400_000_000L) {
        throw new DateTimeException("time of day out of range: " + micros + " microseconds");
      }
      return LocalTime.ofNanoOfDay(micros * 1_000L);
    }
  }

  /**
   * A {@code timestamp_us} field.
   *
   * @param micros microseconds since the epoch, UTC
   */
  record TimestampValue(long micros) implements TickValue {
    /**
     * Returns the instant.
     *
     * @return the instant
     */
    public Instant toInstant() {
      return Instant.ofEpochSecond(
          Math.floorDiv(micros, 1_000_000L), Math.floorMod(micros, 1_000_000L) * 1_000L);
    }
  }

  /** A field present as an explicit clear (a present {@code null}). */
  record Cleared() implements TickValue {}
}
