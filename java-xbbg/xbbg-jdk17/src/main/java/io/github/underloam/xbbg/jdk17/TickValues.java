package io.github.underloam.xbbg.jdk17;

import io.github.underloam.xbbg.subscription.FieldHandle;
import io.github.underloam.xbbg.subscription.Tick;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

/** Reads {@link Tick} fields as {@link TickValue}s. */
public final class TickValues {
  private TickValues() {}

  /**
   * Returns a field of an update, typed by its layout kind ({@link Tick#fieldKind(String)}).
   *
   * @param tick the update
   * @param field the field name
   * @return the value ({@link TickValue.Cleared} for an explicit clear), or empty when the update
   *     does not carry the field
   */
  public static Optional<TickValue> get(Tick tick, String field) {
    Objects.requireNonNull(tick, "tick");
    Objects.requireNonNull(field, "field");
    if (!tick.has(field)) {
      return Optional.empty();
    }
    return Optional.of(classify(tick.get(field), tick.fieldKind(field).orElse("unknown")));
  }

  /**
   * Returns a field of an update, typed by its layout kind ({@link Tick#fieldKind(FieldHandle)}).
   *
   * @param tick the update
   * @param field the field handle
   * @return the value ({@link TickValue.Cleared} for an explicit clear), or empty when the update
   *     does not carry the field
   */
  public static Optional<TickValue> get(Tick tick, FieldHandle field) {
    Objects.requireNonNull(tick, "tick");
    Objects.requireNonNull(field, "field");
    if (!tick.has(field)) {
      return Optional.empty();
    }
    return Optional.of(classify(tick.get(field), tick.fieldKind(field).orElse("unknown")));
  }

  private static TickValue classify(Object value, String kind) {
    if (value == null) {
      return new TickValue.Cleared();
    }
    if (value instanceof Boolean flag) {
      return new TickValue.BoolValue(flag);
    }
    if (value instanceof Integer number) {
      return new TickValue.IntValue(number);
    }
    if (value instanceof Long number) {
      return switch (kind) {
        case "time64_us" -> new TickValue.TimeValue(number);
        case "timestamp_us" -> new TickValue.TimestampValue(number);
        default -> new TickValue.LongValue(number);
      };
    }
    if (value instanceof Double number) {
      return new TickValue.DoubleValue(number);
    }
    if (value instanceof String text) {
      return new TickValue.StringValue(text);
    }
    if (value instanceof LocalDate date) {
      return new TickValue.DateValue(date);
    }
    throw new IllegalStateException(
        "unexpected tick value type " + value.getClass().getName() + " for kind " + kind);
  }
}
