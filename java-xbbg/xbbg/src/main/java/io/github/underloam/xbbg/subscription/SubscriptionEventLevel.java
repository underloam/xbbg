package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;

/** Severity of a {@link SubscriptionEvent}. */
public enum SubscriptionEventLevel {
  /** Informational. */
  INFO("info"),
  /** Degraded but continuing. */
  WARNING("warning"),
  /** A failure. */
  ERROR("error");

  private final String wireValue;

  SubscriptionEventLevel(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the native name.
   *
   * @return the lower-case level name
   */
  public String wireValue() {
    return wireValue;
  }

  static SubscriptionEventLevel fromWire(String value) {
    for (SubscriptionEventLevel level : values()) {
      if (level.wireValue.equals(value)) {
        return level;
      }
    }
    throw JsonFields.malformed("event level", "is unknown: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
