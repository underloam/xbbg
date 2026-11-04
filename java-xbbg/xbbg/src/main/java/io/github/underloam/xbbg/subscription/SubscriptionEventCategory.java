package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;

/** What produced a {@link SubscriptionEvent}. */
public enum SubscriptionEventCategory {
  /** Session status messages ({@code SessionStarted}, {@code SessionTerminated}, ...). */
  SESSION("session"),
  /** Service status messages. */
  SERVICE("service"),
  /** Admin messages such as slow-consumer warnings and data loss. */
  ADMIN("admin"),
  /** Subscription status messages for a topic. */
  SUBSCRIPTION("subscription"),
  /** xbbg lifecycle events such as delayed streams, rejected fields and feed recovery. */
  LIFECYCLE("lifecycle");

  private final String wireValue;

  SubscriptionEventCategory(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the native name.
   *
   * @return the lower-case category name
   */
  public String wireValue() {
    return wireValue;
  }

  static SubscriptionEventCategory fromWire(String value) {
    for (SubscriptionEventCategory category : values()) {
      if (category.wireValue.equals(value)) {
        return category;
      }
    }
    throw JsonFields.malformed("event category", "is unknown: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
