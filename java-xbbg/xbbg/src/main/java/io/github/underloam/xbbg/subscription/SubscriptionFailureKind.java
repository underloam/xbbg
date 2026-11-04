package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;

/** How a topic left the subscription in a {@link SubscriptionFailure}. */
public enum SubscriptionFailureKind {
  /** Bloomberg rejected or failed the topic. */
  FAILURE("failure"),
  /** Bloomberg terminated an established topic. */
  TERMINATED("terminated");

  private final String wireValue;

  SubscriptionFailureKind(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the native name.
   *
   * @return the lower-case kind name
   */
  public String wireValue() {
    return wireValue;
  }

  static SubscriptionFailureKind fromWire(String value) {
    for (SubscriptionFailureKind kind : values()) {
      if (kind.wireValue.equals(value)) {
        return kind;
      }
    }
    throw JsonFields.malformed("failure kind", "is unknown: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
