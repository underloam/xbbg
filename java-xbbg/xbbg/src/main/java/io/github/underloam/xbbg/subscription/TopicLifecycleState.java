package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;

/** Lifecycle of one subscribed topic in a {@link TopicState}. */
public enum TopicLifecycleState {
  /** Requested, not yet confirmed. */
  PENDING("pending"),
  /** Confirmed by Bloomberg, no data yet. */
  STARTED("started"),
  /** Receiving data. */
  STREAMING("streaming"),
  /** Removal in progress. */
  UNSUBSCRIBING("unsubscribing"),
  /** Removed. */
  UNSUBSCRIBED("unsubscribed"),
  /** Rejected or failed. */
  FAILED("failed"),
  /** Terminated by Bloomberg. */
  TERMINATED("terminated");

  private final String wireValue;

  TopicLifecycleState(String wireValue) {
    this.wireValue = wireValue;
  }

  /**
   * Returns the native name.
   *
   * @return the lower-case state name
   */
  public String wireValue() {
    return wireValue;
  }

  static TopicLifecycleState fromWire(String value) {
    for (TopicLifecycleState state : values()) {
      if (state.wireValue.equals(value)) {
        return state;
      }
    }
    throw JsonFields.malformed("topic state", "is unknown: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
