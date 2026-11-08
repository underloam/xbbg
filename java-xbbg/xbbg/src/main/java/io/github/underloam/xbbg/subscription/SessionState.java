package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;

/** State of the session behind a subscription in {@link SessionStatus}. */
public enum SessionState {
  /** Connecting. */
  STARTING("starting"),
  /** Connected. */
  UP("up"),
  /** Disconnected; the SDK may reconnect. */
  DOWN("down"),
  /** Terminated; no further data. */
  TERMINATED("terminated");

  private final String wireValue;

  SessionState(String wireValue) {
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

  static SessionState fromWire(String value) {
    for (SessionState state : values()) {
      if (state.wireValue.equals(value)) {
        return state;
      }
    }
    throw JsonFields.malformed("session state", "is unknown: " + value);
  }

  @Override
  public String toString() {
    return wireValue;
  }
}
