package io.github.underloam.xbbg.subscription;

import java.util.Objects;

/** State of the Bloomberg session behind a subscription. */
public final class SessionStatus {
  private final SessionState state;
  private final long lastChangeUs;
  private final long disconnectCount;
  private final long reconnectCount;

  SessionStatus(SessionState state, long lastChangeUs, long disconnectCount, long reconnectCount) {
    this.state = state;
    this.lastChangeUs = lastChangeUs;
    this.disconnectCount = disconnectCount;
    this.reconnectCount = reconnectCount;
  }

  /**
   * Returns the session state.
   *
   * @return the state
   */
  public SessionState state() {
    return state;
  }

  /**
   * Returns when the state last changed.
   *
   * @return microseconds since the epoch
   */
  public long lastChangeUs() {
    return lastChangeUs;
  }

  /**
   * Returns how often the session disconnected.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long disconnectCount() {
    return disconnectCount;
  }

  /**
   * Returns how often the session reconnected.
   *
   * @return the count (saturated at {@link Long#MAX_VALUE})
   */
  public long reconnectCount() {
    return reconnectCount;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SessionStatus)) {
      return false;
    }
    SessionStatus that = (SessionStatus) other;
    return state == that.state
        && lastChangeUs == that.lastChangeUs
        && disconnectCount == that.disconnectCount
        && reconnectCount == that.reconnectCount;
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, lastChangeUs, disconnectCount, reconnectCount);
  }

  @Override
  public String toString() {
    return "SessionStatus{state="
        + state
        + ", lastChangeUs="
        + lastChangeUs
        + ", disconnectCount="
        + disconnectCount
        + ", reconnectCount="
        + reconnectCount
        + "}";
  }
}
