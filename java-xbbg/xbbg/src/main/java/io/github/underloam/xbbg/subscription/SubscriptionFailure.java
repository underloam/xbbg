package io.github.underloam.xbbg.subscription;

import java.util.Objects;

/** A topic that failed or was terminated; the other topics keep streaming. */
public final class SubscriptionFailure {
  private final String topic;
  private final String reason;
  private final SubscriptionFailureKind kind;
  private final long atUs;

  SubscriptionFailure(String topic, String reason, SubscriptionFailureKind kind, long atUs) {
    this.topic = topic;
    this.reason = reason;
    this.kind = kind;
    this.atUs = atUs;
  }

  /**
   * Returns the consumer label of the failed topic.
   *
   * @return the topic
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns Bloomberg's reason.
   *
   * @return the reason text
   */
  public String reason() {
    return reason;
  }

  /**
   * Returns whether the topic failed or was terminated.
   *
   * @return the kind
   */
  public SubscriptionFailureKind kind() {
    return kind;
  }

  /**
   * Returns when the failure was recorded.
   *
   * @return microseconds since the epoch
   */
  public long atUs() {
    return atUs;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SubscriptionFailure)) {
      return false;
    }
    SubscriptionFailure that = (SubscriptionFailure) other;
    return topic.equals(that.topic)
        && reason.equals(that.reason)
        && kind == that.kind
        && atUs == that.atUs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(topic, reason, kind, atUs);
  }

  @Override
  public String toString() {
    return "SubscriptionFailure{topic="
        + topic
        + ", reason="
        + reason
        + ", kind="
        + kind
        + ", atUs="
        + atUs
        + "}";
  }
}
