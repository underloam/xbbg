package io.github.underloam.xbbg.subscription;

import java.util.Objects;
import java.util.Optional;

/**
 * One entry of a subscription's status history (the newest 128 are kept): Bloomberg session,
 * service, admin and subscription messages, and xbbg lifecycle events.
 */
public final class SubscriptionEvent {
  private final long atUs;
  private final SubscriptionEventCategory category;
  private final SubscriptionEventLevel level;
  private final String messageType;
  private final String topic;
  private final String detail;

  SubscriptionEvent(
      long atUs,
      SubscriptionEventCategory category,
      SubscriptionEventLevel level,
      String messageType,
      String topic,
      String detail) {
    this.atUs = atUs;
    this.category = category;
    this.level = level;
    this.messageType = messageType;
    this.topic = topic;
    this.detail = detail;
  }

  /**
   * Returns when the event was recorded.
   *
   * @return microseconds since the epoch
   */
  public long atUs() {
    return atUs;
  }

  /**
   * Returns the event's source.
   *
   * @return the category
   */
  public SubscriptionEventCategory category() {
    return category;
  }

  /**
   * Returns the severity.
   *
   * @return the level
   */
  public SubscriptionEventLevel level() {
    return level;
  }

  /**
   * Returns the Bloomberg message type or xbbg event name, such as {@code DataLoss}, {@code
   * DelayedStream} or {@code FieldException}.
   *
   * @return the message type
   */
  public String messageType() {
    return messageType;
  }

  /**
   * Returns the affected consumer label.
   *
   * @return the topic, or empty for session-wide events
   */
  public Optional<String> topic() {
    return Optional.ofNullable(topic);
  }

  /**
   * Returns additional text.
   *
   * @return the detail, or empty
   */
  public Optional<String> detail() {
    return Optional.ofNullable(detail);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SubscriptionEvent)) {
      return false;
    }
    SubscriptionEvent that = (SubscriptionEvent) other;
    return atUs == that.atUs
        && category == that.category
        && level == that.level
        && messageType.equals(that.messageType)
        && Objects.equals(topic, that.topic)
        && Objects.equals(detail, that.detail);
  }

  @Override
  public int hashCode() {
    return Objects.hash(atUs, category, level, messageType, topic, detail);
  }

  @Override
  public String toString() {
    return "SubscriptionEvent{atUs="
        + atUs
        + ", category="
        + category
        + ", level="
        + level
        + ", messageType="
        + messageType
        + (topic == null ? "" : ", topic=" + topic)
        + (detail == null ? "" : ", detail=" + detail)
        + "}";
  }
}
