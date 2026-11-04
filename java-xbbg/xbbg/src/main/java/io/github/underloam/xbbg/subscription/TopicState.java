package io.github.underloam.xbbg.subscription;

import java.util.Objects;
import java.util.Optional;

/** Lifecycle and stream state of one consumer label. */
public final class TopicState {
  private final String topic;
  private final String feedTopic;
  private final Boolean delayed;
  private final TopicLifecycleState state;
  private final long lastChangeUs;
  private final boolean streamsActive;
  private final long streamsChangedUs;

  TopicState(
      String topic,
      String feedTopic,
      Boolean delayed,
      TopicLifecycleState state,
      long lastChangeUs,
      boolean streamsActive,
      long streamsChangedUs) {
    this.topic = topic;
    this.feedTopic = feedTopic;
    this.delayed = delayed;
    this.state = state;
    this.lastChangeUs = lastChangeUs;
    this.streamsActive = streamsActive;
    this.streamsChangedUs = streamsChangedUs;
  }

  /**
   * Returns the consumer label.
   *
   * @return the label
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the Bloomberg topic before aliasing.
   *
   * @return the feed topic
   */
  public String feedTopic() {
    return feedTopic;
  }

  /**
   * Returns whether Bloomberg delivers delayed data for the topic.
   *
   * @return the flag, or empty before the first image
   */
  public Optional<Boolean> delayed() {
    return Optional.ofNullable(delayed);
  }

  /**
   * Returns the lifecycle state.
   *
   * @return the state
   */
  public TopicLifecycleState state() {
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
   * Whether Bloomberg currently has active streams for the topic (a stream can be alive but
   * temporarily silent across reconnects).
   *
   * @return the flag
   */
  public boolean streamsActive() {
    return streamsActive;
  }

  /**
   * Returns when {@link #streamsActive()} last changed.
   *
   * @return microseconds since the epoch
   */
  public long streamsChangedUs() {
    return streamsChangedUs;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TopicState)) {
      return false;
    }
    TopicState that = (TopicState) other;
    return topic.equals(that.topic)
        && feedTopic.equals(that.feedTopic)
        && Objects.equals(delayed, that.delayed)
        && state == that.state
        && lastChangeUs == that.lastChangeUs
        && streamsActive == that.streamsActive
        && streamsChangedUs == that.streamsChangedUs;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        topic, feedTopic, delayed, state, lastChangeUs, streamsActive, streamsChangedUs);
  }

  @Override
  public String toString() {
    return "TopicState{topic="
        + topic
        + ", feedTopic="
        + feedTopic
        + (delayed == null ? "" : ", delayed=" + delayed)
        + ", state="
        + state
        + ", lastChangeUs="
        + lastChangeUs
        + ", streamsActive="
        + streamsActive
        + ", streamsChangedUs="
        + streamsChangedUs
        + "}";
  }
}
