package io.github.underloam.xbbg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A shared subscription feed of an engine ({@link Engine#subscriptionFeeds()}), without identity or
 * transport details.
 */
public final class FeedInfo {
  private final String service;
  private final String topic;
  private final List<String> options;
  private final List<String> fields;
  private final long consumers;
  private final Boolean delayed;
  private final String state;
  private final boolean isolated;
  private final Map<String, String> fieldErrors;

  /**
   * Creates a value.
   *
   * @param service the Bloomberg service
   * @param topic the feed topic
   * @param options the subscription options
   * @param fields the union of fields the feed carries
   * @param consumers how many subscriptions share the feed
   * @param delayed whether the feed delivers delayed data, or {@code null} when unknown
   * @param state the topic state ({@code pending}, {@code started}, {@code streaming}, ...)
   * @param isolated whether the feed is private to one subscription
   * @param fieldErrors field to error category for fields Bloomberg rejected
   */
  public FeedInfo(
      String service,
      String topic,
      List<String> options,
      List<String> fields,
      long consumers,
      Boolean delayed,
      String state,
      boolean isolated,
      Map<String, String> fieldErrors) {
    this.service = Objects.requireNonNull(service, "service");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.options = Collections.unmodifiableList(new ArrayList<String>(options));
    this.fields = Collections.unmodifiableList(new ArrayList<String>(fields));
    this.consumers = consumers;
    this.delayed = delayed;
    this.state = Objects.requireNonNull(state, "state");
    this.isolated = isolated;
    this.fieldErrors = Collections.unmodifiableMap(new LinkedHashMap<String, String>(fieldErrors));
  }

  /**
   * Returns the Bloomberg service.
   *
   * @return the service URI
   */
  public String service() {
    return service;
  }

  /**
   * Returns the feed topic.
   *
   * @return the topic
   */
  public String topic() {
    return topic;
  }

  /**
   * Returns the subscription options.
   *
   * @return an unmodifiable list
   */
  public List<String> options() {
    return options;
  }

  /**
   * Returns the fields the feed carries.
   *
   * @return an unmodifiable list
   */
  public List<String> fields() {
    return fields;
  }

  /**
   * Returns how many subscriptions share the feed.
   *
   * @return the consumer count
   */
  public long consumers() {
    return consumers;
  }

  /**
   * Returns whether the feed delivers delayed data.
   *
   * @return the flag, or empty when unknown
   */
  public Optional<Boolean> delayed() {
    return Optional.ofNullable(delayed);
  }

  /**
   * Returns the topic state.
   *
   * @return the state name
   */
  public String state() {
    return state;
  }

  /**
   * Whether the feed is private to one subscription.
   *
   * @return the flag
   */
  public boolean isolated() {
    return isolated;
  }

  /**
   * Returns the error category of each field Bloomberg rejected.
   *
   * @return an unmodifiable map
   */
  public Map<String, String> fieldErrors() {
    return fieldErrors;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FeedInfo)) {
      return false;
    }
    FeedInfo that = (FeedInfo) other;
    return consumers == that.consumers
        && isolated == that.isolated
        && service.equals(that.service)
        && topic.equals(that.topic)
        && options.equals(that.options)
        && fields.equals(that.fields)
        && Objects.equals(delayed, that.delayed)
        && state.equals(that.state)
        && fieldErrors.equals(that.fieldErrors);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        service, topic, options, fields, consumers, delayed, state, isolated, fieldErrors);
  }

  @Override
  public String toString() {
    return "FeedInfo{service="
        + service
        + ", topic="
        + topic
        + ", options="
        + options
        + ", fields="
        + fields
        + ", consumers="
        + consumers
        + ", delayed="
        + delayed
        + ", state="
        + state
        + ", isolated="
        + isolated
        + ", fieldErrors="
        + fieldErrors
        + "}";
  }
}
