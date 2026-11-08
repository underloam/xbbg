package io.github.underloam.xbbg.subscription;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** One consistent snapshot of every subscription diagnostic. */
public final class SubscriptionStatus {
  private final List<SubscriptionEvent> events;
  private final List<SubscriptionFailure> failures;
  private final List<String> failedTickers;
  private final Map<String, TopicState> topicStates;
  private final Map<String, Map<String, String>> fieldErrors;
  private final SessionStatus session;
  private final Map<String, ServiceStatus> services;
  private final AdminStatus admin;

  SubscriptionStatus(
      List<SubscriptionEvent> events,
      List<SubscriptionFailure> failures,
      List<String> failedTickers,
      Map<String, TopicState> topicStates,
      Map<String, Map<String, String>> fieldErrors,
      SessionStatus session,
      Map<String, ServiceStatus> services,
      AdminStatus admin) {
    this.events = Collections.unmodifiableList(events);
    this.failures = Collections.unmodifiableList(failures);
    this.failedTickers = Collections.unmodifiableList(failedTickers);
    this.topicStates = Collections.unmodifiableMap(topicStates);
    this.fieldErrors = Collections.unmodifiableMap(fieldErrors);
    this.session = session;
    this.services = Collections.unmodifiableMap(services);
    this.admin = admin;
  }

  /**
   * Returns the status history (newest 128).
   *
   * @return the events, oldest first
   */
  public List<SubscriptionEvent> events() {
    return events;
  }

  /**
   * Returns the failed or terminated topics.
   *
   * @return the failures, in occurrence order
   */
  public List<SubscriptionFailure> failures() {
    return failures;
  }

  /**
   * Returns the topics of {@link #failures()}.
   *
   * @return the failed labels
   */
  public List<String> failedTickers() {
    return failedTickers;
  }

  /**
   * Returns the lifecycle of every consumer label.
   *
   * @return label to state, sorted by label
   */
  public Map<String, TopicState> topicStates() {
    return topicStates;
  }

  /**
   * Returns the fields Bloomberg rejected.
   *
   * @return label to field to error category
   */
  public Map<String, Map<String, String>> fieldErrors() {
    return fieldErrors;
  }

  /**
   * Returns the session state.
   *
   * @return the session status
   */
  public SessionStatus session() {
    return session;
  }

  /**
   * Returns the availability of each service used.
   *
   * @return service URI to status, sorted by service
   */
  public Map<String, ServiceStatus> services() {
    return services;
  }

  /**
   * Returns the slow-consumer and data-loss counters.
   *
   * @return the admin status
   */
  public AdminStatus admin() {
    return admin;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof SubscriptionStatus)) {
      return false;
    }
    SubscriptionStatus that = (SubscriptionStatus) other;
    return events.equals(that.events)
        && failures.equals(that.failures)
        && failedTickers.equals(that.failedTickers)
        && topicStates.equals(that.topicStates)
        && fieldErrors.equals(that.fieldErrors)
        && session.equals(that.session)
        && services.equals(that.services)
        && admin.equals(that.admin);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        events, failures, failedTickers, topicStates, fieldErrors, session, services, admin);
  }

  @Override
  public String toString() {
    return "SubscriptionStatus{events="
        + events
        + ", failures="
        + failures
        + ", topicStates="
        + topicStates
        + ", fieldErrors="
        + fieldErrors
        + ", session="
        + session
        + ", services="
        + services
        + ", admin="
        + admin
        + "}";
  }
}
