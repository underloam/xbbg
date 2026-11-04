package io.github.underloam.xbbg.subscription;

import io.github.underloam.xbbg.internal.JsonFields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Decodes the native status snapshots (camelCase JSON) into the status value classes. */
final class StatusJson {
  private StatusJson() {}

  static List<String> strings(String json, String what) {
    return Collections.unmodifiableList(JsonFields.stringList(JsonFields.parse(json, what), what));
  }

  static SubscriptionStats stats(String json) {
    Map<String, Object> stats = JsonFields.object(JsonFields.parse(json, "stats"), "stats");
    return new SubscriptionStats(
        JsonFields.longValue(stats, "messagesReceived"),
        JsonFields.longValue(stats, "droppedBatches"),
        JsonFields.longValue(stats, "batchesSent"),
        JsonFields.bool(stats, "slowConsumer"));
  }

  static List<SubscriptionEvent> events(String json) {
    return events(JsonFields.parse(json, "events"));
  }

  private static List<SubscriptionEvent> events(Object value) {
    List<SubscriptionEvent> events = new ArrayList<SubscriptionEvent>();
    for (Object item : JsonFields.array(value, "events")) {
      Map<String, Object> event = JsonFields.object(item, "event");
      events.add(
          new SubscriptionEvent(
              JsonFields.longValue(event, "atUs"),
              SubscriptionEventCategory.fromWire(JsonFields.string(event, "category")),
              SubscriptionEventLevel.fromWire(JsonFields.string(event, "level")),
              JsonFields.string(event, "messageType"),
              JsonFields.optString(event, "topic"),
              JsonFields.optString(event, "detail")));
    }
    return Collections.unmodifiableList(events);
  }

  static List<SubscriptionFailure> failures(String json) {
    return failures(JsonFields.parse(json, "failures"));
  }

  private static List<SubscriptionFailure> failures(Object value) {
    List<SubscriptionFailure> failures = new ArrayList<SubscriptionFailure>();
    for (Object item : JsonFields.array(value, "failures")) {
      Map<String, Object> failure = JsonFields.object(item, "failure");
      failures.add(
          new SubscriptionFailure(
              JsonFields.string(failure, "topic"),
              JsonFields.string(failure, "reason"),
              SubscriptionFailureKind.fromWire(JsonFields.string(failure, "kind")),
              JsonFields.longValue(failure, "atUs")));
    }
    return Collections.unmodifiableList(failures);
  }

  static Map<String, TopicState> topicStates(String json) {
    return topicStates(JsonFields.parse(json, "topicStates"));
  }

  private static Map<String, TopicState> topicStates(Object value) {
    Map<String, TopicState> states = new LinkedHashMap<String, TopicState>();
    for (Map.Entry<String, Object> entry : JsonFields.object(value, "topicStates").entrySet()) {
      Map<String, Object> state = JsonFields.object(entry.getValue(), "topic state");
      states.put(
          entry.getKey(),
          new TopicState(
              JsonFields.string(state, "topic"),
              JsonFields.string(state, "feedTopic"),
              JsonFields.optBool(state, "delayed"),
              TopicLifecycleState.fromWire(JsonFields.string(state, "state")),
              JsonFields.longValue(state, "lastChangeUs"),
              JsonFields.bool(state, "streamsActive"),
              JsonFields.longValue(state, "streamsChangedUs")));
    }
    return Collections.unmodifiableMap(states);
  }

  static Map<String, Map<String, String>> fieldErrors(String json) {
    return fieldErrors(JsonFields.parse(json, "fieldErrors"));
  }

  private static Map<String, Map<String, String>> fieldErrors(Object value) {
    Map<String, Map<String, String>> errors = new LinkedHashMap<String, Map<String, String>>();
    for (Map.Entry<String, Object> entry : JsonFields.object(value, "fieldErrors").entrySet()) {
      Map<String, String> fields = new LinkedHashMap<String, String>();
      for (Map.Entry<String, Object> field :
          JsonFields.object(entry.getValue(), "field errors").entrySet()) {
        Object category = field.getValue();
        if (!(category instanceof String)) {
          throw JsonFields.malformed("field error category", "is not a string");
        }
        fields.put(field.getKey(), (String) category);
      }
      errors.put(entry.getKey(), Collections.unmodifiableMap(fields));
    }
    return Collections.unmodifiableMap(errors);
  }

  static SessionStatus session(String json) {
    return session(JsonFields.parse(json, "sessionStatus"));
  }

  private static SessionStatus session(Object value) {
    Map<String, Object> session = JsonFields.object(value, "session");
    return new SessionStatus(
        SessionState.fromWire(JsonFields.string(session, "state")),
        JsonFields.longValue(session, "lastChangeUs"),
        JsonFields.longValue(session, "disconnectCount"),
        JsonFields.longValue(session, "reconnectCount"));
  }

  static Map<String, ServiceStatus> services(String json) {
    return services(JsonFields.parse(json, "serviceStatus"));
  }

  private static Map<String, ServiceStatus> services(Object value) {
    Map<String, ServiceStatus> services = new LinkedHashMap<String, ServiceStatus>();
    for (Map.Entry<String, Object> entry : JsonFields.object(value, "services").entrySet()) {
      Map<String, Object> service = JsonFields.object(entry.getValue(), "service");
      services.put(
          entry.getKey(),
          new ServiceStatus(
              JsonFields.string(service, "service"),
              JsonFields.bool(service, "up"),
              JsonFields.longValue(service, "lastChangeUs")));
    }
    return Collections.unmodifiableMap(services);
  }

  static AdminStatus admin(String json) {
    return admin(JsonFields.parse(json, "adminStatus"));
  }

  private static AdminStatus admin(Object value) {
    Map<String, Object> admin = JsonFields.object(value, "admin");
    return new AdminStatus(
        JsonFields.bool(admin, "slowConsumerWarningActive"),
        JsonFields.longValue(admin, "slowConsumerWarningCount"),
        JsonFields.longValue(admin, "slowConsumerClearedCount"),
        JsonFields.longValue(admin, "dataLossCount"),
        JsonFields.optLong(admin, "lastWarningUs"),
        JsonFields.optLong(admin, "lastClearedUs"),
        JsonFields.optLong(admin, "lastDataLossUs"));
  }

  static SubscriptionStatus status(String json) {
    Map<String, Object> status = JsonFields.object(JsonFields.parse(json, "status"), "status");
    return new SubscriptionStatus(
        events(JsonFields.list(status, "events")),
        failures(JsonFields.list(status, "failures")),
        JsonFields.strings(status, "failedTickers"),
        topicStates(JsonFields.obj(status, "topicStates")),
        fieldErrors(JsonFields.obj(status, "fieldErrors")),
        session(JsonFields.obj(status, "session")),
        services(JsonFields.obj(status, "services")),
        admin(JsonFields.obj(status, "admin")));
  }
}
