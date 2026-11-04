package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.Json;
import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import io.github.underloam.xbbg.internal.JsonValue;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * A generic Bloomberg request for {@link Engine#request(RequestSpec)}: any service and operation,
 * including raw requests through {@code requestOperation}, {@code elements} and {@code
 * jsonElements}. Every field except {@code service} is optional; the engine fills defaults from the
 * operation and validates the combination.
 *
 * <p>String dates are sent as given; {@code java.time} overloads format with {@link Dates} ({@code
 * YYYYMMDD} for dates, ISO 8601 for date-times).
 */
public final class RequestSpec {
  private final String service;
  private final String operation;
  private final String requestOperation;
  private final String requestId;
  private final String extractor;
  private final List<String> securities;
  private final String security;
  private final List<String> fields;
  private final Overrides overrides;
  private final List<StringPair> elements;
  private final List<StringPair> kwargs;
  private final String jsonElements;
  private final String startDate;
  private final String endDate;
  private final String startDatetime;
  private final String endDatetime;
  private final String requestTz;
  private final String outputTz;
  private final String eventType;
  private final List<String> eventTypes;
  private final Integer interval;
  private final List<StringPair> options;
  private final List<StringPair> fieldTypes;
  private final Boolean includeSecurityErrors;
  private final Boolean returnEids;
  private final DelayedPolicy onDelayed;
  private final Boolean validateFields;
  private final String searchSpec;
  private final List<String> fieldIds;
  private final String format;

  private RequestSpec(Builder b) {
    service = b.service;
    operation = b.operation;
    requestOperation = b.requestOperation;
    requestId = b.requestId;
    extractor = b.extractor;
    securities = copy(b.securities);
    security = b.security;
    fields = copy(b.fields);
    overrides = b.overrides;
    elements = copy(b.elements);
    kwargs = copy(b.kwargs);
    jsonElements = b.jsonElements;
    startDate = b.startDate;
    endDate = b.endDate;
    startDatetime = b.startDatetime;
    endDatetime = b.endDatetime;
    requestTz = b.requestTz;
    outputTz = b.outputTz;
    eventType = b.eventType;
    eventTypes = copy(b.eventTypes);
    interval = b.interval;
    options = copy(b.options);
    fieldTypes = copy(b.fieldTypes);
    includeSecurityErrors = b.includeSecurityErrors;
    returnEids = b.returnEids;
    onDelayed = b.onDelayed;
    validateFields = b.validateFields;
    searchSpec = b.searchSpec;
    fieldIds = copy(b.fieldIds);
    format = b.format;
  }

  /**
   * Starts a request for a service.
   *
   * @param service the Bloomberg service URI (for example {@code //blp/refdata}); an empty string
   *     lets the engine derive it from the operation
   * @return a builder
   */
  public static Builder builder(String service) {
    return new Builder(service);
  }

  /**
   * Returns the Bloomberg service URI.
   *
   * @return the Bloomberg service URI
   */
  public String service() {
    return service;
  }

  /**
   * Returns the operation, for example {@code ReferenceDataRequest}, or empty.
   *
   * @return the operation, for example {@code ReferenceDataRequest}, or empty
   */
  public Optional<String> operation() {
    return Optional.ofNullable(operation);
  }

  /**
   * Returns the operation of a raw request, or empty.
   *
   * @return the operation of a raw request, or empty
   */
  public Optional<String> requestOperation() {
    return Optional.ofNullable(requestOperation);
  }

  /**
   * Returns xbbg's label for the request (it appears in errors), or empty.
   *
   * @return xbbg's label for the request (it appears in errors), or empty
   */
  public Optional<String> requestId() {
    return Optional.ofNullable(requestId);
  }

  /**
   * Returns the result extractor, or empty for the operation's default.
   *
   * @return the result extractor, or empty for the operation's default
   */
  public Optional<String> extractor() {
    return Optional.ofNullable(extractor);
  }

  /**
   * Returns the securities of a multi-security request, or empty.
   *
   * @return the securities of a multi-security request, or empty
   */
  public Optional<List<String>> securities() {
    return Optional.ofNullable(securities);
  }

  /**
   * Returns the security of a single-security request, or empty.
   *
   * @return the security of a single-security request, or empty
   */
  public Optional<String> security() {
    return Optional.ofNullable(security);
  }

  /**
   * Returns the fields, or empty.
   *
   * @return the fields, or empty
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /**
   * Returns the overrides, or empty.
   *
   * @return the overrides, or empty
   */
  public Optional<Overrides> overrides() {
    return Optional.ofNullable(overrides);
  }

  /**
   * Returns raw request elements, in order, or empty.
   *
   * @return raw request elements, in order, or empty
   */
  public Optional<List<StringPair>> elements() {
    return Optional.ofNullable(elements);
  }

  /**
   * Returns extra arguments the engine routes to elements or overrides, or empty.
   *
   * @return extra arguments the engine routes to elements or overrides, or empty
   */
  public Optional<List<StringPair>> kwargs() {
    return Optional.ofNullable(kwargs);
  }

  /**
   * Returns a JSON object flattened into request elements, or empty.
   *
   * @return a JSON object flattened into request elements, or empty
   */
  public Optional<String> jsonElements() {
    return Optional.ofNullable(jsonElements);
  }

  /**
   * Returns the start date, or empty.
   *
   * @return the start date, or empty
   */
  public Optional<String> startDate() {
    return Optional.ofNullable(startDate);
  }

  /**
   * Returns the end date, or empty.
   *
   * @return the end date, or empty
   */
  public Optional<String> endDate() {
    return Optional.ofNullable(endDate);
  }

  /**
   * Returns the start date-time, or empty.
   *
   * @return the start date-time, or empty
   */
  public Optional<String> startDatetime() {
    return Optional.ofNullable(startDatetime);
  }

  /**
   * Returns the end date-time, or empty.
   *
   * @return the end date-time, or empty
   */
  public Optional<String> endDatetime() {
    return Optional.ofNullable(endDatetime);
  }

  /**
   * Returns the time zone of naive request date-times, or empty.
   *
   * @return the time zone of naive request date-times, or empty
   */
  public Optional<String> requestTz() {
    return Optional.ofNullable(requestTz);
  }

  /**
   * Returns the time zone of result timestamps, or empty.
   *
   * @return the time zone of result timestamps, or empty
   */
  public Optional<String> outputTz() {
    return Optional.ofNullable(outputTz);
  }

  /**
   * Returns the intraday bar event type, or empty.
   *
   * @return the intraday bar event type, or empty
   */
  public Optional<String> eventType() {
    return Optional.ofNullable(eventType);
  }

  /**
   * Returns the intraday tick event types, or empty.
   *
   * @return the intraday tick event types, or empty
   */
  public Optional<List<String>> eventTypes() {
    return Optional.ofNullable(eventTypes);
  }

  /**
   * Returns the intraday bar interval in minutes, or empty.
   *
   * @return the intraday bar interval in minutes, or empty
   */
  public OptionalInt interval() {
    return interval == null ? OptionalInt.empty() : OptionalInt.of(interval);
  }

  /**
   * Returns named request parameters applied after the elements, or empty.
   *
   * @return named request parameters applied after the elements, or empty
   */
  public Optional<List<StringPair>> options() {
    return Optional.ofNullable(options);
  }

  /**
   * Returns field to Arrow type name, or empty.
   *
   * @return field to Arrow type name, or empty
   */
  public Optional<List<StringPair>> fieldTypes() {
    return Optional.ofNullable(fieldTypes);
  }

  /**
   * Returns whether per-security errors are returned as metadata, or empty (false).
   *
   * @return whether per-security errors are returned as metadata, or empty (false)
   */
  public Optional<Boolean> includeSecurityErrors() {
    return Optional.ofNullable(includeSecurityErrors);
  }

  /**
   * Returns whether entitlement ids are returned as metadata, or empty (false).
   *
   * @return whether entitlement ids are returned as metadata, or empty (false)
   */
  public Optional<Boolean> returnEids() {
    return Optional.ofNullable(returnEids);
  }

  /**
   * Returns the delayed-data policy, or empty for no check.
   *
   * @return the delayed-data policy, or empty for no check
   */
  public Optional<DelayedPolicy> onDelayed() {
    return Optional.ofNullable(onDelayed);
  }

  /**
   * Returns the per-request field validation toggle, or empty.
   *
   * @return the per-request field validation toggle, or empty
   */
  public Optional<Boolean> validateFields() {
    return Optional.ofNullable(validateFields);
  }

  /**
   * Returns the field search text, or empty.
   *
   * @return the field search text, or empty
   */
  public Optional<String> searchSpec() {
    return Optional.ofNullable(searchSpec);
  }

  /**
   * Returns the field ids of a field-info request, or empty.
   *
   * @return the field ids of a field-info request, or empty
   */
  public Optional<List<String>> fieldIds() {
    return Optional.ofNullable(fieldIds);
  }

  /**
   * Returns the output format or alias, or empty for {@code long}.
   *
   * @return the output format or alias, or empty for {@code long}
   */
  public Optional<String> format() {
    return Optional.ofNullable(format);
  }

  /** Serializes the request as the engine's {@code RequestInput} JSON. */
  JsonValue toJson() {
    JsonObjectBuilder json = Json.object();
    json.put("service", service);
    json.put("operation", operation == null ? "" : operation);
    json.put("requestOperation", requestOperation);
    json.put("requestId", requestId);
    json.put("extractor", extractor);
    json.put("securities", securities);
    json.put("security", security);
    json.put("fields", fields);
    if (overrides != null) {
      overrides.encodeRequestParts(json);
    }
    json.put("elements", elements == null ? null : Pairs.json(elements));
    json.put("kwargs", kwargs == null ? null : Pairs.json(kwargs));
    json.put("jsonElements", jsonElements);
    json.put("startDate", startDate);
    json.put("endDate", endDate);
    json.put("startDatetime", startDatetime);
    json.put("endDatetime", endDatetime);
    json.put("requestTz", requestTz);
    json.put("outputTz", outputTz);
    json.put("eventType", eventType);
    json.put("eventTypes", eventTypes);
    json.put("interval", interval);
    json.put("options", options == null ? null : Pairs.json(options));
    json.put("fieldTypes", fieldTypes == null ? null : Pairs.json(fieldTypes));
    json.put("includeSecurityErrors", includeSecurityErrors);
    json.put("returnEids", returnEids);
    json.put("onDelayed", onDelayed == null ? null : onDelayed.wireValue());
    json.put("validateFields", validateFields);
    json.put("searchSpec", searchSpec);
    json.put("fieldIds", fieldIds);
    json.put("format", format);
    return json.build();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RequestSpec && toJson().equals(((RequestSpec) other).toJson());
  }

  @Override
  public int hashCode() {
    return toJson().hashCode();
  }

  @Override
  public String toString() {
    return "RequestSpec" + toJson();
  }

  private static <T> List<T> copy(List<T> values) {
    return values == null ? null : Collections.unmodifiableList(new ArrayList<T>(values));
  }

  /** Builds a {@link RequestSpec}. Setting a field to {@code null} omits it again. */
  public static final class Builder {
    private final String service;
    private String operation;
    private String requestOperation;
    private String requestId;
    private String extractor;
    private List<String> securities;
    private String security;
    private List<String> fields;
    private Overrides overrides;
    private List<StringPair> elements;
    private List<StringPair> kwargs;
    private String jsonElements;
    private String startDate;
    private String endDate;
    private String startDatetime;
    private String endDatetime;
    private String requestTz;
    private String outputTz;
    private String eventType;
    private List<String> eventTypes;
    private Integer interval;
    private List<StringPair> options;
    private List<StringPair> fieldTypes;
    private Boolean includeSecurityErrors;
    private Boolean returnEids;
    private DelayedPolicy onDelayed;
    private Boolean validateFields;
    private String searchSpec;
    private List<String> fieldIds;
    private String format;

    private Builder(String service) {
      this.service = Objects.requireNonNull(service, "service");
    }

    /**
     * Sets the operation, for example {@code ReferenceDataRequest}.
     *
     * @param value the operation
     * @return this builder
     */
    public Builder operation(String value) {
      operation = value;
      return this;
    }

    /**
     * Makes this a raw request for an operation the engine has no extractor plan for.
     *
     * @param value the operation name
     * @return this builder
     */
    public Builder requestOperation(String value) {
      requestOperation = value;
      return this;
    }

    /**
     * Labels the request; the label appears in errors.
     *
     * @param value the label
     * @return this builder
     */
    public Builder requestId(String value) {
      requestId = value;
      return this;
    }

    /**
     * Chooses the result extractor: {@code refdata}, {@code histdata}, {@code bulk}, {@code
     * intraday_bar}, {@code intraday_tick}, {@code generic}, {@code bql}, {@code bsrch} or {@code
     * fieldinfo}.
     *
     * @param value the extractor
     * @return this builder
     */
    public Builder extractor(String value) {
      extractor = value;
      return this;
    }

    /**
     * Sets the securities of a multi-security request.
     *
     * @param values the securities
     * @return this builder
     */
    public Builder securities(Collection<String> values) {
      securities = values == null ? null : new ArrayList<String>(values);
      return this;
    }

    /**
     * Sets the securities of a multi-security request.
     *
     * @param values the securities
     * @return this builder
     */
    public Builder securities(String... values) {
      return securities(Arrays.asList(values));
    }

    /**
     * Sets the security of a single-security request.
     *
     * @param value the security
     * @return this builder
     */
    public Builder security(String value) {
      security = value;
      return this;
    }

    /**
     * Sets the fields.
     *
     * @param values the fields
     * @return this builder
     */
    public Builder fields(Collection<String> values) {
      fields = values == null ? null : new ArrayList<String>(values);
      return this;
    }

    /**
     * Sets the fields.
     *
     * @param values the fields
     * @return this builder
     */
    public Builder fields(String... values) {
      return fields(Arrays.asList(values));
    }

    /**
     * Sets the overrides, including per-security overrides (ReferenceData and HistoricalData only).
     *
     * @param value the overrides
     * @return this builder
     */
    public Builder overrides(Overrides value) {
      overrides = value;
      return this;
    }

    /**
     * Sets the overrides from a map; see {@link Overrides#of(Map)}.
     *
     * @param value field to value
     * @return this builder
     */
    public Builder overrides(Map<String, ?> value) {
      overrides = value == null ? null : Overrides.of(value);
      return this;
    }

    /**
     * Sets raw request elements; a dotted name sets a nested element.
     *
     * @param values the elements, in order
     * @return this builder
     */
    public Builder elements(Collection<StringPair> values) {
      elements = values == null ? null : new ArrayList<StringPair>(values);
      return this;
    }

    /**
     * Appends one raw request element.
     *
     * @param name the element name (dotted for nested elements)
     * @param value the value
     * @return this builder
     */
    public Builder element(String name, String value) {
      if (elements == null) {
        elements = new ArrayList<StringPair>();
      }
      elements.add(new StringPair(name, value));
      return this;
    }

    /**
     * Sets extra arguments the engine routes to request elements or overrides by schema.
     *
     * @param values the arguments
     * @return this builder
     */
    public Builder kwargs(Collection<StringPair> values) {
      kwargs = values == null ? null : new ArrayList<StringPair>(values);
      return this;
    }

    /**
     * Sets extra arguments from a map; values are converted like JavaScript's {@code String()}.
     *
     * @param values name to value
     * @return this builder
     */
    public Builder kwargs(Map<String, ?> values) {
      kwargs = Pairs.fromMap(values);
      return this;
    }

    /**
     * Sets a JSON object that the engine flattens into request elements (nested objects become
     * dotted names; a top-level array repeats its name).
     *
     * @param value the JSON object text
     * @return this builder
     */
    public Builder jsonElements(String value) {
      jsonElements = value;
      return this;
    }

    /**
     * Sets {@code jsonElements} from a map, serialized as JSON.
     *
     * @param value the object (strings, numbers, booleans, lists and nested maps)
     * @return this builder
     */
    public Builder jsonElements(Map<String, ?> value) {
      jsonElements = value == null ? null : Json.write(value);
      return this;
    }

    /**
     * Sets the start date, sent as given (Bloomberg expects {@code YYYYMMDD}).
     *
     * @param value the date text
     * @return this builder
     */
    public Builder startDate(String value) {
      startDate = value;
      return this;
    }

    /**
     * Sets the start date as {@code YYYYMMDD}.
     *
     * @param value the date
     * @return this builder
     */
    public Builder startDate(TemporalAccessor value) {
      startDate = Dates.formatDate(value);
      return this;
    }

    /**
     * Sets the end date, sent as given.
     *
     * @param value the date text
     * @return this builder
     */
    public Builder endDate(String value) {
      endDate = value;
      return this;
    }

    /**
     * Sets the end date as {@code YYYYMMDD}.
     *
     * @param value the date
     * @return this builder
     */
    public Builder endDate(TemporalAccessor value) {
      endDate = Dates.formatDate(value);
      return this;
    }

    /**
     * Sets the start date-time, sent as given.
     *
     * @param value the date-time text
     * @return this builder
     */
    public Builder startDatetime(String value) {
      startDatetime = value;
      return this;
    }

    /**
     * Sets the start date-time; see {@link Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param value the date-time
     * @return this builder
     */
    public Builder startDatetime(TemporalAccessor value) {
      startDatetime = Dates.formatDateTime(value);
      return this;
    }

    /**
     * Sets the end date-time, sent as given.
     *
     * @param value the date-time text
     * @return this builder
     */
    public Builder endDatetime(String value) {
      endDatetime = value;
      return this;
    }

    /**
     * Sets the end date-time; see {@link Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param value the date-time
     * @return this builder
     */
    public Builder endDatetime(TemporalAccessor value) {
      endDatetime = Dates.formatDateTime(value);
      return this;
    }

    /**
     * Sets the time zone of naive request date-times (for example {@code NY} or an IANA name).
     *
     * @param value the zone
     * @return this builder
     */
    public Builder requestTz(String value) {
      requestTz = value;
      return this;
    }

    /**
     * Sets the time zone of result timestamps.
     *
     * @param value the zone
     * @return this builder
     */
    public Builder outputTz(String value) {
      outputTz = value;
      return this;
    }

    /**
     * Sets the intraday bar event type, for example {@code TRADE}.
     *
     * @param value the event type
     * @return this builder
     */
    public Builder eventType(String value) {
      eventType = value;
      return this;
    }

    /**
     * Sets the intraday tick event types.
     *
     * @param values the event types
     * @return this builder
     */
    public Builder eventTypes(Collection<String> values) {
      eventTypes = values == null ? null : new ArrayList<String>(values);
      return this;
    }

    /**
     * Sets the intraday tick event types.
     *
     * @param values the event types
     * @return this builder
     */
    public Builder eventTypes(String... values) {
      return eventTypes(Arrays.asList(values));
    }

    /**
     * Sets the intraday bar interval.
     *
     * @param minutes the interval in minutes, not negative
     * @return this builder
     */
    public Builder interval(int minutes) {
      interval = EngineConfig.checkUnsigned("interval", minutes);
      return this;
    }

    /**
     * Sets named request parameters applied after the elements.
     *
     * @param values the parameters
     * @return this builder
     */
    public Builder options(Collection<StringPair> values) {
      options = values == null ? null : new ArrayList<StringPair>(values);
      return this;
    }

    /**
     * Sets field to Arrow type name ({@code float64}, {@code int64}, {@code int32}, {@code string},
     * {@code bool}, {@code date32}, {@code timestamp}, {@code time64}).
     *
     * @param values the field types
     * @return this builder
     */
    public Builder fieldTypes(Map<String, String> values) {
      fieldTypes = Pairs.fromMap(values);
      return this;
    }

    /**
     * Returns per-security errors as result metadata instead of failing.
     *
     * @param value the flag
     * @return this builder
     */
    public Builder includeSecurityErrors(boolean value) {
      includeSecurityErrors = value;
      return this;
    }

    /**
     * Returns entitlement ids as result metadata.
     *
     * @param value the flag
     * @return this builder
     */
    public Builder returnEids(boolean value) {
      returnEids = value;
      return this;
    }

    /**
     * Checks for delayed data: {@code WARN} emits {@code BlpDelayedDataWarning}s, {@code RAISE}
     * fails the request, {@code IGNORE} only records the flags.
     *
     * @param value the policy
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy value) {
      onDelayed = value;
      return this;
    }

    /**
     * Turns field validation on or off for this request.
     *
     * @param value the flag
     * @return this builder
     */
    public Builder validateFields(boolean value) {
      validateFields = value;
      return this;
    }

    /**
     * Sets the text of a field search request.
     *
     * @param value the search text
     * @return this builder
     */
    public Builder searchSpec(String value) {
      searchSpec = value;
      return this;
    }

    /**
     * Sets the field ids of a field-info request.
     *
     * @param values the field ids
     * @return this builder
     */
    public Builder fieldIds(Collection<String> values) {
      fieldIds = values == null ? null : new ArrayList<String>(values);
      return this;
    }

    /**
     * Sets the output format; the engine also accepts aliases.
     *
     * @param value the format or alias
     * @return this builder
     */
    public Builder format(String value) {
      format = value;
      return this;
    }

    /**
     * Sets the output format.
     *
     * @param value the format
     * @return this builder
     */
    public Builder format(Format value) {
      format = value == null ? null : value.wireValue();
      return this;
    }

    /**
     * Builds the request.
     *
     * @return the request
     */
    public RequestSpec build() {
      return new RequestSpec(this);
    }
  }
}
