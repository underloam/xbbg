package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bdtick} intraday tick requests (TS {@code BdtickOptions}). The typed {@code
 * include*} flags are sent as request parameters and beat a {@code kwargs} entry of the same name.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bdtick(String, BdtickOptions)}.
 */
public final class BdtickOptions {
  private static final BdtickOptions DEFAULTS = builder().build();

  private final String start;
  private final String end;
  private final String requestTz;
  private final String outputTz;
  private final List<String> eventTypes;
  private final Boolean includeConditionCodes;
  private final Boolean includeExchangeCodes;
  private final Boolean includeBrokerCodes;
  private final Boolean includeRpsCodes;
  private final Boolean includeBicMicCodes;
  private final Boolean includeNonPlottableEvents;
  private final Boolean includeBloombergStandardConditionCodes;
  private final Map<String, Object> kwargs;
  private final Boolean returnEids;
  private final DelayedPolicy onDelayed;

  private BdtickOptions(Builder builder) {
    this.start = builder.start;
    this.end = builder.end;
    this.requestTz = builder.requestTz;
    this.outputTz = builder.outputTz;
    this.eventTypes = builder.eventTypes;
    this.includeConditionCodes = builder.includeConditionCodes;
    this.includeExchangeCodes = builder.includeExchangeCodes;
    this.includeBrokerCodes = builder.includeBrokerCodes;
    this.includeRpsCodes = builder.includeRpsCodes;
    this.includeBicMicCodes = builder.includeBicMicCodes;
    this.includeNonPlottableEvents = builder.includeNonPlottableEvents;
    this.includeBloombergStandardConditionCodes = builder.includeBloombergStandardConditionCodes;
    this.kwargs = builder.kwargs;
    this.returnEids = builder.returnEids;
    this.onDelayed = builder.onDelayed;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BdtickOptions defaults() {
    return DEFAULTS;
  }

  /**
   * Returns a builder with nothing set.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a builder initialised with these options.
   *
   * @return a new builder
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  /**
   * Returns {@code start}: the first tick time; naive text is read in {@code requestTz}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns {@code end}: the last tick time; naive text is read in {@code requestTz}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns {@code requestTz}: how naive {@code start}/{@code end} are read: {@code UTC}, {@code
   * local}, {@code exchange}, an alias such as {@code NY}, a ticker, or an IANA zone.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> requestTz() {
    return Optional.ofNullable(requestTz);
  }

  /**
   * Returns {@code outputTz}: the zone the result's {@code time} column is labelled in (same tokens
   * as {@code requestTz}).
   *
   * @return the value, or empty when unset
   */
  public Optional<String> outputTz() {
    return Optional.ofNullable(outputTz);
  }

  /**
   * Returns {@code eventTypes}: the tick event types; unset means {@code [TRADE]}.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> eventTypes() {
    return Optional.ofNullable(eventTypes);
  }

  /**
   * Returns {@code includeConditionCodes}: whether to include condition codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeConditionCodes() {
    return Optional.ofNullable(includeConditionCodes);
  }

  /**
   * Returns {@code includeExchangeCodes}: whether to include exchange codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeExchangeCodes() {
    return Optional.ofNullable(includeExchangeCodes);
  }

  /**
   * Returns {@code includeBrokerCodes}: whether to include broker codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeBrokerCodes() {
    return Optional.ofNullable(includeBrokerCodes);
  }

  /**
   * Returns {@code includeRpsCodes}: whether to include RPS codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeRpsCodes() {
    return Optional.ofNullable(includeRpsCodes);
  }

  /**
   * Returns {@code includeBicMicCodes}: whether to include BIC and MIC codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeBicMicCodes() {
    return Optional.ofNullable(includeBicMicCodes);
  }

  /**
   * Returns {@code includeNonPlottableEvents}: whether to include non-plottable events.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeNonPlottableEvents() {
    return Optional.ofNullable(includeNonPlottableEvents);
  }

  /**
   * Returns {@code includeBloombergStandardConditionCodes}: whether to include Bloomberg standard
   * condition codes.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> includeBloombergStandardConditionCodes() {
    return Optional.ofNullable(includeBloombergStandardConditionCodes);
  }

  /**
   * Returns {@code kwargs}: extra request parameters (strings, numbers or booleans) the engine
   * routes into request elements or overrides by schema; numbers are sent as JavaScript {@code
   * String(value)} renders them.
   *
   * @return the value, or empty when unset
   */
  public Optional<Map<String, Object>> kwargs() {
    return Optional.ofNullable(kwargs);
  }

  /**
   * Returns {@code returnEids}: whether to request entitlement ids; they surface in the result
   * metadata.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> returnEids() {
    return Optional.ofNullable(returnEids);
  }

  /**
   * Returns {@code onDelayed}: the delayed-data check; unset skips it.
   *
   * @return the value, or empty when unset
   */
  public Optional<DelayedPolicy> onDelayed() {
    return Optional.ofNullable(onDelayed);
  }

  /** Writes the set options as request argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("start", start);
    args.put("end", end);
    args.put("requestTz", requestTz);
    args.put("outputTz", outputTz);
    args.put("eventTypes", eventTypes);
    args.put("includeConditionCodes", includeConditionCodes);
    args.put("includeExchangeCodes", includeExchangeCodes);
    args.put("includeBrokerCodes", includeBrokerCodes);
    args.put("includeRpsCodes", includeRpsCodes);
    args.put("includeBicMicCodes", includeBicMicCodes);
    args.put("includeNonPlottableEvents", includeNonPlottableEvents);
    args.put("includeBloombergStandardConditionCodes", includeBloombergStandardConditionCodes);
    args.put("kwargs", kwargs);
    args.put("returnEids", returnEids);
    if (onDelayed != null) {
      args.put("onDelayed", onDelayed.wireValue());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BdtickOptions)) {
      return false;
    }
    BdtickOptions other = (BdtickOptions) object;
    return Objects.equals(start, other.start)
        && Objects.equals(end, other.end)
        && Objects.equals(requestTz, other.requestTz)
        && Objects.equals(outputTz, other.outputTz)
        && Objects.equals(eventTypes, other.eventTypes)
        && Objects.equals(includeConditionCodes, other.includeConditionCodes)
        && Objects.equals(includeExchangeCodes, other.includeExchangeCodes)
        && Objects.equals(includeBrokerCodes, other.includeBrokerCodes)
        && Objects.equals(includeRpsCodes, other.includeRpsCodes)
        && Objects.equals(includeBicMicCodes, other.includeBicMicCodes)
        && Objects.equals(includeNonPlottableEvents, other.includeNonPlottableEvents)
        && Objects.equals(
            includeBloombergStandardConditionCodes, other.includeBloombergStandardConditionCodes)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(returnEids, other.returnEids)
        && Objects.equals(onDelayed, other.onDelayed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        start,
        end,
        requestTz,
        outputTz,
        eventTypes,
        includeConditionCodes,
        includeExchangeCodes,
        includeBrokerCodes,
        includeRpsCodes,
        includeBicMicCodes,
        includeNonPlottableEvents,
        includeBloombergStandardConditionCodes,
        RequestArgs.orderedHash(kwargs),
        returnEids,
        onDelayed);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BdtickOptions",
        "start",
        start,
        "end",
        end,
        "requestTz",
        requestTz,
        "outputTz",
        outputTz,
        "eventTypes",
        eventTypes,
        "includeConditionCodes",
        includeConditionCodes,
        "includeExchangeCodes",
        includeExchangeCodes,
        "includeBrokerCodes",
        includeBrokerCodes,
        "includeRpsCodes",
        includeRpsCodes,
        "includeBicMicCodes",
        includeBicMicCodes,
        "includeNonPlottableEvents",
        includeNonPlottableEvents,
        "includeBloombergStandardConditionCodes",
        includeBloombergStandardConditionCodes,
        "kwargs",
        kwargs,
        "returnEids",
        returnEids,
        "onDelayed",
        onDelayed);
  }

  /** Builds {@link BdtickOptions}; not thread-safe. */
  public static final class Builder {
    private String start;
    private String end;
    private String requestTz;
    private String outputTz;
    private List<String> eventTypes;
    private Boolean includeConditionCodes;
    private Boolean includeExchangeCodes;
    private Boolean includeBrokerCodes;
    private Boolean includeRpsCodes;
    private Boolean includeBicMicCodes;
    private Boolean includeNonPlottableEvents;
    private Boolean includeBloombergStandardConditionCodes;
    private Map<String, Object> kwargs;
    private Boolean returnEids;
    private DelayedPolicy onDelayed;

    private Builder() {}

    private Builder(BdtickOptions options) {
      this.start = options.start;
      this.end = options.end;
      this.requestTz = options.requestTz;
      this.outputTz = options.outputTz;
      this.eventTypes = options.eventTypes;
      this.includeConditionCodes = options.includeConditionCodes;
      this.includeExchangeCodes = options.includeExchangeCodes;
      this.includeBrokerCodes = options.includeBrokerCodes;
      this.includeRpsCodes = options.includeRpsCodes;
      this.includeBicMicCodes = options.includeBicMicCodes;
      this.includeNonPlottableEvents = options.includeNonPlottableEvents;
      this.includeBloombergStandardConditionCodes = options.includeBloombergStandardConditionCodes;
      this.kwargs = options.kwargs;
      this.returnEids = options.returnEids;
      this.onDelayed = options.onDelayed;
    }

    /**
     * Sets {@code start} from text: the first tick time; naive text is read in {@code requestTz}.
     * The text is formatted with {@link Dates#formatDateTime(String)}; blank text unsets it.
     *
     * @param start ISO 8601 or Bloomberg date-time text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date-time
     */
    public Builder start(String start) {
      this.start = start == null ? null : Dates.formatDateTime(start);
      return this;
    }

    /**
     * Sets {@code start} from a {@code java.time} value: the first tick time; naive text is read in
     * {@code requestTz}. The value is formatted with {@link
     * Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param start the date-time, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date-time
     */
    public Builder start(TemporalAccessor start) {
      this.start = start == null ? null : Dates.formatDateTime(start);
      return this;
    }

    /**
     * Sets {@code end} from text: the last tick time; naive text is read in {@code requestTz}. The
     * text is formatted with {@link Dates#formatDateTime(String)}; blank text unsets it.
     *
     * @param end ISO 8601 or Bloomberg date-time text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date-time
     */
    public Builder end(String end) {
      this.end = end == null ? null : Dates.formatDateTime(end);
      return this;
    }

    /**
     * Sets {@code end} from a {@code java.time} value: the last tick time; naive text is read in
     * {@code requestTz}. The value is formatted with {@link
     * Dates#formatDateTime(TemporalAccessor)}.
     *
     * @param end the date-time, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date-time
     */
    public Builder end(TemporalAccessor end) {
      this.end = end == null ? null : Dates.formatDateTime(end);
      return this;
    }

    /**
     * Sets {@code requestTz}: how naive {@code start}/{@code end} are read: {@code UTC}, {@code
     * local}, {@code exchange}, an alias such as {@code NY}, a ticker, or an IANA zone.
     *
     * @param requestTz the value, or {@code null} to unset
     * @return this builder
     */
    public Builder requestTz(String requestTz) {
      this.requestTz = requestTz;
      return this;
    }

    /**
     * Sets {@code outputTz}: the zone the result's {@code time} column is labelled in (same tokens
     * as {@code requestTz}).
     *
     * @param outputTz the value, or {@code null} to unset
     * @return this builder
     */
    public Builder outputTz(String outputTz) {
      this.outputTz = outputTz;
      return this;
    }

    /**
     * Sets {@code eventTypes}: the tick event types; unset means {@code [TRADE]}.
     *
     * @param eventTypes the values, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if a value is {@code null}
     */
    public Builder eventTypes(Collection<String> eventTypes) {
      this.eventTypes = eventTypes == null ? null : RequestArgs.strings("eventTypes", eventTypes);
      return this;
    }

    /**
     * Sets {@code eventTypes}: the tick event types; unset means {@code [TRADE]}.
     *
     * @param eventTypes the values
     * @return this builder
     * @throws NullPointerException if a value is {@code null}
     */
    public Builder eventTypes(String... eventTypes) {
      return eventTypes(Arrays.asList(eventTypes));
    }

    /**
     * Sets {@code includeConditionCodes}: whether to include condition codes.
     *
     * @param includeConditionCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeConditionCodes(Boolean includeConditionCodes) {
      this.includeConditionCodes = includeConditionCodes;
      return this;
    }

    /**
     * Sets {@code includeExchangeCodes}: whether to include exchange codes.
     *
     * @param includeExchangeCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeExchangeCodes(Boolean includeExchangeCodes) {
      this.includeExchangeCodes = includeExchangeCodes;
      return this;
    }

    /**
     * Sets {@code includeBrokerCodes}: whether to include broker codes.
     *
     * @param includeBrokerCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeBrokerCodes(Boolean includeBrokerCodes) {
      this.includeBrokerCodes = includeBrokerCodes;
      return this;
    }

    /**
     * Sets {@code includeRpsCodes}: whether to include RPS codes.
     *
     * @param includeRpsCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeRpsCodes(Boolean includeRpsCodes) {
      this.includeRpsCodes = includeRpsCodes;
      return this;
    }

    /**
     * Sets {@code includeBicMicCodes}: whether to include BIC and MIC codes.
     *
     * @param includeBicMicCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeBicMicCodes(Boolean includeBicMicCodes) {
      this.includeBicMicCodes = includeBicMicCodes;
      return this;
    }

    /**
     * Sets {@code includeNonPlottableEvents}: whether to include non-plottable events.
     *
     * @param includeNonPlottableEvents the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeNonPlottableEvents(Boolean includeNonPlottableEvents) {
      this.includeNonPlottableEvents = includeNonPlottableEvents;
      return this;
    }

    /**
     * Sets {@code includeBloombergStandardConditionCodes}: whether to include Bloomberg standard
     * condition codes.
     *
     * @param includeBloombergStandardConditionCodes the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder includeBloombergStandardConditionCodes(
        Boolean includeBloombergStandardConditionCodes) {
      this.includeBloombergStandardConditionCodes = includeBloombergStandardConditionCodes;
      return this;
    }

    /**
     * Replaces {@code kwargs}: extra request parameters (strings, numbers or booleans) the engine
     * routes into request elements or overrides by schema; numbers are sent as JavaScript {@code
     * String(value)} renders them.
     *
     * <p>Keys keep the map's iteration order; use a {@link java.util.LinkedHashMap} to control it.
     *
     * @param kwargs the entries, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if a value is not a string, number or boolean
     */
    public Builder kwargs(Map<String, ?> kwargs) {
      this.kwargs = kwargs == null ? null : RequestArgs.scalars("kwargs", kwargs, false);
      return this;
    }

    /**
     * Adds or replaces one {@code kwargs} entry, keeping the position of an existing key.
     *
     * @param key the entry name
     * @param value a string, number or boolean
     * @return this builder
     * @throws IllegalArgumentException if the value is not a string, number or boolean
     */
    public Builder kwarg(String key, Object value) {
      this.kwargs = RequestArgs.withScalar("kwargs", this.kwargs, key, value, false);
      return this;
    }

    /**
     * Sets {@code returnEids}: whether to request entitlement ids; they surface in the result
     * metadata.
     *
     * @param returnEids the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder returnEids(Boolean returnEids) {
      this.returnEids = returnEids;
      return this;
    }

    /**
     * Sets {@code onDelayed}: the delayed-data check; unset skips it.
     *
     * @param onDelayed the policy, or {@code null} to unset
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy onDelayed) {
      this.onDelayed = onDelayed;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public BdtickOptions build() {
      return new BdtickOptions(this);
    }
  }
}
