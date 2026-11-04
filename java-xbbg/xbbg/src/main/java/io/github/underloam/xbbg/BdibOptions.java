package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bdib} intraday bar requests (TS {@code BdibOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bdib(String, BdibOptions)}.
 */
public final class BdibOptions {
  private static final BdibOptions DEFAULTS = builder().build();

  private final String start;
  private final String end;
  private final String day;
  private final String requestTz;
  private final String outputTz;
  private final String eventType;
  private final Integer interval;
  private final Map<String, Object> kwargs;
  private final Boolean returnEids;
  private final DelayedPolicy onDelayed;

  private BdibOptions(Builder builder) {
    this.start = builder.start;
    this.end = builder.end;
    this.day = builder.day;
    this.requestTz = builder.requestTz;
    this.outputTz = builder.outputTz;
    this.eventType = builder.eventType;
    this.interval = builder.interval;
    this.kwargs = builder.kwargs;
    this.returnEids = builder.returnEids;
    this.onDelayed = builder.onDelayed;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BdibOptions defaults() {
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
   * Returns {@code start}: the first bar time; naive text is read in {@code requestTz}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns {@code end}: the last bar time; naive text is read in {@code requestTz}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns {@code day}: a whole trading day as {@code YYYYMMDD} (the JS {@code abdib} positional
   * {@code dt}): the engine requests {@code T00:00:00} to {@code T23:59:59} of that naive day; it
   * cannot be combined with {@code start} or {@code end}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> day() {
    return Optional.ofNullable(day);
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
   * Returns {@code eventType}: the bar event type; unset means {@code TRADE}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> eventType() {
    return Optional.ofNullable(eventType);
  }

  /**
   * Returns {@code interval}: the bar size in minutes; unset means {@code 1}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Integer> interval() {
    return Optional.ofNullable(interval);
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
    args.put("dt", day);
    args.put("requestTz", requestTz);
    args.put("outputTz", outputTz);
    args.put("eventType", eventType);
    args.put("interval", interval);
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
    if (!(object instanceof BdibOptions)) {
      return false;
    }
    BdibOptions other = (BdibOptions) object;
    return Objects.equals(start, other.start)
        && Objects.equals(end, other.end)
        && Objects.equals(day, other.day)
        && Objects.equals(requestTz, other.requestTz)
        && Objects.equals(outputTz, other.outputTz)
        && Objects.equals(eventType, other.eventType)
        && Objects.equals(interval, other.interval)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(returnEids, other.returnEids)
        && Objects.equals(onDelayed, other.onDelayed);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        start,
        end,
        day,
        requestTz,
        outputTz,
        eventType,
        interval,
        RequestArgs.orderedHash(kwargs),
        returnEids,
        onDelayed);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BdibOptions",
        "start",
        start,
        "end",
        end,
        "day",
        day,
        "requestTz",
        requestTz,
        "outputTz",
        outputTz,
        "eventType",
        eventType,
        "interval",
        interval,
        "kwargs",
        kwargs,
        "returnEids",
        returnEids,
        "onDelayed",
        onDelayed);
  }

  /** Builds {@link BdibOptions}; not thread-safe. */
  public static final class Builder {
    private String start;
    private String end;
    private String day;
    private String requestTz;
    private String outputTz;
    private String eventType;
    private Integer interval;
    private Map<String, Object> kwargs;
    private Boolean returnEids;
    private DelayedPolicy onDelayed;

    private Builder() {}

    private Builder(BdibOptions options) {
      this.start = options.start;
      this.end = options.end;
      this.day = options.day;
      this.requestTz = options.requestTz;
      this.outputTz = options.outputTz;
      this.eventType = options.eventType;
      this.interval = options.interval;
      this.kwargs = options.kwargs;
      this.returnEids = options.returnEids;
      this.onDelayed = options.onDelayed;
    }

    /**
     * Sets {@code start} from text: the first bar time; naive text is read in {@code requestTz}.
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
     * Sets {@code start} from a {@code java.time} value: the first bar time; naive text is read in
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
     * Sets {@code end} from text: the last bar time; naive text is read in {@code requestTz}. The
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
     * Sets {@code end} from a {@code java.time} value: the last bar time; naive text is read in
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
     * Sets {@code day} from text: a whole trading day as {@code YYYYMMDD} (the JS {@code abdib}
     * positional {@code dt}): the engine requests {@code T00:00:00} to {@code T23:59:59} of that
     * naive day; it cannot be combined with {@code start} or {@code end}. The text is formatted
     * with {@link Dates#formatDate(String)}; blank text is rejected.
     *
     * @param day ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous, blank or not a date
     */
    public Builder day(String day) {
      if (day == null) {
        this.day = null;
        return this;
      }
      String formatted = Dates.formatDate(day);
      if (formatted == null) {
        throw new IllegalArgumentException("dt must be a non-empty date-like value");
      }
      this.day = formatted;
      return this;
    }

    /**
     * Sets {@code day} from a {@code java.time} value: a whole trading day as {@code YYYYMMDD} (the
     * JS {@code abdib} positional {@code dt}): the engine requests {@code T00:00:00} to {@code
     * T23:59:59} of that naive day; it cannot be combined with {@code start} or {@code end}. The
     * value is formatted with {@link Dates#formatDate(TemporalAccessor)}.
     *
     * @param day the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder day(TemporalAccessor day) {
      this.day = day == null ? null : Dates.formatDate(day);
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
     * Sets {@code eventType}: the bar event type; unset means {@code TRADE}.
     *
     * @param eventType the value, or {@code null} to unset
     * @return this builder
     */
    public Builder eventType(String eventType) {
      this.eventType = eventType;
      return this;
    }

    /**
     * Sets {@code interval}: the bar size in minutes; unset means {@code 1}.
     *
     * @param interval the value, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value is negative
     */
    public Builder interval(Integer interval) {
      if (interval != null && interval < 0) {
        throw new IllegalArgumentException("interval must not be negative, got " + interval);
      }
      this.interval = interval;
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
    public BdibOptions build() {
      return new BdibOptions(this);
    }
  }
}
