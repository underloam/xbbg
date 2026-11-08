package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bdh} historical requests (TS {@code BdhOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bdh(java.util.Collection,
 * java.util.Collection, BdhOptions)}.
 */
public final class BdhOptions {
  private static final BdhOptions DEFAULTS = builder().build();

  private final String start;
  private final String end;
  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;
  private final Boolean returnEids;
  private final DelayedPolicy onDelayed;
  private final Boolean validateFields;

  private BdhOptions(Builder builder) {
    this.start = builder.start;
    this.end = builder.end;
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
    this.returnEids = builder.returnEids;
    this.onDelayed = builder.onDelayed;
    this.validateFields = builder.validateFields;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BdhOptions defaults() {
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
   * Returns {@code start}: the first date, as {@code YYYYMMDD}; Bloomberg requires it.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> start() {
    return Optional.ofNullable(start);
  }

  /**
   * Returns {@code end}: the last date, as {@code YYYYMMDD}; Bloomberg requires it.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> end() {
    return Optional.ofNullable(end);
  }

  /**
   * Returns {@code overrides}: Bloomberg field overrides, built with {@link Overrides}.
   *
   * @return the value, or empty when unset
   */
  public Optional<Overrides> overrides() {
    return Optional.ofNullable(overrides);
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
   * Returns {@code format}: the result layout; unset uses the engine default ({@link Format#LONG}).
   *
   * @return the value, or empty when unset
   */
  public Optional<Format> format() {
    return Optional.ofNullable(format);
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
   * Returns {@code onDelayed}: the delayed-data check: the engine sends one extra reference
   * request, and only when the window reaches into the last 24 hours; unset skips the check.
   *
   * @return the value, or empty when unset
   */
  public Optional<DelayedPolicy> onDelayed() {
    return Optional.ofNullable(onDelayed);
  }

  /**
   * Returns {@code validateFields}: per-request field validation: {@code true} forces strict
   * validation, {@code false} disables it, unset follows the engine's validation mode.
   *
   * @return the value, or empty when unset
   */
  public Optional<Boolean> validateFields() {
    return Optional.ofNullable(validateFields);
  }

  /** Writes the set options as request argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("start", start);
    args.put("end", end);
    if (overrides != null) {
      overrides.encodeRequestParts(args);
    }
    args.put("kwargs", kwargs);
    if (format != null) {
      args.put("format", format.wireValue());
    }
    args.put("returnEids", returnEids);
    if (onDelayed != null) {
      args.put("onDelayed", onDelayed.wireValue());
    }
    args.put("validateFields", validateFields);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BdhOptions)) {
      return false;
    }
    BdhOptions other = (BdhOptions) object;
    return Objects.equals(start, other.start)
        && Objects.equals(end, other.end)
        && Objects.equals(overrides, other.overrides)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format)
        && Objects.equals(returnEids, other.returnEids)
        && Objects.equals(onDelayed, other.onDelayed)
        && Objects.equals(validateFields, other.validateFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        start,
        end,
        overrides,
        RequestArgs.orderedHash(kwargs),
        format,
        returnEids,
        onDelayed,
        validateFields);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BdhOptions",
        "start",
        start,
        "end",
        end,
        "overrides",
        overrides,
        "kwargs",
        kwargs,
        "format",
        format,
        "returnEids",
        returnEids,
        "onDelayed",
        onDelayed,
        "validateFields",
        validateFields);
  }

  /** Builds {@link BdhOptions}; not thread-safe. */
  public static final class Builder {
    private String start;
    private String end;
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;
    private Boolean returnEids;
    private DelayedPolicy onDelayed;
    private Boolean validateFields;

    private Builder() {}

    private Builder(BdhOptions options) {
      this.start = options.start;
      this.end = options.end;
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
      this.returnEids = options.returnEids;
      this.onDelayed = options.onDelayed;
      this.validateFields = options.validateFields;
    }

    /**
     * Sets {@code start} from text: the first date, as {@code YYYYMMDD}; Bloomberg requires it. The
     * text is formatted with {@link Dates#formatDate(String)}; blank text unsets it.
     *
     * @param start ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder start(String start) {
      this.start = start == null ? null : Dates.formatDate(start);
      return this;
    }

    /**
     * Sets {@code start} from a {@code java.time} value: the first date, as {@code YYYYMMDD};
     * Bloomberg requires it. The value is formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param start the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder start(TemporalAccessor start) {
      this.start = start == null ? null : Dates.formatDate(start);
      return this;
    }

    /**
     * Sets {@code end} from text: the last date, as {@code YYYYMMDD}; Bloomberg requires it. The
     * text is formatted with {@link Dates#formatDate(String)}; blank text unsets it.
     *
     * @param end ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder end(String end) {
      this.end = end == null ? null : Dates.formatDate(end);
      return this;
    }

    /**
     * Sets {@code end} from a {@code java.time} value: the last date, as {@code YYYYMMDD};
     * Bloomberg requires it. The value is formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param end the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder end(TemporalAccessor end) {
      this.end = end == null ? null : Dates.formatDate(end);
      return this;
    }

    /**
     * Sets the overrides: Bloomberg field overrides, built with {@link Overrides}.
     *
     * @param overrides the overrides, or {@code null} to unset
     * @return this builder
     */
    public Builder overrides(Overrides overrides) {
      this.overrides = overrides;
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
     * Sets the format: the result layout; unset uses the engine default ({@link Format#LONG}).
     *
     * @param format the format, or {@code null} to unset
     * @return this builder
     */
    public Builder format(Format format) {
      this.format = format;
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
     * Sets {@code onDelayed}: the delayed-data check: the engine sends one extra reference request,
     * and only when the window reaches into the last 24 hours; unset skips the check.
     *
     * @param onDelayed the policy, or {@code null} to unset
     * @return this builder
     */
    public Builder onDelayed(DelayedPolicy onDelayed) {
      this.onDelayed = onDelayed;
      return this;
    }

    /**
     * Sets {@code validateFields}: per-request field validation: {@code true} forces strict
     * validation, {@code false} disables it, unset follows the engine's validation mode.
     *
     * @param validateFields the flag, or {@code null} to unset
     * @return this builder
     */
    public Builder validateFields(Boolean validateFields) {
      this.validateFields = validateFields;
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public BdhOptions build() {
      return new BdhOptions(this);
    }
  }
}
