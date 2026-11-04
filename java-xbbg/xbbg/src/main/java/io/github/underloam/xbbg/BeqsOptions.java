package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code beqs} equity screens (TS {@code BeqsOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#beqs(String, BeqsOptions)}.
 */
public final class BeqsOptions {
  private static final BeqsOptions DEFAULTS = builder().build();

  private final String asof;
  private final String screenType;
  private final String group;
  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;

  private BeqsOptions(Builder builder) {
    this.asof = builder.asof;
    this.screenType = builder.screenType;
    this.group = builder.group;
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BeqsOptions defaults() {
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
   * Returns {@code asof}: the screen date as {@code YYYYMMDD}; unset runs the screen as of today.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> asof() {
    return Optional.ofNullable(asof);
  }

  /**
   * Returns {@code screenType}: the screen type; unset means {@code PRIVATE}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> screenType() {
    return Optional.ofNullable(screenType);
  }

  /**
   * Returns {@code group}: the screen group; unset means {@code General}.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> group() {
    return Optional.ofNullable(group);
  }

  /**
   * Returns {@code overrides}: Bloomberg field overrides, built with {@link Overrides};
   * per-security overrides are rejected.
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

  /** Writes the set options as request argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("asof", asof);
    args.put("screenType", screenType);
    args.put("group", group);
    if (overrides != null) {
      overrides.encodeRequestParts(args);
    }
    args.put("kwargs", kwargs);
    if (format != null) {
      args.put("format", format.wireValue());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BeqsOptions)) {
      return false;
    }
    BeqsOptions other = (BeqsOptions) object;
    return Objects.equals(asof, other.asof)
        && Objects.equals(screenType, other.screenType)
        && Objects.equals(group, other.group)
        && Objects.equals(overrides, other.overrides)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        asof, screenType, group, overrides, RequestArgs.orderedHash(kwargs), format);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BeqsOptions",
        "asof",
        asof,
        "screenType",
        screenType,
        "group",
        group,
        "overrides",
        overrides,
        "kwargs",
        kwargs,
        "format",
        format);
  }

  /** Builds {@link BeqsOptions}; not thread-safe. */
  public static final class Builder {
    private String asof;
    private String screenType;
    private String group;
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;

    private Builder() {}

    private Builder(BeqsOptions options) {
      this.asof = options.asof;
      this.screenType = options.screenType;
      this.group = options.group;
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
    }

    /**
     * Sets {@code asof} from text: the screen date as {@code YYYYMMDD}; unset runs the screen as of
     * today. The text is formatted with {@link Dates#formatDate(String)}; blank text unsets it.
     *
     * @param asof ISO 8601 or Bloomberg date text, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the text is ambiguous or not a date
     */
    public Builder asof(String asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Sets {@code asof} from a {@code java.time} value: the screen date as {@code YYYYMMDD}; unset
     * runs the screen as of today. The value is formatted with {@link
     * Dates#formatDate(TemporalAccessor)}.
     *
     * @param asof the date, or {@code null} to unset
     * @return this builder
     * @throws IllegalArgumentException if the value carries no date
     */
    public Builder asof(TemporalAccessor asof) {
      this.asof = asof == null ? null : Dates.formatDate(asof);
      return this;
    }

    /**
     * Sets {@code screenType}: the screen type; unset means {@code PRIVATE}.
     *
     * @param screenType the value, or {@code null} to unset
     * @return this builder
     */
    public Builder screenType(String screenType) {
      this.screenType = screenType;
      return this;
    }

    /**
     * Sets {@code group}: the screen group; unset means {@code General}.
     *
     * @param group the value, or {@code null} to unset
     * @return this builder
     */
    public Builder group(String group) {
      this.group = group;
      return this;
    }

    /**
     * Sets the overrides: Bloomberg field overrides, built with {@link Overrides}; per-security
     * overrides are rejected.
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
     * Builds the options.
     *
     * @return immutable options
     */
    public BeqsOptions build() {
      return new BeqsOptions(this);
    }
  }
}
