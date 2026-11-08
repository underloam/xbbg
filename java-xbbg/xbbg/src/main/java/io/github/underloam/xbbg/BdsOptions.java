package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bds} bulk reference-data requests (the TS {@code bdp} options without {@code
 * onDelayed}; {@code includeSecurityErrors} is omitted because {@code bds} never sends it).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bds(java.util.Collection,
 * java.util.Collection, BdsOptions)}.
 */
public final class BdsOptions {
  private static final BdsOptions DEFAULTS = builder().build();

  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;
  private final Boolean returnEids;
  private final Boolean validateFields;

  private BdsOptions(Builder builder) {
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
    this.returnEids = builder.returnEids;
    this.validateFields = builder.validateFields;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BdsOptions defaults() {
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
    if (overrides != null) {
      overrides.encodeRequestParts(args);
    }
    args.put("kwargs", kwargs);
    if (format != null) {
      args.put("format", format.wireValue());
    }
    args.put("returnEids", returnEids);
    args.put("validateFields", validateFields);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof BdsOptions)) {
      return false;
    }
    BdsOptions other = (BdsOptions) object;
    return Objects.equals(overrides, other.overrides)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format)
        && Objects.equals(returnEids, other.returnEids)
        && Objects.equals(validateFields, other.validateFields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        overrides, RequestArgs.orderedHash(kwargs), format, returnEids, validateFields);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BdsOptions",
        "overrides",
        overrides,
        "kwargs",
        kwargs,
        "format",
        format,
        "returnEids",
        returnEids,
        "validateFields",
        validateFields);
  }

  /** Builds {@link BdsOptions}; not thread-safe. */
  public static final class Builder {
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;
    private Boolean returnEids;
    private Boolean validateFields;

    private Builder() {}

    private Builder(BdsOptions options) {
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
      this.returnEids = options.returnEids;
      this.validateFields = options.validateFields;
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
    public BdsOptions build() {
      return new BdsOptions(this);
    }
  }
}
