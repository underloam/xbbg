package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bport}, {@code bcurves} and {@code bgovts} (TS {@code RequestOptions}). {@code
 * bcurves} and {@code bgovts} reject overrides.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bport(String, java.util.Collection,
 * RequestOptions)}.
 */
public final class RequestOptions {
  private static final RequestOptions DEFAULTS = builder().build();

  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;

  private RequestOptions(Builder builder) {
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static RequestOptions defaults() {
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
    if (!(object instanceof RequestOptions)) {
      return false;
    }
    RequestOptions other = (RequestOptions) object;
    return Objects.equals(overrides, other.overrides)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(overrides, RequestArgs.orderedHash(kwargs), format);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "RequestOptions", "overrides", overrides, "kwargs", kwargs, "format", format);
  }

  /** Builds {@link RequestOptions}; not thread-safe. */
  public static final class Builder {
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;

    private Builder() {}

    private Builder(RequestOptions options) {
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
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
    public RequestOptions build() {
      return new RequestOptions(this);
    }
  }
}
