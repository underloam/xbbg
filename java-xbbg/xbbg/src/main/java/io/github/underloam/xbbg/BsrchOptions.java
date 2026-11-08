package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bsrch} saved searches (TS {@code BsrchOptions}). Overrides and kwargs become
 * search parameters (grid elements), not Bloomberg overrides.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bsrch(String, BsrchOptions)}.
 */
public final class BsrchOptions {
  private static final BsrchOptions DEFAULTS = builder().build();

  private final Overrides overrides;
  private final Map<String, Object> kwargs;
  private final Format format;

  private BsrchOptions(Builder builder) {
    this.overrides = builder.overrides;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BsrchOptions defaults() {
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
   * Returns {@code overrides}: search parameters as override pairs, sent after {@code Domain};
   * per-security overrides are rejected.
   *
   * @return the value, or empty when unset
   */
  public Optional<Overrides> overrides() {
    return Optional.ofNullable(overrides);
  }

  /**
   * Returns {@code kwargs}: search parameters sent after the override pairs; numbers are sent as
   * JavaScript {@code String(value)} renders them.
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
    if (!(object instanceof BsrchOptions)) {
      return false;
    }
    BsrchOptions other = (BsrchOptions) object;
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
        "BsrchOptions", "overrides", overrides, "kwargs", kwargs, "format", format);
  }

  /** Builds {@link BsrchOptions}; not thread-safe. */
  public static final class Builder {
    private Overrides overrides;
    private Map<String, Object> kwargs;
    private Format format;

    private Builder() {}

    private Builder(BsrchOptions options) {
      this.overrides = options.overrides;
      this.kwargs = options.kwargs;
      this.format = options.format;
    }

    /**
     * Sets the overrides: search parameters as override pairs, sent after {@code Domain};
     * per-security overrides are rejected.
     *
     * @param overrides the overrides, or {@code null} to unset
     * @return this builder
     */
    public Builder overrides(Overrides overrides) {
      this.overrides = overrides;
      return this;
    }

    /**
     * Replaces {@code kwargs}: search parameters sent after the override pairs; numbers are sent as
     * JavaScript {@code String(value)} renders them.
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
    public BsrchOptions build() {
      return new BsrchOptions(this);
    }
  }
}
