package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code blkp} instrument lookups (TS {@code BlkpOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#blkp(String, BlkpOptions)}.
 */
public final class BlkpOptions {
  private static final BlkpOptions DEFAULTS = builder().build();

  private final Map<String, Object> kwargs;
  private final Format format;

  private BlkpOptions(Builder builder) {
    this.kwargs = builder.kwargs;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BlkpOptions defaults() {
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
    if (!(object instanceof BlkpOptions)) {
      return false;
    }
    BlkpOptions other = (BlkpOptions) object;
    return RequestArgs.orderedEquals(kwargs, other.kwargs) && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(RequestArgs.orderedHash(kwargs), format);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("BlkpOptions", "kwargs", kwargs, "format", format);
  }

  /** Builds {@link BlkpOptions}; not thread-safe. */
  public static final class Builder {
    private Map<String, Object> kwargs;
    private Format format;

    private Builder() {}

    private Builder(BlkpOptions options) {
      this.kwargs = options.kwargs;
      this.format = options.format;
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
    public BlkpOptions build() {
      return new BlkpOptions(this);
    }
  }
}
