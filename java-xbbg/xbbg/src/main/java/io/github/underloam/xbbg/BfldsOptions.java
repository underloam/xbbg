package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for {@code bflds} field metadata requests (TS {@code BfldsOptions}). With {@code
 * searchSpec} set the request is a field search and {@code fields} is unused; otherwise it is a
 * field-info request over {@code fields}.
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent, so the engine default applies. Used by {@link Engine#bflds(BfldsOptions)}.
 */
public final class BfldsOptions {
  private static final BfldsOptions DEFAULTS = builder().build();

  private final List<String> fields;
  private final String searchSpec;
  private final Map<String, Object> kwargs;
  private final Format format;

  private BfldsOptions(Builder builder) {
    this.fields = builder.fields;
    this.searchSpec = builder.searchSpec;
    this.kwargs = builder.kwargs;
    this.format = builder.format;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static BfldsOptions defaults() {
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
   * Returns {@code fields}: the field mnemonics to describe.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /**
   * Returns {@code searchSpec}: a free-text field search.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> searchSpec() {
    return Optional.ofNullable(searchSpec);
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
    args.put("fields", fields);
    args.put("searchSpec", searchSpec);
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
    if (!(object instanceof BfldsOptions)) {
      return false;
    }
    BfldsOptions other = (BfldsOptions) object;
    return Objects.equals(fields, other.fields)
        && Objects.equals(searchSpec, other.searchSpec)
        && RequestArgs.orderedEquals(kwargs, other.kwargs)
        && Objects.equals(format, other.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, searchSpec, RequestArgs.orderedHash(kwargs), format);
  }

  @Override
  public String toString() {
    return RequestArgs.describe(
        "BfldsOptions",
        "fields",
        fields,
        "searchSpec",
        searchSpec,
        "kwargs",
        kwargs,
        "format",
        format);
  }

  /** Builds {@link BfldsOptions}; not thread-safe. */
  public static final class Builder {
    private List<String> fields;
    private String searchSpec;
    private Map<String, Object> kwargs;
    private Format format;

    private Builder() {}

    private Builder(BfldsOptions options) {
      this.fields = options.fields;
      this.searchSpec = options.searchSpec;
      this.kwargs = options.kwargs;
      this.format = options.format;
    }

    /**
     * Sets {@code fields}: the field mnemonics to describe.
     *
     * @param fields the values, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if a value is {@code null}
     */
    public Builder fields(Collection<String> fields) {
      this.fields = fields == null ? null : RequestArgs.strings("fields", fields);
      return this;
    }

    /**
     * Sets {@code fields}: the field mnemonics to describe.
     *
     * @param fields the values
     * @return this builder
     * @throws NullPointerException if a value is {@code null}
     */
    public Builder fields(String... fields) {
      return fields(Arrays.asList(fields));
    }

    /**
     * Sets {@code searchSpec}: a free-text field search.
     *
     * @param searchSpec the value, or {@code null} to unset
     * @return this builder
     */
    public Builder searchSpec(String searchSpec) {
      this.searchSpec = searchSpec;
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
    public BfldsOptions build() {
      return new BfldsOptions(this);
    }
  }
}
