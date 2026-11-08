package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code etfHoldings} BQL recipe (TS {@code EtfHoldingsOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent. Used by {@link Engine#etfHoldings(String, EtfHoldingsOptions)}.
 */
public final class EtfHoldingsOptions {
  private static final EtfHoldingsOptions DEFAULTS = builder().build();

  private final List<String> fields;

  private EtfHoldingsOptions(Builder builder) {
    this.fields = builder.fields;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static EtfHoldingsOptions defaults() {
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
   * Returns {@code fields}: BQL fields requested after the default {@code id_isin, weights,
   * id().position}.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("fields", fields);
  }

  @Override
  public boolean equals(Object object) {
    return this == object
        || (object instanceof EtfHoldingsOptions
            && Objects.equals(fields, ((EtfHoldingsOptions) object).fields));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(fields);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("EtfHoldingsOptions", "fields", fields);
  }

  /** Builds {@link EtfHoldingsOptions}; not thread-safe. */
  public static final class Builder {
    private List<String> fields;

    private Builder() {}

    private Builder(EtfHoldingsOptions options) {
      this.fields = options.fields;
    }

    /**
     * Sets {@code fields}: extra BQL fields.
     *
     * @param fields the fields, or {@code null} to unset
     * @return this builder
     * @throws NullPointerException if an item is null
     */
    public Builder fields(Collection<String> fields) {
      this.fields = fields == null ? null : RequestArgs.strings("fields", fields);
      return this;
    }

    /**
     * Builds the options.
     *
     * @return immutable options
     */
    public EtfHoldingsOptions build() {
      return new EtfHoldingsOptions(this);
    }
  }
}
