package io.github.underloam.xbbg;

import io.github.underloam.xbbg.internal.JsonObjectBuilder;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Options for the {@code corporateBonds} BQL recipe (TS {@code CorporateBondsOptions}).
 *
 * <p>Immutable; build with {@link #builder()}. Every option is optional: an unset option is not
 * sent. Used by {@link Engine#corporateBonds(String, CorporateBondsOptions)}.
 */
public final class CorporateBondsOptions {
  private static final CorporateBondsOptions DEFAULTS = builder().build();

  private final String ccy;
  private final List<String> fields;

  private CorporateBondsOptions(Builder builder) {
    this.ccy = builder.ccy;
    this.fields = builder.fields;
  }

  /**
   * Returns the options with nothing set.
   *
   * @return the default options
   */
  public static CorporateBondsOptions defaults() {
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
   * Returns {@code ccy}: the currency filter; unset means every currency.
   *
   * @return the value, or empty when unset
   */
  public Optional<String> ccy() {
    return Optional.ofNullable(ccy);
  }

  /**
   * Returns {@code fields}: BQL fields requested after the default {@code id}.
   *
   * @return the value, or empty when unset
   */
  public Optional<List<String>> fields() {
    return Optional.ofNullable(fields);
  }

  /** Writes the set options as recipe argument fields. */
  void encodeInto(JsonObjectBuilder args) {
    args.put("ccy", ccy);
    args.put("fields", fields);
  }

  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (!(object instanceof CorporateBondsOptions)) {
      return false;
    }
    CorporateBondsOptions other = (CorporateBondsOptions) object;
    return Objects.equals(ccy, other.ccy) && Objects.equals(fields, other.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ccy, fields);
  }

  @Override
  public String toString() {
    return RequestArgs.describe("CorporateBondsOptions", "ccy", ccy, "fields", fields);
  }

  /** Builds {@link CorporateBondsOptions}; not thread-safe. */
  public static final class Builder {
    private String ccy;
    private List<String> fields;

    private Builder() {}

    private Builder(CorporateBondsOptions options) {
      this.ccy = options.ccy;
      this.fields = options.fields;
    }

    /**
     * Sets {@code ccy}: the currency filter, for example {@code "USD"}.
     *
     * @param ccy the currency, or {@code null} to unset
     * @return this builder
     */
    public Builder ccy(String ccy) {
      this.ccy = ccy;
      return this;
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
    public CorporateBondsOptions build() {
      return new CorporateBondsOptions(this);
    }
  }
}
